//! A scriptable obs-websocket server for tests. It speaks raw JSON and MessagePack frames, so it
//! does not depend on the crate's own message types.

// tungstenite fixes the signature of the handshake callback.
#![allow(clippy::result_large_err)]

use std::future::Future;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async};

pub const HELLO: u8 = 0;
pub const IDENTIFY: u8 = 1;
pub const IDENTIFIED: u8 = 2;
pub const REIDENTIFY: u8 = 3;
pub const EVENT: u8 = 5;
pub const REQUEST: u8 = 6;
pub const REQUEST_RESPONSE: u8 = 7;
pub const REQUEST_BATCH: u8 = 8;
pub const REQUEST_BATCH_RESPONSE: u8 = 9;

/// Fails a test instead of letting it hang.
pub async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("test step timed out")
}

/// One accepted client connection.
pub struct Conn {
    ws: WebSocketStream<TcpStream>,
    /// The `Sec-WebSocket-Protocol` header sent by the client.
    pub requested_protocol: String,
    msgpack: bool,
}

/// A frame received from the client.
#[derive(Debug)]
pub enum Received {
    Message { op: u8, data: Value },
    Close(Option<CloseFrame>),
    Pong,
    Ended,
}

impl Conn {
    /// Sends a protocol message in the negotiated encoding.
    pub async fn send(&mut self, op: u8, data: Value) {
        let envelope = json!({"op": op, "d": data});
        let frame = if self.msgpack {
            Message::Binary(rmp_serde::to_vec_named(&envelope).unwrap().into())
        } else {
            Message::Text(envelope.to_string().into())
        };
        self.ws.send(frame).await.expect("server failed to send");
    }

    /// Sends a raw frame without encoding it.
    pub async fn send_frame(&mut self, frame: Message) {
        self.ws.send(frame).await.expect("server failed to send");
    }

    /// Receives the next frame, decoding protocol messages in the negotiated encoding.
    pub async fn recv(&mut self) -> Received {
        loop {
            let Some(frame) = self.ws.next().await else {
                return Received::Ended;
            };
            let envelope: Value = match frame {
                Ok(Message::Text(text)) => {
                    assert!(!self.msgpack, "client sent a text frame in msgpack mode");
                    serde_json::from_str(&text).expect("client sent invalid JSON")
                }
                Ok(Message::Binary(bytes)) => {
                    assert!(self.msgpack, "client sent a binary frame in JSON mode");
                    rmp_serde::from_slice(&bytes).expect("client sent invalid msgpack")
                }
                Ok(Message::Close(frame)) => return Received::Close(frame),
                Ok(Message::Pong(_)) => return Received::Pong,
                Ok(_) => continue,
                Err(_) => return Received::Ended,
            };
            let op = envelope["op"].as_u64().expect("message has a numeric op") as u8;
            return Received::Message {
                op,
                data: envelope["d"].clone(),
            };
        }
    }

    /// Receives a message and asserts its opcode.
    pub async fn expect(&mut self, expected: u8) -> Value {
        match self.recv().await {
            Received::Message { op, data } if op == expected => data,
            other => panic!("expected op {expected}, got {other:?}"),
        }
    }

    /// Sends `Hello` without an auth challenge, reads `Identify`, and sends `Identified`.
    /// Returns the `Identify` data.
    pub async fn handshake(&mut self) -> Value {
        self.send(HELLO, hello(None)).await;
        let identify = self.expect(IDENTIFY).await;
        self.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
        identify
    }

    /// Replies to a request with a success status.
    pub async fn respond(&mut self, request: &Value, response_data: Option<Value>) {
        let mut data = json!({
            "requestType": request["requestType"],
            "requestId": request["requestId"],
            "requestStatus": {"result": true, "code": 100},
        });
        if let Some(response_data) = response_data {
            data["responseData"] = response_data;
        }
        self.send(REQUEST_RESPONSE, data).await;
    }

    /// Replies to a request with a failure status.
    pub async fn respond_error(&mut self, request: &Value, code: u16, comment: &str) {
        self.send(
            REQUEST_RESPONSE,
            json!({
                "requestType": request["requestType"],
                "requestId": request["requestId"],
                "requestStatus": {"result": false, "code": code, "comment": comment},
            }),
        )
        .await;
    }

    /// Answers one request with a success and no data. Used to synchronize with the client.
    pub async fn answer_next_request(&mut self) {
        let request = self.expect(REQUEST).await;
        self.respond(&request, None).await;
    }

    /// Sends a close frame and waits for the client's reply.
    pub async fn close(&mut self, code: u16, reason: &str) {
        self.ws
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.to_owned().into(),
            })))
            .await
            .expect("server failed to send close");
        while let Some(Ok(_)) = self.ws.next().await {}
    }
}

/// The data of a `Hello` message.
pub fn hello(authentication: Option<Value>) -> Value {
    let mut data = json!({
        "obsStudioVersion": "33.0.1",
        "obsWebSocketVersion": "5.7.6",
        "rpcVersion": 1,
    });
    if let Some(authentication) = authentication {
        data["authentication"] = authentication;
    }
    data
}

/// Accepts one connection on an ephemeral port and runs `handler` on it. The returned handle
/// surfaces assertion failures in the handler, so tests must await it.
pub async fn serve<F, Fut>(handler: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(Conn) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    serve_with_protocol_reply(true, handler).await
}

/// Like [`serve`], but can decline to echo the subprotocol header.
pub async fn serve_with_protocol_reply<F, Fut>(echo: bool, handler: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(Conn) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut requested = String::new();
        let callback =
            |request: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
                if let Some(protocol) = request.headers().get("Sec-WebSocket-Protocol") {
                    requested = protocol.to_str().unwrap().to_owned();
                    if echo {
                        response.headers_mut().insert(
                            "Sec-WebSocket-Protocol",
                            HeaderValue::from_str(&requested).unwrap(),
                        );
                    }
                }
                Ok(response)
            };
        let Ok(ws) = accept_hdr_async(stream, callback).await else {
            return; // The client rejected the handshake response.
        };
        let msgpack = requested == "obswebsocket.msgpack";
        handler(Conn {
            ws,
            requested_protocol: requested,
            msgpack,
        })
        .await;
    });
    (url, handle)
}
