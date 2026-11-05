//! The wire protocol: message envelopes, encodings, and message payloads.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message as Frame;

use crate::enums::{EventSubscription, RequestBatchExecutionType, RequestStatus, WebSocketOpCode};
use crate::error::{Error, RequestError, Result};
use crate::events::RawEvent;

/// The RPC version this crate implements.
pub const RPC_VERSION: u32 = 1;

/// How messages are encoded on the wire.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Encoding {
    /// JSON over text frames.
    #[default]
    Json,
    /// MessagePack over binary frames.
    MsgPack,
}

impl Encoding {
    /// The WebSocket subprotocol that selects this encoding.
    pub const fn subprotocol(self) -> &'static str {
        match self {
            Self::Json => "obswebsocket.json",
            Self::MsgPack => "obswebsocket.msgpack",
        }
    }

    /// Wraps `data` in the `{op, d}` envelope and encodes it into a frame.
    pub(crate) fn encode<T: Serialize>(self, op: WebSocketOpCode, data: &T) -> Result<Frame> {
        let envelope = OutgoingEnvelope {
            op: op.code(),
            d: data,
        };
        let context = || format!("{op:?} message");
        match self {
            Self::Json => serde_json::to_string(&envelope)
                .map(|text| Frame::Text(text.into()))
                .map_err(|source| Error::Encode {
                    context: context(),
                    source: source.into(),
                }),
            // `_named` writes structs as maps. The default writes them as arrays.
            Self::MsgPack => rmp_serde::to_vec_named(&envelope)
                .map(|bytes| Frame::Binary(bytes.into()))
                .map_err(|source| Error::Encode {
                    context: context(),
                    source: source.into(),
                }),
        }
    }

    /// Decodes a text or binary frame. A frame of the wrong kind for this encoding is an error.
    pub(crate) fn decode(self, frame: &Frame) -> Result<Incoming> {
        let context = "server message";
        let envelope: IncomingEnvelope = match (self, frame) {
            (Self::Json, Frame::Text(text)) => {
                serde_json::from_str(text).map_err(|source| decode_error(context, source))?
            }
            (Self::MsgPack, Frame::Binary(bytes)) => {
                rmp_serde::from_slice(bytes).map_err(|source| decode_error(context, source))?
            }
            _ => {
                let source = format!("unexpected frame type for {self:?} encoding");
                return Err(decode_error(context, source));
            }
        };
        envelope.into_message()
    }
}

fn decode_error(
    context: &str,
    source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> Error {
    Error::Decode {
        context: context.to_owned(),
        source: source.into(),
    }
}

/// Decodes `responseData` into `T`. Missing data decodes from an empty object, so that a type
/// with required fields reports which field is missing.
pub(crate) fn decode_response<T: DeserializeOwned>(
    data: Option<Value>,
    request_type: &str,
) -> Result<T> {
    let data = data.unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    serde_json::from_value(data)
        .map_err(|source| decode_error(&format!("{request_type} response"), source))
}

#[derive(Serialize)]
struct OutgoingEnvelope<'a, T> {
    op: u8,
    d: &'a T,
}

#[derive(Deserialize)]
struct IncomingEnvelope {
    op: u64,
    #[serde(default)]
    d: Value,
}

impl IncomingEnvelope {
    fn into_message(self) -> Result<Incoming> {
        let Some(op) = u8::try_from(self.op)
            .ok()
            .and_then(WebSocketOpCode::from_code)
        else {
            return Ok(Incoming::Other { op: self.op });
        };
        let d = self.d;
        Ok(match op {
            WebSocketOpCode::Hello => Incoming::Hello(data(op, d)?),
            WebSocketOpCode::Identified => Incoming::Identified(data(op, d)?),
            WebSocketOpCode::Event => Incoming::Event(data(op, d)?),
            WebSocketOpCode::RequestResponse => Incoming::RequestResponse(data(op, d)?),
            WebSocketOpCode::RequestBatchResponse => Incoming::RequestBatchResponse(data(op, d)?),
            _ => Incoming::Other { op: self.op },
        })
    }
}

fn data<T: DeserializeOwned>(op: WebSocketOpCode, value: Value) -> Result<T> {
    serde_json::from_value(value)
        .map_err(|source| decode_error(&format!("server {op:?} message"), source))
}

/// A message sent by the server.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Incoming {
    Hello(Hello),
    Identified(Identified),
    Event(RawEvent),
    RequestResponse(RequestResponse),
    RequestBatchResponse(RequestBatchResponse),
    /// An opcode the server should not send, or one added by a newer version. Ignored.
    Other {
        op: u64,
    },
}

/// The `Hello` (OpCode 0) message the server sends first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Hello {
    /// The version of OBS Studio.
    pub obs_studio_version: String,
    /// The version of obs-websocket. A hint at available features, not a guarantee.
    pub obs_web_socket_version: String,
    /// The RPC version the server would like to use.
    pub rpc_version: u32,
    /// The authentication challenge, if the server requires a password.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<AuthChallenge>,
}

/// The authentication challenge in [`Hello`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct AuthChallenge {
    /// The per-connection challenge.
    pub challenge: String,
    /// The salt of the stored password.
    pub salt: String,
}

/// The `Identify` (OpCode 1) message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Identify {
    pub rpc_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authentication: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_subscriptions: Option<EventSubscription>,
}

/// The `Identified` (OpCode 2) message.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Identified {
    pub negotiated_rpc_version: u32,
}

/// The `Reidentify` (OpCode 3) message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Reidentify {
    pub event_subscriptions: EventSubscription,
}

/// The `Request` (OpCode 6) message, and each entry of a request batch.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestMessage {
    pub request_type: String,
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_data: Option<Value>,
}

/// The `RequestBatch` (OpCode 8) message.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestBatchMessage {
    pub request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub halt_on_failure: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_type: Option<RequestBatchExecutionType>,
    pub requests: Vec<RequestMessage>,
}

/// The `RequestBatchResponse` (OpCode 9) message.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestBatchResponse {
    pub request_id: String,
    pub results: Vec<RequestResponse>,
}

/// The `RequestResponse` (OpCode 7) message, and each result of a request batch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct RequestResponse {
    /// A mirror of the request's type.
    pub request_type: String,
    /// A mirror of the request's id, if it had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Whether the request succeeded.
    pub request_status: ResponseStatus,
    /// The response's data, if it has any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_data: Option<Value>,
}

impl RequestResponse {
    /// The response data if the request succeeded, or else the server's failure.
    pub fn into_result(self) -> Result<Option<Value>, RequestError> {
        if self.request_status.result {
            Ok(self.response_data)
        } else {
            Err(RequestError {
                request_type: self.request_type,
                code: self.request_status.code,
                comment: self.request_status.comment,
            })
        }
    }
}

/// The `requestStatus` object of a [`RequestResponse`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ResponseStatus {
    /// `true` if the request succeeded.
    pub result: bool,
    /// The raw status code. See [`status`](Self::status).
    pub code: u16,
    /// The server's explanation, if it gave one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

impl ResponseStatus {
    /// The status as an enum, or `None` if this crate does not know the code.
    pub fn status(&self) -> Option<RequestStatus> {
        RequestStatus::from_code(self.code)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn decode_json(text: &str) -> Result<Incoming> {
        Encoding::Json.decode(&Frame::Text(text.to_owned().into()))
    }

    fn encoded_json<T: Serialize>(op: WebSocketOpCode, data: &T) -> Value {
        let Frame::Text(text) = Encoding::Json.encode(op, data).unwrap() else {
            panic!("JSON must use text frames");
        };
        serde_json::from_str(&text).unwrap()
    }

    // The JSON documents below are the example messages from the protocol spec.

    #[test]
    fn decodes_the_spec_hello_with_authentication() {
        let message = decode_json(
            r#"{"op": 0, "d": {
                "obsStudioVersion": "30.2.2",
                "obsWebSocketVersion": "5.5.2",
                "rpcVersion": 1,
                "authentication": {
                    "challenge": "+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=",
                    "salt": "lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI="
                }
            }}"#,
        )
        .unwrap();
        let Incoming::Hello(hello) = message else {
            panic!("expected Hello, got {message:?}");
        };
        assert_eq!(hello.obs_studio_version, "30.2.2");
        assert_eq!(hello.obs_web_socket_version, "5.5.2");
        assert_eq!(hello.rpc_version, 1);
        let auth = hello.authentication.unwrap();
        assert_eq!(
            auth.challenge,
            "+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY="
        );
        assert_eq!(auth.salt, "lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI=");
    }

    #[test]
    fn decodes_the_spec_hello_without_authentication() {
        let message = decode_json(
            r#"{"op": 0, "d": {"obsStudioVersion": "30.2.2", "obsWebSocketVersion": "5.5.2", "rpcVersion": 1}}"#,
        )
        .unwrap();
        let Incoming::Hello(hello) = message else {
            panic!("expected Hello, got {message:?}");
        };
        assert_eq!(hello.authentication, None);
    }

    #[test]
    fn decodes_the_spec_identified() {
        let message = decode_json(r#"{"op": 2, "d": {"negotiatedRpcVersion": 1}}"#).unwrap();
        assert_eq!(
            message,
            Incoming::Identified(Identified {
                negotiated_rpc_version: 1
            })
        );
    }

    #[test]
    fn decodes_the_spec_event() {
        let message = decode_json(
            r#"{"op": 5, "d": {
                "eventType": "StudioModeStateChanged",
                "eventIntent": 1,
                "eventData": {"studioModeEnabled": true}
            }}"#,
        )
        .unwrap();
        assert_eq!(
            message,
            Incoming::Event(RawEvent {
                event_type: "StudioModeStateChanged".into(),
                event_intent: EventSubscription::GENERAL,
                event_data: Some(json!({"studioModeEnabled": true})),
            })
        );
    }

    #[test]
    fn decodes_the_spec_successful_response() {
        let message = decode_json(
            r#"{"op": 7, "d": {
                "requestType": "SetCurrentProgramScene",
                "requestId": "f819dcf0-89cc-11eb-8f0e-382c4ac93b9c",
                "requestStatus": {"result": true, "code": 100}
            }}"#,
        )
        .unwrap();
        let Incoming::RequestResponse(response) = message else {
            panic!("expected a response, got {message:?}");
        };
        assert_eq!(
            response.request_id.as_deref(),
            Some("f819dcf0-89cc-11eb-8f0e-382c4ac93b9c")
        );
        assert_eq!(
            response.request_status.status(),
            Some(RequestStatus::Success)
        );
        assert_eq!(response.into_result().unwrap(), None);
    }

    #[test]
    fn decodes_the_spec_failure_response() {
        let message = decode_json(
            r#"{"op": 7, "d": {
                "requestType": "SetCurrentProgramScene",
                "requestId": "f819dcf0-89cc-11eb-8f0e-382c4ac93b9c",
                "requestStatus": {"result": false, "code": 608, "comment": "Parameter: sceneName"}
            }}"#,
        )
        .unwrap();
        let Incoming::RequestResponse(response) = message else {
            panic!("expected a response, got {message:?}");
        };
        let error = response.into_result().unwrap_err();
        assert_eq!(error.code, 608);
        assert_eq!(error.request_type, "SetCurrentProgramScene");
        assert_eq!(error.comment.as_deref(), Some("Parameter: sceneName"));
    }

    #[test]
    fn decodes_a_batch_response_whose_results_have_no_ids() {
        let message = decode_json(
            r#"{"op": 9, "d": {"requestId": "b", "results": [
                {"requestType": "A", "requestStatus": {"result": true, "code": 100}}
            ]}}"#,
        )
        .unwrap();
        let Incoming::RequestBatchResponse(response) = message else {
            panic!("expected a batch response, got {message:?}");
        };
        assert_eq!(response.request_id, "b");
        assert_eq!(response.results[0].request_id, None);
    }

    #[test]
    fn encodes_identify_like_the_spec_example() {
        let identify = Identify {
            rpc_version: 1,
            authentication: Some("Dj6cLS+jrNA0HpCArRg0Z/Fc+YHdt2FQfAvgD1mip6Y=".into()),
            event_subscriptions: Some(EventSubscription::from_bits(33)),
        };
        assert_eq!(
            encoded_json(WebSocketOpCode::Identify, &identify),
            json!({"op": 1, "d": {
                "rpcVersion": 1,
                "authentication": "Dj6cLS+jrNA0HpCArRg0Z/Fc+YHdt2FQfAvgD1mip6Y=",
                "eventSubscriptions": 33,
            }})
        );
    }

    #[test]
    fn encodes_identify_without_optional_fields() {
        let identify = Identify {
            rpc_version: 1,
            authentication: None,
            event_subscriptions: None,
        };
        assert_eq!(
            encoded_json(WebSocketOpCode::Identify, &identify),
            json!({"op": 1, "d": {"rpcVersion": 1}})
        );
    }

    #[test]
    fn encodes_a_request_like_the_spec_example() {
        let request = RequestMessage {
            request_type: "SetCurrentProgramScene".into(),
            request_id: "f819dcf0-89cc-11eb-8f0e-382c4ac93b9c".into(),
            request_data: Some(json!({"sceneName": "Scene 12"})),
        };
        assert_eq!(
            encoded_json(WebSocketOpCode::Request, &request),
            json!({"op": 6, "d": {
                "requestType": "SetCurrentProgramScene",
                "requestId": "f819dcf0-89cc-11eb-8f0e-382c4ac93b9c",
                "requestData": {"sceneName": "Scene 12"},
            }})
        );
    }

    #[test]
    fn msgpack_writes_structs_as_maps_in_binary_frames() {
        let identify = Identify {
            rpc_version: 1,
            authentication: None,
            event_subscriptions: None,
        };
        let Frame::Binary(bytes) = Encoding::MsgPack
            .encode(WebSocketOpCode::Identify, &identify)
            .unwrap()
        else {
            panic!("MessagePack must use binary frames");
        };
        let decoded: Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded, json!({"op": 1, "d": {"rpcVersion": 1}}));
    }

    #[test]
    fn msgpack_decodes_server_messages() {
        let envelope = json!({"op": 2, "d": {"negotiatedRpcVersion": 1}});
        let frame = Frame::Binary(rmp_serde::to_vec_named(&envelope).unwrap().into());
        assert_eq!(
            Encoding::MsgPack.decode(&frame).unwrap(),
            Incoming::Identified(Identified {
                negotiated_rpc_version: 1
            })
        );
    }

    #[test]
    fn rejects_a_frame_in_the_wrong_encoding() {
        let text = Frame::Text("{}".into());
        assert!(matches!(
            Encoding::MsgPack.decode(&text),
            Err(Error::Decode { .. })
        ));
        let binary = Frame::Binary(vec![1].into());
        assert!(matches!(
            Encoding::Json.decode(&binary),
            Err(Error::Decode { .. })
        ));
    }

    #[test]
    fn unknown_and_client_only_opcodes_are_not_errors() {
        for op in [1, 3, 4, 6, 8, 10, 99, 100_000] {
            let message = decode_json(&json!({"op": op, "d": {}}).to_string()).unwrap();
            assert_eq!(message, Incoming::Other { op });
        }
    }

    #[test]
    fn rejects_malformed_messages() {
        for text in [
            "not json",
            "[]",
            r#"{"d": {}}"#,
            r#"{"op": "0", "d": {}}"#,
            r#"{"op": 0}"#,
            r#"{"op": 0, "d": {"rpcVersion": "1"}}"#,
            r#"{"op": 7, "d": {"requestType": "A"}}"#,
        ] {
            assert!(
                matches!(decode_json(text), Err(Error::Decode { .. })),
                "{text} should not decode"
            );
        }
    }

    #[test]
    fn subprotocol_names_match_the_spec() {
        assert_eq!(Encoding::Json.subprotocol(), "obswebsocket.json");
        assert_eq!(Encoding::MsgPack.subprotocol(), "obswebsocket.msgpack");
        assert_eq!(Encoding::default(), Encoding::Json);
    }

    #[test]
    fn decode_response_treats_missing_data_as_an_empty_object() {
        use crate::requests::{Empty, GetVersionResponse};

        decode_response::<Empty>(None, "A").unwrap();
        let error = decode_response::<GetVersionResponse>(None, "GetVersion").unwrap_err();
        assert!(error.to_string().contains("GetVersion response"), "{error}");
        assert!(error.to_string().contains("missing field"), "{error}");
    }
}
