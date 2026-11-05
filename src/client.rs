//! The connection handshake, the [`Client`], and the task that owns the socket.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio_tungstenite::tungstenite::Message as Frame;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::auth::authentication_string;
use crate::batch::{BatchResponse, RequestBatch};
use crate::enums::{EventSubscription, WebSocketOpCode};
use crate::error::{CloseInfo, Error, Result};
use crate::events::Event;
use crate::protocol::{
    Encoding, Hello, Identified, Identify, Incoming, RPC_VERSION, Reidentify, RequestBatchResponse,
    RequestMessage, RequestResponse, decode_response,
};
use crate::requests::Request;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// How many outgoing messages may wait for the socket before callers wait too.
const COMMAND_BUFFER: usize = 64;
/// How long to wait for the far side to finish a close handshake.
const CLOSE_GRACE: Duration = Duration::from_secs(1);
const DEFAULT_EVENT_CAPACITY: usize = 256;
/// The largest message to accept. A native-size `bmp` screenshot of an 8K source is about 180 MiB
/// of base64 in a single frame, well past tungstenite's defaults of 16 MiB per frame and 64 MiB per
/// message. Going over the limit closes the connection.
const MAX_MESSAGE_SIZE: usize = 512 * 1024 * 1024;

/// Options for connecting to obs-websocket.
///
/// ```no_run
/// # async fn demo() -> obsr::Result<()> {
/// use obsr::{ConnectOptions, EventSubscription};
///
/// let client = ConnectOptions::new("ws://localhost:4455")
///     .password("secret")
///     .event_subscriptions(EventSubscription::SCENES | EventSubscription::OUTPUTS)
///     .connect()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ConnectOptions {
    url: String,
    password: Option<String>,
    event_subscriptions: Option<EventSubscription>,
    encoding: Encoding,
    event_capacity: usize,
}

impl ConnectOptions {
    /// Creates options for the server at `url`, such as `ws://localhost:4455`. obs-websocket does
    /// not serve TLS, so `wss://` URLs are not supported.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            password: None,
            event_subscriptions: None,
            encoding: Encoding::default(),
            event_capacity: DEFAULT_EVENT_CAPACITY,
        }
    }

    /// Sets the password, used only if the server asks for one.
    #[must_use]
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Sets which events to receive. Without this, the server sends
    /// [`EventSubscription::ALL`], which excludes high-volume events.
    #[must_use]
    pub fn event_subscriptions(mut self, subscriptions: EventSubscription) -> Self {
        self.event_subscriptions = Some(subscriptions);
        self
    }

    /// Sets the message encoding. The default is [`Encoding::Json`].
    #[must_use]
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Sets how many events are buffered for each receiver before a slow one starts missing
    /// events. The default is 256, and the minimum is 1.
    #[must_use]
    pub fn event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity.max(1);
        self
    }

    /// Connects and identifies with the server.
    pub async fn connect(self) -> Result<Client> {
        let mut request = self.url.as_str().into_client_request()?;
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static(self.encoding.subprotocol()),
        );
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE_SIZE))
            .max_frame_size(Some(MAX_MESSAGE_SIZE));
        // Requests are small and each one waits for a reply, so Nagle's algorithm only adds delay.
        let (mut socket, _) = connect_async_with_config(request, Some(config), true).await?;

        let hello = identify(&mut socket, &self).await;
        let hello = match hello {
            Ok(hello) => hello,
            Err(error) => {
                // Best effort: the handshake error is the one worth reporting.
                let _ = socket.close(None).await;
                return Err(error);
            }
        };

        let (commands, command_receiver) = mpsc::channel(COMMAND_BUFFER);
        let (events, _) = broadcast::channel(self.event_capacity);
        let shared = Arc::new(Shared::new(events.clone()));
        tokio::spawn(run_connection(
            socket,
            command_receiver,
            Arc::clone(&shared),
            self.encoding,
            events,
        ));

        Ok(Client {
            inner: Arc::new(Inner {
                commands,
                shared,
                next_id: AtomicU64::new(0),
                encoding: self.encoding,
                server_info: hello,
            }),
        })
    }
}

impl fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("url", &self.url)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("event_subscriptions", &self.event_subscriptions)
            .field("encoding", &self.encoding)
            .field("event_capacity", &self.event_capacity)
            .finish()
    }
}

/// Runs the protocol handshake and returns the server's `Hello`.
async fn identify(socket: &mut Socket, options: &ConnectOptions) -> Result<Hello> {
    let encoding = options.encoding;
    let hello = match next_message(socket, encoding).await? {
        Incoming::Hello(hello) => hello,
        other => {
            return Err(Error::Protocol(format!("expected Hello, got {other:?}")));
        }
    };

    let authentication = match (&hello.authentication, &options.password) {
        (None, _) => None,
        (Some(_), None) => return Err(Error::PasswordRequired),
        (Some(auth), Some(password)) => {
            Some(authentication_string(password, &auth.salt, &auth.challenge))
        }
    };

    // RPC_VERSION is the only version this crate has, so it is also the closest one to offer
    // when the server's differs.
    let identify = Identify {
        rpc_version: RPC_VERSION,
        authentication,
        event_subscriptions: options.event_subscriptions,
    };
    socket
        .send(encoding.encode(WebSocketOpCode::Identify, &identify)?)
        .await?;

    let identified = loop {
        match next_message(socket, encoding).await? {
            Incoming::Identified(identified) => break identified,
            // The server starts sending events just before `Identified`. Nobody can be
            // subscribed yet, so they are dropped.
            Incoming::Event(_) => {}
            other => {
                return Err(Error::Protocol(format!(
                    "expected Identified, got {other:?}"
                )));
            }
        }
    };
    if identified.negotiated_rpc_version != RPC_VERSION {
        return Err(Error::Protocol(format!(
            "the server negotiated RPC version {}, but this crate supports {RPC_VERSION}",
            identified.negotiated_rpc_version
        )));
    }
    Ok(hello)
}

/// Reads frames until a protocol message arrives. Used only during the handshake.
async fn next_message(socket: &mut Socket, encoding: Encoding) -> Result<Incoming> {
    while let Some(frame) = socket.next().await {
        match frame? {
            frame @ (Frame::Text(_) | Frame::Binary(_)) => return encoding.decode(&frame),
            Frame::Close(frame) => return Err(Error::Closed(close_info(frame))),
            _ => {}
        }
    }
    Err(Error::Closed(CloseInfo::local(
        "the connection ended during the handshake",
    )))
}

/// An identified connection to obs-websocket.
///
/// Cloning is cheap, and all clones share one connection. The connection closes when
/// [`close`](Self::close) is called or the last clone is dropped.
///
/// The client has no built-in timeouts. All of its futures can be cancelled safely, so wrap any
/// call in [`tokio::time::timeout`] to bound it.
///
/// ```no_run
/// # async fn demo() -> obsr::Result<()> {
/// use obsr::Client;
/// use obsr::requests::GetVersion;
///
/// let client = Client::connect("ws://localhost:4455").await?;
/// let version = client.request(GetVersion).await?;
/// println!("OBS {}", version.obs_version);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    commands: mpsc::Sender<Command>,
    shared: Arc<Shared>,
    next_id: AtomicU64,
    encoding: Encoding,
    server_info: Hello,
}

impl Client {
    /// Connects to `url` with default options. Use [`ConnectOptions`] to set a password or to
    /// choose which events to receive.
    pub async fn connect(url: impl Into<String>) -> Result<Self> {
        ConnectOptions::new(url).connect().await
    }

    /// Sends a typed request and waits for its response.
    ///
    /// Returns [`Error::Request`] if the server reports a failure.
    pub async fn request<R: Request>(&self, request: R) -> Result<R::Response> {
        let data = request_data(&request)?;
        let response = self.request_raw(R::REQUEST_TYPE, data).await?;
        decode_response(response, R::REQUEST_TYPE)
    }

    /// Sends a request by name and returns its raw `responseData`, for requests this crate does
    /// not know about yet.
    pub async fn request_raw(
        &self,
        request_type: &str,
        data: Option<Value>,
    ) -> Result<Option<Value>> {
        let id = self.next_id();
        let frame = self.inner.encoding.encode(
            WebSocketOpCode::Request,
            &RequestMessage {
                request_type: request_type.to_owned(),
                request_id: id.clone(),
                request_data: data,
            },
        )?;
        match self.exchange(id, frame).await? {
            Reply::Single(response) => Ok(response.into_result()?),
            Reply::Batch(_) => Err(Error::Protocol(
                "the server answered a request with a batch response".to_owned(),
            )),
        }
    }

    /// Sends a request batch and waits for its results.
    ///
    /// This only fails if the batch as a whole does. Failures of individual requests are reported
    /// by [`BatchResponse::get`].
    pub async fn batch(&self, batch: RequestBatch) -> Result<BatchResponse> {
        let id = self.next_id();
        let frame = self.inner.encoding.encode(
            WebSocketOpCode::RequestBatch,
            &batch.into_message(id.clone()),
        )?;
        match self.exchange(id, frame).await? {
            Reply::Batch(response) => Ok(BatchResponse::new(response.results)),
            Reply::Single(_) => Err(Error::Protocol(
                "the server answered a batch with a single response".to_owned(),
            )),
        }
    }

    /// Replaces the event subscriptions of this session and waits for the server to confirm.
    pub async fn reidentify(&self, subscriptions: EventSubscription) -> Result<()> {
        let frame = self.inner.encoding.encode(
            WebSocketOpCode::Reidentify,
            &Reidentify {
                event_subscriptions: subscriptions,
            },
        )?;
        let permit = self.reserve().await?;
        let Some(receiver) = self.inner.shared.register_reidentify() else {
            return Err(self.closed_error().await);
        };
        permit.send(Command::Send(frame));
        match receiver.await {
            Ok(()) => Ok(()),
            Err(_) => Err(self.closed_error().await),
        }
    }

    /// Returns a new receiver for events that arrive from now on.
    ///
    /// Each receiver gets every event. One that falls behind receives
    /// [`RecvError::Lagged`](broadcast::error::RecvError::Lagged) and then continues with the
    /// oldest event still buffered. After the connection closes, receivers get
    /// [`RecvError::Closed`](broadcast::error::RecvError::Closed). A receiver does not keep the
    /// connection open, so keep a [`Client`] alive while receiving.
    ///
    /// ```no_run
    /// # async fn demo(client: obsr::Client) {
    /// use obsr::Event;
    /// use tokio::sync::broadcast::error::RecvError;
    ///
    /// let mut events = client.events();
    /// loop {
    ///     match events.recv().await {
    ///         Ok(Event::CurrentProgramSceneChanged(scene)) => println!("{}", scene.scene_name),
    ///         Ok(_) => {}
    ///         Err(RecvError::Lagged(missed)) => eprintln!("missed {missed} events"),
    ///         Err(RecvError::Closed) => break,
    ///     }
    /// }
    /// # }
    /// ```
    pub fn events(&self) -> broadcast::Receiver<Event> {
        match &self.inner.shared.lock().events {
            Some(events) => events.subscribe(),
            // Dropping the sender right away yields a receiver that reports `Closed`.
            None => broadcast::channel(1).1,
        }
    }

    /// The server's `Hello` message, which has its OBS and obs-websocket versions.
    pub fn server_info(&self) -> &Hello {
        &self.inner.server_info
    }

    /// The RPC version in use for this connection.
    pub fn negotiated_rpc_version(&self) -> u32 {
        RPC_VERSION
    }

    /// Closes the connection for all clones and waits until it has closed.
    pub async fn close(&self) {
        // A failed send means the connection task has already stopped.
        let _ = self.inner.commands.send(Command::Close).await;
        self.closed().await;
    }

    /// Waits until the connection has closed and returns why.
    pub async fn closed(&self) -> CloseInfo {
        let mut closed = self.inner.shared.closed.subscribe();
        match closed.wait_for(Option::is_some).await {
            Ok(info) => (*info).clone().expect("wait_for checked this value"),
            // Unreachable while `Shared` is alive, because it owns the sender.
            Err(_) => CloseInfo::local("the connection task stopped"),
        }
    }

    /// Returns `true` once the connection has closed.
    pub fn is_closed(&self) -> bool {
        self.inner.shared.closed.borrow().is_some()
    }

    fn next_id(&self) -> String {
        self.inner
            .next_id
            .fetch_add(1, Ordering::Relaxed)
            .to_string()
    }

    /// Sends a frame and waits for the reply with the matching id.
    async fn exchange(&self, id: String, frame: Frame) -> Result<Reply> {
        let shared = &self.inner.shared;
        // Reserving first means registering and sending happen with no await in between, so a
        // cancelled caller can never leave a registered request that was not sent.
        let permit = self.reserve().await?;
        let Some(receiver) = shared.register(&id) else {
            return Err(self.closed_error().await);
        };
        let _guard = PendingGuard { shared, id: &id };
        permit.send(Command::Send(frame));
        match receiver.await {
            Ok(reply) => Ok(reply),
            Err(_) => Err(self.closed_error().await),
        }
    }

    async fn reserve(&self) -> Result<mpsc::Permit<'_, Command>> {
        match self.inner.commands.reserve().await {
            Ok(permit) => Ok(permit),
            Err(_) => Err(self.closed_error().await),
        }
    }

    async fn closed_error(&self) -> Error {
        Error::Closed(self.closed().await)
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("server_info", &self.inner.server_info)
            .field("encoding", &self.inner.encoding)
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Serializes a request's fields. A request without fields has no `requestData`.
pub(crate) fn request_data<R: Request>(request: &R) -> Result<Option<Value>> {
    let data = serde_json::to_value(request).map_err(|source| Error::Encode {
        context: format!("{} request", R::REQUEST_TYPE),
        source: source.into(),
    })?;
    Ok((!data.is_null()).then_some(data))
}

enum Command {
    Send(Frame),
    Close,
}

/// A response on its way to the caller that is waiting for it.
#[derive(Debug)]
enum Reply {
    Single(RequestResponse),
    Batch(RequestBatchResponse),
}

/// State shared between callers and the connection task.
struct Shared {
    state: Mutex<State>,
    closed: watch::Sender<Option<CloseInfo>>,
}

struct State {
    pending: HashMap<String, oneshot::Sender<Reply>>,
    /// `Identified` has no id, so acknowledgements are matched to `Reidentify` messages in order.
    reidentify: VecDeque<oneshot::Sender<()>>,
    /// `None` once closed, which is what ends the receivers.
    events: Option<broadcast::Sender<Event>>,
}

impl Shared {
    fn new(events: broadcast::Sender<Event>) -> Self {
        Self {
            state: Mutex::new(State {
                pending: HashMap::new(),
                reidentify: VecDeque::new(),
                events: Some(events),
            }),
            closed: watch::Sender::new(None),
        }
    }

    // The state is valid at every point, so a panic in another thread does not corrupt it.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers a pending request, or returns `None` if the connection has closed.
    fn register(&self, id: &str) -> Option<oneshot::Receiver<Reply>> {
        let mut state = self.lock();
        if self.closed.borrow().is_some() {
            return None;
        }
        let (sender, receiver) = oneshot::channel();
        state.pending.insert(id.to_owned(), sender);
        Some(receiver)
    }

    /// Registers a pending `Reidentify`, or returns `None` if the connection has closed.
    fn register_reidentify(&self) -> Option<oneshot::Receiver<()>> {
        let mut state = self.lock();
        if self.closed.borrow().is_some() {
            return None;
        }
        let (sender, receiver) = oneshot::channel();
        state.reidentify.push_back(sender);
        Some(receiver)
    }

    fn complete(&self, id: Option<&str>, reply: Reply) {
        let sender = id.and_then(|id| self.lock().pending.remove(id));
        if let Some(sender) = sender {
            // The caller may have stopped waiting, which is fine.
            let _ = sender.send(reply);
        }
    }

    /// Hands an incoming message to whoever is waiting for it.
    fn dispatch(&self, message: Incoming, events: &broadcast::Sender<Event>) {
        match message {
            Incoming::Event(raw) => {
                if events.receiver_count() > 0 {
                    // A send only fails when the last receiver has just gone away.
                    let _ = events.send(Event::from_raw(raw));
                }
            }
            Incoming::RequestResponse(response) => {
                let id = response.request_id.clone();
                self.complete(id.as_deref(), Reply::Single(response));
            }
            Incoming::RequestBatchResponse(response) => {
                let id = response.request_id.clone();
                self.complete(Some(&id), Reply::Batch(response));
            }
            Incoming::Identified(Identified { .. }) => {
                if let Some(sender) = self.lock().reidentify.pop_front() {
                    let _ = sender.send(());
                }
            }
            Incoming::Hello(_) | Incoming::Other { .. } => {}
        }
    }

    /// Records why the connection closed and wakes everything waiting on it. Only the first call
    /// has an effect.
    fn finish(&self, info: CloseInfo) {
        let mut state = self.lock();
        if self.closed.borrow().is_some() {
            return;
        }
        // Set while holding the lock, so that `register` either sees the closure or has its entry
        // cleared here.
        self.closed.send_replace(Some(info));
        // Dropping the senders wakes every waiter with an error.
        state.pending.clear();
        state.reidentify.clear();
        state.events = None;
    }
}

/// Removes a pending request if its caller stops waiting.
struct PendingGuard<'a> {
    shared: &'a Shared,
    id: &'a str,
}

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.shared.lock().pending.remove(self.id);
    }
}

/// Marks the connection as closed however the task ends, so that callers cannot hang.
struct FinishOnDrop(Arc<Shared>);

impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        self.0
            .finish(CloseInfo::local("the connection task stopped"));
    }
}

/// Owns the socket for the life of the connection.
async fn run_connection(
    socket: Socket,
    commands: mpsc::Receiver<Command>,
    shared: Arc<Shared>,
    encoding: Encoding,
    events: broadcast::Sender<Event>,
) {
    let _finish = FinishOnDrop(Arc::clone(&shared));
    let (sink, mut stream) = socket.split();

    let info = {
        let reader = read_frames(&mut stream, encoding, &shared, events);
        let writer = write_frames(sink, commands);
        tokio::pin!(reader, writer);
        // Reading and writing make progress concurrently, so a large write cannot deadlock with a
        // server that is itself blocked writing to us.
        tokio::select! {
            info = &mut reader => info,
            result = &mut writer => match result {
                Ok(()) => {
                    // Our close frame is out. Wait for the reply, but not indefinitely.
                    let _ = tokio::time::timeout(CLOSE_GRACE, &mut reader).await;
                    CloseInfo {
                        code: Some(CloseCode::Normal.into()),
                        reason: "closed by the client".to_owned(),
                    }
                }
                // The reader usually knows more, such as a close frame already received.
                Err(error) => tokio::time::timeout(CLOSE_GRACE, &mut reader)
                    .await
                    .unwrap_or_else(|_| CloseInfo::local(format!("failed to send: {error}"))),
            },
        }
    };
    shared.finish(info);

    // tungstenite sends its reply to a close frame during the next read.
    let drain = async { while stream.next().await.is_some() {} };
    let _ = tokio::time::timeout(CLOSE_GRACE, drain).await;
}

/// Dispatches incoming messages until the connection ends, and returns why it did.
async fn read_frames(
    stream: &mut SplitStream<Socket>,
    encoding: Encoding,
    shared: &Shared,
    events: broadcast::Sender<Event>,
) -> CloseInfo {
    while let Some(frame) = stream.next().await {
        match frame {
            Ok(frame @ (Frame::Text(_) | Frame::Binary(_))) => match encoding.decode(&frame) {
                Ok(message) => shared.dispatch(message, &events),
                // Waiting callers could otherwise hang on a reply that was lost in this message.
                Err(error) => return CloseInfo::local(error.to_string()),
            },
            Ok(Frame::Close(frame)) => return close_info(frame),
            // tungstenite answers pings on its own.
            Ok(_) => {}
            Err(error) => return CloseInfo::local(format!("the connection was lost: {error}")),
        }
    }
    CloseInfo::local("the connection ended without a close frame")
}

/// Sends queued frames until asked to close or until every client is gone, then sends a close
/// frame.
async fn write_frames(
    mut sink: SplitSink<Socket, Frame>,
    mut commands: mpsc::Receiver<Command>,
) -> std::result::Result<(), tokio_tungstenite::tungstenite::Error> {
    while let Some(Command::Send(frame)) = commands.recv().await {
        sink.send(frame).await?;
    }
    sink.send(Frame::Close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "".into(),
    })))
    .await
}

fn close_info(frame: Option<CloseFrame>) -> CloseInfo {
    match frame {
        Some(frame) => CloseInfo {
            code: Some(frame.code.into()),
            reason: frame.reason.to_string(),
        },
        None => CloseInfo::local("the server closed the connection without a reason"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn shared() -> Shared {
        Shared::new(broadcast::channel(1).0)
    }

    #[test]
    fn dropping_the_guard_forgets_a_cancelled_request() {
        let shared = shared();
        let receiver = shared.register("1").unwrap();
        assert_eq!(shared.lock().pending.len(), 1);
        {
            let _guard = PendingGuard {
                shared: &shared,
                id: "1",
            };
        }
        assert!(shared.lock().pending.is_empty());
        drop(receiver);
    }

    #[test]
    fn finish_clears_waiters_and_blocks_new_registrations() {
        let shared = shared();
        let mut request = shared.register("1").unwrap();
        let mut reidentify = shared.register_reidentify().unwrap();

        shared.finish(CloseInfo::local("first"));
        shared.finish(CloseInfo::local("second"));

        assert!(request.try_recv().is_err());
        assert!(reidentify.try_recv().is_err());
        assert!(shared.register("2").is_none());
        assert!(shared.register_reidentify().is_none());
        assert!(shared.lock().events.is_none());
        assert_eq!(shared.closed.borrow().as_ref().unwrap().reason, "first");
    }

    #[test]
    fn a_reply_is_delivered_once_and_only_to_its_id() {
        let shared = shared();
        let mut first = shared.register("1").unwrap();
        let mut second = shared.register("2").unwrap();
        let response = |id: &str| {
            serde_json::from_value::<RequestResponse>(json!({
                "requestType": "A",
                "requestId": id,
                "requestStatus": {"result": true, "code": 100},
            }))
            .unwrap()
        };
        let events = broadcast::channel(1).0;

        shared.dispatch(Incoming::RequestResponse(response("2")), &events);
        assert!(matches!(second.try_recv(), Ok(Reply::Single(_))));
        assert!(first.try_recv().is_err());
        assert_eq!(shared.lock().pending.len(), 1);

        shared.dispatch(Incoming::RequestResponse(response("2")), &events);
        assert!(first.try_recv().is_err());
    }

    #[test]
    fn close_info_carries_the_frame_contents() {
        let info = close_info(Some(CloseFrame {
            code: CloseCode::from(4009),
            reason: "no".into(),
        }));
        assert_eq!(info.code, Some(4009));
        assert_eq!(info.reason, "no");
        assert_eq!(close_info(None).code, None);
    }
}
