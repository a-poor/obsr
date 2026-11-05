//! Error types.

use std::fmt;

use tokio_tungstenite::tungstenite;

use crate::enums::{RequestStatus, WebSocketCloseCode};

/// A [`Result`](std::result::Result) defaulting to this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Everything that can go wrong when talking to obs-websocket.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The WebSocket connection could not be established or failed during the handshake.
    #[error("WebSocket error: {0}")]
    WebSocket(#[source] Box<tungstenite::Error>),

    /// The server requires a password, but none was configured.
    #[error("the server requires a password, but none was provided")]
    PasswordRequired,

    /// The connection is closed. A rejected password appears here with the close code
    /// [`AuthenticationFailed`](WebSocketCloseCode::AuthenticationFailed).
    #[error("connection closed: {0}")]
    Closed(CloseInfo),

    /// The server processed a request and reported a failure.
    #[error(transparent)]
    Request(#[from] RequestError),

    /// The server did not follow the protocol.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// A message or request could not be encoded.
    #[error("failed to encode {context}: {source}")]
    Encode {
        /// What was being encoded.
        context: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A message or response could not be decoded.
    #[error("failed to decode {context}: {source}")]
    Decode {
        /// What was being decoded.
        context: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A request in a batch did not run, because the batch halted on an earlier failure.
    #[error("{request_type} did not run because the batch halted on an earlier failure")]
    NotExecuted {
        /// The type of the request that did not run.
        request_type: String,
    },
}

impl From<tungstenite::Error> for Error {
    fn from(error: tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(error))
    }
}

/// A failure reported by the server for one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestError {
    /// The type of the request that failed.
    pub request_type: String,
    /// The raw status code. See [`status`](Self::status).
    pub code: u16,
    /// The server's explanation, if it gave one.
    pub comment: Option<String>,
}

impl RequestError {
    /// The status as an enum, or `None` if this crate does not know the code.
    pub fn status(&self) -> Option<RequestStatus> {
        RequestStatus::from_code(self.code)
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} failed with status {}", self.request_type, self.code)?;
        if let Some(status) = self.status() {
            write!(f, " ({status:?})")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, ": {comment}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RequestError {}

/// Why a connection closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseInfo {
    /// The WebSocket close code, or `None` if the connection ended without one.
    pub code: Option<u16>,
    /// The server's reason, or a description of what went wrong locally.
    pub reason: String,
}

impl CloseInfo {
    /// The close code as an obs-websocket enum, or `None` if it is absent or not one of that
    /// enum's codes.
    pub fn close_code(&self) -> Option<WebSocketCloseCode> {
        self.code.and_then(WebSocketCloseCode::from_code)
    }

    /// A closure that did not come with a close code.
    pub(crate) fn local(reason: impl Into<String>) -> Self {
        Self {
            code: None,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for CloseInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code {
            write!(f, "code {code}")?;
            if let Some(close_code) = self.close_code() {
                write!(f, " ({close_code:?})")?;
            }
            if !self.reason.is_empty() {
                f.write_str(": ")?;
            }
        }
        f.write_str(&self.reason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_errors_read_well() {
        let error = RequestError {
            request_type: "GetSceneItemId".into(),
            code: 600,
            comment: Some("No scene item.".into()),
        };
        assert_eq!(
            error.to_string(),
            "GetSceneItemId failed with status 600 (ResourceNotFound): No scene item."
        );

        let error = RequestError {
            request_type: "A".into(),
            code: 999,
            comment: None,
        };
        assert_eq!(error.to_string(), "A failed with status 999");
    }

    #[test]
    fn close_info_reads_well() {
        let info = CloseInfo {
            code: Some(4009),
            reason: "Authentication failed.".into(),
        };
        assert_eq!(
            info.to_string(),
            "code 4009 (AuthenticationFailed): Authentication failed."
        );

        let info = CloseInfo {
            code: Some(1000),
            reason: String::new(),
        };
        assert_eq!(info.to_string(), "code 1000");
        assert_eq!(info.close_code(), None);

        assert_eq!(CloseInfo::local("gone").to_string(), "gone");
    }
}
