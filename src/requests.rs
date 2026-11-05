//! Typed requests and responses, one pair for each request in the protocol spec.
//!
//! Pass a request to [`Client::request`](crate::Client::request). Optional fields are `Option`s
//! that are left off the wire when `None`, so struct update syntax keeps call sites short:
//!
//! ```
//! use obsr::requests::SetCurrentProgramScene;
//!
//! let request = SetCurrentProgramScene {
//!     scene_name: Some("Live".into()),
//!     ..Default::default()
//! };
//! ```
//!
//! The spec does not describe the contents of `Object` fields, so they are
//! [`serde_json::Value`]s. Many requests identify a resource with a pair of optional fields, such
//! as `scene_name` and `scene_uuid`. Set one of them.

use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

mod generated;
pub use generated::*;

/// A request that can be sent with [`Client::request`](crate::Client::request).
///
/// Implement this to send a request type that this crate does not know about yet.
pub trait Request: Serialize {
    /// The `requestType` sent on the wire.
    const REQUEST_TYPE: &'static str;
    /// The decoded `responseData`.
    type Response: DeserializeOwned;
}

/// The response to a request that returns no data.
///
/// Any payload is ignored, so a server that starts returning data does not cause an error.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Empty;

impl Serialize for Empty {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}

impl<'de> Deserialize<'de> for Empty {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        IgnoredAny::deserialize(deserializer)?;
        Ok(Self)
    }
}

/// Keeps an explicit `null` as `Some(None)`, which serde would otherwise decode as `None`.
fn double_option<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}
