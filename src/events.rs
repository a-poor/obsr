//! Typed events, one for each event in the protocol spec.
//!
//! Receive them from [`Client::events`](crate::Client::events).

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::enums::EventSubscription;

mod generated;
pub use generated::*;

/// The `Event` (OpCode 5) message as sent on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawEvent {
    /// The spec name of the event.
    pub event_type: String,
    /// The subscription required to receive the event.
    pub event_intent: EventSubscription,
    /// The event's data, if it has any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_data: Option<Value>,
}

/// Decodes the data of `raw`, or returns `None` if it does not match `T`.
fn parse_data<T: DeserializeOwned>(raw: &RawEvent) -> Option<T> {
    T::deserialize(raw.event_data.as_ref().unwrap_or(&Value::Null)).ok()
}
