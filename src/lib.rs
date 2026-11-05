//! An async client for [obs-websocket](https://github.com/obsproject/obs-websocket) 5.x, the
//! remote control protocol of OBS Studio, built on Tokio.
//!
//! ```no_run
//! use obsr::requests::{GetSceneList, SetCurrentProgramScene};
//! use obsr::{ConnectOptions, Event};
//!
//! # #[tokio::main]
//! # async fn main() -> obsr::Result<()> {
//! let client = ConnectOptions::new("ws://localhost:4455")
//!     .password("secret")
//!     .connect()
//!     .await?;
//!
//! // Subscribe before acting, to see the events your own requests cause.
//! let mut events = client.events();
//!
//! let scenes = client.request(GetSceneList::default()).await?;
//! println!("{} scenes", scenes.scenes.len());
//!
//! client
//!     .request(SetCurrentProgramScene {
//!         scene_name: Some("Live".into()),
//!         ..Default::default()
//!     })
//!     .await?;
//!
//! while let Ok(event) = events.recv().await {
//!     if let Event::CurrentProgramSceneChanged(scene) = event {
//!         println!("now showing {}", scene.scene_name);
//!         break;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Where to look
//!
//! - [`Client`] sends [`requests`], [`RequestBatch`]es, and subscribes to [`events`].
//! - [`ConnectOptions`] sets the password, event subscriptions, and encoding.
//! - [`Error`] describes failures. A failed request is an [`Error::Request`], and a closed
//!   connection is an [`Error::Closed`].
//!
//! The types in [`requests`], [`events`], and [`enums`] are generated from the protocol's
//! `protocol.json`. Use [`Client::request_raw`] and [`Event::Unknown`] to work with anything newer
//! than this crate.

#![warn(missing_docs)]

mod auth;
mod batch;
mod client;
pub mod enums;
mod error;
pub mod events;
pub mod protocol;
pub mod requests;

pub use batch::{BatchHandle, BatchResponse, RequestBatch};
pub use client::{Client, ConnectOptions};
pub use enums::EventSubscription;
pub use error::{CloseInfo, Error, RequestError, Result};
pub use events::Event;
pub use protocol::Encoding;
pub use requests::Request;
pub use tokio_tungstenite::tungstenite;

/// Compiles the examples in the README.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
