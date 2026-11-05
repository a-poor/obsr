//! Tells apart the failures a caller usually wants to handle differently.
//!
//! ```text
//! cargo run --example errors                 # asks for a scene that doesn't exist
//! cargo run --example errors -- "Some scene" # switches to that scene if it exists
//! ```
//!
//! Set `OBS_URL` and `OBS_PASSWORD` to change where it connects.

use std::time::Duration;

use obsr::enums::{RequestStatus, WebSocketCloseCode};
use obsr::requests::SetCurrentProgramScene;
use obsr::{ConnectOptions, Error};
use tokio::time::timeout;

#[tokio::main]
async fn main() {
    let scene = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "No such scene".to_owned());
    let url = std::env::var("OBS_URL").unwrap_or_else(|_| "ws://localhost:4455".to_owned());
    let mut options = ConnectOptions::new(url);
    if let Ok(password) = std::env::var("OBS_PASSWORD") {
        options = options.password(password);
    }

    // The client has no built-in timeouts, and its futures can be cancelled safely.
    let Ok(result) = timeout(Duration::from_secs(5), switch_scene(options, &scene)).await else {
        println!("OBS did not answer in time");
        return;
    };

    match result {
        Ok(()) => println!("now showing {scene}"),
        // A failed request carries the server's status and comment.
        Err(Error::Request(error)) if error.status() == Some(RequestStatus::ResourceNotFound) => {
            println!("no scene named {scene}");
        }
        Err(Error::PasswordRequired) => println!("OBS wants a password: set OBS_PASSWORD"),
        // A closed connection carries the close code. A rejected password appears this way too.
        Err(Error::Closed(info))
            if info.close_code() == Some(WebSocketCloseCode::AuthenticationFailed) =>
        {
            println!("OBS rejected the password");
        }
        Err(error) => println!("failed: {error}"),
    }
}

async fn switch_scene(options: ConnectOptions, scene: &str) -> obsr::Result<()> {
    let client = options.connect().await?;
    client
        .request(SetCurrentProgramScene {
            scene_name: Some(scene.to_owned()),
            ..Default::default()
        })
        .await?;
    client.close().await;
    Ok(())
}
