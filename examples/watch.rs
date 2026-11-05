//! Prints information about an OBS instance, then prints events until OBS exits or Ctrl-C.
//!
//! ```text
//! cargo run --example watch
//! ```
//!
//! Set `OBS_URL` and `OBS_PASSWORD` to change where it connects.

use obsr::requests::{GetSceneList, GetVersion};
use obsr::{ConnectOptions, Event, Result};
use tokio::sync::broadcast::error::RecvError;

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("OBS_URL").unwrap_or_else(|_| "ws://localhost:4455".to_owned());
    let mut options = ConnectOptions::new(url);
    if let Ok(password) = std::env::var("OBS_PASSWORD") {
        options = options.password(password);
    }

    let client = options.connect().await?;
    let mut events = client.events();

    let version = client.request(GetVersion).await?;
    println!(
        "OBS {} on {}",
        version.obs_version, version.platform_description
    );
    println!("{} requests available", version.available_requests.len());

    let scenes = client.request(GetSceneList::default()).await?;
    println!("{} scenes", scenes.scenes.len());
    if let Some(current) = scenes.current_program_scene_name {
        println!("now showing {current}");
    }

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            event = events.recv() => match event {
                Ok(Event::CurrentProgramSceneChanged(scene)) => {
                    println!("now showing {}", scene.scene_name);
                }
                Ok(event) => println!("{event:?}"),
                Err(RecvError::Lagged(missed)) => eprintln!("missed {missed} events"),
                Err(RecvError::Closed) => break,
            },
        }
    }

    client.close().await;
    println!("closed: {}", client.closed().await);
    Ok(())
}
