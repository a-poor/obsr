//! Sends several requests in one message and reads each typed result. It only reads from OBS.
//!
//! ```text
//! cargo run --example batch
//! ```
//!
//! Set `OBS_URL` and `OBS_PASSWORD` to change where it connects.

use obsr::requests::{GetSceneItemList, GetSceneList, GetStats, GetVersion};
use obsr::{Client, ConnectOptions, RequestBatch, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = connect().await?;

    // Each `push` returns a handle that reads that request's result later.
    let mut batch = RequestBatch::new();
    let version = batch.push(GetVersion)?;
    let scenes = batch.push(GetSceneList::default())?;
    let missing = batch.push(GetSceneItemList {
        scene_name: Some("No such scene".to_owned()),
        ..Default::default()
    })?;
    let stats = batch.push(GetStats)?;

    let response = client.batch(batch).await?;

    println!("OBS {}", response.get(&version)?.obs_version);
    println!("{} scenes", response.get(&scenes)?.scenes.len());

    // A failed request only fails its own result. The requests after it still ran, unless the
    // batch set `halt_on_failure`.
    if let Err(error) = response.get(&missing) {
        println!("expected failure: {error}");
    }
    println!("{:.1} fps", response.get(&stats)?.active_fps);

    client.close().await;
    Ok(())
}

async fn connect() -> Result<Client> {
    let url = std::env::var("OBS_URL").unwrap_or_else(|_| "ws://localhost:4455".to_owned());
    let mut options = ConnectOptions::new(url);
    if let Ok(password) = std::env::var("OBS_PASSWORD") {
        options = options.password(password);
    }
    options.connect().await
}
