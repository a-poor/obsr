//! Lists scenes, creates a scene, or switches to one.
//!
//! ```text
//! cargo run --example scenes                # list the scenes
//! cargo run --example scenes -- create BRB  # add a scene
//! cargo run --example scenes -- switch BRB  # put a scene on air
//! ```
//!
//! Set `OBS_URL` and `OBS_PASSWORD` to change where it connects.

use obsr::requests::{CreateScene, GetSceneList, SetCurrentProgramScene};
use obsr::{Client, ConnectOptions, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = connect().await?;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        [] => list_scenes(&client).await?,
        ["create", name] => {
            let created = client
                .request(CreateScene {
                    scene_name: (*name).to_owned(),
                    ..Default::default()
                })
                .await?;
            println!("created {name} ({})", created.scene_uuid);
        }
        ["switch", name] => {
            client
                .request(SetCurrentProgramScene {
                    scene_name: Some((*name).to_owned()),
                    ..Default::default()
                })
                .await?;
            println!("now showing {name}");
        }
        _ => eprintln!("usage: scenes [create <name> | switch <name>]"),
    }

    client.close().await;
    Ok(())
}

/// Prints each scene, marking the one on air.
async fn list_scenes(client: &Client) -> Result<()> {
    let list = client.request(GetSceneList::default()).await?;
    for scene in &list.scenes {
        // Fields the protocol leaves untyped, like these scenes, are `serde_json::Value`.
        let name = scene["sceneName"].as_str().unwrap_or_default();
        let on_air = list.current_program_scene_name.as_deref() == Some(name);
        println!("{} {name}", if on_air { "*" } else { " " });
    }
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
