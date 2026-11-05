//! Lists audio inputs, or toggles mute on one.
//!
//! ```text
//! cargo run --example mute               # list the audio inputs
//! cargo run --example mute -- "Mic/Aux"  # mute or unmute an input
//! ```
//!
//! Set `OBS_URL` and `OBS_PASSWORD` to change where it connects.

use obsr::requests::{GetInputList, GetInputMute, ToggleInputMute};
use obsr::{Client, ConnectOptions, Error, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = connect().await?;

    match std::env::args().nth(1) {
        None => list_audio_inputs(&client).await?,
        Some(name) => {
            let toggled = client
                .request(ToggleInputMute {
                    input_name: Some(name.clone()),
                    ..Default::default()
                })
                .await?;
            println!("{name}: {}", describe(toggled.input_muted));
        }
    }

    client.close().await;
    Ok(())
}

async fn list_audio_inputs(client: &Client) -> Result<()> {
    let list = client.request(GetInputList::default()).await?;
    for input in &list.inputs {
        let name = input["inputName"].as_str().unwrap_or_default();
        let request = GetInputMute {
            input_name: Some(name.to_owned()),
            ..Default::default()
        };
        match client.request(request).await {
            Ok(mute) => println!("{name}: {}", describe(mute.input_muted)),
            // Inputs without audio, such as a display capture, reject this request.
            Err(Error::Request(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn describe(muted: bool) -> &'static str {
    if muted { "muted" } else { "live" }
}

async fn connect() -> Result<Client> {
    let url = std::env::var("OBS_URL").unwrap_or_else(|_| "ws://localhost:4455".to_owned());
    let mut options = ConnectOptions::new(url);
    if let Ok(password) = std::env::var("OBS_PASSWORD") {
        options = options.password(password);
    }
    options.connect().await
}
