# obsr

An async [obs-websocket](https://github.com/obsproject/obs-websocket) 5.x client for Rust, built on
Tokio and `tokio-tungstenite`.

- A typed struct for all 147 requests and 60 events in the protocol, generated from its
  `protocol.json`
- Request batches, with typed results
- Password authentication, event subscriptions, and JSON or MessagePack encoding
- Cheap, cloneable clients that can be shared across tasks

## Usage

```rust,no_run
use obsr::requests::{GetSceneList, GetVersion, SetCurrentProgramScene};
use obsr::{ConnectOptions, Event};

#[tokio::main]
async fn main() -> obsr::Result<()> {
    let client = ConnectOptions::new("ws://localhost:4455")
        .password("secret")
        .connect()
        .await?;

    // Subscribe before acting, to see the events your own requests cause.
    let mut events = client.events();

    let version = client.request(GetVersion).await?;
    println!("OBS {}", version.obs_version);

    let scenes = client.request(GetSceneList::default()).await?;
    println!("{} scenes", scenes.scenes.len());

    client
        .request(SetCurrentProgramScene {
            scene_name: Some("Live".into()),
            ..Default::default()
        })
        .await?;

    while let Ok(event) = events.recv().await {
        if let Event::CurrentProgramSceneChanged(scene) = event {
            println!("now showing {}", scene.scene_name);
            break;
        }
    }

    client.close().await;
    Ok(())
}
```

`examples/watch.rs` is a small program that prints what a running OBS is doing:

```sh
cargo run --example watch -- ws://localhost:4455 your-password
```

### Batches

```rust,no_run
use obsr::requests::{GetSceneList, GetVersion};
use obsr::{Client, RequestBatch};

async fn report(client: &Client) -> obsr::Result<()> {
    let mut batch = RequestBatch::new();
    let version = batch.push(GetVersion)?;
    let scenes = batch.push(GetSceneList::default())?;

    let response = client.batch(batch).await?;
    println!("{}", response.get(&version)?.obs_version);
    println!("{}", response.get(&scenes)?.scenes.len());
    Ok(())
}
```

### Errors

A failed request is an `Error::Request`, with the server's status and comment. A closed connection is
an `Error::Closed`, with the close code. A rejected password appears that way too:

```rust,no_run
use obsr::enums::{RequestStatus, WebSocketCloseCode};
use obsr::requests::SetCurrentProgramScene;
use obsr::{ConnectOptions, Error};

async fn switch_scene(url: &str, password: &str, scene: &str) -> obsr::Result<()> {
    let client = match ConnectOptions::new(url).password(password).connect().await {
        Err(Error::Closed(info))
            if info.close_code() == Some(WebSocketCloseCode::AuthenticationFailed) =>
        {
            eprintln!("wrong password");
            return Ok(());
        }
        other => other?,
    };

    let request = SetCurrentProgramScene {
        scene_name: Some(scene.to_owned()),
        ..Default::default()
    };
    match client.request(request).await {
        Err(Error::Request(error)) if error.status() == Some(RequestStatus::ResourceNotFound) => {
            eprintln!("no scene named {scene}");
            Ok(())
        }
        other => other.map(|_| ()),
    }
}
```

### Timeouts

The client has no built-in timeouts. All of its futures can be cancelled safely, so wrap any call in
`tokio::time::timeout`.

### Anything newer than this crate

`Client::request_raw` sends any request by name, and events this crate does not know arrive as
`Event::Unknown` with their payload intact. You can also implement the `Request` trait for your own
types.

## Where this differs from the spec

The generated types follow `spec/protocol.json`, with these corrections from the server source. Each
one was confirmed against a real OBS or by reading the server source, and `xtask/src/main.rs` records
them.

| Spec | Reality | Here |
| --- | --- | --- |
| `Number` | Integer or float, depending on the field | `i64` or `f64` |
| `CustomEvent` has an `eventData` field | The custom object is the event's data | `event_data` holds that object |
| `GetSpecialInputs` fields are strings | `null` for a channel with no device | `Option<String>` |
| `GetOutputStatus.outputCongestion` is a number | Can be NaN, which is sent as `null` | `Option<f64>` |
| `outputState`, `mediaAction` are strings | They hold a spec enum | `ObsOutputState`, `ObsMediaInputAction` |

Fields documented as "Specify `null` to remove" become `Option<Option<T>>`: `None` leaves the value
unchanged and `Some(None)` removes it.

`ToggleRecordPause` also returns an undocumented `outputPaused`. It is not in the spec, so the typed
request ignores it and `request_raw` can read it.

## Development

The code in `src/*/generated.rs` and `tests/conformance/cases.rs` is generated. To update the
protocol, replace `spec/protocol.json` and regenerate:

```sh
curl -o spec/protocol.json https://raw.githubusercontent.com/obsproject/obs-websocket/master/docs/generated/protocol.json
cargo xtask
```

A new `Number` field fails generation until it is classified as an integer or a float in
`xtask/src/main.rs`.

### Tests

```sh
cargo test --workspace
```

- `tests/conformance` checks the generated types against the spec, including every field name and
  which fields are required.
- `tests/client.rs` runs the client end to end against a scripted server.
- `xtask` fails if the generated code is stale.

`tests/live.rs` runs against a real OBS and is ignored by default. **It creates and removes scenes,
inputs, profiles, and recordings, so use a throwaway OBS.**

```sh
OBSR_TEST_URL=ws://localhost:4455 OBSR_TEST_PASSWORD=secret OBSR_TEST_MEDIA_FILE=/path/to/clip.mp4 \
    cargo test --test live -- --ignored --nocapture --test-threads=1
```

`OBSR_TEST_PASSWORD` and `OBSR_TEST_MEDIA_FILE` are optional.
