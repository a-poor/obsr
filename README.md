# obsr

[![Crates.io](https://img.shields.io/crates/v/obsr.svg)](https://crates.io/crates/obsr)
[![Docs.rs](https://img.shields.io/docsrs/obsr)](https://docs.rs/obsr)
[![CI](https://github.com/a-poor/obsr/actions/workflows/ci.yml/badge.svg)](https://github.com/a-poor/obsr/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

An async [obs-websocket](https://github.com/obsproject/obs-websocket) 5.x client for Rust, built on
Tokio and `tokio-tungstenite`.

- A typed struct for all 147 requests and 60 events in the protocol, generated from its
  `protocol.json`
- Request batches, with typed results
- Password authentication, event subscriptions, and JSON or MessagePack encoding
- Cheap, cloneable clients that can be shared across tasks

## Installation

```sh
cargo add obsr
cargo add tokio --features macros,rt-multi-thread
```

Turn on the server in OBS under **Tools > WebSocket Server Settings**.

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

The client has no built-in timeouts. All of its futures can be cancelled safely, so wrap any call in
`tokio::time::timeout`.

## Examples

| Example | What it does |
| --- | --- |
| [`scenes`](examples/scenes.rs) | Lists scenes, creates one, or switches to one |
| [`mute`](examples/mute.rs) | Lists audio inputs, or toggles mute on one |
| [`batch`](examples/batch.rs) | Sends several requests in one message, with typed results |
| [`errors`](examples/errors.rs) | Handles a wrong password, a missing scene, and a timeout |
| [`watch`](examples/watch.rs) | Prints what a running OBS is doing |

They connect to `ws://localhost:4455` without a password unless you set `OBS_URL` and `OBS_PASSWORD`:

```sh
OBS_PASSWORD=your-password cargo run --example scenes -- switch BRB
```

## Compatibility

The types are generated from the spec for obs-websocket 5.7.6 (RPC version 1), which has been tested
against OBS 33.0.1. The older versions below come from comparing the spec at each release, and have
not been tested directly.

| obs-websocket | What to expect |
| --- | --- |
| 5.6.0 and newer | All typed responses and events decode |
| 5.4.0 to 5.5.x | The same, except `InputCreated` events arrive as `Event::Unknown` |
| 5.0.0 to 5.3.x | Connects, but common responses like `GetCurrentProgramScene` return a decode error and many events arrive as `Event::Unknown` |
| 4.x | Not supported |

A request the server doesn't have yet returns an `Error::Request`.

For a server newer than this crate, `Client::request_raw` sends any request by name, and events this
crate doesn't know arrive as `Event::Unknown` with their payload intact. You can also implement the
`Request` trait for your own types.

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

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE.txt) or [MIT](LICENSE-MIT.txt), at your option.
