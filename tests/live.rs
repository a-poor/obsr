//! Tests against a real OBS instance. They are ignored by default.
//!
//! **These tests create, change, and remove scenes, inputs, profiles, and recordings, so point
//! them at a throwaway OBS, not one you care about.**
//!
//! ```text
//! OBSR_TEST_URL=ws://localhost:4455 OBSR_TEST_PASSWORD=secret \
//!     cargo test --test live -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Set `OBSR_TEST_MEDIA_FILE` to a short video to also exercise media playback, and enable the
//! replay buffer in the OBS profile to exercise it too. Requests that open windows or start the
//! virtual camera are not sent.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use obsr::enums::{
    ObsMediaInputAction, ObsOutputState, RequestBatchExecutionType, RequestStatus,
    WebSocketCloseCode,
};
use obsr::requests::*;
use obsr::{Client, ConnectOptions, Encoding, Error, Event, EventSubscription, RequestBatch};
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio::time::timeout;

const STEP_TIMEOUT: Duration = Duration::from_secs(20);

/// Requests the server source marks as "Intentionally undocumented", so they are not in the spec.
const UNDOCUMENTED_REQUESTS: &[&str] = &[
    "GetSceneItemPrivateSettings",
    "GetSourcePrivateSettings",
    "SetSceneItemPrivateSettings",
    "SetSourcePrivateSettings",
];

fn url() -> String {
    std::env::var("OBSR_TEST_URL").expect("set OBSR_TEST_URL to the address of a throwaway OBS")
}

fn options() -> ConnectOptions {
    let options = ConnectOptions::new(url());
    match std::env::var("OBSR_TEST_PASSWORD") {
        Ok(password) => options.password(password),
        Err(_) => options,
    }
}

fn spec_requests() -> BTreeSet<String> {
    let spec: Value = serde_json::from_str(include_str!("../spec/protocol.json")).unwrap();
    spec["requests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|request| request["requestType"].as_str().unwrap().to_owned())
        .collect()
}

/// Sends requests and records which ones were answered successfully.
struct Sweep {
    client: Client,
    succeeded: BTreeSet<&'static str>,
    failed: BTreeSet<String>,
}

impl Sweep {
    /// Sends a request that must succeed.
    async fn ok<R: Request>(&mut self, request: R) -> R::Response {
        let result = timeout(STEP_TIMEOUT, self.client.request(request))
            .await
            .unwrap_or_else(|_| panic!("{} timed out", R::REQUEST_TYPE));
        let response = result.unwrap_or_else(|error| panic!("{}: {error}", R::REQUEST_TYPE));
        self.succeeded.insert(R::REQUEST_TYPE);
        response
    }

    /// Sends a request that may fail depending on the state of OBS. A failure reported by the
    /// server is accepted, but a response that does not decode is not.
    async fn allow_failure<R: Request>(&mut self, request: R) -> Option<R::Response> {
        let result = timeout(STEP_TIMEOUT, self.client.request(request))
            .await
            .unwrap_or_else(|_| panic!("{} timed out", R::REQUEST_TYPE));
        match result {
            Ok(response) => {
                self.succeeded.insert(R::REQUEST_TYPE);
                Some(response)
            }
            Err(Error::Request(error)) => {
                self.failed.insert(error.to_string());
                None
            }
            Err(error) => panic!("{}: {error}", R::REQUEST_TYPE),
        }
    }
}

/// Waits for the first event accepted by `select`.
async fn wait_for<T>(
    events: &mut broadcast::Receiver<Event>,
    description: &str,
    mut select: impl FnMut(&Event) -> Option<T>,
) -> T {
    let wait = async {
        loop {
            match events.recv().await {
                Ok(event) => {
                    if let Some(found) = select(&event) {
                        return found;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => {
                    panic!("connection closed while waiting for {description}")
                }
            }
        }
    };
    timeout(STEP_TIMEOUT, wait)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {description}"))
}

#[tokio::test]
#[ignore = "needs a throwaway OBS: set OBSR_TEST_URL"]
async fn handshake_reports_the_server_and_rejects_a_wrong_password() {
    let client = options().connect().await.unwrap();
    let info = client.server_info();
    println!(
        "OBS {} with obs-websocket {} (RPC {})",
        info.obs_studio_version, info.obs_web_socket_version, info.rpc_version
    );
    assert_eq!(client.negotiated_rpc_version(), 1);
    let requires_password = info.authentication.is_some();
    client.close().await;

    if requires_password {
        let error = Client::connect(url()).await.unwrap_err();
        assert!(matches!(error, Error::PasswordRequired), "{error:?}");

        let error = ConnectOptions::new(url())
            .password("definitely wrong")
            .connect()
            .await
            .unwrap_err();
        let Error::Closed(info) = error else {
            panic!("unexpected error: {error:?}");
        };
        assert_eq!(
            info.close_code(),
            Some(WebSocketCloseCode::AuthenticationFailed)
        );
    } else {
        println!("the server does not require a password, so that path was not exercised");
    }
}

#[tokio::test]
#[ignore = "needs a throwaway OBS: set OBSR_TEST_URL"]
async fn the_server_offers_no_request_missing_from_the_spec() {
    let client = options().connect().await.unwrap();
    let version = client.request(GetVersion).await.unwrap();
    let offered: BTreeSet<String> = version.available_requests.into_iter().collect();
    let known = spec_requests();
    let unknown: Vec<_> = offered
        .difference(&known)
        .filter(|name| !UNDOCUMENTED_REQUESTS.contains(&name.as_str()))
        .collect();
    let not_offered: Vec<_> = known.difference(&offered).collect();
    println!("the server offers {} requests", offered.len());
    println!("in the spec but not on this server: {not_offered:?}");
    assert!(unknown.is_empty(), "not in the vendored spec: {unknown:?}");

    // They have no generated types, but `request_raw` can still reach them.
    let error = client
        .request_raw(
            "GetSourcePrivateSettings",
            Some(json!({"sourceName": "obsr no such source"})),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::Request(ref error) if error.status() == Some(RequestStatus::ResourceNotFound)
        ),
        "{error:?}"
    );
    client.close().await;
}

#[tokio::test]
#[ignore = "needs a throwaway OBS: set OBSR_TEST_URL"]
async fn sweep_over_json() {
    sweep(Encoding::Json).await;
}

#[tokio::test]
#[ignore = "needs a throwaway OBS: set OBSR_TEST_URL"]
async fn sweep_over_msgpack() {
    sweep(Encoding::MsgPack).await;
}

/// Exercises as many requests and events as a local OBS allows. The main thing being tested is
/// that real responses and events decode into the generated types.
async fn sweep(encoding: Encoding) {
    let client = options()
        .encoding(encoding)
        .event_capacity(16_384)
        .connect()
        .await
        .unwrap();
    // A separate receiver, drained at the end, checks that every event decoded.
    let mut all_events = client.events();
    let mut events = client.events();
    let mut sweep = Sweep {
        client: client.clone(),
        succeeded: BTreeSet::new(),
        failed: BTreeSet::new(),
    };

    timeout(
        STEP_TIMEOUT,
        client.reidentify(
            EventSubscription::ALL
                | EventSubscription::INPUT_VOLUME_METERS
                | EventSubscription::INPUT_ACTIVE_STATE_CHANGED
                | EventSubscription::INPUT_SHOW_STATE_CHANGED
                | EventSubscription::SCENE_ITEM_TRANSFORM_CHANGED,
        ),
    )
    .await
    .expect("reidentify timed out")
    .unwrap();

    // Unique names let a run succeed after an earlier one failed partway through.
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let suffix = format!("{encoding:?} {run}");
    let scene = format!("obsr scene {suffix}");
    let renamed_scene = format!("obsr renamed scene {suffix}");
    let other_scene = format!("obsr other scene {suffix}");
    let color = format!("obsr color {suffix}");
    let media = format!("obsr media {suffix}");
    let profile = format!("obsr profile {suffix}");
    let collection = format!("obsr collection {suffix}");
    let filter = "obsr filter";

    // An earlier failed run may have left a recording going, which blocks some settings changes.
    if sweep.ok(GetRecordStatus).await.output_active {
        sweep.ok(StopRecord).await;
        wait_for_record_state(&mut events, ObsOutputState::Stopped).await;
    }

    general(&mut sweep, &client, &mut events).await;
    config(&mut sweep, &profile, &mut events).await;
    let original_scene = sweep.ok(GetCurrentProgramScene).await.scene_name;
    sweep.ok(GetSceneList::default()).await;
    sweep.ok(GetGroupList).await;

    // Scenes.
    let created = sweep
        .ok(CreateScene {
            scene_name: scene.clone(),
            ..Default::default()
        })
        .await;
    assert!(!created.scene_uuid.is_empty());
    wait_for(&mut events, "SceneCreated", |event| match event {
        Event::SceneCreated(data) if data.scene_name == scene => Some(()),
        _ => None,
    })
    .await;
    sweep
        .ok(CreateScene {
            scene_name: other_scene.clone(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetSceneName {
            scene_name: Some(scene.clone()),
            new_scene_name: renamed_scene.clone(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetSceneName {
            scene_name: Some(renamed_scene.clone()),
            new_scene_name: scene.clone(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetCurrentProgramScene {
            scene_name: Some(scene.clone()),
            ..Default::default()
        })
        .await;
    wait_for(
        &mut events,
        "CurrentProgramSceneChanged",
        |event| match event {
            Event::CurrentProgramSceneChanged(data) if data.scene_name == scene => Some(()),
            _ => None,
        },
    )
    .await;

    transitions(&mut sweep, &scene).await;
    studio_mode(&mut sweep, &scene, &other_scene, &mut events).await;

    // A color source for scene items, filters, and screenshots.
    let input = sweep
        .ok(CreateInput {
            scene_name: Some(scene.clone()),
            input_name: color.clone(),
            input_kind: "color_source_v3".to_owned(),
            input_settings: Some(json!({"width": 64, "height": 64})),
            scene_item_enabled: Some(true),
            ..Default::default()
        })
        .await;
    assert!(input.scene_item_id > 0);
    scene_items(
        &mut sweep,
        &scene,
        &other_scene,
        &color,
        input.scene_item_id,
    )
    .await;
    filters(&mut sweep, &color, filter).await;
    sources(&mut sweep, &color).await;

    // A media source for the audio and media requests.
    let created = sweep
        .ok(CreateInput {
            scene_name: Some(scene.clone()),
            input_name: media.clone(),
            input_kind: "ffmpeg_source".to_owned(),
            ..Default::default()
        })
        .await;
    assert!(!created.input_uuid.is_empty());
    inputs(&mut sweep, &media, &mut events).await;
    media_playback(&mut sweep, &media, &mut events).await;

    batches(&sweep.client.clone(), &scene).await;
    outputs(&mut sweep, &mut events).await;
    collections(&mut sweep, &collection, &mut events).await;

    // Clean up.
    sweep
        .ok(SetCurrentProgramScene {
            scene_name: Some(original_scene),
            ..Default::default()
        })
        .await;
    for input_name in [&color, &media] {
        sweep
            .ok(RemoveInput {
                input_name: Some(input_name.clone()),
                ..Default::default()
            })
            .await;
    }
    for scene_name in [&scene, &other_scene] {
        sweep
            .ok(RemoveScene {
                scene_name: Some(scene_name.clone()),
                ..Default::default()
            })
            .await;
    }
    // Let the last events arrive.
    sweep.ok(GetVersion).await;

    // Every event must have decoded into its type.
    let mut observed = BTreeSet::new();
    let mut unknown = Vec::new();
    loop {
        match all_events.try_recv() {
            Ok(Event::Unknown(raw)) => unknown.push(raw),
            Ok(event) => {
                observed.insert(event.event_type().to_owned());
            }
            Err(broadcast::error::TryRecvError::Lagged(missed)) => {
                panic!("missed {missed} events, so raise event_capacity")
            }
            Err(_) => break,
        }
    }

    let spec = spec_requests();
    let not_covered: Vec<_> = spec
        .iter()
        .filter(|name| !sweep.succeeded.contains(name.as_str()))
        .collect();
    println!("--- {encoding:?} ---");
    println!(
        "{}/{} request types succeeded",
        sweep.succeeded.len(),
        spec.len()
    );
    println!("not exercised successfully: {not_covered:?}");
    println!(
        "failures reported by the server (allowed): {:#?}",
        sweep.failed
    );
    println!("{} event types seen: {observed:?}", observed.len());
    assert!(
        unknown.is_empty(),
        "events that did not decode: {unknown:#?}"
    );

    client.close().await;
    assert_eq!(client.closed().await.code, Some(1000));
}

async fn general(sweep: &mut Sweep, client: &Client, events: &mut broadcast::Receiver<Event>) {
    let version = sweep.ok(GetVersion).await;
    assert_eq!(version.rpc_version, 1);
    assert!(!version.available_requests.is_empty());

    let stats = sweep.ok(GetStats).await;
    assert!(stats.active_fps >= 0.0);
    assert!(stats.web_socket_session_outgoing_messages > 0);

    let hotkeys = sweep.ok(GetHotkeyList).await;
    assert!(!hotkeys.hotkeys.is_empty());
    sweep
        .allow_failure(TriggerHotkeyByName {
            hotkey_name: "obsr.no.such.hotkey".to_owned(),
            context_name: None,
        })
        .await;
    sweep
        .ok(TriggerHotkeyByKeySequence {
            key_id: Some("OBS_KEY_F13".to_owned()),
            key_modifiers: Some(KeyModifiers {
                shift: Some(true),
                ..Default::default()
            }),
        })
        .await;
    sweep
        .allow_failure(CallVendorRequest {
            vendor_name: "obsr-no-such-vendor".to_owned(),
            request_type: "Nothing".to_owned(),
            request_data: None,
        })
        .await;
    sweep.allow_failure(GetCanvasList).await;

    sweep
        .ok(BroadcastCustomEvent {
            event_data: json!({"from": "obsr"}),
        })
        .await;
    let custom = wait_for(events, "CustomEvent", |event| match event {
        Event::CustomEvent(data) => Some(data.event_data.clone()),
        _ => None,
    })
    .await;
    assert_eq!(custom, json!({"from": "obsr"}));

    // A failure surfaces with its status, and the connection stays usable.
    let error = client
        .request(SetCurrentProgramScene {
            scene_name: Some("obsr no such scene".to_owned()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    let Error::Request(error) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(error.status(), Some(RequestStatus::ResourceNotFound));
    assert!(error.comment.is_some());
}

async fn config(sweep: &mut Sweep, profile: &str, events: &mut broadcast::Receiver<Event>) {
    let realm = "OBS_WEBSOCKET_DATA_REALM_GLOBAL";
    sweep
        .ok(SetPersistentData {
            realm: realm.to_owned(),
            slot_name: "obsr".to_owned(),
            slot_value: json!({"nested": [1, 2]}),
        })
        .await;
    let stored = sweep
        .ok(GetPersistentData {
            realm: realm.to_owned(),
            slot_name: "obsr".to_owned(),
        })
        .await;
    assert_eq!(stored.slot_value, Some(json!({"nested": [1, 2]})));
    let unset = sweep
        .ok(GetPersistentData {
            realm: realm.to_owned(),
            slot_name: "obsr-unset".to_owned(),
        })
        .await;
    assert_eq!(unset.slot_value, None);

    let list = sweep.ok(GetProfileList).await;
    assert!(list.profiles.contains(&list.current_profile_name));
    let original_profile = list.current_profile_name;

    // OBS applies profile changes on its UI thread, after the request has already returned. Only
    // the events say when a change has taken effect.
    sweep
        .ok(CreateProfile {
            profile_name: profile.to_owned(),
        })
        .await;
    wait_for(
        events,
        "CurrentProfileChanged to the new profile",
        |event| match event {
            Event::CurrentProfileChanged(data) if data.profile_name == profile => Some(()),
            _ => None,
        },
    )
    .await;
    sweep
        .ok(SetCurrentProfile {
            profile_name: original_profile.clone(),
        })
        .await;
    wait_for(events, "CurrentProfileChanged back", |event| match event {
        Event::CurrentProfileChanged(data) if data.profile_name == original_profile => Some(()),
        _ => None,
    })
    .await;
    sweep
        .ok(RemoveProfile {
            profile_name: profile.to_owned(),
        })
        .await;
    wait_for(
        events,
        "ProfileListChanged without the profile",
        |event| match event {
            Event::ProfileListChanged(data) if !data.profiles.iter().any(|p| p == profile) => {
                Some(())
            }
            _ => None,
        },
    )
    .await;

    let name = |value| SetProfileParameter {
        parameter_category: "Obsr".to_owned(),
        parameter_name: "Test".to_owned(),
        parameter_value: value,
    };
    let get = || GetProfileParameter {
        parameter_category: "Obsr".to_owned(),
        parameter_name: "Test".to_owned(),
    };
    sweep.ok(name(Some("value".to_owned()))).await;
    assert_eq!(
        sweep.ok(get()).await.parameter_value.as_deref(),
        Some("value")
    );
    sweep.ok(name(None)).await;
    assert_eq!(sweep.ok(get()).await.parameter_value, None);

    let video = sweep.ok(GetVideoSettings).await;
    assert!(video.base_width > 0 && video.fps_numerator > 0);
    sweep
        .ok(SetVideoSettings {
            fps_numerator: Some(video.fps_numerator),
            fps_denominator: Some(video.fps_denominator),
            base_width: Some(video.base_width),
            base_height: Some(video.base_height),
            output_width: Some(video.output_width),
            output_height: Some(video.output_height),
        })
        .await;

    sweep.ok(GetStreamServiceSettings).await;
    sweep
        .ok(SetStreamServiceSettings {
            stream_service_type: "rtmp_custom".to_owned(),
            stream_service_settings: json!({"server": "rtmp://127.0.0.1:1/live", "key": "obsr"}),
        })
        .await;

    sweep.ok(GetRecordDirectory).await;
    sweep
        .ok(SetRecordDirectory {
            record_directory: scratch_dir().display().to_string(),
        })
        .await;

    sweep.ok(GetInputKindList::default()).await;
    sweep.ok(GetSpecialInputs).await;
    sweep.ok(GetSourceFilterKindList).await;
    sweep.ok(GetTransitionKindList).await;
    sweep.ok(GetMonitorList).await;
}

fn scratch_dir() -> PathBuf {
    let directory = std::env::temp_dir().join("obsr-live-test");
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

async fn transitions(sweep: &mut Sweep, scene: &str) {
    let list = sweep.ok(GetSceneTransitionList).await;
    assert!(!list.transitions.is_empty());
    let current = sweep.ok(GetCurrentSceneTransition).await;
    sweep
        .ok(SetCurrentSceneTransition {
            transition_name: current.transition_name.clone(),
        })
        .await;
    sweep
        .allow_failure(SetCurrentSceneTransitionDuration {
            transition_duration: 400,
        })
        .await;
    sweep
        .allow_failure(SetCurrentSceneTransitionSettings {
            transition_settings: json!({}),
            overlay: None,
        })
        .await;
    let cursor = sweep.ok(GetCurrentSceneTransitionCursor).await;
    assert!((0.0..=1.0).contains(&cursor.transition_cursor));

    // `None` leaves an override alone, and `Some(None)` removes it.
    let set = SetSceneSceneTransitionOverride {
        scene_name: Some(scene.to_owned()),
        transition_name: Some(Some(current.transition_name.clone())),
        transition_duration: Some(Some(321)),
        ..Default::default()
    };
    sweep.ok(set).await;
    let get = || GetSceneSceneTransitionOverride {
        scene_name: Some(scene.to_owned()),
        ..Default::default()
    };
    let overridden = sweep.ok(get()).await;
    assert_eq!(overridden.transition_name, Some(current.transition_name));
    assert_eq!(overridden.transition_duration, Some(321));
    sweep
        .ok(SetSceneSceneTransitionOverride {
            scene_name: Some(scene.to_owned()),
            transition_name: Some(None),
            transition_duration: Some(None),
            ..Default::default()
        })
        .await;
    let cleared = sweep.ok(get()).await;
    assert_eq!(cleared.transition_name, None);
    assert_eq!(cleared.transition_duration, None);
}

/// Turns studio mode on or off. OBS applies this on its UI thread, so the event says when it is done.
async fn set_studio_mode(
    sweep: &mut Sweep,
    enabled: bool,
    events: &mut broadcast::Receiver<Event>,
) {
    sweep
        .ok(SetStudioModeEnabled {
            studio_mode_enabled: enabled,
        })
        .await;
    wait_for(events, "StudioModeStateChanged", |event| match event {
        Event::StudioModeStateChanged(data) if data.studio_mode_enabled == enabled => Some(()),
        _ => None,
    })
    .await;
}

async fn studio_mode(
    sweep: &mut Sweep,
    scene: &str,
    other_scene: &str,
    events: &mut broadcast::Receiver<Event>,
) {
    // An earlier failed run may have left it on.
    if sweep.ok(GetStudioModeEnabled).await.studio_mode_enabled {
        set_studio_mode(sweep, false, events).await;
    }
    let error = sweep
        .client
        .request(GetCurrentPreviewScene)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Request(ref error) if error.status() == Some(RequestStatus::StudioModeNotActive)
    ));

    set_studio_mode(sweep, true, events).await;
    sweep
        .ok(SetCurrentPreviewScene {
            scene_name: Some(other_scene.to_owned()),
            ..Default::default()
        })
        .await;
    wait_for(events, "CurrentPreviewSceneChanged", |event| match event {
        Event::CurrentPreviewSceneChanged(data) if data.scene_name == other_scene => Some(()),
        _ => None,
    })
    .await;
    assert_eq!(
        sweep.ok(GetCurrentPreviewScene).await.scene_name,
        other_scene
    );
    let list = sweep.ok(GetSceneList::default()).await;
    assert_eq!(
        list.current_preview_scene_name.as_deref(),
        Some(other_scene)
    );
    sweep
        .allow_failure(SetTBarPosition {
            position: 0.25,
            release: Some(false),
        })
        .await;
    sweep
        .allow_failure(SetTBarPosition {
            position: 1.0,
            release: Some(true),
        })
        .await;
    sweep.allow_failure(TriggerStudioModeTransition).await;

    set_studio_mode(sweep, false, events).await;
    sweep
        .ok(SetCurrentProgramScene {
            scene_name: Some(scene.to_owned()),
            ..Default::default()
        })
        .await;
    let list = sweep.ok(GetSceneList::default()).await;
    assert_eq!(list.current_preview_scene_name, None);
}

async fn scene_items(sweep: &mut Sweep, scene: &str, other_scene: &str, source: &str, id: i64) {
    let in_scene = || Some(scene.to_owned());

    let list = sweep
        .ok(GetSceneItemList {
            scene_name: in_scene(),
            ..Default::default()
        })
        .await;
    assert_eq!(list.scene_items.len(), 1);
    sweep
        .allow_failure(GetGroupSceneItemList {
            scene_name: in_scene(),
            ..Default::default()
        })
        .await;
    let found = sweep
        .ok(GetSceneItemId {
            scene_name: in_scene(),
            source_name: source.to_owned(),
            search_offset: Some(0),
            ..Default::default()
        })
        .await;
    assert_eq!(found.scene_item_id, id);
    let source_info = sweep
        .ok(GetSceneItemSource {
            scene_name: in_scene(),
            scene_item_id: id,
            ..Default::default()
        })
        .await;
    assert_eq!(source_info.source_name, source);

    let second = sweep
        .ok(CreateSceneItem {
            scene_name: Some(other_scene.to_owned()),
            source_name: Some(source.to_owned()),
            scene_item_enabled: Some(false),
            ..Default::default()
        })
        .await;
    let duplicate = sweep
        .ok(DuplicateSceneItem {
            scene_name: Some(other_scene.to_owned()),
            scene_item_id: second.scene_item_id,
            destination_scene_name: in_scene(),
            ..Default::default()
        })
        .await;

    let transform = sweep
        .ok(GetSceneItemTransform {
            scene_name: in_scene(),
            scene_item_id: id,
            ..Default::default()
        })
        .await;
    assert!(transform.scene_item_transform.is_object());
    sweep
        .ok(SetSceneItemTransform {
            scene_name: in_scene(),
            scene_item_id: id,
            scene_item_transform: json!({"positionX": 10.5, "positionY": 2.0}),
            ..Default::default()
        })
        .await;
    let transform = sweep
        .ok(GetSceneItemTransform {
            scene_name: in_scene(),
            scene_item_id: id,
            ..Default::default()
        })
        .await;
    assert_eq!(transform.scene_item_transform["positionX"], json!(10.5));

    sweep
        .ok(SetSceneItemEnabled {
            scene_name: in_scene(),
            scene_item_id: id,
            scene_item_enabled: false,
            ..Default::default()
        })
        .await;
    let enabled = sweep
        .ok(GetSceneItemEnabled {
            scene_name: in_scene(),
            scene_item_id: id,
            ..Default::default()
        })
        .await;
    assert!(!enabled.scene_item_enabled);
    sweep
        .ok(SetSceneItemEnabled {
            scene_name: in_scene(),
            scene_item_id: id,
            scene_item_enabled: true,
            ..Default::default()
        })
        .await;

    sweep
        .ok(SetSceneItemLocked {
            scene_name: in_scene(),
            scene_item_id: id,
            scene_item_locked: true,
            ..Default::default()
        })
        .await;
    let locked = sweep
        .ok(GetSceneItemLocked {
            scene_name: in_scene(),
            scene_item_id: id,
            ..Default::default()
        })
        .await;
    assert!(locked.scene_item_locked);
    sweep
        .ok(SetSceneItemLocked {
            scene_name: in_scene(),
            scene_item_id: id,
            scene_item_locked: false,
            ..Default::default()
        })
        .await;

    sweep
        .ok(SetSceneItemIndex {
            scene_name: in_scene(),
            scene_item_id: id,
            scene_item_index: 1,
            ..Default::default()
        })
        .await;
    let index = sweep
        .ok(GetSceneItemIndex {
            scene_name: in_scene(),
            scene_item_id: id,
            ..Default::default()
        })
        .await;
    assert_eq!(index.scene_item_index, 1);

    sweep
        .ok(SetSceneItemBlendMode {
            scene_name: in_scene(),
            scene_item_id: id,
            scene_item_blend_mode: "OBS_BLEND_ADDITIVE".to_owned(),
            ..Default::default()
        })
        .await;
    let blend = sweep
        .ok(GetSceneItemBlendMode {
            scene_name: in_scene(),
            scene_item_id: id,
            ..Default::default()
        })
        .await;
    assert_eq!(blend.scene_item_blend_mode, "OBS_BLEND_ADDITIVE");

    sweep
        .ok(RemoveSceneItem {
            scene_name: in_scene(),
            scene_item_id: duplicate.scene_item_id,
            ..Default::default()
        })
        .await;
}

async fn filters(sweep: &mut Sweep, source: &str, filter: &str) {
    let kinds = sweep.ok(GetSourceFilterKindList).await.source_filter_kinds;
    let kind = kinds
        .iter()
        .find(|kind| kind.starts_with("color_filter"))
        .cloned()
        .unwrap_or_else(|| kinds[0].clone());
    sweep
        .ok(GetSourceFilterDefaultSettings {
            filter_kind: kind.clone(),
        })
        .await;

    let source_name = || Some(source.to_owned());
    sweep
        .ok(CreateSourceFilter {
            source_name: source_name(),
            filter_name: filter.to_owned(),
            filter_kind: kind.clone(),
            ..Default::default()
        })
        .await;
    let second = format!("{filter} 2");
    sweep
        .ok(CreateSourceFilter {
            source_name: source_name(),
            filter_name: second.clone(),
            filter_kind: kind.clone(),
            ..Default::default()
        })
        .await;
    let list = sweep
        .ok(GetSourceFilterList {
            source_name: source_name(),
            ..Default::default()
        })
        .await;
    assert_eq!(list.filters.len(), 2);
    let found = sweep
        .ok(GetSourceFilter {
            source_name: source_name(),
            filter_name: filter.to_owned(),
            ..Default::default()
        })
        .await;
    assert_eq!(found.filter_kind, kind);
    assert_eq!(found.filter_index, 0);

    sweep
        .ok(SetSourceFilterIndex {
            source_name: source_name(),
            filter_name: filter.to_owned(),
            filter_index: 1,
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetSourceFilterSettings {
            source_name: source_name(),
            filter_name: filter.to_owned(),
            filter_settings: json!({"opacity": 0.5}),
            overlay: Some(true),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetSourceFilterEnabled {
            source_name: source_name(),
            filter_name: filter.to_owned(),
            filter_enabled: false,
            ..Default::default()
        })
        .await;
    let renamed = format!("{filter} renamed");
    sweep
        .ok(SetSourceFilterName {
            source_name: source_name(),
            filter_name: filter.to_owned(),
            new_filter_name: renamed.clone(),
            ..Default::default()
        })
        .await;
    for filter_name in [renamed, second] {
        sweep
            .ok(RemoveSourceFilter {
                source_name: source_name(),
                filter_name,
                ..Default::default()
            })
            .await;
    }
}

async fn sources(sweep: &mut Sweep, source: &str) {
    let active = sweep
        .ok(GetSourceActive {
            source_name: Some(source.to_owned()),
            ..Default::default()
        })
        .await;
    assert!(active.video_active);
    let screenshot = sweep
        .ok(GetSourceScreenshot {
            source_name: Some(source.to_owned()),
            image_format: "png".to_owned(),
            image_width: Some(32),
            image_height: Some(32),
            ..Default::default()
        })
        .await;
    assert!(screenshot.image_data.starts_with("data:image/png;base64,"));
    // Larger than tungstenite's default limits on frames and messages.
    let large = sweep
        .ok(GetSourceScreenshot {
            source_name: Some(source.to_owned()),
            image_format: "bmp".to_owned(),
            image_width: Some(4096),
            image_height: Some(4096),
            ..Default::default()
        })
        .await;
    assert!(large.image_data.len() > 64 * 1024 * 1024);

    let path = scratch_dir().join("screenshot.png");
    sweep
        .ok(SaveSourceScreenshot {
            source_name: Some(source.to_owned()),
            image_format: "png".to_owned(),
            image_file_path: path.display().to_string(),
            image_compression_quality: Some(-1),
            ..Default::default()
        })
        .await;
    assert!(path.exists());
}

async fn inputs(sweep: &mut Sweep, media: &str, events: &mut broadcast::Receiver<Event>) {
    let name = || Some(media.to_owned());

    let list = sweep.ok(GetInputList::default()).await;
    assert!(!list.inputs.is_empty());
    sweep
        .ok(GetInputDefaultSettings {
            input_kind: "ffmpeg_source".to_owned(),
        })
        .await;
    sweep
        .ok(GetInputSettings {
            input_name: name(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetInputSettings {
            input_name: name(),
            input_settings: json!({"looping": true}),
            overlay: Some(true),
            ..Default::default()
        })
        .await;
    let renamed = format!("{media} renamed");
    sweep
        .ok(SetInputName {
            input_name: name(),
            new_input_name: renamed.clone(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetInputName {
            input_name: Some(renamed),
            new_input_name: media.to_owned(),
            ..Default::default()
        })
        .await;
    sweep
        .allow_failure(GetInputPropertiesListPropertyItems {
            input_name: name(),
            property_name: "color_range".to_owned(),
            ..Default::default()
        })
        .await;
    sweep
        .allow_failure(PressInputPropertiesButton {
            input_name: name(),
            property_name: "no_such_button".to_owned(),
            ..Default::default()
        })
        .await;

    sweep
        .ok(SetInputMute {
            input_name: name(),
            input_muted: true,
            ..Default::default()
        })
        .await;
    assert!(
        sweep
            .ok(GetInputMute {
                input_name: name(),
                ..Default::default()
            })
            .await
            .input_muted
    );
    let toggled = sweep
        .ok(ToggleInputMute {
            input_name: name(),
            ..Default::default()
        })
        .await;
    assert!(!toggled.input_muted);
    wait_for(events, "InputMuteStateChanged", |event| match event {
        Event::InputMuteStateChanged(data) if !data.input_muted => Some(()),
        _ => None,
    })
    .await;

    sweep
        .ok(SetInputVolume {
            input_name: name(),
            input_volume_mul: Some(0.5),
            ..Default::default()
        })
        .await;
    let volume = sweep
        .ok(GetInputVolume {
            input_name: name(),
            ..Default::default()
        })
        .await;
    assert!((volume.input_volume_mul - 0.5).abs() < 0.001);
    sweep
        .ok(SetInputVolume {
            input_name: name(),
            input_volume_db: Some(-100.0),
            ..Default::default()
        })
        .await;
    sweep
        .ok(GetInputVolume {
            input_name: name(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetInputVolume {
            input_name: name(),
            input_volume_mul: Some(0.0),
            ..Default::default()
        })
        .await;
    // A silent input has a volume of negative infinity decibels, which JSON cannot represent.
    let silent = sweep
        .ok(GetInputVolume {
            input_name: name(),
            ..Default::default()
        })
        .await;
    assert_eq!(silent.input_volume_mul, 0.0);

    sweep
        .ok(SetInputAudioBalance {
            input_name: name(),
            input_audio_balance: 0.25,
            ..Default::default()
        })
        .await;
    let balance = sweep
        .ok(GetInputAudioBalance {
            input_name: name(),
            ..Default::default()
        })
        .await;
    assert!((balance.input_audio_balance - 0.25).abs() < 0.001);

    sweep
        .ok(SetInputAudioSyncOffset {
            input_name: name(),
            input_audio_sync_offset: -500,
            ..Default::default()
        })
        .await;
    let offset = sweep
        .ok(GetInputAudioSyncOffset {
            input_name: name(),
            ..Default::default()
        })
        .await;
    assert_eq!(offset.input_audio_sync_offset, -500);

    sweep
        .ok(SetInputAudioMonitorType {
            input_name: name(),
            monitor_type: "OBS_MONITORING_TYPE_NONE".to_owned(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(GetInputAudioMonitorType {
            input_name: name(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetInputAudioTracks {
            input_name: name(),
            input_audio_tracks: json!({"1": true, "2": false}),
            ..Default::default()
        })
        .await;
    let tracks = sweep
        .ok(GetInputAudioTracks {
            input_name: name(),
            ..Default::default()
        })
        .await;
    assert_eq!(tracks.input_audio_tracks["2"], json!(false));

    sweep
        .ok(SetInputDeinterlaceMode {
            input_name: name(),
            input_deinterlace_mode: "OBS_DEINTERLACE_MODE_DISABLE".to_owned(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(GetInputDeinterlaceMode {
            input_name: name(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(SetInputDeinterlaceFieldOrder {
            input_name: name(),
            input_deinterlace_field_order: "OBS_DEINTERLACE_FIELD_ORDER_TOP".to_owned(),
            ..Default::default()
        })
        .await;
    sweep
        .ok(GetInputDeinterlaceFieldOrder {
            input_name: name(),
            ..Default::default()
        })
        .await;

    // Nothing is playing, so the position fields are null.
    let status = sweep
        .ok(GetMediaInputStatus {
            input_name: name(),
            ..Default::default()
        })
        .await;
    println!("media state: {}", status.media_state);
    sweep
        .ok(TriggerMediaInputAction {
            input_name: name(),
            media_action: ObsMediaInputAction::Stop,
            ..Default::default()
        })
        .await;
    sweep
        .allow_failure(SetMediaInputCursor {
            input_name: name(),
            media_cursor: 0,
            ..Default::default()
        })
        .await;
    sweep
        .allow_failure(OffsetMediaInputCursor {
            input_name: name(),
            media_cursor_offset: -10,
            ..Default::default()
        })
        .await;
}

/// Plays a real file, so that the media position fields hold real numbers.
async fn media_playback(sweep: &mut Sweep, media: &str, events: &mut broadcast::Receiver<Event>) {
    let Ok(file) = std::env::var("OBSR_TEST_MEDIA_FILE") else {
        println!("OBSR_TEST_MEDIA_FILE is not set, so media playback was not exercised");
        return;
    };
    let name = || Some(media.to_owned());
    sweep
        .ok(SetInputSettings {
            input_name: name(),
            input_settings: json!({
                "is_local_file": true,
                "local_file": file,
                "looping": true,
                "restart_on_activate": true,
            }),
            overlay: Some(true),
            ..Default::default()
        })
        .await;
    wait_for(events, "MediaInputPlaybackStarted", |event| match event {
        Event::MediaInputPlaybackStarted(data) if data.input_name == media => Some(()),
        _ => None,
    })
    .await;
    settle().await;

    let status = sweep
        .ok(GetMediaInputStatus {
            input_name: name(),
            ..Default::default()
        })
        .await;
    assert_eq!(status.media_state, "OBS_MEDIA_STATE_PLAYING");
    let duration = status
        .media_duration
        .expect("a playing input has a duration");
    assert!(duration > 0);
    assert!(status.media_cursor.is_some());

    sweep
        .ok(SetMediaInputCursor {
            input_name: name(),
            media_cursor: duration / 2,
            ..Default::default()
        })
        .await;
    sweep
        .ok(OffsetMediaInputCursor {
            input_name: name(),
            media_cursor_offset: -10,
            ..Default::default()
        })
        .await;
    sweep
        .ok(TriggerMediaInputAction {
            input_name: name(),
            media_action: ObsMediaInputAction::Pause,
            ..Default::default()
        })
        .await;
    wait_for(events, "MediaInputActionTriggered", |event| match event {
        Event::MediaInputActionTriggered(data)
            if data.input_name == media && data.media_action == ObsMediaInputAction::Pause =>
        {
            Some(())
        }
        _ => None,
    })
    .await;
    sweep
        .ok(TriggerMediaInputAction {
            input_name: name(),
            media_action: ObsMediaInputAction::Stop,
            ..Default::default()
        })
        .await;
}

async fn replay_buffer(sweep: &mut Sweep, events: &mut broadcast::Receiver<Event>) {
    let Some(status) = sweep.allow_failure(GetReplayBufferStatus).await else {
        println!("this OBS profile has no replay buffer, so it was not exercised");
        sweep.allow_failure(StopReplayBuffer).await;
        sweep.allow_failure(SaveReplayBuffer).await;
        sweep.allow_failure(GetLastReplayBufferReplay).await;
        return;
    };
    assert!(!status.output_active);

    sweep.ok(StartReplayBuffer).await;
    wait_for_replay_state(events, ObsOutputState::Started).await;
    // Let it buffer something worth saving.
    settle().await;
    settle().await;
    sweep.ok(SaveReplayBuffer).await;
    let saved = wait_for(events, "ReplayBufferSaved", |event| match event {
        Event::ReplayBufferSaved(data) => Some(data.saved_replay_path.clone()),
        _ => None,
    })
    .await;
    assert!(!saved.is_empty());
    let last = sweep.ok(GetLastReplayBufferReplay).await;
    assert_eq!(last.saved_replay_path, saved);

    sweep.ok(ToggleReplayBuffer).await;
    wait_for_replay_state(events, ObsOutputState::Stopped).await;
    sweep.ok(ToggleReplayBuffer).await;
    wait_for_replay_state(events, ObsOutputState::Started).await;
    settle().await;
    sweep.ok(StopReplayBuffer).await;
    wait_for_replay_state(events, ObsOutputState::Stopped).await;
}

async fn wait_for_replay_state(events: &mut broadcast::Receiver<Event>, state: ObsOutputState) {
    wait_for(
        events,
        &format!("ReplayBufferStateChanged {state:?}"),
        |event| match event {
            Event::ReplayBufferStateChanged(data) if data.output_state == state => Some(()),
            _ => None,
        },
    )
    .await;
}

async fn batches(client: &Client, scene: &str) {
    // Serial, with a failure in the middle that does not halt the batch.
    let mut batch = RequestBatch::new();
    let version = batch.push(GetVersion).unwrap();
    let missing = batch
        .push(GetSceneItemList {
            scene_name: Some("obsr no such scene".to_owned()),
            ..Default::default()
        })
        .unwrap();
    let sleep = batch
        .push(Sleep {
            sleep_millis: Some(10),
            sleep_frames: None,
        })
        .unwrap();
    let items = batch
        .push(GetSceneItemList {
            scene_name: Some(scene.to_owned()),
            ..Default::default()
        })
        .unwrap();
    let raw = batch.push_raw("GetStudioModeEnabled", None);
    let response = client.batch(batch).await.unwrap();
    assert_eq!(response.results().len(), 5);
    assert_eq!(response.get(&version).unwrap().rpc_version, 1);
    let Err(Error::Request(error)) = response.get(&missing) else {
        panic!("expected the missing scene to fail");
    };
    assert_eq!(error.status(), Some(RequestStatus::ResourceNotFound));
    response.get(&sleep).unwrap();
    assert!(!response.get(&items).unwrap().scene_items.is_empty());
    assert_eq!(
        response.get(&raw).unwrap()["studioModeEnabled"],
        json!(false)
    );

    // Halting on failure skips everything after the failure.
    let mut batch = RequestBatch::new().halt_on_failure(true);
    let missing = batch
        .push(GetSceneItemList {
            scene_name: Some("obsr no such scene".to_owned()),
            ..Default::default()
        })
        .unwrap();
    let skipped = batch.push(GetVersion).unwrap();
    let response = client.batch(batch).await.unwrap();
    assert_eq!(response.results().len(), 1);
    assert!(matches!(response.get(&missing), Err(Error::Request(_))));
    assert!(matches!(
        response.get(&skipped),
        Err(Error::NotExecuted { .. })
    ));

    // Frame-synchronized execution, with a sleep measured in frames.
    let mut batch = RequestBatch::new().execution_type(RequestBatchExecutionType::SerialFrame);
    let sleep = batch
        .push(Sleep {
            sleep_millis: None,
            sleep_frames: Some(2),
        })
        .unwrap();
    let version = batch.push(GetVersion).unwrap();
    let response = client.batch(batch).await.unwrap();
    response.get(&sleep).unwrap();
    response.get(&version).unwrap();

    // Parallel execution, where results are still found by id.
    let mut batch = RequestBatch::new().execution_type(RequestBatchExecutionType::Parallel);
    let handles: Vec<_> = (0..8).map(|_| batch.push(GetVersion).unwrap()).collect();
    let response = client.batch(batch).await.unwrap();
    for handle in &handles {
        assert_eq!(response.get(handle).unwrap().rpc_version, 1);
    }

    let response = client.batch(RequestBatch::new()).await.unwrap();
    assert!(response.results().is_empty());
}

async fn outputs(sweep: &mut Sweep, events: &mut broadcast::Receiver<Event>) {
    let outputs = sweep.ok(GetOutputList).await.outputs;
    println!(
        "outputs: {:?}",
        outputs
            .iter()
            .filter_map(|output| output["outputName"].as_str())
            .collect::<Vec<_>>()
    );
    for output in &outputs {
        let output_name = output["outputName"].as_str().unwrap().to_owned();
        sweep
            .allow_failure(GetOutputStatus {
                output_name: output_name.clone(),
            })
            .await;
        sweep.allow_failure(GetOutputSettings { output_name }).await;
    }
    let missing = || "obsr no such output".to_owned();
    sweep
        .allow_failure(SetOutputSettings {
            output_name: missing(),
            output_settings: json!({}),
        })
        .await;
    sweep
        .allow_failure(StartOutput {
            output_name: missing(),
        })
        .await;
    sweep
        .allow_failure(StopOutput {
            output_name: missing(),
        })
        .await;
    sweep
        .allow_failure(ToggleOutput {
            output_name: missing(),
        })
        .await;

    sweep.allow_failure(GetVirtualCamStatus).await;
    sweep.allow_failure(StopVirtualCam).await;
    replay_buffer(sweep, events).await;

    // The stream service was pointed at a closed local port, so this fails to connect quickly.
    assert!(!sweep.ok(GetStreamStatus).await.output_active);
    sweep.allow_failure(StopStream).await;
    sweep
        .allow_failure(SendStreamCaption {
            caption_text: "obsr".to_owned(),
        })
        .await;
    sweep.allow_failure(StartStream).await;
    sweep.ok(GetStreamStatus).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    sweep.allow_failure(StopStream).await;
    sweep.allow_failure(ToggleStream).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let stream = sweep.ok(GetStreamStatus).await;
    if stream.output_active {
        sweep.allow_failure(StopStream).await;
    }

    // Record to the scratch directory set earlier.
    assert!(!sweep.ok(GetRecordStatus).await.output_active);
    sweep.ok(StartRecord).await;
    wait_for_record_state(events, ObsOutputState::Started).await;
    assert!(sweep.ok(GetRecordStatus).await.output_active);
    // OBS silently ignores a pause when the recording shares the streaming encoder, which is the
    // default. Only the event shows whether it took effect.
    settle().await;
    sweep.ok(PauseRecord).await;
    let paused = wait_for_record_state(events, ObsOutputState::Paused);
    if timeout(Duration::from_secs(2), paused).await.is_ok() {
        settle().await;
        sweep.ok(ResumeRecord).await;
        wait_for_record_state(events, ObsOutputState::Resumed).await;
        settle().await;
        // OBS answers with success whether or not it honors this. It declined a second pause with
        // a replay buffer enabled and honored one without, so only the event can tell.
        sweep.ok(ToggleRecordPause).await;
        let paused = wait_for_record_state(events, ObsOutputState::Paused);
        if timeout(Duration::from_secs(2), paused).await.is_ok() {
            settle().await;
            sweep.ok(ToggleRecordPause).await;
            wait_for_record_state(events, ObsOutputState::Resumed).await;
        } else {
            println!("OBS did not honor a second pause");
        }
        settle().await;
    } else {
        println!("this OBS profile cannot pause recordings, so that path was not exercised");
    }
    sweep.allow_failure(SplitRecordFile).await;
    sweep
        .allow_failure(CreateRecordChapter {
            chapter_name: Some("obsr".to_owned()),
        })
        .await;
    let stopped = sweep.ok(StopRecord).await;
    assert!(stopped.output_path.ends_with(".mkv") || !stopped.output_path.is_empty());
    let saved_path = wait_for_record_state(events, ObsOutputState::Stopped).await;
    assert!(saved_path.is_some());

    let toggled = sweep.ok(ToggleRecord).await;
    assert!(toggled.output_active);
    wait_for_record_state(events, ObsOutputState::Started).await;
    assert!(!sweep.ok(ToggleRecord).await.output_active);
    wait_for_record_state(events, ObsOutputState::Stopped).await;
}

/// Leaves time for several video frames. libobs ignores a pause or resume that arrives before the
/// previous one has reached the video pipeline, and it can wedge the recording.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// Waits for the recording to reach `state`, and returns the path the event carries.
async fn wait_for_record_state(
    events: &mut broadcast::Receiver<Event>,
    state: ObsOutputState,
) -> Option<String> {
    wait_for(
        events,
        &format!("RecordStateChanged {state:?}"),
        |event| match event {
            Event::RecordStateChanged(data) if data.output_state == state => {
                Some(data.output_path.clone())
            }
            _ => None,
        },
    )
    .await
}

async fn collections(sweep: &mut Sweep, collection: &str, events: &mut broadcast::Receiver<Event>) {
    let list = sweep.ok(GetSceneCollectionList).await;
    let original = list.current_scene_collection_name;

    // Like profiles, scene collections change on the UI thread.
    sweep
        .ok(CreateSceneCollection {
            scene_collection_name: collection.to_owned(),
        })
        .await;
    wait_for(
        events,
        "CurrentSceneCollectionChanged to the new one",
        |event| match event {
            Event::CurrentSceneCollectionChanged(data)
                if data.scene_collection_name == collection =>
            {
                Some(())
            }
            _ => None,
        },
    )
    .await;
    sweep
        .ok(SetCurrentSceneCollection {
            scene_collection_name: original.clone(),
        })
        .await;
    wait_for(
        events,
        "CurrentSceneCollectionChanged back",
        |event| match event {
            Event::CurrentSceneCollectionChanged(data)
                if data.scene_collection_name == original =>
            {
                Some(())
            }
            _ => None,
        },
    )
    .await;
}
