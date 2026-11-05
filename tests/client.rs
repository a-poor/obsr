//! End-to-end tests of `Client` against a scripted server.

mod common;

use std::time::Duration;

use common::*;
use futures_util::FutureExt;
use obsr::enums::{ObsOutputState, RequestBatchExecutionType, RequestStatus, WebSocketCloseCode};
use obsr::requests::{
    GetPersistentData, GetVersion, Request, SetCurrentProgramScene, Sleep,
    TriggerHotkeyByKeySequence,
};
use obsr::{Client, ConnectOptions, Encoding, Error, Event, EventSubscription, RequestBatch};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::broadcast::error::RecvError;
use tokio_tungstenite::tungstenite::Message;

const VERSION_DATA: &str = r#"{
    "obsVersion": "33.0.1",
    "obsWebSocketVersion": "5.7.6",
    "rpcVersion": 1,
    "availableRequests": ["GetVersion"],
    "supportedImageFormats": ["png"],
    "platform": "macos",
    "platformDescription": "macOS 27"
}"#;

fn version_data() -> Value {
    serde_json::from_str(VERSION_DATA).unwrap()
}

// --- Handshake ---

#[tokio::test]
async fn connects_without_authentication() {
    let (url, server) = serve(|mut conn| async move {
        assert_eq!(conn.requested_protocol, "obswebsocket.json");
        conn.send(HELLO, hello(None)).await;
        let identify = conn.expect(IDENTIFY).await;
        assert_eq!(identify, json!({"rpcVersion": 1}));
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
        conn.answer_next_request().await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    assert_eq!(client.server_info().obs_studio_version, "33.0.1");
    assert_eq!(client.server_info().obs_web_socket_version, "5.7.6");
    assert_eq!(client.negotiated_rpc_version(), 1);
    within(client.request_raw("Sync", None)).await.unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn answers_the_auth_challenge_with_the_expected_string() {
    let (url, server) = serve(|mut conn| async move {
        conn.send(
            HELLO,
            hello(Some(json!({
                "challenge": "+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=",
                "salt": "lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI=",
            }))),
        )
        .await;
        let identify = conn.expect(IDENTIFY).await;
        // Computed independently with Python's hashlib.
        assert_eq!(
            identify["authentication"],
            "1Ct943GAT+6YQUUX47Ia/ncufilbe6+oD6lY+5kaCu4="
        );
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
    })
    .await;

    within(
        ConnectOptions::new(url)
            .password("supersecretpassword")
            .connect(),
    )
    .await
    .unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn sends_no_authentication_when_the_server_does_not_ask_for_it() {
    let (url, server) = serve(|mut conn| async move {
        let identify = conn.handshake().await;
        assert!(identify.get("authentication").is_none());
    })
    .await;

    within(ConnectOptions::new(url).password("unused").connect())
        .await
        .unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn reports_a_missing_password_without_sending_identify() {
    let (url, server) = serve(|mut conn| async move {
        conn.send(HELLO, hello(Some(json!({"challenge": "c", "salt": "s"}))))
            .await;
        assert!(matches!(
            conn.recv().await,
            Received::Close(_) | Received::Ended
        ));
    })
    .await;

    let error = within(Client::connect(url)).await.unwrap_err();
    assert!(matches!(error, Error::PasswordRequired), "{error:?}");
    within(server).await.unwrap();
}

#[tokio::test]
async fn reports_a_rejected_password_through_the_close_code() {
    let (url, server) = serve(|mut conn| async move {
        conn.send(HELLO, hello(Some(json!({"challenge": "c", "salt": "s"}))))
            .await;
        conn.expect(IDENTIFY).await;
        conn.close(4009, "Authentication failed.").await;
    })
    .await;

    let error = within(ConnectOptions::new(url).password("wrong").connect())
        .await
        .unwrap_err();
    let Error::Closed(info) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(
        info.close_code(),
        Some(WebSocketCloseCode::AuthenticationFailed)
    );
    assert_eq!(info.reason, "Authentication failed.");
    within(server).await.unwrap();
}

#[tokio::test]
async fn offers_its_own_rpc_version_when_the_server_is_newer() {
    let (url, server) = serve(|mut conn| async move {
        let mut data = hello(None);
        data["rpcVersion"] = json!(2);
        conn.send(HELLO, data).await;
        assert_eq!(conn.expect(IDENTIFY).await["rpcVersion"], 1);
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
    })
    .await;

    within(Client::connect(url)).await.unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn rejects_a_negotiated_rpc_version_it_does_not_support() {
    let (url, server) = serve(|mut conn| async move {
        conn.send(HELLO, hello(None)).await;
        conn.expect(IDENTIFY).await;
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 2}))
            .await;
    })
    .await;

    let error = within(Client::connect(url)).await.unwrap_err();
    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
    within(server).await.unwrap();
}

#[tokio::test]
async fn sends_requested_event_subscriptions_in_identify() {
    let (url, server) = serve(|mut conn| async move {
        let identify = conn.handshake().await;
        assert_eq!(identify["eventSubscriptions"], (1 << 2) | (1 << 16));
    })
    .await;

    within(
        ConnectOptions::new(url)
            .event_subscriptions(EventSubscription::SCENES | EventSubscription::INPUT_VOLUME_METERS)
            .connect(),
    )
    .await
    .unwrap();
    within(server).await.unwrap();
}

// The server marks a session as identified, and so starts sending it events, before it sends
// `Identified`. An event can therefore arrive first.
#[tokio::test]
async fn tolerates_an_event_arriving_before_identified() {
    let (url, server) = serve(|mut conn| async move {
        conn.send(HELLO, hello(None)).await;
        conn.expect(IDENTIFY).await;
        conn.send(EVENT, json!({"eventType": "ExitStarted", "eventIntent": 1}))
            .await;
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
    })
    .await;

    within(Client::connect(url)).await.unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn fails_when_the_first_message_is_not_hello() {
    let (url, server) = serve(|mut conn| async move {
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
    })
    .await;

    let error = within(Client::connect(url)).await.unwrap_err();
    assert!(matches!(error, Error::Protocol(_)), "{error:?}");
    within(server).await.unwrap();
}

#[tokio::test]
async fn fails_when_the_server_sends_garbage_during_the_handshake() {
    let (url, server) = serve(|mut conn| async move {
        conn.send_frame(Message::Text("not json".into())).await;
    })
    .await;

    let error = within(Client::connect(url)).await.unwrap_err();
    assert!(matches!(error, Error::Decode { .. }), "{error:?}");
    within(server).await.unwrap();
}

#[tokio::test]
async fn fails_when_the_server_closes_before_hello() {
    let (url, server) = serve(|mut conn| async move {
        conn.close(4000, "nope").await;
    })
    .await;

    let error = within(Client::connect(url)).await.unwrap_err();
    let Error::Closed(info) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(info.code, Some(4000));
    within(server).await.unwrap();
}

#[tokio::test]
async fn fails_when_the_server_does_not_accept_the_subprotocol() {
    let (url, server) = serve_with_protocol_reply(false, |_conn| async {}).await;

    let error = within(Client::connect(url)).await.unwrap_err();
    assert!(matches!(error, Error::WebSocket(_)), "{error:?}");
    within(server).await.unwrap();
}

#[tokio::test]
async fn reports_invalid_urls_and_refused_connections() {
    let error = within(Client::connect("not a url")).await.unwrap_err();
    assert!(matches!(error, Error::WebSocket(_)), "{error:?}");

    // Bind and release a port so that nothing is listening on it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let error = within(Client::connect(format!("ws://{address}")))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::WebSocket(_)), "{error:?}");
}

#[tokio::test]
async fn connect_can_be_timed_out_by_the_caller() {
    let (url, server) = serve(|mut conn| async move {
        // Never sends `Hello`, then sees the client go away.
        assert!(matches!(
            conn.recv().await,
            Received::Ended | Received::Close(_)
        ));
    })
    .await;

    let result = tokio::time::timeout(Duration::from_millis(100), Client::connect(url)).await;
    assert!(result.is_err());
    within(server).await.unwrap();
}

#[test]
fn debug_output_redacts_the_password() {
    let options = ConnectOptions::new("ws://localhost:4455").password("hunter2");
    let debug = format!("{options:?}");
    assert!(!debug.contains("hunter2"), "{debug}");
    assert!(debug.contains("ws://localhost:4455"), "{debug}");
}

// --- Requests ---

#[tokio::test]
async fn sends_a_typed_request_and_decodes_its_response() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let request = conn.expect(REQUEST).await;
        assert_eq!(request["requestType"], "GetVersion");
        assert!(request["requestId"].is_string());
        // A request without fields carries no `requestData`.
        assert!(request.get("requestData").is_none());
        conn.respond(&request, Some(version_data())).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let version = within(client.request(GetVersion)).await.unwrap();
    assert_eq!(version.obs_version, "33.0.1");
    assert_eq!(version.rpc_version, 1);
    assert_eq!(version.available_requests, ["GetVersion"]);
    within(server).await.unwrap();
}

#[tokio::test]
async fn omits_unset_optional_fields_and_nests_key_modifiers() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;

        let request = conn.expect(REQUEST).await;
        assert_eq!(request["requestData"], json!({"sceneName": "Live"}));
        conn.respond(&request, None).await;

        let request = conn.expect(REQUEST).await;
        assert_eq!(
            request["requestData"],
            json!({"keyId": "OBS_KEY_A", "keyModifiers": {"control": true}})
        );
        conn.respond(&request, None).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    within(client.request(SetCurrentProgramScene {
        scene_name: Some("Live".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    within(client.request(TriggerHotkeyByKeySequence {
        key_id: Some("OBS_KEY_A".into()),
        key_modifiers: Some(obsr::requests::KeyModifiers {
            control: Some(true),
            ..Default::default()
        }),
    }))
    .await
    .unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn correlates_responses_that_arrive_out_of_order() {
    const COUNT: usize = 50;
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let mut requests = Vec::new();
        for _ in 0..COUNT {
            requests.push(conn.expect(REQUEST).await);
        }
        let ids: std::collections::HashSet<_> = requests
            .iter()
            .map(|r| r["requestId"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(ids.len(), COUNT, "request ids must be unique");
        requests.reverse();
        for request in requests {
            let slot = request["requestData"]["slotName"].clone();
            conn.respond(&request, Some(json!({"slotValue": slot})))
                .await;
        }
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let mut tasks = Vec::new();
    for index in 0..COUNT {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let slot = format!("slot-{index}");
            let response = client
                .request(GetPersistentData {
                    realm: "OBS_WEBSOCKET_DATA_REALM_GLOBAL".into(),
                    slot_name: slot.clone(),
                })
                .await
                .unwrap();
            assert_eq!(response.slot_value, Some(json!(slot)));
        }));
    }
    for task in tasks {
        within(task).await.unwrap();
    }
    within(server).await.unwrap();
}

#[tokio::test]
async fn surfaces_failed_requests_with_status_and_comment() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let request = conn.expect(REQUEST).await;
        conn.respond_error(&request, 600, "No source was found by the name of `x`.")
            .await;
        let request = conn.expect(REQUEST).await;
        conn.respond_error(&request, 999, "from the future").await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let error = within(client.request(SetCurrentProgramScene::default()))
        .await
        .unwrap_err();
    let Error::Request(error) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(error.request_type, "SetCurrentProgramScene");
    assert_eq!(error.code, 600);
    assert_eq!(error.status(), Some(RequestStatus::ResourceNotFound));
    assert_eq!(
        error.comment.as_deref(),
        Some("No source was found by the name of `x`.")
    );
    assert!(error.to_string().contains("ResourceNotFound"), "{error}");

    let error = within(client.request(SetCurrentProgramScene::default()))
        .await
        .unwrap_err();
    let Error::Request(error) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(error.code, 999);
    assert_eq!(error.status(), None);
    within(server).await.unwrap();
}

#[tokio::test]
async fn reports_mismatched_response_data_without_dropping_the_connection() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let request = conn.expect(REQUEST).await;
        conn.respond(&request, Some(json!({"unexpected": true})))
            .await;
        let request = conn.expect(REQUEST).await;
        conn.respond(&request, Some(version_data())).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let error = within(client.request(GetVersion)).await.unwrap_err();
    assert!(matches!(error, Error::Decode { .. }), "{error:?}");
    assert!(error.to_string().contains("GetVersion"), "{error}");
    within(client.request(GetVersion)).await.unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn a_request_that_returns_no_data_accepts_any_response_data() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let request = conn.expect(REQUEST).await;
        conn.respond(&request, None).await;
        let request = conn.expect(REQUEST).await;
        conn.respond(&request, Some(json!({}))).await;
        let request = conn.expect(REQUEST).await;
        conn.respond(&request, Some(json!({"addedLater": 1}))).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    for _ in 0..3 {
        within(client.request(SetCurrentProgramScene::default()))
            .await
            .unwrap();
    }
    within(server).await.unwrap();
}

#[tokio::test]
async fn request_raw_passes_data_through() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let request = conn.expect(REQUEST).await;
        assert_eq!(request["requestType"], "FutureRequest");
        assert_eq!(request["requestData"], json!({"a": 1}));
        conn.respond(&request, Some(json!({"b": 2}))).await;
        let request = conn.expect(REQUEST).await;
        assert!(request.get("requestData").is_none());
        conn.respond(&request, None).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let data = within(client.request_raw("FutureRequest", Some(json!({"a": 1}))))
        .await
        .unwrap();
    assert_eq!(data, Some(json!({"b": 2})));
    let data = within(client.request_raw("FutureRequest", None))
        .await
        .unwrap();
    assert_eq!(data, None);
    within(server).await.unwrap();
}

#[tokio::test]
async fn supports_user_defined_requests() {
    #[derive(Serialize)]
    #[serde(rename_all = "camelCase")]
    struct Future {
        some_value: u8,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct FutureResponse {
        other_value: u8,
    }
    impl Request for Future {
        const REQUEST_TYPE: &'static str = "Future";
        type Response = FutureResponse;
    }

    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let request = conn.expect(REQUEST).await;
        assert_eq!(request["requestType"], "Future");
        assert_eq!(request["requestData"], json!({"someValue": 1}));
        conn.respond(&request, Some(json!({"otherValue": 2}))).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let response = within(client.request(Future { some_value: 1 }))
        .await
        .unwrap();
    assert_eq!(response.other_value, 2);
    within(server).await.unwrap();
}

#[tokio::test]
async fn a_cancelled_request_does_not_disturb_later_ones() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let abandoned = conn.expect(REQUEST).await;
        let next = conn.expect(REQUEST).await;
        // The reply to the abandoned request arrives after the caller stopped waiting for it.
        conn.respond(&abandoned, Some(version_data())).await;
        conn.respond(&next, Some(version_data())).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let abandoned =
        tokio::time::timeout(Duration::from_millis(50), client.request(GetVersion)).await;
    assert!(abandoned.is_err());
    within(client.request(GetVersion)).await.unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn ignores_responses_nobody_is_waiting_for_and_unknown_opcodes() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let request = conn.expect(REQUEST).await;
        conn.send(4, json!({"reserved": true})).await;
        conn.send(99, json!({"future": true})).await;
        conn.send(
            REQUEST_RESPONSE,
            json!({
                "requestType": "GetVersion",
                "requestId": "nobody-asked",
                "requestStatus": {"result": true, "code": 100},
            }),
        )
        .await;
        conn.send(
            REQUEST_BATCH_RESPONSE,
            json!({"requestId": "nobody-asked", "results": []}),
        )
        .await;
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
        conn.respond(&request, Some(version_data())).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    within(client.request(GetVersion)).await.unwrap();
    within(server).await.unwrap();
}

// A large screenshot arrives as one frame, and a read error would drop the whole connection. This
// is larger than tungstenite's default limits of 16 MiB per frame and 64 MiB per message.
#[tokio::test]
async fn receives_messages_as_large_as_a_big_screenshot() {
    const SIZE: usize = 70 * 1024 * 1024;
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let request = conn.expect(REQUEST).await;
        conn.respond(&request, Some(json!({"imageData": "x".repeat(SIZE)})))
            .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let response = within(client.request(obsr::requests::GetSourceScreenshot {
        source_name: Some("Scene".into()),
        image_format: "bmp".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    assert_eq!(response.image_data.len(), SIZE);
    within(server).await.unwrap();
}

// --- Events ---

#[tokio::test]
async fn delivers_typed_events_to_every_subscriber() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.answer_next_request().await;
        conn.send(
            EVENT,
            json!({
                "eventType": "CurrentProgramSceneChanged",
                "eventIntent": 4,
                "eventData": {"sceneName": "Live", "sceneUuid": "abc"},
            }),
        )
        .await;
        conn.send(EVENT, json!({"eventType": "ExitStarted", "eventIntent": 1}))
            .await;
        conn.send(
            EVENT,
            json!({
                "eventType": "StreamStateChanged",
                "eventIntent": 64,
                "eventData": {"outputActive": true, "outputState": "OBS_WEBSOCKET_OUTPUT_STARTED"},
            }),
        )
        .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let mut first = client.events();
    let mut second = client.events();
    within(client.request_raw("Sync", None)).await.unwrap();

    for events in [&mut first, &mut second] {
        let Event::CurrentProgramSceneChanged(scene) = within(events.recv()).await.unwrap() else {
            panic!("expected a scene change");
        };
        assert_eq!(scene.scene_name, "Live");
        assert_eq!(scene.scene_uuid, "abc");
        assert!(matches!(
            within(events.recv()).await.unwrap(),
            Event::ExitStarted
        ));
        let Event::StreamStateChanged(stream) = within(events.recv()).await.unwrap() else {
            panic!("expected a stream state change");
        };
        assert!(stream.output_active);
        assert_eq!(stream.output_state, ObsOutputState::Started);
    }
    within(server).await.unwrap();
}

#[tokio::test]
async fn keeps_unknown_and_malformed_events_as_raw() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.answer_next_request().await;
        conn.send(
            EVENT,
            json!({"eventType": "FromTheFuture", "eventIntent": 1, "eventData": {"a": 1}}),
        )
        .await;
        conn.send(
            EVENT,
            json!({"eventType": "SceneCreated", "eventIntent": 4, "eventData": {"wrong": true}}),
        )
        .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let mut events = client.events();
    within(client.request_raw("Sync", None)).await.unwrap();

    let Event::Unknown(raw) = within(events.recv()).await.unwrap() else {
        panic!("expected a raw event");
    };
    assert_eq!(raw.event_type, "FromTheFuture");
    assert_eq!(raw.event_data, Some(json!({"a": 1})));
    let Event::Unknown(raw) = within(events.recv()).await.unwrap() else {
        panic!("expected a raw event");
    };
    assert_eq!(raw.event_type, "SceneCreated");
    within(server).await.unwrap();
}

// The spec documents a nested `eventData` field, but the server sends the custom object itself as
// the event's data. This is the shape a real OBS sends.
#[tokio::test]
async fn custom_events_expose_the_object_the_server_actually_sends() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.answer_next_request().await;
        conn.send(
            EVENT,
            json!({"eventType": "CustomEvent", "eventIntent": 1, "eventData": {"from": "obsr"}}),
        )
        .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let mut events = client.events();
    within(client.request_raw("Sync", None)).await.unwrap();

    let event = within(events.recv()).await.unwrap();
    let Event::CustomEvent(custom) = event else {
        panic!("expected a custom event, got {event:?}");
    };
    assert_eq!(custom.event_data, json!({"from": "obsr"}));
    within(server).await.unwrap();
}

#[tokio::test]
async fn a_slow_subscriber_is_told_how_many_events_it_missed() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.answer_next_request().await;
        for _ in 0..3 {
            conn.send(EVENT, json!({"eventType": "ExitStarted", "eventIntent": 1}))
                .await;
        }
        conn.answer_next_request().await;
    })
    .await;

    let client = within(ConnectOptions::new(url).event_capacity(1).connect())
        .await
        .unwrap();
    let mut events = client.events();
    within(client.request_raw("Sync", None)).await.unwrap();
    within(client.request_raw("Sync", None)).await.unwrap();

    assert!(matches!(
        within(events.recv()).await,
        Err(RecvError::Lagged(2))
    ));
    assert!(matches!(
        within(events.recv()).await,
        Ok(Event::ExitStarted)
    ));
    within(server).await.unwrap();
}

#[tokio::test]
async fn reidentify_updates_subscriptions_and_waits_for_identified() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let reidentify = conn.expect(REIDENTIFY).await;
        assert_eq!(reidentify, json!({"eventSubscriptions": 1 << 3}));
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;

        let reidentify = conn.expect(REIDENTIFY).await;
        assert_eq!(reidentify, json!({"eventSubscriptions": 0}));
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    within(client.reidentify(EventSubscription::INPUTS))
        .await
        .unwrap();
    within(client.reidentify(EventSubscription::NONE))
        .await
        .unwrap();
    within(server).await.unwrap();
}

#[tokio::test]
async fn a_cancelled_reidentify_does_not_steal_the_next_acknowledgement() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        assert_eq!(conn.expect(REIDENTIFY).await["eventSubscriptions"], 1);
        assert_eq!(conn.expect(REIDENTIFY).await["eventSubscriptions"], 2);
        // This acknowledges the first call, so it must not complete the second one.
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
        conn.answer_next_request().await;
        // The second acknowledgement is held back until the client has checked for that.
        let request = conn.expect(REQUEST).await;
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
        conn.respond(&request, None).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let cancelled = tokio::time::timeout(
        Duration::from_millis(50),
        client.reidentify(EventSubscription::GENERAL),
    )
    .await;
    assert!(cancelled.is_err());

    let second = client.reidentify(EventSubscription::CONFIG);
    tokio::pin!(second);
    // Polling once sends it.
    assert!((&mut second).now_or_never().is_none());

    // Messages are handled in order, so the first acknowledgement has been processed by the time
    // this request returns.
    within(client.request_raw("Sync", None)).await.unwrap();
    assert!(
        (&mut second).now_or_never().is_none(),
        "the second call took the first acknowledgement"
    );

    within(client.request_raw("Sync", None)).await.unwrap();
    within(second).await.unwrap();
    within(server).await.unwrap();
}

// --- Batches ---

#[tokio::test]
async fn sends_a_batch_with_defaults_left_to_the_server() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let batch = conn.expect(REQUEST_BATCH).await;
        assert!(batch["requestId"].is_string());
        assert!(batch.get("haltOnFailure").is_none());
        assert!(batch.get("executionType").is_none());
        let requests = batch["requests"].as_array().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["requestType"], "GetVersion");
        assert!(requests[0].get("requestData").is_none());
        assert_eq!(requests[1]["requestType"], "SetCurrentProgramScene");
        assert_eq!(requests[1]["requestData"], json!({"sceneName": "Live"}));

        // Results are matched by the id each request carries, not by their position.
        conn.send(
            REQUEST_BATCH_RESPONSE,
            json!({
                "requestId": batch["requestId"],
                "results": [
                    {
                        "requestType": "SetCurrentProgramScene",
                        "requestId": requests[1]["requestId"],
                        "requestStatus": {"result": false, "code": 600, "comment": "missing"},
                    },
                    {
                        "requestType": "GetVersion",
                        "requestId": requests[0]["requestId"],
                        "requestStatus": {"result": true, "code": 100},
                        "responseData": version_data(),
                    },
                ],
            }),
        )
        .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let mut batch = RequestBatch::new();
    let version = batch.push(GetVersion).unwrap();
    let scene = batch
        .push(SetCurrentProgramScene {
            scene_name: Some("Live".into()),
            ..Default::default()
        })
        .unwrap();

    let response = within(client.batch(batch)).await.unwrap();
    assert_eq!(response.results().len(), 2);
    assert_eq!(response.get(&version).unwrap().obs_version, "33.0.1");
    let Error::Request(error) = response.get(&scene).unwrap_err() else {
        panic!("expected a request error");
    };
    assert_eq!(error.status(), Some(RequestStatus::ResourceNotFound));
    assert_eq!(error.comment.as_deref(), Some("missing"));
    within(server).await.unwrap();
}

#[tokio::test]
async fn batch_options_reach_the_wire_and_halted_requests_are_reported() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let batch = conn.expect(REQUEST_BATCH).await;
        assert_eq!(batch["haltOnFailure"], true);
        assert_eq!(batch["executionType"], 2);
        assert_eq!(
            batch["requests"][0]["requestData"],
            json!({"sleepMillis": 5})
        );
        // Only the first request ran.
        conn.send(
            REQUEST_BATCH_RESPONSE,
            json!({
                "requestId": batch["requestId"],
                "results": [{
                    "requestType": "Sleep",
                    "requestId": batch["requests"][0]["requestId"],
                    "requestStatus": {"result": true, "code": 100},
                }],
            }),
        )
        .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let mut batch = RequestBatch::new()
        .halt_on_failure(true)
        .execution_type(RequestBatchExecutionType::Parallel);
    let first = batch
        .push(Sleep {
            sleep_millis: Some(5),
            sleep_frames: None,
        })
        .unwrap();
    let second = batch.push(GetVersion).unwrap();

    let response = within(client.batch(batch)).await.unwrap();
    response.get(&first).unwrap();
    let error = response.get(&second).unwrap_err();
    assert!(matches!(error, Error::NotExecuted { .. }), "{error:?}");
    assert!(error.to_string().contains("GetVersion"), "{error}");
    within(server).await.unwrap();
}

#[tokio::test]
async fn execution_type_none_is_not_sent_because_the_server_would_close_the_connection() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let batch = conn.expect(REQUEST_BATCH).await;
        assert!(batch.get("executionType").is_none());
        conn.send(
            REQUEST_BATCH_RESPONSE,
            json!({"requestId": batch["requestId"], "results": []}),
        )
        .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let batch = RequestBatch::new().execution_type(RequestBatchExecutionType::None);
    let response = within(client.batch(batch)).await.unwrap();
    assert!(response.results().is_empty());
    within(server).await.unwrap();
}

#[tokio::test]
async fn batch_results_can_decode_raw_requests() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let batch = conn.expect(REQUEST_BATCH).await;
        assert_eq!(batch["requests"][0]["requestType"], "Future");
        assert_eq!(batch["requests"][0]["requestData"], json!({"a": 1}));
        conn.send(
            REQUEST_BATCH_RESPONSE,
            json!({
                "requestId": batch["requestId"],
                "results": [{
                    "requestType": "Future",
                    "requestId": batch["requests"][0]["requestId"],
                    "requestStatus": {"result": true, "code": 100},
                    "responseData": {"b": 2},
                }],
            }),
        )
        .await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let mut batch = RequestBatch::new();
    let raw = batch.push_raw("Future", Some(json!({"a": 1})));
    let response = within(client.batch(batch)).await.unwrap();
    assert_eq!(response.get(&raw).unwrap(), json!({"b": 2}));
    within(server).await.unwrap();
}

// --- MessagePack ---

#[tokio::test]
async fn msgpack_works_end_to_end_over_binary_frames() {
    let (url, server) = serve(|mut conn| async move {
        assert_eq!(conn.requested_protocol, "obswebsocket.msgpack");
        conn.handshake().await;

        let request = conn.expect(REQUEST).await;
        assert_eq!(request["requestType"], "GetInputVolume");
        assert_eq!(request["requestData"], json!({"inputName": "Mic"}));
        conn.respond(
            &request,
            Some(json!({"inputVolumeMul": 0.5, "inputVolumeDb": -6.25})),
        )
        .await;

        conn.send(
            EVENT,
            json!({"eventType": "SceneCreated", "eventIntent": 4, "eventData": {
                "sceneName": "A", "sceneUuid": "u", "isGroup": false,
            }}),
        )
        .await;

        let batch = conn.expect(REQUEST_BATCH).await;
        assert_eq!(batch["requests"][0]["requestType"], "GetVersion");
        conn.send(
            REQUEST_BATCH_RESPONSE,
            json!({
                "requestId": batch["requestId"],
                "results": [{
                    "requestType": "GetVersion",
                    "requestId": batch["requests"][0]["requestId"],
                    "requestStatus": {"result": true, "code": 100},
                    "responseData": version_data(),
                }],
            }),
        )
        .await;
    })
    .await;

    let client = within(
        ConnectOptions::new(url)
            .encoding(Encoding::MsgPack)
            .connect(),
    )
    .await
    .unwrap();
    let mut events = client.events();
    let volume = within(client.request(obsr::requests::GetInputVolume {
        input_name: Some("Mic".into()),
        input_uuid: None,
    }))
    .await
    .unwrap();
    assert_eq!(volume.input_volume_mul, 0.5);
    assert_eq!(volume.input_volume_db, -6.25);

    let Event::SceneCreated(scene) = within(events.recv()).await.unwrap() else {
        panic!("expected a scene event");
    };
    assert_eq!(scene.scene_name, "A");

    let mut batch = RequestBatch::new();
    let version = batch.push(GetVersion).unwrap();
    let response = within(client.batch(batch)).await.unwrap();
    assert_eq!(response.get(&version).unwrap().rpc_version, 1);
    within(server).await.unwrap();
}

#[tokio::test]
async fn msgpack_password_handshake_works() {
    let (url, server) = serve(|mut conn| async move {
        conn.send(HELLO, hello(Some(json!({"challenge": "c", "salt": "s"}))))
            .await;
        let identify = conn.expect(IDENTIFY).await;
        assert!(identify["authentication"].is_string());
        conn.send(IDENTIFIED, json!({"negotiatedRpcVersion": 1}))
            .await;
    })
    .await;

    within(
        ConnectOptions::new(url)
            .password("p")
            .encoding(Encoding::MsgPack)
            .connect(),
    )
    .await
    .unwrap();
    within(server).await.unwrap();
}

// --- Connection lifecycle ---

#[tokio::test]
async fn a_server_close_fails_pending_and_later_requests_with_the_close_code() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.expect(REQUEST).await;
        conn.close(4011, "kicked").await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let mut events = client.events();
    let error = within(client.request(GetVersion)).await.unwrap_err();
    let Error::Closed(info) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(
        info.close_code(),
        Some(WebSocketCloseCode::SessionInvalidated)
    );
    assert_eq!(info.reason, "kicked");

    let error = within(client.request(GetVersion)).await.unwrap_err();
    assert!(
        matches!(error, Error::Closed(ref later) if later == &info),
        "{error:?}"
    );
    let error = within(client.reidentify(EventSubscription::ALL))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Closed(_)), "{error:?}");
    let error = within(client.batch(RequestBatch::new())).await.unwrap_err();
    assert!(matches!(error, Error::Closed(_)), "{error:?}");

    assert_eq!(within(client.closed()).await, info);
    assert!(client.is_closed());
    assert!(matches!(
        within(events.recv()).await,
        Err(RecvError::Closed)
    ));
    assert!(matches!(
        within(client.events().recv()).await,
        Err(RecvError::Closed)
    ));
    within(server).await.unwrap();
}

#[tokio::test]
async fn a_lost_connection_fails_pending_work() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.expect(REQUEST).await;
        conn.expect(REIDENTIFY).await;
        conn.expect(REQUEST_BATCH).await;
        drop(conn); // No close frame.
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let batch = RequestBatch::new();
    let (request, reidentify, batch) = within(async {
        tokio::join!(
            client.request(GetVersion),
            client.reidentify(EventSubscription::ALL),
            client.batch(batch),
        )
    })
    .await;
    for error in [
        request.unwrap_err(),
        reidentify.unwrap_err(),
        batch.unwrap_err(),
    ] {
        let Error::Closed(info) = error else {
            panic!("unexpected error: {error:?}");
        };
        assert_eq!(info.code, None);
    }
    within(server).await.unwrap();
}

#[tokio::test]
async fn replies_to_a_server_close_frame() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.send_frame(Message::Close(Some(
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: 4011u16.into(),
                reason: "kicked".into(),
            },
        )))
        .await;
        // The client must complete the close handshake rather than just dropping the socket.
        assert!(
            matches!(conn.recv().await, Received::Close(_)),
            "no close reply"
        );
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    within(client.closed()).await;
    within(server).await.unwrap();
}

#[tokio::test]
async fn invalid_messages_close_the_connection_and_fail_pending_requests() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.expect(REQUEST).await;
        conn.send_frame(Message::Text("not json".into())).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let error = within(client.request(GetVersion)).await.unwrap_err();
    let Error::Closed(info) = error else {
        panic!("unexpected error: {error:?}");
    };
    assert_eq!(info.code, None);
    assert!(info.reason.contains("decode"), "{}", info.reason);
    within(server).await.unwrap();
}

#[tokio::test]
async fn a_frame_in_the_wrong_encoding_closes_the_connection() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.expect(REQUEST).await;
        conn.send_frame(Message::Binary(vec![1, 2, 3].into())).await;
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let error = within(client.request(GetVersion)).await.unwrap_err();
    assert!(matches!(error, Error::Closed(_)), "{error:?}");
    within(server).await.unwrap();
}

#[tokio::test]
async fn close_sends_a_normal_close_frame_and_affects_every_clone() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let Received::Close(Some(frame)) = conn.recv().await else {
            panic!("expected a close frame");
        };
        assert_eq!(u16::from(frame.code), 1000);
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let clone = client.clone();
    let mut events = client.events();
    within(client.close()).await;

    assert!(clone.is_closed());
    assert_eq!(within(clone.closed()).await.code, Some(1000));
    let error = within(clone.request(GetVersion)).await.unwrap_err();
    assert!(matches!(error, Error::Closed(_)), "{error:?}");
    assert!(matches!(
        within(events.recv()).await,
        Err(RecvError::Closed)
    ));
    // Closing again is a no-op.
    within(client.close()).await;
    within(server).await.unwrap();
}

#[tokio::test]
async fn dropping_every_clone_closes_the_connection_and_ends_event_streams() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        let Received::Close(Some(frame)) = conn.recv().await else {
            panic!("expected a close frame");
        };
        assert_eq!(u16::from(frame.code), 1000);
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    let clone = client.clone();
    let mut events = client.events();
    drop(client);
    drop(clone);
    assert!(matches!(
        within(events.recv()).await,
        Err(RecvError::Closed)
    ));
    within(server).await.unwrap();
}

#[tokio::test]
async fn close_finishes_even_if_the_server_never_replies() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        // Hold the socket open without reading, so the close handshake never completes.
        tokio::time::sleep(Duration::from_secs(3)).await;
        drop(conn);
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    within(client.close()).await;
    assert!(client.is_closed());
    server.abort();
}

#[tokio::test]
async fn answers_server_pings() {
    let (url, server) = serve(|mut conn| async move {
        conn.handshake().await;
        conn.send_frame(Message::Ping(vec![1].into())).await;
        assert!(matches!(conn.recv().await, Received::Pong), "no pong");
    })
    .await;

    let client = within(Client::connect(url)).await.unwrap();
    within(server).await.unwrap();
    drop(client);
}

#[test]
fn clients_and_their_futures_can_cross_tasks() {
    fn assert_send_sync<T: Send + Sync + Clone + 'static>() {}
    fn assert_send<T: Send>(_: T) {}

    assert_send_sync::<Client>();
    // Building the futures is enough. The compiler checks them without running them.
    fn check(client: &Client) {
        assert_send(client.request(GetVersion));
        assert_send(client.request_raw("A", None));
        assert_send(client.batch(RequestBatch::new()));
        assert_send(client.reidentify(EventSubscription::ALL));
        assert_send(client.close());
        assert_send(client.closed());
    }
    let _ = check;
}
