//! Checks the generated types against `spec/protocol.json`, the source of truth.
//!
//! `cases.rs` only lists the generated types. Every expectation (names, fields, and which fields
//! are required) is read from the spec here, so a generator bug cannot hide in its own output.

mod cases;

use std::collections::BTreeSet;

use obsr::Event;
use obsr::enums::{
    EventSubscription, ObsMediaInputAction, ObsOutputState, RequestBatchExecutionType,
    RequestStatus, WebSocketCloseCode, WebSocketOpCode,
};
use obsr::events::RawEvent;
use serde_json::{Map, Value, json};

fn spec() -> Value {
    serde_json::from_str(include_str!("../../spec/protocol.json")).expect("spec is valid JSON")
}

fn spec_list<'a>(spec: &'a Value, key: &str) -> &'a [Value] {
    spec[key].as_array().expect("spec list")
}

fn fields<'a>(owner: &'a Value, key: &str) -> &'a [Value] {
    owner[key].as_array().expect("spec fields")
}

fn field_name(field: &Value) -> &str {
    field["valueName"].as_str().expect("field name")
}

fn is_optional(field: &Value) -> bool {
    field["valueOptional"].as_bool().unwrap_or(false)
}

fn is_nullable(field: &Value) -> bool {
    field["valueDescription"]
        .as_str()
        .is_some_and(|description| description.to_lowercase().contains("null"))
}

/// Fields the server source can send as `null` although the spec does not say so.
const SOURCE_NULLABLE: &[(&str, &str)] = &[
    ("GetOutputStatus", "outputCongestion"),
    ("GetSpecialInputs", "desktop1"),
    ("GetSpecialInputs", "desktop2"),
    ("GetSpecialInputs", "mic1"),
    ("GetSpecialInputs", "mic2"),
    ("GetSpecialInputs", "mic3"),
    ("GetSpecialInputs", "mic4"),
];

/// A valid sample value for `field`, chosen from its spec type.
fn sample(field: &Value) -> Value {
    match field_name(field) {
        "outputState" => return json!("OBS_WEBSOCKET_OUTPUT_STARTED"),
        "mediaAction" => return json!("OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PLAY"),
        _ => {}
    }
    match field["valueType"].as_str().expect("field type") {
        "String" => json!("text"),
        "Number" => json!(7),
        "Boolean" => json!(true),
        "Object" | "Any" => json!({"key": "value"}),
        "Array<Object>" => json!([{"key": "value"}]),
        "Array<String>" => json!(["text"]),
        other => panic!("spec field type not covered by this test: {other}"),
    }
}

/// Builds a JSON payload from spec fields. A dotted name such as `keyModifiers.shift` nests.
fn payload<'a>(selected: impl Iterator<Item = &'a Value>) -> Value {
    let selected: Vec<_> = selected.collect();
    let mut object = Map::new();
    for field in &selected {
        let name = field_name(field);
        if selected
            .iter()
            .any(|other| field_name(other).starts_with(&format!("{name}.")))
        {
            continue; // The parent's object is built from its dotted children.
        }
        match name.split_once('.') {
            Some((parent, child)) => {
                object
                    .entry(parent)
                    .or_insert_with(|| json!({}))
                    .as_object_mut()
                    .expect("nested object")
                    .insert(child.to_owned(), sample(field));
            }
            None => {
                object.insert(name.to_owned(), sample(field));
            }
        }
    }
    if selected.is_empty() {
        Value::Null
    } else {
        Value::Object(object)
    }
}

/// Deep equality that treats `7` and `7.0` as equal, since float fields re-encode with a fraction.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(key, value)| y.get(key).is_some_and(|other| same(value, other)))
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(l, r)| same(l, r))
        }
        _ => a == b,
    }
}

#[track_caller]
fn assert_round_trips(context: &str, round_trip: cases::RoundTrip, input: Value) {
    let output = round_trip(input.clone())
        .unwrap_or_else(|error| panic!("{context}: failed to decode {input}: {error}"));
    assert!(
        same(&input, &output),
        "{context}: sent {input}, got {output}"
    );
}

fn names(items: impl Iterator<Item = String>) -> BTreeSet<String> {
    items.collect()
}

#[test]
fn every_spec_request_is_generated_and_nothing_else() {
    let spec = spec();
    let expected = names(
        spec_list(&spec, "requests")
            .iter()
            .map(|r| r["requestType"].as_str().unwrap().to_owned()),
    );
    let actual = names(cases::REQUESTS.iter().map(|c| c.name.to_owned()));
    assert_eq!(expected, actual);
    assert_eq!(
        expected.len(),
        147,
        "spec revision changed: review the new requests"
    );
}

#[test]
fn request_type_constants_match_spec_names() {
    for case in cases::REQUESTS {
        assert_eq!(case.name, case.request_type);
    }
}

#[test]
fn every_spec_event_is_generated_and_nothing_else() {
    let spec = spec();
    let expected = names(
        spec_list(&spec, "events")
            .iter()
            .map(|e| e["eventType"].as_str().unwrap().to_owned()),
    );
    let actual = names(cases::EVENTS.iter().map(|c| c.name.to_owned()));
    assert_eq!(expected, actual);
    assert_eq!(
        expected.len(),
        60,
        "spec revision changed: review the new events"
    );
}

fn request_case(name: &str) -> &'static cases::RequestCase {
    cases::REQUESTS
        .iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("no generated case for {name}"))
}

#[test]
fn requests_carry_every_field_with_its_exact_wire_name() {
    let spec = spec();
    for request in spec_list(&spec, "requests") {
        let name = request["requestType"].as_str().unwrap();
        let input = payload(fields(request, "requestFields").iter());
        assert_round_trips(name, request_case(name).request, input);
    }
}

#[test]
fn requests_omit_unset_optional_fields_from_the_wire() {
    let spec = spec();
    for request in spec_list(&spec, "requests") {
        let name = request["requestType"].as_str().unwrap();
        let all = fields(request, "requestFields");
        let input = payload(all.iter().filter(|field| !is_optional(field)));
        // A request whose fields are all optional decodes from an empty object.
        let input = if input.is_null() && !all.is_empty() {
            json!({})
        } else {
            input
        };
        assert_round_trips(name, request_case(name).request, input);
    }
}

#[test]
fn requests_reject_missing_required_fields() {
    let spec = spec();
    let mut checked = 0;
    for request in spec_list(&spec, "requests") {
        let name = request["requestType"].as_str().unwrap();
        let all = fields(request, "requestFields");
        for required in all.iter().filter(|f| !is_optional(f) && !is_nullable(f)) {
            let without = payload(all.iter().filter(|f| field_name(f) != field_name(required)));
            let without = if without.is_null() {
                json!({})
            } else {
                without
            };
            assert!(
                (request_case(name).request)(without).is_err(),
                "{name}.{} should be required",
                field_name(required)
            );
            checked += 1;
        }
    }
    assert_eq!(
        checked, 89,
        "spec revision changed: review the required request fields"
    );
}

#[test]
fn responses_carry_every_field_with_its_exact_wire_name() {
    let spec = spec();
    for request in spec_list(&spec, "requests") {
        let name = request["requestType"].as_str().unwrap();
        let input = payload(fields(request, "responseFields").iter());
        assert_round_trips(name, request_case(name).response, input);
    }
}

#[test]
fn responses_require_fields_unless_the_spec_says_they_can_be_null() {
    let spec = spec();
    for request in spec_list(&spec, "requests") {
        let name = request["requestType"].as_str().unwrap();
        let round_trip = request_case(name).response;
        let all = fields(request, "responseFields");
        for field in all {
            let field_key = field_name(field);
            let mut object = payload(all.iter());
            let object_map = object.as_object_mut().expect("response object");
            if is_nullable(field) {
                object_map.insert(field_key.to_owned(), Value::Null);
                round_trip(object)
                    .unwrap_or_else(|e| panic!("{name}.{field_key} should allow null: {e}"));
            } else if field["valueType"] != "Any" && !SOURCE_NULLABLE.contains(&(name, field_key)) {
                object_map.remove(field_key);
                assert!(
                    round_trip(object).is_err(),
                    "{name}.{field_key} should be required"
                );
            }
        }
    }
}

#[test]
fn events_carry_every_field_and_dispatch_to_their_own_variant() {
    let spec = spec();
    for event in spec_list(&spec, "events") {
        let name = event["eventType"].as_str().unwrap();
        let case = cases::EVENTS
            .iter()
            .find(|case| case.name == name)
            .unwrap_or_else(|| panic!("no generated case for {name}"));
        let data = payload(fields(event, "dataFields").iter());
        if let Some(round_trip) = case.data {
            assert_round_trips(name, round_trip, data.clone());
        } else {
            assert!(data.is_null(), "{name} has data fields but no data type");
        }

        let parsed = Event::from_raw(RawEvent {
            event_type: name.to_owned(),
            event_intent: EventSubscription::ALL,
            event_data: (!data.is_null()).then_some(data),
        });
        assert!(
            (case.is_variant)(&parsed),
            "{name} dispatched to {parsed:?}"
        );
        assert_eq!(parsed.event_type(), name);
    }
}

#[test]
fn events_fall_back_to_raw_when_unknown_or_malformed() {
    let unknown = RawEvent {
        event_type: "FromTheFuture".to_owned(),
        event_intent: EventSubscription::GENERAL,
        event_data: Some(json!({"a": 1})),
    };
    assert_eq!(
        Event::from_raw(unknown.clone()),
        Event::Unknown(unknown.clone())
    );
    assert_eq!(Event::Unknown(unknown).event_type(), "FromTheFuture");

    let malformed = RawEvent {
        event_type: "SceneCreated".to_owned(),
        event_intent: EventSubscription::SCENES,
        event_data: Some(json!({"sceneName": 1})),
    };
    assert_eq!(
        Event::from_raw(malformed.clone()),
        Event::Unknown(malformed)
    );
}

fn enum_entries<'a>(spec: &'a Value, name: &str) -> &'a [Value] {
    spec_list(spec, "enums")
        .iter()
        .find(|e| e["enumType"] == name)
        .and_then(|e| e["enumIdentifiers"].as_array())
        .unwrap_or_else(|| panic!("no enum {name}"))
}

/// Parses an enum value that the spec writes as a number or a numeric string.
fn numeric_value(entry: &Value) -> i64 {
    entry["enumValue"]
        .as_i64()
        .or_else(|| entry["enumValue"].as_str()?.parse().ok())
        .unwrap_or_else(|| panic!("non-numeric enum value in {entry}"))
}

macro_rules! assert_numeric_enum {
    ($spec:expr, $name:ident, $repr:ty) => {
        let entries = enum_entries(&$spec, stringify!($name));
        for entry in entries {
            let code = numeric_value(entry) as $repr;
            let variant = $name::from_code(code)
                .unwrap_or_else(|| panic!("{} has no code {code}", stringify!($name)));
            assert_eq!(variant.code(), code);
            assert_eq!(
                format!("{variant:?}"),
                entry["enumIdentifier"].as_str().unwrap()
            );
        }
        let known: Vec<$repr> = entries.iter().map(|e| numeric_value(e) as $repr).collect();
        let unknown = (<$repr>::MIN..=<$repr>::MAX)
            .find(|code| !known.contains(code))
            .unwrap();
        assert_eq!($name::from_code(unknown), None);
    };
}

#[test]
fn numeric_enums_match_the_spec() {
    let spec = spec();
    assert_numeric_enum!(spec, WebSocketOpCode, u8);
    assert_numeric_enum!(spec, WebSocketCloseCode, u16);
    assert_numeric_enum!(spec, RequestStatus, u16);
    assert_numeric_enum!(spec, RequestBatchExecutionType, i8);
}

#[test]
fn batch_execution_type_serializes_as_its_number() {
    assert_eq!(
        serde_json::to_value(RequestBatchExecutionType::Parallel).unwrap(),
        json!(2)
    );
    assert_eq!(
        serde_json::from_value::<RequestBatchExecutionType>(json!(1)).unwrap(),
        RequestBatchExecutionType::SerialFrame
    );
    assert!(serde_json::from_value::<RequestBatchExecutionType>(json!(9)).is_err());
}

fn string_enum_round_trips<T>(spec: &Value, name: &str)
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    for entry in enum_entries(spec, name) {
        let wire = entry["enumValue"].clone();
        let parsed: T = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), wire);
    }
}

#[test]
fn string_enums_match_the_spec() {
    let spec = spec();
    string_enum_round_trips::<ObsOutputState>(&spec, "ObsOutputState");
    string_enum_round_trips::<ObsMediaInputAction>(&spec, "ObsMediaInputAction");
}

#[test]
fn unrecognized_output_states_decode_as_unknown() {
    let state: ObsOutputState =
        serde_json::from_value(json!("OBS_WEBSOCKET_OUTPUT_FROM_THE_FUTURE")).unwrap();
    assert_eq!(state, ObsOutputState::Unknown);
}

/// Evaluates a spec subscription expression: `0`, `(1 << n)`, or `(A | B | C)`.
fn evaluate_subscription(entries: &[Value], value: &Value) -> u64 {
    if let Some(number) = value.as_u64() {
        return number;
    }
    let text = value.as_str().expect("expression").trim_matches(['(', ')']);
    if let Some(shift) = text.strip_prefix("1 << ") {
        return 1 << shift.parse::<u64>().expect("shift");
    }
    text.split('|')
        .map(|part| {
            let entry = entries
                .iter()
                .find(|e| e["enumIdentifier"] == part.trim())
                .expect("referenced subscription");
            evaluate_subscription(entries, &entry["enumValue"])
        })
        .fold(0, |mask, bit| mask | bit)
}

#[test]
fn event_subscriptions_match_the_spec() {
    let spec = spec();
    let entries = enum_entries(&spec, "EventSubscription");
    let actual = [
        ("None", EventSubscription::NONE),
        ("General", EventSubscription::GENERAL),
        ("Config", EventSubscription::CONFIG),
        ("Scenes", EventSubscription::SCENES),
        ("Inputs", EventSubscription::INPUTS),
        ("Transitions", EventSubscription::TRANSITIONS),
        ("Filters", EventSubscription::FILTERS),
        ("Outputs", EventSubscription::OUTPUTS),
        ("SceneItems", EventSubscription::SCENE_ITEMS),
        ("MediaInputs", EventSubscription::MEDIA_INPUTS),
        ("Vendors", EventSubscription::VENDORS),
        ("Ui", EventSubscription::UI),
        ("Canvases", EventSubscription::CANVASES),
        ("All", EventSubscription::ALL),
        ("InputVolumeMeters", EventSubscription::INPUT_VOLUME_METERS),
        (
            "InputActiveStateChanged",
            EventSubscription::INPUT_ACTIVE_STATE_CHANGED,
        ),
        (
            "InputShowStateChanged",
            EventSubscription::INPUT_SHOW_STATE_CHANGED,
        ),
        (
            "SceneItemTransformChanged",
            EventSubscription::SCENE_ITEM_TRANSFORM_CHANGED,
        ),
    ];
    assert_eq!(entries.len(), actual.len());
    for (name, subscription) in actual {
        let entry = entries
            .iter()
            .find(|e| e["enumIdentifier"] == name)
            .unwrap_or_else(|| panic!("spec has no subscription {name}"));
        assert_eq!(
            subscription.bits(),
            evaluate_subscription(entries, &entry["enumValue"]),
            "{name}"
        );
    }
}

// The server handles intents as `uint64_t`, so a bit beyond 32 must not fail to decode, because
// an undecodable message closes the connection.
#[test]
fn event_intents_wider_than_32_bits_decode() {
    let raw: RawEvent = serde_json::from_value(json!({
        "eventType": "FromTheFuture",
        "eventIntent": 1_u64 << 40,
    }))
    .unwrap();
    assert_eq!(raw.event_intent.bits(), 1 << 40);
}

#[test]
fn event_subscription_flags_combine() {
    let mask = EventSubscription::SCENES | EventSubscription::INPUTS;
    assert!(mask.contains(EventSubscription::SCENES));
    assert!(!mask.contains(EventSubscription::UI));
    // A partial overlap does not count.
    assert!(!mask.contains(EventSubscription::SCENES | EventSubscription::UI));
    assert!(EventSubscription::ALL.contains(mask));
    assert!(!EventSubscription::ALL.contains(EventSubscription::INPUT_VOLUME_METERS));
    assert_eq!(
        serde_json::to_value(mask).unwrap(),
        json!((1 << 2) | (1 << 3))
    );
    assert_eq!(EventSubscription::default(), EventSubscription::ALL);
    let mut combined = EventSubscription::NONE;
    combined |= EventSubscription::UI;
    assert_eq!(combined, EventSubscription::UI);
    assert_eq!(combined & EventSubscription::UI, EventSubscription::UI);
    assert_eq!(EventSubscription::from_bits(combined.bits()), combined);
}

#[test]
fn adding_event_subscription_flags_that_are_already_set_changes_nothing() {
    let mask = EventSubscription::SCENES | EventSubscription::INPUTS;
    assert_eq!(mask | EventSubscription::SCENES, mask);
    let mut combined = mask;
    combined |= EventSubscription::SCENES;
    assert_eq!(combined, mask);
}

#[test]
fn intersecting_event_subscriptions_keeps_only_the_shared_flags() {
    let mask = EventSubscription::SCENES | EventSubscription::INPUTS;
    assert_eq!(mask & EventSubscription::SCENES, EventSubscription::SCENES);
    assert_eq!(mask & EventSubscription::UI, EventSubscription::NONE);
}

#[test]
fn nullable_request_fields_can_be_cleared_or_left_unchanged() {
    use obsr::requests::{SetProfileParameter, SetSceneSceneTransitionOverride};

    let unchanged = SetSceneSceneTransitionOverride {
        scene_name: Some("Scene".into()),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&unchanged).unwrap(),
        json!({"sceneName": "Scene"})
    );

    let cleared = SetSceneSceneTransitionOverride {
        transition_name: Some(None),
        transition_duration: Some(None),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&cleared).unwrap(),
        json!({"transitionName": null, "transitionDuration": null})
    );

    let set = SetSceneSceneTransitionOverride {
        transition_name: Some(Some("Fade".into())),
        transition_duration: Some(Some(300)),
        ..Default::default()
    };
    let wire = serde_json::to_value(&set).unwrap();
    assert_eq!(
        wire,
        json!({"transitionName": "Fade", "transitionDuration": 300})
    );
    assert_eq!(
        serde_json::from_value::<SetSceneSceneTransitionOverride>(wire).unwrap(),
        set
    );
    let wire = serde_json::to_value(&cleared).unwrap();
    assert_eq!(
        serde_json::from_value::<SetSceneSceneTransitionOverride>(wire).unwrap(),
        cleared
    );

    let delete = SetProfileParameter {
        parameter_category: "A".into(),
        parameter_name: "B".into(),
        parameter_value: None,
    };
    assert_eq!(
        serde_json::to_value(&delete).unwrap(),
        json!({"parameterCategory": "A", "parameterName": "B", "parameterValue": null})
    );
}

#[test]
fn integer_and_float_fields_are_typed_from_the_server_source() {
    use obsr::requests::{
        GetInputVolumeResponse, GetSceneItemId, GetSceneItemIdResponse, SetTBarPosition,
    };

    let id: GetSceneItemIdResponse = serde_json::from_value(json!({"sceneItemId": 12})).unwrap();
    assert_eq!(id.scene_item_id, 12_i64);

    let volume: GetInputVolumeResponse =
        serde_json::from_value(json!({"inputVolumeMul": 0.5, "inputVolumeDb": -6.02})).unwrap();
    assert_eq!(volume.input_volume_mul, 0.5_f64);

    let request = GetSceneItemId {
        search_offset: Some(-1),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&request).unwrap()["searchOffset"],
        json!(-1)
    );

    let request = SetTBarPosition {
        position: 0.25,
        release: None,
    };
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        json!({"position": 0.25})
    );
}

#[test]
fn source_nullable_fields_accept_null() {
    use obsr::requests::GetOutputStatusResponse;

    let response: GetOutputStatusResponse = serde_json::from_value(json!({
        "outputActive": true,
        "outputReconnecting": false,
        "outputTimecode": "00:00:01.000",
        "outputDuration": 1000,
        "outputCongestion": null,
        "outputBytes": 10,
        "outputSkippedFrames": 0,
        "outputTotalFrames": 60,
    }))
    .unwrap();
    assert_eq!(response.output_congestion, None);
}

// A channel with no audio device is `null`. This is what a real OBS with no devices sends.
#[test]
fn special_inputs_allow_channels_without_a_device() {
    use obsr::requests::GetSpecialInputsResponse;

    let response: GetSpecialInputsResponse = serde_json::from_value(json!({
        "desktop1": "Desktop Audio",
        "desktop2": null,
        "mic1": null,
        "mic2": null,
        "mic3": null,
        "mic4": null,
    }))
    .unwrap();
    assert_eq!(response.desktop1.as_deref(), Some("Desktop Audio"));
    assert_eq!(response.mic1, None);
}

#[test]
fn nested_key_modifiers_serialize_as_an_object() {
    use obsr::requests::{KeyModifiers, TriggerHotkeyByKeySequence};

    let request = TriggerHotkeyByKeySequence {
        key_id: Some("OBS_KEY_A".into()),
        key_modifiers: Some(KeyModifiers {
            shift: Some(true),
            ..Default::default()
        }),
    };
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        json!({"keyId": "OBS_KEY_A", "keyModifiers": {"shift": true}})
    );
}
