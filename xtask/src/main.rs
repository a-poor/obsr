//! Generates the typed protocol code from `spec/protocol.json`.
//!
//! Run `cargo xtask` after updating the spec. `cargo test -p xtask` fails if the checked-in output
//! is stale.
//!
//! The spec leaves a few things implicit or wrong, so this generator corrects them from the
//! obs-websocket server source: whether a `Number` is an integer or a float, which string fields
//! hold a spec enum, nullable fields the spec does not mention, and the shape of `CustomEvent`. A
//! new `Number` field fails generation until it is classified below.

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Deserialize;

/// `Number` fields that the server handles as integers.
const INTEGER_FIELDS: &[&str] = &[
    "baseHeight",
    "baseWidth",
    "filterIndex",
    "fpsDenominator",
    "fpsNumerator",
    "imageCompressionQuality",
    "imageHeight",
    "imageWidth",
    "inputAudioSyncOffset",
    "inputKindCaps",
    "mediaCursor",
    "mediaCursorOffset",
    "mediaDuration",
    "monitorIndex",
    "outputBytes",
    "outputDuration",
    "outputHeight",
    "outputSkippedFrames",
    "outputTotalFrames",
    "outputWidth",
    "renderSkippedFrames",
    "renderTotalFrames",
    "rpcVersion",
    "sceneItemId",
    "sceneItemIndex",
    "searchOffset",
    "sleepFrames",
    "sleepMillis",
    "transitionDuration",
    "webSocketSessionIncomingMessages",
    "webSocketSessionOutgoingMessages",
];

/// `Number` fields that the server handles as floats.
const FLOAT_FIELDS: &[&str] = &[
    "activeFps",
    "availableDiskSpace",
    "averageFrameRenderTime",
    "cpuUsage",
    "inputAudioBalance",
    "inputVolumeDb",
    "inputVolumeMul",
    "memoryUsage",
    "outputCongestion",
    "position",
    "transitionCursor",
];

/// String fields whose values come from a spec enum.
const ENUM_FIELDS: &[(&str, &str)] = &[
    ("mediaAction", "ObsMediaInputAction"),
    ("outputState", "ObsOutputState"),
];

/// Response fields that can be `null` even though the spec does not say so. `GetOutputStatus` passes
/// the raw congestion through, and the JSON encoder writes a NaN as `null`. `GetSpecialInputs` sends
/// `null` for a channel with no device.
const EXTRA_NULLABLE_FIELDS: &[(&str, &str)] = &[
    ("GetOutputStatus", "outputCongestion"),
    ("GetSpecialInputs", "desktop1"),
    ("GetSpecialInputs", "desktop2"),
    ("GetSpecialInputs", "mic1"),
    ("GetSpecialInputs", "mic2"),
    ("GetSpecialInputs", "mic3"),
    ("GetSpecialInputs", "mic4"),
];

/// Events whose data is the single field's value, not an object holding that field. The spec
/// documents `CustomEvent` with an `eventData` field, but the server sends the custom object itself
/// as the event's data.
const UNWRAPPED_EVENTS: &[&str] = &["CustomEvent"];

/// Item docs and the wire representation for each numeric spec enum.
const NUMERIC_ENUMS: &[(&str, &str, &str)] = &[
    (
        "WebSocketOpCode",
        "u8",
        "The operation code (`op`) of a protocol message.",
    ),
    (
        "WebSocketCloseCode",
        "u16",
        "The code the server uses when it closes a connection.",
    ),
    (
        "RequestBatchExecutionType",
        "i8",
        "How the server executes a request batch.",
    ),
    ("RequestStatus", "u16", "The outcome code of a request."),
];

/// Item docs for each string spec enum. The spec's first entry is also the decoding fallback.
const STRING_ENUMS: &[(&str, &str)] = &[
    (
        "ObsOutputState",
        "The state of an output, as reported by output events.",
    ),
    (
        "ObsMediaInputAction",
        "An action that can be performed on a media input.",
    ),
];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Spec {
    enums: Vec<EnumSpec>,
    requests: Vec<RequestSpec>,
    events: Vec<EventSpec>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnumSpec {
    enum_type: String,
    enum_identifiers: Vec<EnumEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EnumEntry {
    description: String,
    enum_identifier: String,
    deprecated: bool,
    initial_version: String,
    enum_value: serde_json::Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RequestSpec {
    description: String,
    request_type: String,
    deprecated: bool,
    initial_version: String,
    category: String,
    request_fields: Vec<FieldSpec>,
    response_fields: Vec<FieldSpec>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventSpec {
    description: String,
    event_type: String,
    deprecated: bool,
    initial_version: String,
    category: String,
    data_fields: Vec<FieldSpec>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FieldSpec {
    value_name: String,
    value_type: String,
    value_description: String,
    value_restrictions: Option<String>,
    #[serde(default)]
    value_optional: bool,
    value_optional_behavior: Option<String>,
}

impl FieldSpec {
    fn mentions_null(&self) -> bool {
        self.value_description.to_lowercase().contains("null")
    }
}

/// Which side of the protocol a struct belongs to. Only requests are sent by the client, so only
/// their optional fields must be left off the wire when unset.
#[derive(Clone, Copy, PartialEq)]
enum Direction {
    ToServer,
    FromServer,
}

/// All generated files, as `(path relative to the workspace root, formatted contents)`.
fn generate(spec: &Spec) -> Vec<(&'static str, String)> {
    [
        ("src/enums/generated.rs", enums_file(&spec.enums)),
        ("src/requests/generated.rs", requests_file(&spec.requests)),
        ("src/events/generated.rs", events_file(&spec.events)),
        ("tests/conformance/cases.rs", cases_file(spec)),
    ]
    .into_iter()
    .map(|(path, contents)| (path, rustfmt(&contents)))
    .collect()
}

const HEADER: &str = "// @generated by `cargo xtask` from spec/protocol.json. Do not edit.\n\n";

fn enums_file(enums: &[EnumSpec]) -> String {
    let mut out = String::from(HEADER);
    for &(name, repr, docs) in NUMERIC_ENUMS {
        numeric_enum(&mut out, find_enum(enums, name), repr, docs);
    }
    for &(name, docs) in STRING_ENUMS {
        string_enum(&mut out, find_enum(enums, name), docs);
    }
    subscription_consts(&mut out, find_enum(enums, "EventSubscription"));
    out
}

fn find_enum<'a>(enums: &'a [EnumSpec], name: &str) -> &'a EnumSpec {
    enums
        .iter()
        .find(|e| e.enum_type == name)
        .unwrap_or_else(|| panic!("spec has no enum {name}"))
}

fn numeric_enum(out: &mut String, spec: &EnumSpec, repr: &str, docs: &str) {
    let name = &spec.enum_type;
    doc(out, docs);
    writeln!(out, "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]").unwrap();
    writeln!(
        out,
        "#[repr({repr})]\n#[non_exhaustive]\npub enum {name} {{"
    )
    .unwrap();
    let mut arms = String::new();
    for entry in &spec.enum_identifiers {
        let variant = &entry.enum_identifier;
        let code = numeric_code(entry);
        entry_doc(out, entry);
        writeln!(out, "{variant} = {code},").unwrap();
        writeln!(arms, "{code} => Some(Self::{variant}),").unwrap();
    }
    writeln!(
        out,
        "}}

impl {name} {{
    /// The numeric value used on the wire.
    pub const fn code(self) -> {repr} {{
        self as {repr}
    }}

    /// Looks up a variant by its wire value. Returns `None` for values this crate does not know.
    pub const fn from_code(code: {repr}) -> Option<Self> {{
        match code {{
            {arms}
            _ => None,
        }}
    }}
}}

impl serde::Serialize for {name} {{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {{
        serializer.serialize_i64(i64::from(self.code()))
    }}
}}

impl<'de> serde::Deserialize<'de> for {name} {{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {{
        let code = {repr}::deserialize(deserializer)?;
        Self::from_code(code)
            .ok_or_else(|| serde::de::Error::custom(format!(\"unknown {name} code {{code}}\")))
    }}
}}
"
    )
    .unwrap();
}

fn numeric_code(entry: &EnumEntry) -> i64 {
    entry
        .enum_value
        .as_i64()
        .or_else(|| entry.enum_value.as_str()?.parse().ok())
        .unwrap_or_else(|| panic!("{} is not numeric", entry.enum_identifier))
}

fn string_enum(out: &mut String, spec: &EnumSpec, docs: &str) {
    let prefix = common_prefix(spec);
    doc(out, docs);
    writeln!(
        out,
        "#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]"
    )
    .unwrap();
    writeln!(out, "#[non_exhaustive]\npub enum {} {{", spec.enum_type).unwrap();
    // serde requires the `other` fallback to be the last variant, so the spec's first entry moves.
    let (fallback, rest) = spec.enum_identifiers.split_first().expect("non-empty enum");
    for entry in rest.iter().chain([fallback]) {
        let wire = entry.enum_value.as_str().expect("string enum value");
        entry_doc(out, entry);
        if std::ptr::eq(entry, fallback) {
            writeln!(
                out,
                "///\n/// Values this crate does not know also decode to this variant."
            )
            .unwrap();
            writeln!(out, "#[default]").unwrap();
            writeln!(out, "#[serde(rename = \"{wire}\", other)]").unwrap();
        } else {
            writeln!(out, "#[serde(rename = \"{wire}\")]").unwrap();
        }
        let variant = screaming_to_pascal(&entry.enum_identifier[prefix.len()..]);
        writeln!(out, "{variant},").unwrap();
    }
    writeln!(out, "}}\n").unwrap();
}

/// The longest `_`-terminated prefix shared by all identifiers of an enum.
fn common_prefix(spec: &EnumSpec) -> String {
    let first = &spec.enum_identifiers[0].enum_identifier;
    let mut end = 0;
    for (index, _) in first.match_indices('_') {
        let candidate = &first[..=index];
        if spec
            .enum_identifiers
            .iter()
            .all(|e| e.enum_identifier.starts_with(candidate))
        {
            end = index + 1;
        }
    }
    first[..end].to_owned()
}

fn subscription_consts(out: &mut String, spec: &EnumSpec) {
    writeln!(out, "impl super::EventSubscription {{").unwrap();
    for entry in &spec.enum_identifiers {
        entry_doc(out, entry);
        let name = pascal_to_screaming(&entry.enum_identifier);
        let value = match &entry.enum_value {
            serde_json::Value::Number(number) => number.to_string(),
            serde_json::Value::String(text) if text.contains('|') => text
                .trim_matches(['(', ')'])
                .split('|')
                .map(|part| format!("Self::{}.0", pascal_to_screaming(part.trim())))
                .collect::<Vec<_>>()
                .join(" | "),
            serde_json::Value::String(text) => text.trim_matches(['(', ')']).to_owned(),
            other => panic!("unsupported subscription value {other}"),
        };
        writeln!(out, "pub const {name}: Self = Self({value});").unwrap();
    }
    writeln!(out, "}}").unwrap();
}

fn requests_file(requests: &[RequestSpec]) -> String {
    let mut out = String::from(HEADER);
    out.push_str(
        "#[allow(unused_imports)]
use super::{Empty, Request, double_option};
#[allow(unused_imports)]
use crate::enums::{ObsMediaInputAction, ObsOutputState};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use serde_json::Value;

",
    );
    let mut category = "";
    for request in requests {
        if request.category != category {
            category = &request.category;
            writeln!(out, "// Category: {category}\n").unwrap();
        }
        let name = &request.request_type;
        let has_response = !request.response_fields.is_empty();
        let response = if has_response {
            format!("{name}Response")
        } else {
            "Empty".to_owned()
        };

        let mut docs = request.description.clone();
        if has_response {
            write!(docs, "\n\nResponds with [`{response}`].").unwrap();
        }
        since(&mut docs, &request.initial_version);
        record(
            &mut out,
            &Record {
                name,
                docs: &docs,
                deprecated: request.deprecated,
                fields: &request.request_fields,
                direction: Direction::ToServer,
                derive_default: true,
                owner: name,
            },
        );
        writeln!(
            out,
            "impl Request for {name} {{
    const REQUEST_TYPE: &'static str = \"{name}\";
    type Response = {response};
}}
"
        )
        .unwrap();

        if has_response {
            let docs = format!("The response to a [`{name}`] request.");
            record(
                &mut out,
                &Record {
                    name: &response,
                    docs: &docs,
                    deprecated: request.deprecated,
                    fields: &request.response_fields,
                    direction: Direction::FromServer,
                    derive_default: false,
                    owner: name,
                },
            );
        }
    }
    out
}

fn events_file(events: &[EventSpec]) -> String {
    let mut out = String::from(HEADER);
    out.push_str(
        "#[allow(unused_imports)]
use crate::enums::{ObsMediaInputAction, ObsOutputState};
use serde::{Deserialize, Serialize};
#[allow(unused_imports)]
use serde_json::Value;
use super::{RawEvent, parse_data};

",
    );

    let mut variants = String::new();
    let mut parse_arms = String::new();
    let mut type_arms = String::new();
    for event in events {
        let name = &event.event_type;
        let mut docs = event.description.clone();
        since(&mut docs, &event.initial_version);
        let attr = if event.deprecated {
            "#[deprecated]\n"
        } else {
            ""
        };

        doc(&mut variants, &docs);
        variants.push_str(attr);
        if event.data_fields.is_empty() {
            writeln!(variants, "{name},").unwrap();
            writeln!(parse_arms, "\"{name}\" => Some(Self::{name}),").unwrap();
            writeln!(type_arms, "Self::{name} => \"{name}\",").unwrap();
        } else {
            writeln!(variants, "{name}({name}),").unwrap();
            writeln!(
                parse_arms,
                "\"{name}\" => parse_data(&raw).map(Self::{name}),"
            )
            .unwrap();
            writeln!(type_arms, "Self::{name}(_) => \"{name}\",").unwrap();
        }
    }

    writeln!(
        out,
        "/// An event sent by obs-websocket.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Event {{
{variants}
    /// An event this crate has no type for, or whose data did not match the spec. The original
    /// payload is preserved.
    Unknown(RawEvent),
}}

impl Event {{
    /// Converts a raw event into its typed variant, or [`Event::Unknown`] if that is not possible.
    pub fn from_raw(raw: RawEvent) -> Self {{
        let parsed = match raw.event_type.as_str() {{
            {parse_arms}
            _ => None,
        }};
        parsed.unwrap_or(Self::Unknown(raw))
    }}

    /// The spec name of this event, such as `\"CurrentProgramSceneChanged\"`.
    pub fn event_type(&self) -> &str {{
        match self {{
            {type_arms}
            Self::Unknown(raw) => &raw.event_type,
        }}
    }}
}}
"
    )
    .unwrap();

    let mut category = "";
    for event in events {
        if event.data_fields.is_empty() {
            continue;
        }
        if event.category != category {
            category = &event.category;
            writeln!(out, "// Category: {category}\n").unwrap();
        }
        let name = &event.event_type;
        let mut docs = event.description.clone();
        since(&mut docs, &event.initial_version);
        record(
            &mut out,
            &Record {
                name,
                docs: &docs,
                deprecated: event.deprecated,
                fields: &event.data_fields,
                direction: Direction::FromServer,
                derive_default: false,
                owner: name,
            },
        );
    }
    out
}

fn cases_file(spec: &Spec) -> String {
    let mut out = String::from(HEADER);
    out.push_str(
        "use obsr::Event;
use obsr::events;
use obsr::requests::{self, Request};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

/// Decodes a payload into a generated type and encodes it again.
pub type RoundTrip = fn(Value) -> Result<Value, serde_json::Error>;

fn round_trip<T: DeserializeOwned + Serialize>(value: Value) -> Result<Value, serde_json::Error> {
    serde_json::to_value(serde_json::from_value::<T>(value)?)
}

pub struct RequestCase {
    pub name: &'static str,
    pub request_type: &'static str,
    pub request: RoundTrip,
    pub response: RoundTrip,
}

pub struct EventCase {
    pub name: &'static str,
    pub data: Option<RoundTrip>,
    pub is_variant: fn(&Event) -> bool,
}

pub const REQUESTS: &[RequestCase] = &[
",
    );
    for request in &spec.requests {
        let name = &request.request_type;
        writeln!(
            out,
            "RequestCase {{
    name: \"{name}\",
    request_type: requests::{name}::REQUEST_TYPE,
    request: round_trip::<requests::{name}>,
    response: round_trip::<<requests::{name} as Request>::Response>,
}},"
        )
        .unwrap();
    }
    out.push_str("];\n\npub const EVENTS: &[EventCase] = &[\n");
    for event in &spec.events {
        let name = &event.event_type;
        if event.data_fields.is_empty() {
            writeln!(
                out,
                "EventCase {{
    name: \"{name}\",
    data: None,
    is_variant: |event| matches!(event, Event::{name}),
}},"
            )
            .unwrap();
        } else {
            writeln!(
                out,
                "EventCase {{
    name: \"{name}\",
    data: Some(round_trip::<events::{name}>),
    is_variant: |event| matches!(event, Event::{name}(_)),
}},"
            )
            .unwrap();
        }
    }
    out.push_str("];\n");
    out
}

/// One generated struct.
struct Record<'a> {
    name: &'a str,
    docs: &'a str,
    deprecated: bool,
    fields: &'a [FieldSpec],
    direction: Direction,
    derive_default: bool,
    /// The spec name of the request or event this struct belongs to.
    owner: &'a str,
}

fn record(out: &mut String, record: &Record<'_>) {
    let Record {
        name,
        docs,
        deprecated,
        fields,
        direction,
        derive_default,
        owner,
    } = *record;
    let derives = if derive_default {
        "Debug, Clone, Default, PartialEq, Serialize, Deserialize"
    } else {
        "Debug, Clone, PartialEq, Serialize, Deserialize"
    };

    // Children of a dotted field such as `keyModifiers.shift` get their own struct.
    let is_nested = |field: &FieldSpec| field.value_name.contains('.');
    let parents: Vec<&str> = fields
        .iter()
        .filter(|field| !is_nested(field))
        .filter(|field| {
            fields.iter().any(|other| {
                other
                    .value_name
                    .starts_with(&format!("{}.", field.value_name))
            })
        })
        .map(|field| field.value_name.as_str())
        .collect();

    doc(out, docs);
    if deprecated {
        writeln!(out, "#[deprecated]").unwrap();
    }
    writeln!(out, "#[derive({derives})]").unwrap();
    if fields.is_empty() {
        writeln!(out, "pub struct {name};\n").unwrap();
        return;
    }
    let container = if UNWRAPPED_EVENTS.contains(&owner) {
        "transparent"
    } else {
        "rename_all = \"camelCase\""
    };
    writeln!(out, "#[serde({container})]\npub struct {name} {{").unwrap();
    for field in fields.iter().filter(|field| !is_nested(field)) {
        let base = if parents.contains(&field.value_name.as_str()) {
            camel_to_pascal(&field.value_name)
        } else {
            base_type(field)
        };
        struct_field(out, field, &field.value_name, &base, direction, owner);
    }
    writeln!(out, "}}\n").unwrap();

    for parent in parents {
        let prefix = format!("{parent}.");
        let struct_name = camel_to_pascal(parent);
        let description = fields
            .iter()
            .find(|field| field.value_name == parent)
            .map_or("", |field| field.value_description.as_str());
        doc(out, description);
        writeln!(out, "#[derive({derives})]").unwrap();
        writeln!(
            out,
            "#[serde(rename_all = \"camelCase\")]\npub struct {struct_name} {{"
        )
        .unwrap();
        for field in fields
            .iter()
            .filter(|field| field.value_name.starts_with(&prefix))
        {
            let child = &field.value_name[prefix.len()..];
            struct_field(out, field, child, &base_type(field), direction, owner);
        }
        writeln!(out, "}}\n").unwrap();
    }
}

fn struct_field(
    out: &mut String,
    field: &FieldSpec,
    wire_name: &str,
    base: &str,
    direction: Direction,
    owner: &str,
) {
    let mut docs = field.value_description.clone();
    if let Some(restrictions) = &field.value_restrictions {
        write!(docs, "\n\nRestrictions: `{restrictions}`.").unwrap();
    }
    if let Some(behavior) = field.value_optional_behavior.as_deref()
        && field.value_optional
        && behavior != "Unknown"
    {
        write!(docs, "\n\nIf omitted: {behavior}.").unwrap();
    }

    let nullable = field.mentions_null();
    let mut attrs = Vec::new();
    let ty = match direction {
        Direction::ToServer if field.value_optional && nullable => {
            docs.push_str("\n\n`None` leaves this unchanged and `Some(None)` removes it.");
            attrs.push(
                "default, skip_serializing_if = \"Option::is_none\", deserialize_with = \"double_option\"",
            );
            format!("Option<Option<{base}>>")
        }
        Direction::ToServer if field.value_optional => {
            attrs.push("skip_serializing_if = \"Option::is_none\"");
            format!("Option<{base}>")
        }
        Direction::ToServer if nullable => format!("Option<{base}>"),
        Direction::ToServer => base.to_owned(),
        Direction::FromServer
            if nullable || EXTRA_NULLABLE_FIELDS.contains(&(owner, field.value_name.as_str())) =>
        {
            format!("Option<{base}>")
        }
        Direction::FromServer => base.to_owned(),
    };

    let rust_name = camel_to_snake(wire_name);
    let rename =
        (snake_to_camel(&rust_name) != wire_name).then(|| format!("rename = \"{wire_name}\""));

    doc(out, &docs);
    for attr in rename.as_deref().into_iter().chain(attrs) {
        writeln!(out, "#[serde({attr})]").unwrap();
    }
    writeln!(out, "pub {}: {ty},", escape_keyword(&rust_name)).unwrap();
}

fn base_type(field: &FieldSpec) -> String {
    let name = field.value_name.as_str();
    if let Some((_, enum_type)) = ENUM_FIELDS.iter().find(|(field, _)| *field == name) {
        return (*enum_type).to_owned();
    }
    match field.value_type.as_str() {
        "String" => "String",
        "Boolean" => "bool",
        "Object" | "Any" => "Value",
        "Array<Object>" => "Vec<Value>",
        "Array<String>" => "Vec<String>",
        "Number" if INTEGER_FIELDS.contains(&name) => "i64",
        "Number" if FLOAT_FIELDS.contains(&name) => "f64",
        "Number" => panic!(
            "classify the new Number field `{name}` in INTEGER_FIELDS or FLOAT_FIELDS, \
             using how the server source reads or writes it"
        ),
        other => panic!("unsupported spec type `{other}` on field `{name}`"),
    }
    .to_owned()
}

fn entry_doc(out: &mut String, entry: &EnumEntry) {
    let mut docs = entry.description.clone();
    since(&mut docs, &entry.initial_version);
    doc(out, &docs);
    if entry.deprecated {
        writeln!(out, "#[deprecated]").unwrap();
    }
}

/// Notes the version that added an item, except for items present since the 5.0.0 baseline.
fn since(docs: &mut String, version: &str) {
    if version != "5.0.0" {
        write!(docs, "\n\nAdded in obs-websocket {version}.").unwrap();
    }
}

fn doc(out: &mut String, text: &str) {
    for line in text.lines() {
        writeln!(out, "/// {}", link_bare_urls(line.trim_end())).unwrap();
    }
}

/// Wraps bare URLs in angle brackets, which rustdoc requires to render them as links.
fn link_bare_urls(line: &str) -> String {
    line.split(' ')
        .map(|word| {
            if word.starts_with("http://") || word.starts_with("https://") {
                format!("<{word}>")
            } else {
                word.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn camel_to_snake(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for character in name.chars() {
        if character.is_ascii_uppercase() {
            out.push('_');
            out.push(character.to_ascii_lowercase());
        } else {
            out.push(character);
        }
    }
    out
}

/// Mirrors serde's `rename_all = "camelCase"`, to detect names that need an explicit rename.
fn snake_to_camel(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = false;
    for character in name.chars() {
        if character == '_' {
            upper = true;
        } else if upper {
            out.push(character.to_ascii_uppercase());
            upper = false;
        } else {
            out.push(character);
        }
    }
    out
}

fn camel_to_pascal(name: &str) -> String {
    let mut characters = name.chars();
    let first = characters.next().expect("non-empty name");
    first.to_ascii_uppercase().to_string() + characters.as_str()
}

fn screaming_to_pascal(name: &str) -> String {
    name.split('_')
        .map(|word| camel_to_pascal(&word.to_ascii_lowercase()))
        .collect()
}

fn pascal_to_screaming(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (index, character) in name.chars().enumerate() {
        if index > 0 && character.is_ascii_uppercase() {
            out.push('_');
        }
        out.push(character.to_ascii_uppercase());
    }
    out
}

fn escape_keyword(name: &str) -> String {
    const KEYWORDS: &[&str] = &[
        "as", "async", "await", "box", "break", "const", "continue", "crate", "do", "dyn", "else",
        "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl", "in", "let", "loop",
        "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref", "return", "self",
        "static", "struct", "super", "trait", "true", "try", "type", "typeof", "unsafe", "unsized",
        "use", "virtual", "where", "while", "yield",
    ];
    if KEYWORDS.contains(&name) {
        format!("r#{name}")
    } else {
        name.to_owned()
    }
}

fn rustfmt(source: &str) -> String {
    let mut child = Command::new("rustfmt")
        .args(["--edition", "2024"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start rustfmt");
    let mut stdin = child.stdin.take().expect("rustfmt stdin");
    let input = source.to_owned();
    let writer = std::thread::spawn(move || {
        stdin
            .write_all(input.as_bytes())
            .expect("failed to write to rustfmt");
    });
    let output = child.wait_with_output().expect("failed to run rustfmt");
    writer.join().expect("rustfmt writer panicked");
    assert!(
        output.status.success(),
        "rustfmt rejected the generated code:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("rustfmt output is UTF-8")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask lives in the workspace root")
        .to_owned()
}

fn load_spec(root: &Path) -> Spec {
    let path = root.join("spec/protocol.json");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    serde_json::from_str(&text).expect("failed to parse spec/protocol.json")
}

fn main() {
    let root = workspace_root();
    let spec = load_spec(&root);
    for (path, contents) in generate(&spec) {
        let path = root.join(path);
        std::fs::create_dir_all(path.parent().expect("generated files have a parent"))
            .expect("failed to create output directory");
        std::fs::write(&path, contents).expect("failed to write generated file");
        println!("wrote {}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_output_is_up_to_date() {
        let root = workspace_root();
        for (path, expected) in generate(&load_spec(&root)) {
            let actual = std::fs::read_to_string(root.join(path))
                .unwrap_or_else(|error| panic!("failed to read {path}: {error}"));
            assert!(actual == expected, "{path} is stale: run `cargo xtask`");
        }
    }

    #[test]
    fn number_classification_has_no_overlap_or_leftovers() {
        let spec = load_spec(&workspace_root());
        for name in INTEGER_FIELDS {
            assert!(!FLOAT_FIELDS.contains(name), "{name} is in both lists");
        }
        let mut used: Vec<&str> = spec
            .requests
            .iter()
            .flat_map(|r| r.request_fields.iter().chain(&r.response_fields))
            .chain(spec.events.iter().flat_map(|e| &e.data_fields))
            .filter(|field| field.value_type == "Number")
            .map(|field| field.value_name.as_str())
            .collect();
        used.sort_unstable();
        used.dedup();
        let mut listed: Vec<&str> = INTEGER_FIELDS.iter().chain(FLOAT_FIELDS).copied().collect();
        listed.sort_unstable();
        assert_eq!(
            listed, used,
            "the lists must match the spec's Number fields"
        );
    }

    #[test]
    fn bare_urls_become_links() {
        assert_eq!(
            link_bare_urls("See https://example.com/a for `x`"),
            "See <https://example.com/a> for `x`"
        );
        assert_eq!(link_bare_urls("No links here"), "No links here");
    }

    #[test]
    fn naming_helpers_convert_between_cases() {
        assert_eq!(
            camel_to_snake("webSocketSessionIncomingMessages"),
            "web_socket_session_incoming_messages"
        );
        assert_eq!(snake_to_camel("input_volume_db"), "inputVolumeDb");
        assert_eq!(camel_to_pascal("keyModifiers"), "KeyModifiers");
        assert_eq!(screaming_to_pascal("RECONNECTED"), "Reconnected");
        assert_eq!(
            pascal_to_screaming("SceneItemTransformChanged"),
            "SCENE_ITEM_TRANSFORM_CHANGED"
        );
        assert_eq!(escape_keyword("type"), "r#type");
        assert_eq!(escape_keyword("name"), "name");
    }
}
