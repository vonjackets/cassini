
use ractor::ActorRef;
use serde::{Deserialize, Serialize};
use topic::TopicManagerMessage;
pub mod topic;
pub mod listener;
pub mod broker;

//TODO: Standardize logging messages 
pub const ACTOR_STARTUP_MSG: &str =  "Started {myself:?}";

/// Client-facing messages for the Broker.
/// These messages are serialized/deserialized to/from JSON
/// and are **not** meant to be handled directly by Actix actors.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum BrokerMessage {
    /// Registration request from the client.
    RegistrationRequest {
        client_id: String,
    },
    /// Registration response to the client.
    RegistrationResponse {
        client_id: String,
        success: bool,
        error: Option<String>, // Optional error message if registration failed
    },
    /// Publish request from the client.
    PublishRequest {
        client_id: String,
        topic: String,
        payload: String,
    },
    /// Publish response to the client.
    PublishResponse {
        topic: String,
        payload: String,
        result: Result<(), String>, // Ok for success, Err with error message
    },
    /// Subscribe request from the client.
    SubscribeRequest {
        client_id: String,
        topic: String,
    },
    /// Subscribe acknowledgment to the client.
    SubscribeAcknowledgment {
        client_id: String,
        topic: String,
        result: Result<(), String>, // Ok for success, Err with error message
    },
    /// Unsubscribe request from the client.
    UnsubscribeRequest {
        client_id: String,
        topic: String,
    },
    /// Unsubscribe acknowledgment to the client.
    UnsubscribeAcknowledgment {
        client_id: String,
        topic: String,
        result: Result<(), String>, // Ok for success, Err with error message
    },
    /// Disconnect request from the client.
    DisconnectRequest {
        client_id: String,
    },
    /// Error message to the client.
    ErrorMessage {
        client_id: String,
        error: String,
    },
    /// Ping message to the client to check connectivity.
    PingMessage {
        client_id: String,
    },
    /// Pong message received from the client in response to a ping.
    PongMessage {
        client_id: String,
    },
}

pub fn init_logging() {
    let dir = tracing_subscriber::filter::Directive::from(tracing::Level::DEBUG);

    use std::io::stderr;
    use std::io::IsTerminal;
    use tracing_glog::Glog;
    use tracing_glog::GlogFields;
    use tracing_subscriber::filter::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    let fmt = tracing_subscriber::fmt::Layer::default()
        .with_ansi(stderr().is_terminal())
        .with_writer(std::io::stderr)
        .event_format(Glog::default().with_timer(tracing_glog::LocalTime::default()))
        .fmt_fields(GlogFields::default().compact());

    let filter = vec![dir]
        .into_iter()
        .fold(EnvFilter::from_default_env(), |filter, directive| {
            filter.add_directive(directive)
        });

    let subscriber = Registry::default().with(filter).with(fmt);
    tracing::subscriber::set_global_default(subscriber).expect("to set global subscriber");
}



