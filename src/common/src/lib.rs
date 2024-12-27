use ractor::RpcReplyPort;
use serde::{Deserialize, Serialize};
use tracing::warn;

//TODO: Standardize logging messages 
pub const ACTOR_STARTUP_MSG: &str =  "Started {myself:?}";
pub const LISTENER_MANAGER_NAME: &str = "LISTENER_MANAGER";

pub const SESSION_MANAGER_NAME: &str = "SESSION_MANAGER";

pub const TOPIC_MANAGER_NAME: &str = "TOPIC_MANAGER";

pub const SUBSCRIBER_MANAGER_NAME: &str = "SUBSCRIBER_MANAGER";
/// Internal messagetypes for the Broker.
/// 
#[derive(Debug)]
pub enum BrokerMessage {
    /// Registration request from the client.
    RegistrationRequest {
    //Id for a new, potentially unauthenticated/unauthorized client client
    client_id: String,
    },
    /// Registration response to the client after attempting registration
    RegistrationResponse {
        registration_id: String, //new and final id for a client successfully registered
        client_id: String,
        success: bool,
        error: Option<String>, // Optional error message if registration failed
    },
    /// Publish request from the client.
    PublishRequest {
        registration_id: Option<String>, //TODO: should this be client id or registration id?
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
    // This request originates externally, so a registration_id is not added until it is received by the session
    SubscribeRequest {
        registration_id: Option<String>,
        topic: String,
    },
    /// Subscribe acknowledgment to the client.
    SubscribeAcknowledgment {
        registration_id: String,
        topic: String,
        result: Result<(), String>, // Ok for success, Err with error message
    },
    /// Unsubscribe request from the client.
    UnsubscribeRequest {
        registration_id: Option<String>,
        topic: String,
    },
    /// Unsubscribe acknowledgment to the client.
    UnsubscribeAcknowledgment {
        registration_id: String,
        topic: String,
        result: Result<(), String>, // Ok for success, Err with error message
    },
    /// Disconnect request from the client.
    DisconnectRequest {
        client_id: String, //listener id
    },
    /// Error message to the client.
    ErrorMessage {
        client_id: String,
        error: String,
    },
    /// Ping message to the client to check connectivity.
    PingMessage {
        registration_id : String,
        client_id: String
    },
    /// Pong message received from the client in response to a ping.
    PongMessage {
        registration_id: String,
    },
    TimeoutMessage {
        registration_id: String, //name of the session agent that died
    }
}

///External Messagetypes for client comms
/// These messages are serialized/deserialized to/from JSON
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum ClientMessage {
        /// Publish request from the client.
        PublishRequest {
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
        // This request originates externally, so a registration_id is not added until it is received by the session
        SubscribeRequest {
            topic: String,
        },
        /// Subscribe acknowledgment to the client.
        SubscribeAcknowledgment {
            topic: String,
            result: Result<(), String>, // Ok for success, Err with error message
        },
        /// Unsubscribe request from the client.
        UnsubscribeRequest {
            topic: String,
        },
        UnsubscribeAcknowledgment {
            topic: String,
            result: Result<(), String>
       },
        /// 
        /// Ping message to the client to check connectivity.
        PingMessage,
        /// Pong message received from the client in response to a ping.
        PongMessage
}

impl BrokerMessage {
    pub fn from_client_message(msg: ClientMessage, client_id: String, registration_id: Option<String>) -> Self {
        match msg {
            // ClientMessage::RegistrationRequest { client_id: _ } => {
            //     BrokerMessage::RegistrationRequest {
            //         client_id,
            //     }
            // },
            ClientMessage::PublishRequest { topic, payload } => {
                BrokerMessage::PublishRequest {
                    registration_id,
                    topic,
                    payload,
                }
            },
            ClientMessage::SubscribeRequest {  topic } => {
                BrokerMessage::SubscribeRequest {
                    registration_id,
                    topic,
                }
            },
            ClientMessage::UnsubscribeRequest {  topic } => {
                BrokerMessage::UnsubscribeRequest {
                    registration_id,
                    topic,
                }
            },
            
            // ClientMessage::DisconnectRequest { client_id } => {
            //     BrokerMessage::DisconnectRequest {
            //         registration_id: None,
            //         client_id
            //     }
            // },
            // ClientMessage::PingMessage { client_id } => {
            //     BrokerMessage::PingMessage { client_id }
            // },
            // Handle unexpected messages
            _ => {
                todo!()
            }
        }
    }
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


