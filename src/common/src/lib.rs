use serde::{Deserialize, Serialize};

//TODO: Standardize logging messages 
pub const ACTOR_STARTUP_MSG: &str =  "Started {myself:?}";
pub const BROKER_NAME: &str = "BROKER_SUPERVISOR";
pub const LISTENER_MANAGER_NAME: &str = "LISTENER_MANAGER";

pub const SESSION_MANAGER_NAME: &str = "SESSION_MANAGER";

pub const TOPIC_MANAGER_NAME: &str = "TOPIC_MANAGER";

pub const SUBSCRIBER_MANAGER_NAME: &str = "SUBSCRIBER_MANAGER";
/// Internal messagetypes for the Broker.
/// 
#[derive(Debug)]
pub enum BrokerMessage {
    /// Registration request from the client.
    /// When a client connects over TCP, it cannot send messages until it receives a registrationID and a session has been created for it
    /// In the event of a disconnect, a client should be able to either resume their session by providing that registration ID, or
    /// have a new one assigned to it by sending an empty registration request
    RegistrationRequest {
    //Id for a new, potentially unauthenticated/unauthorized client client
    registration_id: Option<String>,
    client_id: String
    },
    /// Registration response to the client after attempting registration
    RegistrationResponse {
        registration_id: Option<String>, //new and final id for a client successfully registered
        client_id: String,
        success: bool,
        error: Option<String>, // Optional error message if registration failed
    },
    /// Publish request from the client.
    PublishRequest {
        registration_id: Option<String>, //TODO: Reemove option, listener checks for registration_id before forwarding
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
        registration_id: Option<String>, //TODO: Remove option
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
        registration_id: Option<String>, //TODO: Remove option
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
        client_id: String,
        registration_id: Option<String>
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
        client_id: String,
        registration_id: Option<String>, //name of the session agent that died
        error: Option<String>
    }
}

///External Messages for client comms
/// These messages are serialized/deserialized to/from JSON
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "data")]
pub enum ClientMessage {
        RegistrationRequest {
            registration_id: Option<String>,
        },
        RegistrationResponse {
            registration_id: String, //new and final id for a client successfully registered
            success: bool,
            error: Option<String>, // Optional error message if registration failed
        },
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
        SubscribeRequest(String),
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
        /// Ping message to the client to check connectivity.
        PingMessage,
        /// Pong message received from the client in response to a ping.
        PongMessage,
        ///Disconnect intentionally with a client_id
        DisconnectRequest(String),
        ///Mostly for testing purposes, intentional timeout message with a client_id
        TimeoutMessage(String)
}

impl BrokerMessage {
    pub fn from_client_message(msg: ClientMessage, client_id: String, registration_id: Option<String>) -> Self {
        match msg {
            ClientMessage::RegistrationRequest { registration_id } => {
                BrokerMessage::RegistrationRequest {
                    registration_id,
                    client_id,
                }
            },
            ClientMessage::PublishRequest { topic, payload } => {
                BrokerMessage::PublishRequest {
                    registration_id,
                    topic,
                    payload,
                }
            },
            ClientMessage::SubscribeRequest(topic) => {
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
            
            ClientMessage::DisconnectRequest(client_id) => {
                BrokerMessage::DisconnectRequest {
                    client_id,
                    registration_id
                }
            },
            ClientMessage::TimeoutMessage(registration_id) => BrokerMessage::TimeoutMessage { client_id, registration_id: Some(registration_id), error: None },
            
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


