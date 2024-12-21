use serde::{Deserialize, Serialize};

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
        client_id: String, //Id for a new, potentially unauthenticated/unauthorized client client
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
        registration_id: String, //TODO: should this be client id or registration id?
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
        registration_id: String,
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
        registration_id: String, // session agent id that disconnected
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
        // ping_count: usize,
    },
    /// Pong message received from the client in response to a ping.
    PongMessage {
        registration_id: String,
    },
    TimeoutMessage {
        registration_id: String, //name of the session agent that died
    }
}