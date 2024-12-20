use serde::{Deserialize, Serialize};

//TODO: Standardize logging messages 
pub const ACTOR_STARTUP_MSG: &str =  "Started {myself:?}";


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PublishRequest {
    pub client_id: String,
    pub topic: String,
    pub payload: String, //TODO: use Polar MessageType instead
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeRequest {
    pub client_id: String,
    //pub client_ref: ActorRef<ListenerMessage>,//TODO: can't deserialize, just use client_ids
    pub topic: String,
}

/// Represents the response to a registration attempt.
// #[rtype(result = "()")]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BrokerResponse {
    Ok(String),    // registration_id
    Err(String),   // error message
}
