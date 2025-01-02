use tracing::{info, warn};
use crate::{listener::{ListenerManager, ListenerManagerArgs}, session::{SessionManager, SessionManagerArgs}, subscriber::SubscriberManager, topic::{TopicManager, TopicManagerArgs}, UNEXPECTED_MESSAGE_STR};
use crate::{BrokerMessage, LISTENER_MANAGER_NAME, SESSION_MANAGER_NAME, SUBSCRIBER_MANAGER_NAME, TOPIC_MANAGER_NAME};
use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};


// ============================== Broker Supervisor Actor Definition ============================== //

pub struct Broker;

//State object containing references to worker actors.
pub struct BrokerState;
/// Would-be configurations for the broker actor itself, as well its workers
#[derive(Clone)]
pub struct BrokerArgs {
    /// Socket address to listen for connections on
    pub bind_addr: String,
    /// The amount of time (in seconds) before a session times out
    /// Should be between 10 and 300 seconds
    pub session_timeout: Option<u64>,
    pub server_cert_file: String,
    pub private_key_file: String,
    pub ca_cert_file: String
}

#[async_trait]
impl Actor for Broker {
    type Msg = BrokerMessage;
    type State = BrokerState;
    type Arguments = BrokerArgs;
    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: BrokerArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        tracing::info!("Broker: Starting {myself:?}");
        
        let listener_manager_args =  ListenerManagerArgs {
            bind_addr: args.bind_addr,
            server_cert_file: args.server_cert_file,
            private_key_file: args.private_key_file,
            ca_cert_file: args.ca_cert_file
        };

        Actor::spawn(Some(LISTENER_MANAGER_NAME.to_owned()), ListenerManager, listener_manager_args).await.expect("Expected to start Listener Manager");


        //set default timeout for sessions, or use args
        let mut session_timeout: u64 = 90;
        if let Some(timeout) = args.session_timeout {
            session_timeout = timeout;
        }

        Actor::spawn(Some(SESSION_MANAGER_NAME.to_string()), SessionManager, SessionManagerArgs { session_timeout: session_timeout }).await.expect("Expected Session Manager to start");

        //TODO: Read some topics from configuration based on services we want to observer/consumer messages for
        let topic_mgr_args = TopicManagerArgs {topics: None};

        Actor::spawn(Some(TOPIC_MANAGER_NAME.to_owned()), TopicManager, topic_mgr_args).await.expect("Expected to start Topic Manager");    

        Actor::spawn(Some(SUBSCRIBER_MANAGER_NAME.to_string()), SubscriberManager, ()).await.expect("Expected to start Subscriber Manager");

        let state = BrokerState;
        Ok(state)
    }

    async fn handle_supervisor_evt(&self, _myself: ActorRef<Self::Msg>, msg: SupervisionEvent, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match msg {
            SupervisionEvent::ActorStarted(actor_cell) => info!("Worker agent: {0:?}:{1:?} started", actor_cell.get_name(), actor_cell.get_id()),
            
            SupervisionEvent::ActorTerminated(actor_cell, ..) => info!("Worker {0:?}:{1:?} terminated, restarting..", actor_cell.get_name(), actor_cell.get_id()),
            
            SupervisionEvent::ActorFailed(actor_cell, e) => {
                warn!("Worker agent: {0:?}:{1:?} failed! {e}", actor_cell.get_name(), actor_cell.get_id());
                //determine type of actor that failed and restart
                //NOTE: "Remote" actors can't have their types checked? But they do send serializable messages
                // If we can deserialize them to a datatype here, that may be another acceptable means of determining type
                //TODO: Figure out what panics/failures we can/can't recover from
                // Missing certificate files and the inability to forward some messages count as bad states
                _myself.stop(Some("ACTOR_FAILED".to_string()));

            },
            SupervisionEvent::ProcessGroupChanged(_) => (),
        }

        Ok(())
    }
    
    async fn post_start(&self, myself: ActorRef<Self::Msg>, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
        info!("Broker: Started {myself:?}");
        Ok(())
    }
    
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        _: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        //TODO: Implement? The master node seems purely responsible for managing actor lifecycles, doesn't really do any message brokering on its own
        match message {
            BrokerMessage::RegistrationRequest { registration_id, client_id } => {                
                match &where_is(SESSION_MANAGER_NAME.to_string())
                {
                    Some(session_mgr) => {
                        session_mgr.send_message(BrokerMessage::RegistrationRequest { registration_id, client_id }).expect("Failed to forward registration response to listener manager");
                    }, 
                    None => todo!()
                }
            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                where_is(SUBSCRIBER_MANAGER_NAME.to_owned()).unwrap().send_message(BrokerMessage::SubscribeRequest { registration_id: registration_id.clone(), topic: topic.clone() }).expect("Failed to forward subscribeRequest to subscriber manager");
                match where_is(TOPIC_MANAGER_NAME.to_owned()) {
                    Some(actor) => {
                        actor.send_message(BrokerMessage::SubscribeRequest { registration_id, topic }).expect("Failed to forward subscribeRequest to topic manager");
                    },
                    None => todo!(),
                }
            },
            BrokerMessage::UnsubscribeRequest { registration_id, topic } => {
                where_is(SUBSCRIBER_MANAGER_NAME.to_owned()).unwrap().send_message(BrokerMessage::UnsubscribeRequest { registration_id: registration_id.clone(), topic: topic.clone() }).expect("Failed to forward request to subscriber manager");
            }
            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, .. } => {
                where_is(registration_id.clone()).map(|session_agent_ref|{
                    session_agent_ref.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id: registration_id.clone(), topic: topic.clone(), result: Ok(()) }).unwrap();
                });
            },
            BrokerMessage::PublishRequest { registration_id, topic, payload } => {
                //send to topic manager
                match where_is(TOPIC_MANAGER_NAME.to_owned()) {
                    Some(actor) => {
                        actor.send_message(BrokerMessage::PublishRequest { registration_id, topic, payload }).expect("Failed to forward subscribeRequest to topic manager");
                    },
                    None => todo!(),
                }
                
            }
            BrokerMessage::PublishResponse { topic, payload, result } => {
                match where_is(SUBSCRIBER_MANAGER_NAME.to_owned()) {
                    Some(actor) => actor.send_message(BrokerMessage::PublishResponse {topic, payload, result }).expect("Failed to forward notification to subscriber manager"),
                    None => tracing::error!("Failed to lookup subscriber manager!")
                } 
                
            },
            BrokerMessage::ErrorMessage { error, .. } => {
                warn!("Error Received: {error}");
            },
            BrokerMessage::DisconnectRequest { client_id, registration_id } => {
                //start cleanup
                info!("Cleaning up session {registration_id:?}");
                if let Some(manager) = where_is(SUBSCRIBER_MANAGER_NAME.to_string()) {
                    manager.send_message(BrokerMessage::DisconnectRequest {
                        client_id: client_id.clone(),
                        registration_id: registration_id.clone(),
                    }).expect("Expected to forward message");
                    
                }
                // Tell listener manager to kill listener, it's not coming back
                if let Some(manager) = where_is(LISTENER_MANAGER_NAME.to_string()) {
                    manager.send_message(BrokerMessage::DisconnectRequest {
                        client_id: client_id.clone(),
                        registration_id,
                    }).expect("Expected to forward message");
                }
            }
            BrokerMessage::TimeoutMessage { client_id, registration_id, error } => {
                //cleanup subscribers
                
                if let Some(manager) = where_is(SUBSCRIBER_MANAGER_NAME.to_string()) {
                    manager.send_message(BrokerMessage::TimeoutMessage {
                        client_id: client_id.clone(),
                        registration_id: registration_id.clone(),
                        error: error.clone()
                    }).expect("Expected to forward message");
                    
                }
            }
            _ => warn!(UNEXPECTED_MESSAGE_STR)
        }
        Ok(())
    }
}   
