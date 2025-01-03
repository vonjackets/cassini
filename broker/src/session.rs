use std::collections::HashMap;
use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;
use crate::{BrokerMessage, BROKER_NAME};

use crate::UNEXPECTED_MESSAGE_STR;

/// The manager process for our concept of client sessions.
/// When thje broker receives word of a new connection from the listenerManager, it requests
/// that the client be "registered". Signifying the client as legitimate.
pub struct SessionManager;

/// Our representation of a connected session, and how close it is to timing out
/// TODO: What other fields might we want to have here?
pub struct Session {
    agent_ref: ActorRef<BrokerMessage>,
}


/// Define the state for the actor
pub struct SessionManagerState {
    /// Map of registration_id to Session ActorRefes
    sessions: HashMap<String, Session>,           
    session_timeout: u64,
    cancellation_tokens: HashMap<String, CancellationToken>,
}

pub struct SessionManagerArgs {
    pub session_timeout: u64
}


/// Message to set the TopicManager ref in SessionManager or other actors.
#[derive(Debug)]
pub struct SetTopicManagerRef(pub ActorRef<BrokerMessage>);


/// Message to set the ListenerManager ref in other actors.
#[derive(Debug)]
pub struct SetListenerManagerActorRef(pub ActorRef<BrokerMessage>);


#[async_trait]
impl Actor for SessionManager {
    type Msg = BrokerMessage; // Messages this actor handles
    type State = SessionManagerState; // Internal state
    type Arguments = SessionManagerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: SessionManagerArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        debug!("{myself:?} starting");
        //parse args. if any
        let state = SessionManagerState {
            sessions: HashMap::new(),
            cancellation_tokens: HashMap::new(),
            session_timeout: args.session_timeout
        };

        Ok(state)
    }
    async fn post_start(&self, myself: ActorRef<Self::Msg>, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        debug!("{myself:?} started");
  
        Ok(())
    }
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {
        match message {
            BrokerMessage::RegistrationRequest { client_id, ..} => {
                
                // Start brand new session
                let new_id = Uuid::new_v4().to_string();

                info!("SessionManager: Stating session for client: {client_id} with registration ID {new_id}");

                match myself.try_get_supervisor() {
                    Some(broker_ref) => {

                        let listener_ref = where_is(client_id.clone()).expect("Couldn't find listenerAgent with id {client_id}");
                        let args = SessionAgentArgs { registration_id: new_id.clone(), client_ref: listener_ref.into() , broker_ref: broker_ref.clone().into()};
                            
                        let (session_agent, _) = Actor::spawn_linked(Some(new_id.clone()), SessionAgent, args, myself.clone().into()).await.expect("Couldn't start session agent!");
                        state.sessions.insert(new_id.clone(), Session {
                            agent_ref: session_agent                            
                        });                            

                    } None => error!("Couldn't lookup root broker actor!")
                }   
                
            }
            BrokerMessage::RegistrationResponse { registration_id, client_id, .. } => {
                //A client has (re)registered. Cancel timeout thread
                // debug!("SessionManager: Alerted of session registration");
                if let Some(registration_id) = registration_id {
                    match where_is(registration_id.clone()) {
                        Some(_) => {
                            if let Some(token) = &state.cancellation_tokens.get(&registration_id) {
                                debug!("Aborting session cleanup for {registration_id}");
                                token.cancel();
                                state.cancellation_tokens.remove(&registration_id);
                            }
                        }, None => {
                            warn!("No session found for id {registration_id}");   
                        }
                    }
                } else {
                    warn!("Received registration response from unknown client: {client_id}");
                }
            }
            BrokerMessage::PublishRequest { registration_id, topic, payload } => {
                //forward to broker
                match myself.try_get_supervisor() {
                    Some(broker)=> {
                        broker.send_message(BrokerMessage::PublishRequest { registration_id, topic, payload }).unwrap();
                    }, None => warn!("Couldn't find broker supervisor")
                }
            }
            BrokerMessage::DisconnectRequest { client_id, registration_id } => {
                //forward to broker, kill session
                if let Some(registration_id) = registration_id {
                    match where_is(registration_id.clone()) {
                        Some(session) => session.stop(Some("CLIENT_DISCONNECTED".to_owned())),
                        None => warn!("Failed to find session {registration_id}")
                    }

                    match myself.try_get_supervisor() {
                        Some(broker) => broker.send_message(
                            BrokerMessage::DisconnectRequest { client_id, registration_id: Some(registration_id) })
                            .expect("Expected to forward message"),

                        None => warn!("Failed to find supervisor")
                    }
                    
                }
            }
            BrokerMessage::TimeoutMessage { client_id, registration_id, error } => {
                if let Some(registration_id) = registration_id {
                    
                    if let Some(session) = state.sessions.get(&registration_id) {
                        warn!("Session {registration_id} timing out, waiting for reconnect...");
                        let ref_clone = session.agent_ref.clone();    
                        let token = CancellationToken::new();

                        state.cancellation_tokens.insert(registration_id.clone(), token.clone());
                        let timeout = state.session_timeout.clone();
                        let _ = tokio::spawn(async move {
                            tokio::select! {
                                // Use cloned token to listen to cancellation requests
                                _ = token.cancelled() => {
                                    // The timer was cancelled, task can shut down
                                }
                                // wait before killing session
                                _ = tokio::time::sleep(std::time::Duration::from_secs(timeout)) => {
                                    match myself.try_get_supervisor() {
                                        Some(manager) => {
                                            info!("Ending session {ref_clone:?}");
                                            manager.send_message(BrokerMessage::TimeoutMessage {
                                                client_id,
                                                registration_id: Some(registration_id),
                                                error })
                                                .expect("Expected to forward to manager")
                                        }
    
                                        None => warn!("Could not find broker supervisor!")
                                    }
                                    ref_clone.stop(Some("TIMEDOUT".to_string()));
                                
                                }
                            }
                        });
                    }
                }
            }, _ => warn!("Received unexpected message: {message:?}")
        }
        Ok(())
    }

    async fn handle_supervisor_evt(&self, _: ActorRef<Self::Msg>, msg: SupervisionEvent, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match msg {
            SupervisionEvent::ActorStarted(_) => (),
            SupervisionEvent::ActorTerminated(actor_cell, _, reason) => {
                debug!("Session: {0:?}:{1:?} terminated. {reason:?}", actor_cell.get_name(), actor_cell.get_id());
            },
            SupervisionEvent:: ActorFailed(actor_cell, error) => warn!("Worker agent: {0:?}:{1:?} failed! {error}", actor_cell.get_name(), actor_cell.get_id()),
            SupervisionEvent::ProcessGroupChanged(_) => (),
        }
        Ok(())
    }

}

/// Worker process for handling client sessions.
pub struct SessionAgent;
pub struct SessionAgentArgs {
    pub registration_id: String,
    pub client_ref: ActorRef<BrokerMessage>,
    pub broker_ref: ActorRef<BrokerMessage>
}
pub struct SessionAgentState {
    pub client_ref: ActorRef<BrokerMessage>,     ///id of client listener to connect to
    pub broker: ActorRef<BrokerMessage>
}

#[async_trait]
impl Actor for SessionAgent {
    type Msg = BrokerMessage; // Messages this actor handles
    type State = SessionAgentState; // Internal state
    type Arguments = SessionAgentArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: SessionAgentArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("{myself:?} starting");
        //parse args. if any
        let state = SessionAgentState { client_ref: args.client_ref.clone(), broker: args.broker_ref};

        Ok(state)
    }

    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        
        let client_id = state.client_ref.get_name().unwrap_or_default();
        let registration_id = myself.get_name().unwrap_or_default();
        info!("Started session {myself:?} for client {client_id}. Contacting");      

        //send ack to client listener
       state.client_ref.send_message(BrokerMessage::RegistrationResponse { 
            registration_id: Some(registration_id),
            client_id,
            success: true,
            error: None 
        }).expect("Expected to send message to client listener");

        Ok(())

    }


    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {
        match message {
            BrokerMessage::RegistrationRequest { registration_id, client_id, .. } => {
                //A a new connection has been established, update state to send messages to new listener actor
                match where_is(client_id.clone()) {
                    Some(listener) => {     
                        state.client_ref = ActorRef::from(listener);
                        //ack
                        info!("Established comms with client {client_id}");
                        state.client_ref.send_message(BrokerMessage::RegistrationResponse {
                            registration_id: registration_id.clone(),
                            client_id: client_id.clone(),
                            success: true,
                            error: None })
                        .expect("Expected to send ack to listener");
                        // Alert session manager we've got our listener so it doesn't potentially kill it
                        match myself.try_get_supervisor() {
                            Some(manager) => manager.send_message(BrokerMessage::RegistrationResponse { registration_id, client_id, success: true , error: None }).expect("Expected to send message to manager"),
                            None => warn!("Couldn't find supervisor for session agent!")
                        }
                    } None => {
                        warn!("Could not find listener for client: {client_id}");
                    }
                }
                
            }
            BrokerMessage::PublishRequest { registration_id, topic, payload } => {
                //forward to broker
                state.broker.send_message(BrokerMessage::PublishRequest { registration_id, topic, payload }).unwrap();
            },
            BrokerMessage::PublishResponse { topic, payload, result } => {
                //Forward to listener
                state.client_ref.send_message(BrokerMessage::PublishResponse { topic, payload, result }).expect("expected to forward to listener");
                // TODO: Send message reply back to subscriber via rpc if we fail to forward it to the listener here consider using call_and_forward
                // The subscriber would make a call to the session and await a reply, if no reply is given, or some error is pushed back, it can store messages in a DLQ
                // If the message is successfully sent, it forwards that response to the topic agent. 
            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                state.broker.send_message(BrokerMessage::SubscribeRequest { registration_id, topic}).expect("Failed to forward request to broker for session: {registration_id}");
            },
            BrokerMessage::UnsubscribeRequest { registration_id, topic } => {
                state.broker.send_message(BrokerMessage::UnsubscribeRequest { registration_id, topic}).expect("Failed to forward request to subscriber manager for session: {registration_id}");
            },
            BrokerMessage::UnsubscribeAcknowledgment { registration_id, topic, result } => {
                state.client_ref.send_message(BrokerMessage::UnsubscribeAcknowledgment { registration_id, topic, result }).expect("Expected to forward ack to client");
            }
            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result } => {        
                state.client_ref.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result }).expect("Failed to forward subscribe ack to client");
            }
            BrokerMessage::DisconnectRequest { client_id,registration_id } => {
                //client disconnected, clean up after it then die with honor
                debug!("client {client_id} disconnected");
                match myself.try_get_supervisor() {
                    Some(manager) => manager.send_message(BrokerMessage::DisconnectRequest { client_id, registration_id }).expect("Expected to forward to manager"),
                    None=> tracing::error!("Couldn't find supervisor.")
                }
                
            }
            BrokerMessage::TimeoutMessage { client_id, registration_id, error } => {
                match myself.try_get_supervisor() {
                    Some(manager) => manager.send_message(BrokerMessage::TimeoutMessage { client_id, registration_id, error }).expect("Expected to forward to manager"),
                    None=> tracing::error!("Couldn't find supervisor.")
                }
            }
            _ => {
                warn!(UNEXPECTED_MESSAGE_STR);
            }
        }
    
        Ok(())
    }


}