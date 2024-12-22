use std::{borrow::Borrow, collections::HashMap, pin, result, time::Duration};

use ractor::{async_trait, message, registry::where_is, time::send_interval, Actor, ActorProcessingErr, ActorRef, Message, RpcReplyPort};
use tracing::{debug, info, warn};
use tracing_subscriber::field::debug;
use uuid::Uuid;

use crate::{broker::{self, Broker}, TimeoutMessage};
use common::BrokerMessage;

pub struct SessionManager;

/// Our representation of a connected session, and how close it is to timing out
pub struct Session {
    agent_ref: ActorRef<BrokerMessage>,
    ping_count: usize
}
/// Define the messages the actor can handle
/// CAUTION: Avoid the urge to make this serializable, ActorRefs aren't.
/// and the session manager needs to know what listener it's managing a session for
#[derive(Debug)]
pub enum SessionManagerMessage { 
    RegistrationResponse(BrokerMessage),
    DisconnectRequest(BrokerMessage),
    ErrorMessage(BrokerMessage),
    TimeoutMessage(TimeoutMessage),
}

/// Define the state for the actor
pub struct SessionManagerState {
    sessions: HashMap<String, Session>,           // Map of registration_id to Session ActorRefesses
    // topic_mgr_ref: Option<ActorRef<BrokerMessage>>,
    broker_ref: ActorRef<BrokerMessage>,
}

pub struct SessionManagerArgs {
    pub broker_id: String,
}


/// Message to set the TopicManager ref in SessionManager or other actors.
#[derive(Debug)]
// #[rtype(result = "()")]
pub struct SetTopicManagerRef(pub ActorRef<BrokerMessage>);


/// Message to set the ListenerManager ref in other actors.
#[derive(Debug)]
// #[rtype(result = "()")]
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
            broker_ref: ActorRef::from(where_is(args.broker_id).unwrap())
        };

        Ok(state)
    }
    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        debug!("{myself:?} started");

        //link with supervisor
        myself.link(state.broker_ref.get_cell());
        //myself.notify_supervisor(ractor::SupervisionEvent::ActorStarted(myself.get_cell()));
  
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
                // Generate a new registration ID
                let registration_id = Uuid::new_v4().to_string();
                
                
                //The broker successfully generated an id
                info!("SessionManager: Stating session for client with registration ID {}", registration_id);
                
                if let Some(broker_ref) = myself.try_get_supervisor() {

                    let listener_ref = where_is(client_id.clone()).expect("Couldn't find listenerAgent with id {client_id}");
                    let args = SessionAgentArgs { registration_id: registration_id.clone(), client_ref: listener_ref.into() , broker_ref: broker_ref.clone().into()};
                        
                    let (session_agent, _) = Actor::spawn_linked(Some(registration_id.clone()), SessionAgent, args, myself.clone().into()).await.expect("Couldn't start session agent!");
                    state.sessions.insert(registration_id.clone(), Session {
                        agent_ref: session_agent,
                        ping_count: 0
                    });

                    //send ack to broker to be forwarded back
                    broker_ref.send_message(BrokerMessage::RegistrationResponse { registration_id: registration_id, client_id: client_id, success: true, error: None }).expect("Exprected to forward message");

                }
            
            }
            BrokerMessage::PingMessage { registration_id, client_id } => {
                //reset ping count for session
                debug!("Received ping for session {registration_id}");
                let session = state.sessions.get_mut(&registration_id).expect("Failed to lookup session for id: {registration_id}");
                session.ping_count = 0;
                session.agent_ref.send_message(BrokerMessage::PongMessage { registration_id: registration_id.clone() }).expect("Failed to send pong message to session {registration_id}");
            }
            BrokerMessage::TimeoutMessage { registration_id } => {
               warn!("Session {registration_id} timed out due to missed pings");
               //send disconnect to listenermgr
                let session = state.sessions.get_mut(&registration_id).expect("Failed to lookup session for id: {registration_id}");
                

            }, _ => {
                todo!()
            }
        }
        Ok(())
    }
}

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
        //TODO: Start handling timeouts from the listener here, if that actor panics or dies because the client dropped the connection,
        // we want a client not to lose their session
        info!("Started {myself:?}");

        Ok(())

    }


    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {
        match message {
            BrokerMessage::RegistrationRequest { client_id } => todo!(),
            BrokerMessage::RegistrationResponse { registration_id, client_id, success, error } => todo!(),
            BrokerMessage::PublishRequest { registration_id, topic, payload } => todo!(),
            BrokerMessage::PublishResponse { topic, payload, result } => todo!(),
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                debug!("forwarding susbcribe request for {myself:?}");
                state.broker.send_message(BrokerMessage::SubscribeRequest { registration_id, topic}).expect("Failed to forward request to subscriber manager for session: {registration_id}");
            },
            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result } => {
                match result {
                    Ok(()) => {
                        //forard to client
                        state.client_ref.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result }).expect("Failed to forward subscribe ack to client");
                    },
                    Err(_) => todo!(),
                }
                    
                
            }
            
            BrokerMessage::ErrorMessage { client_id, error } => todo!(),
            BrokerMessage::PingMessage { registration_id, client_id } => {
                //forward to broker
                
            },
            BrokerMessage::PongMessage { .. } => {

                debug!("Received pong from broker for session");
                
            },
            _ => {
                todo!()
            }
        }
    
        Ok(())
    }
}