use std::{clone, collections::HashMap};

use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef};
use tracing::{info, warn, Subscriber};

use crate::{broker::Broker, session::{self, SessionAgentArgs}, BrokerMessage};


pub struct SubscriberManager;


/// Define the state for the actor
pub struct SubscriberManagerState {
    subscribers: HashMap<String, ActorRef<BrokerMessage>>,  // Map of registration_id to Subscriber addresses
    topic_manager_ref: ActorRef<BrokerMessage>,
    session_manager_ref: ActorRef<BrokerMessage>,
}

pub struct SubscriberManagerArgs {
    pub topic_mgr_ref: ActorRef<BrokerMessage>,
    pub session_manager_ref: ActorRef<BrokerMessage>,
}


#[async_trait]
impl Actor for SubscriberManager {
    type Msg = BrokerMessage; // Messages this actor handles
    type State = SubscriberManagerState; // Internal state
    type Arguments = SubscriberManagerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: SubscriberManagerArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("{myself:?} starting");
        //parse args. if any
        let state = SubscriberManagerState { subscribers: todo!(), topic_manager_ref: todo!(), session_manager_ref: todo!() };

        Ok(state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {
        match message {
            BrokerMessage::PublishResponse { topic, payload, result } => todo!(),
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                if state.subscribers.contains_key(&registration_id) {
                    warn!("Session agent {registration_id} already subscribed to topic {topic}");
                } else {
                    //TODO: determine naming convention for subscriber agents?
                    // session_id:topic?
                    let agent_name = String::from(registration_id.clone() + ":" + &topic);
                    let args = SubscriberAgentArgs {
                        registration_id: registration_id.clone(),
                        session_agent_ref: state.subscribers.get(&registration_id).unwrap().clone()
                    };
                    let (subscriber_ref, _) = Actor::spawn_linked(Some(agent_name), SubscriberAgent, args, myself.clone().into()).await.expect("Failed to start subscriber agent {agent_name}");

                    state.subscribers.insert(registration_id.clone(), subscriber_ref.clone());

                    //send ack to session

                    let session_agent_ref = where_is(registration_id.clone()).unwrap();
                    session_agent_ref.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result: Ok(()) });
                }
                
                
            },
            BrokerMessage::UnsubscribeRequest { registration_id, topic } => {
                let id = registration_id.clone();
                if let Some(subscriber_agent_ref) = state.subscribers.clone().get(&id) {
                    state.subscribers.remove(&registration_id);
                    //send ack to session\
                    let session_agent_ref = where_is(registration_id.clone()).unwrap();
                    session_agent_ref.send_message(BrokerMessage::UnsubscribeAcknowledgment { registration_id: registration_id.clone(), topic, result: Ok(()) });
                    //kill agent

                    subscriber_agent_ref.kill();
                } else {
                    warn!("Session agent {registration_id} not subscribed to topic {topic}");
                    //TODO: determine naming convention for subscriber agents?
                    // session_id:topic?
                   
                }
            },
            _ => todo!()

        }
            
        
        Ok(())
    }
}

pub struct SubscriberAgent;


/// Define the state for the actor
pub struct SubscriberAgentState {
    registration_id: String,
    session_agent_ref: ActorRef<BrokerMessage>
}

pub struct SubscriberAgentArgs {
    registration_id: String,
    session_agent_ref: ActorRef<BrokerMessage>
}


#[async_trait]
impl Actor for SubscriberAgent {
    type Msg = BrokerMessage; // Messages this actor handles
    type State = SubscriberAgentState; // Internal state
    type Arguments = SubscriberAgentArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: SubscriberAgentArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        info!("{myself:?} starting");
        //parse args. if any
        let state = SubscriberAgentState { registration_id: args.registration_id, session_agent_ref: args.session_agent_ref };

        Ok(state)
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {
        match message {
            BrokerMessage::PublishResponse { topic, payload, result } => {
                //TODO: Subscriber was notified there was a new message published on the subscribed topic, forward payload to session
                todo!()
            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                
            },
            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result } => {
                //forward to session
                if let Some(session_ref) = where_is(registration_id.clone()) {
                    session_ref.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id: registration_id, topic: topic, result: result }).unwrap();
                } else {
                    warn!("Failed to forward acknowledgement to session!");
                }
            }
            BrokerMessage::UnsubscribeRequest { registration_id, topic } => {

            },
            _ => todo!()

        }
            
        
        Ok(())
    }
}
