use std::{clone, collections::HashMap};

use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use tracing::{info, warn, Subscriber};
use tracing_subscriber::field::debug;
use common::BrokerMessage;
use crate::{broker::Broker, session::{self, SessionAgentArgs}};


pub struct SubscriberManager;


/// Define the state for the actor
pub struct SubscriberManagerState {
    subscribers: HashMap<String, ActorRef<BrokerMessage>>,  // Map of registration_id to Subscriber addresses
}

pub struct SubscriberManagerArgs {
    pub broker_id: String,
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
        tracing::debug!("{myself:?} starting");

        //link with supervisor
        myself.link(where_is(args.broker_id).unwrap());
        //parse args. if any
        let state = SubscriberManagerState { subscribers: HashMap::new()};
        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
            tracing::debug!("{myself:?} Started");
            Ok(())

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
                match registration_id {
                    Some(registration_id) => {
                        if state.subscribers.contains_key(&registration_id) {
                            warn!("Session agent {registration_id} already subscribed to topic {topic}");
                        } else {
                            //TODO: determine naming convention for subscriber agents?
                            // session_id:topic?
                            let agent_name = String::from(registration_id.clone() + ":" + &topic);
                            let session = ActorRef::from(where_is(registration_id.clone()).unwrap());
                            let args = SubscriberAgentArgs {
                                registration_id: registration_id.clone(),
                                session_agent_ref: session.clone()
                            };
                            let (subscriber_ref, _) = Actor::spawn_linked(Some(agent_name), SubscriberAgent, args, myself.clone().into()).await.expect("Failed to start subscriber agent {agent_name}");
        
                            state.subscribers.insert(registration_id.clone(), subscriber_ref.clone());
        
                            //send ack to session
        
                            let session_agent_ref = where_is(registration_id.clone()).unwrap();
                            session_agent_ref.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result: Ok(()) });
                        }                        
                    }, 
                    None => {
                        todo!("Error message!")
                    }
                }
            },
            BrokerMessage::UnsubscribeRequest { registration_id, topic } => {
                match registration_id {
                    Some(id) => {
                        
                        if let Some(subscriber_agent_ref) = state.subscribers.clone().get(&id) {
                            state.subscribers.remove(&id);
                            //send ack to session\
                            let session_agent_ref = where_is(id.clone()).unwrap();
                            session_agent_ref.send_message(BrokerMessage::UnsubscribeAcknowledgment { registration_id: id.clone(), topic, result: Ok(()) });
                            //kill agent

                            subscriber_agent_ref.kill();
                        } else {
                            warn!("Session agent {id} not subscribed to topic {topic}");
                            //TODO: determine naming convention for subscriber agents?
                            // session_id:topic?
                        
                        }
                    },
                    None => todo!()
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
                let id = state.registration_id.clone();
                tracing::debug!("Received notification of message on topic: {topic}, forwarding to session: {id}");
                state.session_agent_ref.send_message(BrokerMessage::PublishResponse { topic, payload, result }).expect("Failed to forward message to session");
                //TODO: It's a goal to support resiliency. If we fail to talk to the session for some reason, but aren't told to discard the subscription,
                // How can we ensure a user who get's disconnected temporarily doesn't lose this subscription?
                //TODO: Store dead letter queue here in case of failure to send to session?
            },
            BrokerMessage::UnsubscribeRequest { .. } => {
                //This agent uis no longer needed, it should die with honor
                info!("Stopping {myself:?}");
                myself.kill();
            },
            _ => todo!()

        }
            
        
        Ok(())
    }
}
