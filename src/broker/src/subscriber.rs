use std::{collections::HashMap};

use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef};
use tracing::{info, warn};

use common::BrokerMessage;

/// Our supervisor for the subscribers
/// When a user subscribes to a new topic, this actor is notified inb conjunction with the topic manager.
/// A new process is started to wait and listen for new messages on that topic and forward messages.
/// Clients are only considered subscribed if an actor process exists and is managed by this actor
pub struct SubscriberManager;


/// Define the state for the actor
pub struct SubscriberManagerState {
    subscribers: HashMap<String, Vec<ActorRef<BrokerMessage>>>,  // Map of topics to list of actors subscribed to it
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
            BrokerMessage::PublishResponse { topic, payload, result } => {
                tracing::debug!("New message published on topic: {topic}");
                //Handle ack, a new message was published, alert all subscribed sessions 
                match state.subscribers.get(&topic).to_owned() {
                    Some(vec) => {
                        for subscriber in vec {
                            tracing::debug!("Notifying subcribers of topic: {topic}");
                            subscriber.send_message(BrokerMessage::PublishResponse { topic: topic.clone(), payload: payload.clone() , result:result.clone() }).unwrap();
                        }
                    } None => {
                        //No subscribers for this topic, init new vec
                        state.subscribers.insert(topic, Vec::new());
                    }
                }
            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                match registration_id {
                    Some(registration_id) => {
                        if state.subscribers.contains_key(&registration_id) {
                            warn!("Session agent {registration_id} already subscribed to topic {topic}");
                        } else {
                            //TODO: determine naming convention for subscriber agents?
                            // session_id:topic?
                            let agent_name = format!("{registration_id}:{topic}");
                            let session = ActorRef::from(where_is(registration_id.clone()).unwrap());
                            let args = SubscriberAgentArgs {
                                registration_id: registration_id.clone(),
                                session_agent_ref: session.clone()
                            };
                            let (subscriber_ref, _) = Actor::spawn_linked(Some(agent_name), SubscriberAgent, args, myself.clone().into()).await.expect("Failed to start subscriber agent {agent_name}");
                            
                            // add agent to subscriber list for that topic
                            if let Some(subscribers) = state.subscribers.get(&topic) {
                                
                                subscribers.to_owned().push(subscriber_ref.clone());
                                tracing::debug!("Topic {topic} has {0} subscriber(s)", subscribers.len());
                            } else {
                                //No subscribers, init vec and add subscriber to it
                                let mut vec = Vec::new();
                                vec.push(subscriber_ref.clone());
                                state.subscribers.insert(topic.clone(), vec);
                            }   
                            //send ack to broker
                            //TOOD: Handle what happens if these return none

                            myself.try_get_supervisor().map(|broker| {
                                broker.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result: Ok(()) }).expect("Expected to forward ack to broker");
                            });

                            
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
                            // //kill agent

                            // subscriber_agent_ref.kill();
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


/// Our subscriber actor.
/// The existence of a running "Subscriber" signifies a clients subscription
/// it is responsible for forwarding new messages received on its given topic
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
        tracing::debug!("{myself:?} starting");
        //parse args. if any
        let state = SubscriberAgentState { registration_id: args.registration_id, session_agent_ref: args.session_agent_ref };

        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
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
