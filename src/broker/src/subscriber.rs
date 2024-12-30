use std::collections::{hash_map::Iter, HashMap};

use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use tracing::{debug, error, info, warn};

use common::{BrokerMessage, BROKER_NAME};

use crate::UNEXPECTED_MESSAGE_STR;

/// Our supervisor for the subscribers
/// When a user subscribes to a new topic, this actor is notified inb conjunction with the topic manager.
/// A new process is started to wait and listen for new messages on that topic and forward messages.
/// Clients are only considered subscribed if an actor process exists and is managed by this actor
pub struct SubscriberManager;


/// Define the state for the actor
pub struct SubscriberManagerState {
    subscriptions: HashMap<String, Vec<String>> // Map of topics to list subscriber ids
}
pub struct SubscriberManagerArgs {
    pub broker_id: String,
}

impl SubscriberManager {
    /// Removes all subscriptions for a given session
    fn forget_subscriptions(registration_id: Option<String>, subscriptions: HashMap<String, Vec<String>> ) {
        //cleanup all subscriptions for session
        registration_id.map(|registration_id: String| {
            for topic in subscriptions.keys() {
                let subscriber_name = format!("{registration_id}:{topic}");
                match where_is(subscriber_name.clone()) {
                    Some(subscriber) => subscriber.stop(Some("UNSUBSCRIBED".to_string())),
                    None => warn!("Couldn't find subscription {subscriber_name}")
                }
            }
        });
    }
}
#[async_trait]
impl Actor for SubscriberManager {
    type Msg = BrokerMessage; // Messages this actor handles
    type State = SubscriberManagerState; // Internal state
    type Arguments = SubscriberManagerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _: SubscriberManagerArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        tracing::debug!("{myself:?} starting");

        //link with supervisor
        
        match where_is(BROKER_NAME.to_string()) {
            Some(broker) => myself.link(broker),
            None => todo!()
        }
        //parse args. if any
        let state = SubscriberManagerState { subscriptions: HashMap::new()};
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
                tracing::debug!("New message published on topic: {topic}");
                //Handle ack, a new message was published, alert all subscribed sessions 
                match state.subscriptions.get(&topic).to_owned() {
                    Some(vec) => {
                        for subscriber in vec {
                            where_is(subscriber.to_string()).map_or_else(| | {
                                warn!("Could not find subscriber: {subscriber} for topic: {topic}");
                            }, |subscriber| {
                                subscriber.send_message(BrokerMessage::PublishResponse { topic: topic.clone(), payload: payload.clone() , result:result.clone() }).unwrap();
                            })
                        }
                        //TODO: Send some kind of message to the topic actors that the messages have been consumed so they can update the queue
                        
                    } None => {
                        debug!("No subscriptions for topic: {topic}");
                        //No subscribers for this topic, init new vec
                        state.subscriptions.insert(topic, Vec::new());
                    }
                }
            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                match registration_id {
                    Some(registration_id) => {
                        if state.subscriptions.contains_key(&registration_id) {
                            warn!("Session agent {registration_id} already subscribed to topic {topic}");
                        } else {
                            
                            let subscriber_id = format!("{registration_id}:{topic}");
                            let session = ActorRef::from(where_is(registration_id.clone()).unwrap());
                            let args = SubscriberAgentArgs {
                                registration_id: registration_id.clone(),
                                session_agent_ref: session.clone()
                            };
                            
                            Actor::spawn_linked(Some(subscriber_id.clone()), SubscriberAgent, args, myself.clone().into()).await.expect("Failed to start subscriber agent {subscriber_id}");
                            
                            // add agent to subscriber list for that topic
                            if let Some(subscribers) = state.subscriptions.get_mut(&topic) {
                                
                                subscribers.push(subscriber_id.clone());

                                tracing::debug!("Topic {topic} has {0} subscriber(s)", subscribers.len());
                            } else {
                                //No subscribers, init vec and add subscriber to it
                                let mut vec = Vec::new();
                                vec.push(subscriber_id.clone());
                                state.subscriptions.insert(topic.clone(), vec);
                            }
                            
                            //send ack to broker
                            myself.try_get_supervisor().map(|broker| {
                                broker.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result: Ok(()) }).expect("Expected to forward ack to broker");
                            });
                        }                        
                    } 
                    None => ()
                }
            },
            BrokerMessage::UnsubscribeRequest { registration_id, topic } => {
                match registration_id {
                    Some(id) => {         
                        if let Some(subscribers) = state.subscriptions.get_mut(&topic) {
                            let subscriber_name = format!("{id}:{topic}");

                            where_is(subscriber_name.clone()).map_or_else(
                                || { warn!("Could not find subscriber for client {id}. They may not be subscribed to the topic: {topic}")},
                                |subscriber| {
                                    subscriber.kill();
                                });
                            //TODO: Monitor this approach for effectiveness
                            match subscribers.binary_search(&subscriber_name) {
                                Ok(index) => subscribers.remove(index),
                                Err(_) => todo!("Expected to find index of the subscriber name for client but couldn't, send error message")
                            };


                            //send ack
                            let id_clone = id.clone();
                            where_is(id).map_or_else(|| { error!("Could not find session for client: {id_clone}")}, |session| {
                                session.send_message(BrokerMessage::UnsubscribeAcknowledgment { registration_id: id_clone.clone(), topic, result: Ok(()) }).expect("expected to send ack to session");
                            });
                        } else {
                            warn!("Session agent {id} not subscribed to topic {topic}");
                            //TODO: determine naming convention for subscriber agents?
                            // session_id:topic?
                        
                        }
                    },
                    None => todo!()
                }
            },
            BrokerMessage::DisconnectRequest { registration_id , ..} => {
                SubscriberManager::forget_subscriptions(registration_id, state.subscriptions.clone());
            }
            BrokerMessage::TimeoutMessage { registration_id, .. } => {
                SubscriberManager::forget_subscriptions(registration_id, state.subscriptions.clone());
            }
            _ => warn!(UNEXPECTED_MESSAGE_STR)

        }
        Ok(())
    }


    async fn handle_supervisor_evt(&self, _: ActorRef<Self::Msg>, msg: SupervisionEvent, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match msg {
            SupervisionEvent::ActorStarted(_) => (),
            SupervisionEvent::ActorTerminated(actor_cell,reason, ..) => { debug!("Subscription ended for session {0:?}", actor_cell.get_name()); }
            SupervisionEvent::ActorFailed(..) => todo!("Subscriber failed unexpectedly, restart subscription and update state"),
            SupervisionEvent::ProcessGroupChanged(..) => (),
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

    async fn post_stop(&self, myself: ActorRef<Self::Msg>, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
        tracing::debug!("Successfully stopped {myself:?}");
        Ok(())
    }


    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr>  {
        match message {
            BrokerMessage::PublishResponse { topic, payload, result } => {                
                let id = state.registration_id.clone();
                tracing::debug!("Received notification of message on topic: {topic}, forwarding to session: {id}");
                state.session_agent_ref.send_message(BrokerMessage::PublishResponse { topic, payload, result }).expect("Failed to forward message to session");
                //TODO: Store dead letter queue here in case of failure to send to session
            },
            _ => warn!(UNEXPECTED_MESSAGE_STR)

        }
        Ok(())
    }
}
