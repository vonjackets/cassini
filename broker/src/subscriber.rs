use std::collections::HashMap;

use ractor::SpawnErr;
use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use tracing::{debug, error, warn};

use crate::{session, BrokerMessage, SESSION_NOT_FOUND_TXT, SUBSCRIBE_REQUEST_FAILED_TXT};
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
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _: ()
    ) -> Result<Self::State, ActorProcessingErr> {
        tracing::debug!("{myself:?} starting");

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
                        
                        let subscriber_id = format!("{registration_id}:{topic}");
                        let args = SubscriberAgentArgs {
                            registration_id: registration_id.clone(),
                        };
                        // start new subscriber actor for session
                        Actor::spawn_linked(Some(subscriber_id), SubscriberAgent, args, myself.clone().into()).await
                        .map_err(|e| {
                            let err_msg = format!("{SUBSCRIBE_REQUEST_FAILED_TXT}, {e}");
                            warn!("{err_msg}");
                            //send error message to session
                            let cloned_msg = err_msg.clone();
                            if let Some(session) = where_is(registration_id.clone()) {
                                session.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result: Err(err_msg) })
                                .map_err(|e| {
                                    warn!("{}", format!("{cloned_msg}: {SESSION_NOT_FOUND_TXT} {e}"));
                                }).unwrap();
                            } else {
                                warn!("{SUBSCRIBE_REQUEST_FAILED_TXT}, {SESSION_NOT_FOUND_TXT}");
                            }
                        }).unwrap();
                    } 
                    None => warn!("{SUBSCRIBE_REQUEST_FAILED_TXT}, No registration_id provided!")
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
            SupervisionEvent::ActorTerminated(actor_cell,reason, ..) => { debug!("Subscription ended for session {0:?}, {reason:?}", actor_cell.get_name()); }
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
    topic: String,
}

pub struct SubscriberAgentArgs {
    registration_id: String,
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
        let name = myself.get_name().expect("{SUBSCRIBE_REQUEST_FAILED_TXT}: Expected subscriber to have name");
        if let Some((registration_id, topic)) = name.split_once(':') {
            
            Ok(SubscriberAgentState {
                registration_id: registration_id.to_string(),
                topic: topic.to_string()
            })    
        } else { panic!("{SUBSCRIBE_REQUEST_FAILED_TXT}: Bad name given"); }        
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
            tracing::debug!("{myself:?} Started");
            
            //send ACK to session so they know they're subscribed
            if let Some(session) = where_is(state.registration_id.clone()) {
                session.send_message(BrokerMessage::SubscribeAcknowledgment {
                    registration_id: state.registration_id.clone(),
                    topic: state.topic.clone(),
                    result: Ok(())
                })
                .map_err(|e| {
                    // if we can't ack to the session, it's probably dead
                    myself.stop(Some(format!("SESSION_MISSING {e}")));
                }).unwrap();
            }
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
                tracing::debug!("New message on topic: {topic}, forwarding to session: {id}");
                if let Some(session) = where_is(id.clone()) {
                    session.send_message(BrokerMessage::PublishResponse { topic, payload, result }).expect("{SESSION_NOT_FOUND_TXT}");
                }
                //TODO: Store dead letter queue here in case of failure to send to session
            },
            _ => warn!(UNEXPECTED_MESSAGE_STR)

        }
        Ok(())
    }
}
