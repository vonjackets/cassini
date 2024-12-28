use std::collections::{HashMap, VecDeque};
use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef};
use tracing::{debug, error, info, warn};
use common::{BrokerMessage, BROKER_NAME};

// ============================== Topic Supervisor definition ============================== //
/// Our supervisor for managing topics and their message queues
/// This processes is generally responsible for creating, removing, maintaining which topics
/// a client can know about
pub struct TopicManager;

pub struct TopicManagerState {
    topics: HashMap<String, ActorRef<BrokerMessage>> // Map of topic name to Topic addresses
}

pub struct  TopicManagerArgs {
    pub topics: Option<Vec<String>>,
    pub broker_id: String,
}

#[async_trait]
impl Actor for TopicManager {
    #[doc = " The message type for this actor"]
    type Msg = BrokerMessage;

    #[doc = " The type of state this actor manages internally"]
    type State = TopicManagerState;

    #[doc = " Initialization arguments"]
    type Arguments = TopicManagerArgs;

    #[doc = " Invoked when an actor is being started by the system."]
    #[doc = ""]
    #[doc = " Any initialization inherent to the actor\'s role should be"]
    #[doc = " performed here hence why it returns the initial state."]
    #[doc = ""]
    #[doc = " Panics in `pre_start` do not invoke the"]
    #[doc = " supervision strategy and the actor won\'t be started. [Actor]::`spawn`"]
    #[doc = " will return an error to the caller"]
    #[doc = ""]
    #[doc = " * `myself` - A handle to the [ActorCell] representing this actor"]
    #[doc = " * `args` - Arguments that are passed in the spawning of the actor which might"]
    #[doc = " be necessary to construct the initial state"]
    #[doc = ""]
    #[doc = " Returns an initial [Actor::State] to bootstrap the actor"]
    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: TopicManagerArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        //
        // Try to reinit from a predermined list of topics
        let mut state  = TopicManagerState{
            topics: HashMap::new()            
        };
        //link with supervisor
        match where_is(BROKER_NAME.to_string()) {
            Some(broker) => myself.link(broker),
            None => todo!()
        }
        
        
        if let Some(topics) = args.topics {
            for topic in topics {
                //start topic actors for that topic
                let args = TopicAgentArgs{ topic: topic.clone()};
                match Actor::spawn_linked(Some(topic.clone()), TopicAgent, args, myself.clone().into()).await {
                    Ok(_) => (),
                    Err(_) => error!("Failed to start actor for topic {topic}"),
                }
                    
                
            }
        }

        let state:TopicManagerState = TopicManagerState {
            topics: HashMap::new()
         };
        
        debug!("Starting {myself:?}");
        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
            debug!("{myself:?} Started");
            
            Ok(())

    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

        match message {
            BrokerMessage::PublishRequest { registration_id, topic, payload } => {
                match registration_id {
                    Some(registration_id) => {
                 //forward request to topic agent
                 if let Some(topic_actor) = state.topics.get(&topic.clone()) {
                    topic_actor.send_message(BrokerMessage::PublishRequest { registration_id: Some(registration_id), topic , payload}).expect("Failed for forward publish request")
                } else {
                    warn!("No agent set to handle topic: {topic}, starting new agent...");    
                    //TODO: spin up new topic actor if topic doesn't exist?
                    let broker = myself.try_get_supervisor().unwrap();
                    let args = TopicAgentArgs { topic: topic.clone()};
                    match Actor::spawn_linked(Some(topic.clone()), TopicAgent, args, myself.clone().into()).await {
                    
                    Ok((actor, _)) => {
                        state.topics.insert(topic.clone(), actor.clone());
                    },
                    Err(e) => {
                        tracing::error!("Failed to start actor for topic: {topic}");
                        //TOOD: send error message here
                        match myself.try_get_supervisor() {
                            Some(broker) => broker.send_message(BrokerMessage::ErrorMessage{ client_id: registration_id.clone(), error: e.to_string()}).unwrap(),
                            None => todo!()
                        }
                    }
                    }
                }
                    },
                None => warn!("Received publish request from unknown session. {payload}")
                }
            },
            BrokerMessage::PublishResponse { topic, payload, result } => {
                //forward to broker
                match myself.try_get_supervisor(){
                    Some(broker) => broker.send_message(BrokerMessage::PublishResponse { topic, payload, result: Result::Ok(()) }).expect("Expected to forward message to broker"),
                    None => todo!()
                }
            }
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                match registration_id {
                    Some(registration_id) => {
                                        //check topic exists, validate whether an agent to handle that queue already exists
                        if let Some(_) = state.topics.get(&registration_id.clone()) {
                            warn!("client {registration_id} already registered!");
                            //send ack anyway
                        } else {
                            warn!("No agent set to handle topic: {topic}, starting new agent...");    
                            //TODO: spin up new topic actor if topic doesn't exist?
                            let args = TopicAgentArgs { topic: topic.clone() };
                            match Actor::spawn_linked(Some(topic.clone()), TopicAgent, args, myself.clone().into()).await {
                            
                            Ok((actor, _)) => {
                                state.topics.insert(topic.clone(), actor.clone());
                            },
                            Err(e) => {
                                tracing::error!("Failed to start actor for topic: {topic}, {e}");
                                //TOOD: send error message here
                                match myself.try_get_supervisor() {
                                    Some(broker) => broker.send_message(BrokerMessage::ErrorMessage{ client_id: registration_id.clone(), error: e.to_string()}).unwrap(),
                                    None => todo!()
                                }
                            }
                            }
                        }
                            
                    }, None => {
                        todo!()
                    }
                }
            }
            BrokerMessage::UnsubscribeRequest { registration_id, topic } => todo!(),
            _ => todo!()
        }
        Ok(())
    }
}

// ============================== Topic Worker definition ============================== //
/// Our worker process for managing message queues on a given topic
/// The broker supervisor is generally notified of incoming messages on the message queue.
struct TopicAgent;

struct TopicAgentState {
    topic: String,
    queue: VecDeque<String>
}
struct TopicAgentArgs {
  topic: String
}

#[async_trait]
impl Actor for TopicAgent {

    #[doc = " The message type for this actor"]
    type Msg = BrokerMessage;

    #[doc = " The type of state this actor manages internally"]
    type State = TopicAgentState;

    #[doc = " Initialization arguments"]
    type Arguments = TopicAgentArgs;


    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: TopicAgentArgs
    ) -> Result<Self::State, ActorProcessingErr> {

        let state: TopicAgentState  = TopicAgentState { topic: args.topic.clone() , queue: VecDeque::new()};
        
        debug!("Starting... {myself:?}");

        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
            debug!("{myself:?} Started");
            Ok(())

    }
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

        //TODO: Make way for some message to come in via the broker that tells this actor to update the queue after messages are consumed.
        match message {
            BrokerMessage::PublishRequest{registration_id,topic,payload} => {
                match registration_id {
                    Some(registration_id) => {
                        debug!("{myself:?}: New message from {0}: {1}", registration_id, payload);
                        debug!("{topic} queue has {0} message(s) waiting", state.queue.len());
                        state.queue.push_back(payload.clone());
                        //send ack
                        match myself.try_get_supervisor() {
                            Some(manager) => manager.send_message(BrokerMessage::PublishResponse { topic, payload, result: Result::Ok(()) }).expect(""),
                            None => todo!()
                        }
                    }, 
                    None => {
                        warn!("Received publish request from unknown session: {payload}");
                        //TODO: send error message
                    }
                }
            }
            _ => {
                todo!()
            }
        }
        Ok(())
    }
}