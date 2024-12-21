use std::{collections::{HashMap, VecDeque}, hash::Hash};

use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef};
use tracing::{debug, info, warn};

use crate::BrokerMessage;

// ============================== Topic Supervisor definition ============================== //

pub struct TopicManager;

pub struct TopicManagerState {
    topics: HashMap<String, ActorRef<BrokerMessage>> // Map of topic name to Topic addresses
}

pub struct  TopicManagerArgs {
    pub topics: Option<Vec<String>>
}

impl TopicManager {
    pub async fn add_topic(topic: String, topics: &mut HashMap<String, ActorRef<BrokerMessage>>, supervisor: ActorRef<BrokerMessage>) {
        info!("Creating new topic: {}", topic);
        //spawn new topic agent to handle it
        let agent_name = String::from(topic.clone() + "_agent");
        let args = TopicAgentArgs {topic: topic.clone()};
        let (agent_ref, _) = Actor::spawn_linked(Some(agent_name.clone()), TopicAgent, args, supervisor.into()).await.expect("Failed to start topic agent");
        topics.insert(topic.clone(), agent_ref.clone());
    }
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

        if let Some(topics) = args.topics {
            for topic in topics {
                //start topic actors for that topic
                TopicManager::add_topic(topic, &mut state.topics, myself.clone()).await;
            }
        }

        let state:TopicManagerState = TopicManagerState {
            topics: HashMap::new()
         };
        info!("Started {myself:?}");

        Ok(state)
    }
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

        match message {
            BrokerMessage::PublishRequest { registration_id, topic, payload } => {
                 //forward request to topic agent
                 if let Some(agentRef) = state.topics.get(&registration_id.clone()) {
                    agentRef.send_message(BrokerMessage::PublishRequest { registration_id, topic , payload});
                } else {
                    warn!("No agent set to handle topic: {topic}");
                    //TODO: Spin up new topic agent?
                    let session = where_is(registration_id.clone()).expect("Could not get agentref by id {client_id}");
                    session.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result: Result::Err("No topic agent for topic: {topic}".to_string()) })
                    .expect("Failed to send subscribe ack to client {client_id}");
                }
            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                match registration_id {
                    Some(registration_id) => {
                                        //check topic exists, validate whether an agent to handle that queue already exists
                        if let Some(agentRef) = state.topics.get(&registration_id.clone()) {
                            agentRef.send_message(BrokerMessage::SubscribeRequest { registration_id: Some(registration_id.clone()), topic });
                        } else {
                            warn!("No agent set to handle topic: {topic}");
                            //TODO: Spin up new topic agent?
                            let client = where_is(registration_id.clone()).expect("Could not get agentref by id {client_id}");
                            client.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id:registration_id.clone(), topic, result: Result::Err("No topic agent for topic: {topic}".to_string()) })
                            .expect("Failed to send subscribe ack to client {client_id}");
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


struct TopicAgent;

struct TopicAgentState {
    topic: String,
    queue: VecDeque<String>, //TODO: use polar messagetypes instead
    subscribers: HashMap<String, ActorRef<BrokerMessage>> //client ids and actors
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

        let state: TopicAgentState  = TopicAgentState { topic: args.topic.clone() , queue: VecDeque::new(), subscribers: HashMap::new()};
        
        info!("Started {myself:?}");

        Ok(state)
    }
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {

        match message {
            BrokerMessage::SubscribeRequest{registration_id,topic}=>{
                match registration_id {
                    Some(id) => {
                        info!("Adding subscriber {id} to topic {topic}");
                        let client_ref:ActorRef<BrokerMessage> = ActorRef::where_is(id.clone()).unwrap();
                        state.subscribers.insert(id.clone(),client_ref.clone());
                    },
                    None => todo!("send error message"),
                }
        
            },
            BrokerMessage::PublishRequest{registration_id,topic,payload}=>{
                info!(" {myself:?} Recevied message from {0}: {1}",registration_id,payload);
                state.queue.push_back(payload.clone());info!("{myself:?} notifying subscribers");
                for (registration_id,listener)in &state.subscribers{ 
                    
                    let msg = BrokerMessage::PublishResponse{ topic: state.topic.clone(), payload: payload.clone(), result:Result::Ok(()) };
                    
                    debug!("Sending message to {registration_id}:{listener:?}");listener.send_message(msg).unwrap();
                }
            },
            BrokerMessage::UnsubscribeRequest { registration_id, topic } => todo!(),
            BrokerMessage::ErrorMessage { client_id, error } => todo!(), 
            _ => {
                todo!()
            }
        }
        Ok(())
    }
}