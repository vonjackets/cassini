use tracing::{info, warn};
use crate::{listener::{ListenerManager, ListenerManagerArgs}, session::{SessionManager, SessionManagerArgs}, subscriber::{SubscriberManager, SubscriberManagerArgs}, topic::{TopicManager, TopicManagerArgs}};
use common::{BrokerMessage, LISTENER_MANAGER_NAME, SESSION_MANAGER_NAME, SUBSCRIBER_MANAGER_NAME, TOPIC_MANAGER_NAME};
use ractor::{async_trait, concurrency::JoinHandle, registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};


// ============================== Broker Supervisor Actor Definition ============================== //

pub struct Broker;

//State object containing references to worker actors.
pub struct BrokerState {
    listener: Option<ActorRef<BrokerMessage>>,
    topic_manager: Option<ActorRef<BrokerMessage>>,
    session_manager: Option<ActorRef<BrokerMessage>>,
    subscriber_manager: Option<ActorRef<BrokerMessage>>,
    handles: Vec<JoinHandle<()>>
}
/// Would-be configurations for the broker actor itself, as well its workers
#[derive(Clone)]
pub struct BrokerArgs {
    /// Socket address to listen for connections on
    pub bind_addr: String,
    /// The amount of time (in seconds) before a session times out
    /// Should be between 10 and 300 seconds
    pub session_timeout: Option<u64>,
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
        
        //start listener manager to listen for incoming connections

        let mut handles = Vec::new();
        //clone actor re
        handles.push(tokio::spawn(async move {
            let (_, handle) = Actor::spawn(Some(LISTENER_MANAGER_NAME.to_owned()), ListenerManager, ListenerManagerArgs { bind_addr: args.bind_addr}).await.expect("Failed to start listener manager");
            handle.await.expect("Failed");
        }));

        
        let mut session_timeout: u64 = 90;
        if let Some(timeout) = args.session_timeout {
            session_timeout = timeout;
        }
        handles.push(tokio::spawn(async move {
            let session_mgr_args = SessionManagerArgs {
                session_timeout: session_timeout 
            };
            let (_, handle) = Actor::spawn(Some(SESSION_MANAGER_NAME.to_string()), SessionManager, session_mgr_args).await.expect("");
            handle.await.expect("????");
        }));

        handles.push(tokio::spawn(async move {
        //TODO: read these from configuration
            let topic_mgr_args = TopicManagerArgs {topics: None, broker_id: "BrokerSupervisor".to_string()};
            // start topic manager
            let (_, handle) = Actor::spawn(Some(TOPIC_MANAGER_NAME.to_owned()), TopicManager, topic_mgr_args).await.expect("Could not start topic manager agent");    
            handle.await.expect("????");
        }));

        //let subscriber_mgr_args = SubscriberManagerArgs { topic_mgr_ref: topic_manager.clone(), session_manager_ref: session_mgr.clone() };
        handles.push(tokio::spawn(async move {
            let subscriber_mgr_args = SubscriberManagerArgs {
                broker_id: "BrokerSupervisor".to_string()
            };
            let (_, handle) = Actor::spawn(Some(SUBSCRIBER_MANAGER_NAME.to_string()), SubscriberManager, subscriber_mgr_args).await.expect("Failed to start subscriber manager");
            handle.await.expect("????");
        }));

        let state = BrokerState {
            listener: None,
            topic_manager: None,
            session_manager: None,
            subscriber_manager: None,
            handles: handles
        };
        Ok(state)
    }

    async fn handle_supervisor_evt(&self, _myself: ActorRef<Self::Msg>, msg: SupervisionEvent, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        
        match msg {
            SupervisionEvent::ActorStarted(actor_cell) => {
                match actor_cell.get_name() {
                 Some(name)   => {
                    info!("Worker agent: {name}:{0:?} started", actor_cell.get_id());
                    let worker = ActorRef::from(actor_cell);
                    if name == LISTENER_MANAGER_NAME { state.listener = Some(worker.clone())}
                    else if name == SESSION_MANAGER_NAME  { state.session_manager = Some(worker.clone())}
                    else if name == TOPIC_MANAGER_NAME    { state.topic_manager = Some(worker.clone())}
                    else if name == SUBSCRIBER_MANAGER_NAME { state.subscriber_manager = Some(worker.clone())}
                 },
                 None => todo!()
                }
            }
            SupervisionEvent::ActorTerminated(actor_cell, ..) => {
                
                //determine type of actor that failed and restart
                //NOTE: "Remote" actors can't have their types checked? But they do send serializable messages
                // If we can deserialize them to a datatype here, that may be another acceptable means of determining type
                if let Some(_) = actor_cell.is_message_type_of::<BrokerMessage>() {
                    info!("Worker {0:?}:{1:?} terminated, restarting..", actor_cell.get_name(), actor_cell.get_id());
                    //TODO: Start new actor in its own thread as to not interrupt this one
                }
                
            }
            SupervisionEvent::ActorFailed(actor_cell, _) => {
                warn!("Worker agent: {0:?}:{1:?} failed!", actor_cell.get_name(), actor_cell.get_id());
            },
            SupervisionEvent::ProcessGroupChanged(_) => todo!(),
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
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        //TODO: Implement? The master node seems purely responsible for managing actor lifecycles, doesn't really do any message brokering on its own
        match message {
            BrokerMessage::RegistrationRequest { registration_id, client_id } => {                
                match &state.session_manager
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
                where_is(SUBSCRIBER_MANAGER_NAME.to_owned()).map_or_else(|| {
                    tracing::error!("Failed to lookup subscriber manager!");
                    //TOOD: ?
                }, |actor| {
                    actor.send_message(BrokerMessage::PublishResponse {topic, payload, result }).expect("Failed to forward notification to subscriber manager");
                });
            },
            BrokerMessage::ErrorMessage { error, .. } => {
                warn!("Error Received: {error}");
            },
            BrokerMessage::DisconnectRequest { client_id, registration_id } => {
                //start cleanup
                info!("Cleaning up session {registration_id:?}");
                if let Some(manager) = &state.subscriber_manager {
                    manager.send_message(BrokerMessage::DisconnectRequest {
                        client_id: client_id.clone(),
                        registration_id: registration_id.clone(),
                    }).expect("Expected to forward message");
                    
                }
                // Tell listener manager to kill listener, it's not coming back
                if let Some(manager) = &state.listener {
                    manager.send_message(BrokerMessage::DisconnectRequest {
                        client_id: client_id.clone(),
                        registration_id,
                    }).expect("Expected to forward message");
                }
            }
            BrokerMessage::TimeoutMessage { client_id, registration_id, error } => {
                //cleanup subscribers
                
                if let Some(manager) = &state.subscriber_manager {
                    manager.send_message(BrokerMessage::TimeoutMessage {
                        client_id: client_id.clone(),
                        registration_id: registration_id.clone(),
                        error: error.clone()
                    }).expect("Expected to forward message");
                    
                }
                // Tell listener manager to kill listener, it's not coming back
                if let Some(manager) = &state.listener {
                    manager.send_message(BrokerMessage::TimeoutMessage {
                        client_id: client_id.clone(),
                        registration_id,
                        error
                    }).expect("Expected to forward message");
                }
                
            }
            _ => warn!("Received unexepcted message {message:?}")
        }
        Ok(())
    }
}   
