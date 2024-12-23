use tracing::{info, warn};
use uuid::Uuid;
use crate::{listener::{ListenerManager, ListenerManagerArgs}, session::{SessionManager, SessionManagerArgs}, subscriber::{SubscriberManager, SubscriberManagerArgs}, topic::{TopicManager, TopicManagerArgs}};
use common::{BrokerMessage, LISTENER_MANAGER_NAME, SESSION_MANAGER_NAME, SUBSCRIBER_MANAGER_NAME, TOPIC_MANAGER_NAME};
use ractor::{actor::actor_cell, async_trait, concurrency::JoinHandle, registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};


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

#[async_trait]
impl Actor for Broker {
    type Msg = BrokerMessage;
    type State = BrokerState;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _: (),
    ) -> Result<Self::State, ActorProcessingErr> {
        tracing::info!("Broker: Starting {myself:?}");
        //start listener manager to listen for incoming connections
        let mut handles = Vec::new();
        //clone actor re
        handles.push(tokio::spawn(async move {
            let (_, handle) = Actor::spawn(Some(LISTENER_MANAGER_NAME.to_owned()), ListenerManager, ListenerManagerArgs { broker_id: "BrokerSupervisor".to_string()}).await.expect("Failed to start listener manager");
            handle.await.expect("Failed");
        }));

        handles.push(tokio::spawn(async move {
            let session_mgr_args = SessionManagerArgs {
                broker_id: "BrokerSupervisor".to_string()
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

    async fn handle_supervisor_evt(&self, myself: ActorRef<Self::Msg>, msg: SupervisionEvent, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        
        match msg {
            SupervisionEvent::ActorStarted(actor_cell) => {
                match actor_cell.get_name() {
                 Some(name)   => {
                    info!("Worker agent: {name}:{0:?} started", actor_cell.get_id());
                    let worker = ActorRef::from(actor_cell);
                    if name == LISTENER_MANAGER_NAME { state.listener = Some(worker.clone())}
                    if name == SESSION_MANAGER_NAME  { state.session_manager = Some(ActorRef::from(worker.clone()))}
                    if name == TOPIC_MANAGER_NAME    { state.topic_manager = Some(ActorRef::from(worker.clone()))}
                    if name == SUBSCRIBER_MANAGER_NAME { state.subscriber_manager = Some(ActorRef::from(worker.clone()))}
                 },
                 None => todo!()
                }
            }
            SupervisionEvent::ActorTerminated(actor_cell, boxed_state, _) => {
                
                //determine type of actor that failed and restart
                //NOTE: "Remote" actors can't have their types checked? But they do send serializable messages
                // If we can deserialize them to a datatype here, that may be another acceptable means of determining type
                if let Some(_) = actor_cell.is_message_type_of::<BrokerMessage>() {
                    info!("Worker {0:?}:{1:?} terminated, restarting..", actor_cell.get_name(), actor_cell.get_id());
                    //start new listener manager
                    // todo!()
                }
                
            }
            SupervisionEvent::ActorFailed(actor_cell, _) => {
                warn!("Worker agent: {0:?}:{1:?}failed!", actor_cell.get_name(), actor_cell.get_id());
            },
            SupervisionEvent::ProcessGroupChanged(_) => todo!(),
        }

        Ok(())
    }
    
    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
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
            BrokerMessage::RegistrationRequest { client_id } => {
                //The listenerManager forwarded the request, generate a new ID to serve as their identifier for internal purposes
                //TODO: Is there any reason we'd fail at this point? User can authenticate but isn't authroized?
                info!("Registering client {client_id}");

                // Send RegistrationResponse to SessionManager 
                
                match &state.session_manager
                {
                    Some(session_mgr) => {
                        session_mgr.send_message(BrokerMessage::RegistrationRequest { client_id: client_id.clone() }).expect("Failed to forward registration response to listener manager");
                    }, 
                    None => todo!()
                }
            },
            BrokerMessage::RegistrationResponse { registration_id, client_id, success, error } => {
                //Use enum type as a confirmation that session agent has started, all comms between clients and the broker should now flow through the session agent
                info!("Session started for client: {client_id}");
                match &state.listener
                {
                    Some(listener) => {
                        listener.send_message(BrokerMessage::RegistrationResponse {
                            registration_id: registration_id.clone(),
                            client_id: client_id.clone(),
                            success: true, 
                            error: None 
                        })
                        .expect("Failed to forward registration response to listener manager");
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
            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result } => {
                info!("Received successful subscribe ack for session: {registration_id}");
                where_is(registration_id.clone()).unwrap().send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result }).expect("Failed to forward message to session: {registration_id}");
            },
            BrokerMessage::PublishResponse { topic, payload, result } => todo!(),
            BrokerMessage::ErrorMessage { client_id, error } => {
                warn!("Error Received: {error}");
            },
            BrokerMessage::PongMessage { registration_id } => todo!(),
            BrokerMessage::TimeoutMessage { registration_id } => {
                warn!("Received timeout for registration ID: {registration_id}");                
                //tell listener mgr to handle business
                state.listener.as_ref().unwrap().send_message(BrokerMessage::TimeoutMessage { registration_id }).expect("Failed to forward timeout message to listener manager");
            }
            _ => todo!()
        }
        Ok(())
    }
}   
