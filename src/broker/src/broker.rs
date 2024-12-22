use tracing::{info, warn};
use uuid::Uuid;
use crate::listener::{ListenerManager, ListenerManagerArgs};
use common::BrokerMessage;
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
            let (listener_manager, handle) = Actor::spawn(Some("listenerManager".to_owned()), ListenerManager, ListenerManagerArgs { broker_id: "BrokerSupervisor".to_string()}).await.expect("Failed to start listener manager");
            handle.await.expect("Failed");
        }));



        
        // handles.push(tokio::spawn(async move {
        //     let session_mgr_args = SessionManagerArgs {
        //         broker_id: "BrokerSupervisor".to_string()
        //     };
        //     let (session_mgr, session_handle) = Actor::spawn(Some("sessionManager".to_string()), SessionManager, session_mgr_args).await.expect("");
        //     session_handle.await.expect("????");
        // }));

        // handles.push(tokio::spawn(async move {
        // //TODO: read these from configuration
        //     let topic_mgr_args = TopicManagerArgs {topics: None};
        //     // start topic manager
        //     let (topic_manager, topic_mgr_handle) = Actor::spawn(Some("topicManager".to_owned()), TopicManager, topic_mgr_args).await.expect("Could not start topic manager agent");    
        // }));

        // //let subscriber_mgr_args = SubscriberManagerArgs { topic_mgr_ref: topic_manager.clone(), session_manager_ref: session_mgr.clone() };
        // handles.push(tokio::spawn(async move {
        //     let subscriber_mgr_args = SubscriberManagerArgs {
        //         broker_id: "BrokerSupervisor".to_string()
        //     };
        //     let (subscriber_mgr, subscriber_handle) = Actor::spawn(Some("subscriberManager".to_string()), SubscriberManager, subscriber_mgr_args).await.expect("Failed to start subscriber manager");
        // }));

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
                info!("Worker agent: {0:?}:{1:?} started", actor_cell.get_name(), actor_cell.get_id());
            }
            SupervisionEvent::ActorTerminated(actor_cell, boxed_state, _) => {
                
                //determine type of actor that failed and restart
                //NOTE: "Remote" actors can't have their types checked? But they do send serializable messages
                // If we can deserialize them to a datatype here, that may be another acceptable means of determining type
                if let Some(_) = actor_cell.is_message_type_of::<BrokerMessage>() {
                    info!("Worker Listener Manager: {0:?}:{1:?} terminated, restarting..", actor_cell.get_name(), actor_cell.get_id());
                    //start new listener manager
                    todo!()
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
                // Generate a new registration ID
                let registration_id = Uuid::new_v4().to_string();
                
                
                //TODO: anything to do here state-wise?

                // Send RegistrationResponse to SessionManager and ListenerManager to signal success
                
                // state.session_mgr.send_message(BrokerMessage::RegistrationResponse {
                //     registration_id: registration_id.clone(),
                //     client_id: client_id.clone(),
                //     success: true, 
                //     error: None 
                // })
                // .expect("Failed to forward registration response to session manager");
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
                //forward to subscriber and topic manager to ensure logic is followed
                state.subscriber_manager.as_ref().unwrap().send_message(BrokerMessage::SubscribeRequest { registration_id: registration_id.clone(), topic: topic.clone() }).expect("Failed to forward subscribeRequest to subscriber manager");
                state.topic_manager.as_ref().unwrap().send_message(BrokerMessage::SubscribeRequest { registration_id, topic }).expect("Failed to forward subscribeRequest to topic manager");
            },
            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result } => {
                info!("Received successful subscribe ack for session: {registration_id}");
                where_is(registration_id.clone()).unwrap().send_message(BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result }).expect("Failed to forward message to session: {registration_id}");
            },
            BrokerMessage::PublishResponse { topic, payload, result } => todo!(),
            BrokerMessage::ErrorMessage { client_id, error } => todo!(),
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
