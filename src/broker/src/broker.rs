use tracing::{info, subscriber, warn};
use uuid::Uuid;
use crate::{listener::{ListenerManager, ListenerManagerArgs}, session::{SessionManager, SessionManagerArgs, SessionManagerMessage}, subscriber::{SubscriberManager, SubscriberManagerArgs}, topic::{self, TopicManager, TopicManagerArgs}, BrokerMessage};
use ractor::{async_trait, registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};


// ============================== Broker Supervisor Actor Definition ============================== //

pub struct Broker;

//State object containing references to worker actors.
#[derive(Clone)]
pub struct BrokerState {
    listener: ActorRef<BrokerMessage>,
    topic_manager: ActorRef<BrokerMessage>,
    session_manager: ActorRef<BrokerMessage>,
    subscriber_manager:ActorRef<BrokerMessage>,
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
        tracing::info!("Broker: Started {myself:?}");
        //start listener manager to listen for incoming connections
        
        let listener_mgr_args = ListenerManagerArgs {
            broker_ref: Some(myself.clone())
        };

        let (listener_manager, handle) = Actor::spawn_linked(Some("listenerManager".to_owned()), ListenerManager, listener_mgr_args, myself.clone().into()).await.expect("couldn't start listener manager");
        

        
        //handle.await.expect("Something happened");
        //TODO: read these from configuration
        let topic_mgr_args = TopicManagerArgs {topics: None};
        // start topic manager
        let (topic_manager, topic_mgr_handle) = Actor::spawn_linked(Some("topicManager".to_owned()), TopicManager, topic_mgr_args, myself.clone().into()).await.expect("Could not start topic manager agent");
        topic_mgr_handle.await.expect("something happened with the topic manager");

        let session_mgr_args = SessionManagerArgs {
            topic_mgr_ref: Some(topic_manager.clone()),
            listener_mgr_ref: Some(listener_manager.clone()),
            broker_ref: Some(myself.clone())
        };

        let (session_mgr, _) = Actor::spawn_linked(Some("sessionManager".to_string()), SessionManager, session_mgr_args, myself.clone().into()).await.expect("");

        let subscriber_mgr_args = SubscriberManagerArgs { topic_mgr_ref: topic_manager.clone(), session_manager_ref: session_mgr.clone() };

        let (subscriber_mgr, _) = Actor::spawn_linked(Some("subscriberManager".to_string()), SubscriberManager, subscriber_mgr_args, myself.clone().into()).await.expect("Failed to start subscriber manager");
        let state = BrokerState {
            listener: listener_manager.clone(),
            topic_manager: topic_manager.clone(),
            session_manager: session_mgr.clone(),
            subscriber_manager: subscriber_mgr.clone()
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
                }
                
            }
            SupervisionEvent::ActorFailed(actor_cell, _) => {
                warn!("Worker agent: {0:?}:{1:?}failed!", actor_cell.get_name(), actor_cell.get_id());
            },
            SupervisionEvent::ProcessGroupChanged(_) => todo!(),
        }

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
               
                // Generate a new registration ID
                let registration_id = Uuid::new_v4().to_string();
                
                info!("Registering client {} with registration ID {}", client_id, registration_id);
                //TODO: anything to do here state-wise?

                // Send RegistrationResponse to SessionManager and ListenerManager to signal success
                state.session_manager
                .send_message(BrokerMessage::RegistrationResponse {
                    registration_id: registration_id.clone(),
                    client_id: client_id.clone(),
                    success: true, 
                    error: None 
                })
                .expect("Failed to forward registration response to session manager");
                state.listener
                .send_message(BrokerMessage::RegistrationResponse {
                    registration_id: registration_id.clone(),
                    client_id: client_id.clone(),
                    success: true, 
                    error: None 
                })
                .expect("Failed to forward registration response to listener manager");

                
               

            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                //forward to subscriber and topic manager to ensure logic is followed
                state.subscriber_manager.send_message(BrokerMessage::SubscribeRequest { registration_id: registration_id.clone(), topic: topic.clone() }).expect("Failed to forward subscribeRequest to subscriber manager");
                state.topic_manager.send_message(BrokerMessage::SubscribeRequest { registration_id, topic }).expect("Failed to forward subscribeRequest to topic manager");
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
                state.listener.send_message(BrokerMessage::TimeoutMessage { registration_id }).expect("Failed to forward timeout message to listener manager");
            }
            _ => todo!()
        }
        Ok(())
    }
}   
