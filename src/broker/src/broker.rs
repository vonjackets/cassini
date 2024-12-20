use tracing::{info, warn};
use crate::{listener::ListenerManager, topic::{TopicManager, TopicManagerArgs}, BrokerMessage};
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};


// ============================== Broker Supervisor Actor Definition ============================== //

pub struct Broker;

//State object containing references to worker actors.
#[derive(Clone)]
pub struct BrokerState {
    listener: ActorRef<BrokerMessage>,
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
        //start listener maanger to listen for incoming connections
        
        let (listener_manager, handle) = Actor::spawn_linked(Some("listenerManager".to_owned()), ListenerManager, (), myself.clone().into()).await.expect("couldn't start listener manager");
        let state = BrokerState {
            listener: listener_manager.clone()
        };

        
        //handle.await.expect("Something happened");
        //TODO: read these from configuration
        let args = TopicManagerArgs {topics: None};
        // start topic manager
        let (topic_manager, topic_mgr_handle) = Actor::spawn_linked(Some("topicManager".to_owned()), TopicManager, args, myself.clone().into()).await.expect("Could not start topic manager agent");
        topic_mgr_handle.await.expect("something happened with the topic manager");
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
        todo!();
        Ok(())
    }
}   
