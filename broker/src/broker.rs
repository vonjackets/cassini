use tracing::{error, info, warn};
use crate::{get_subsciber_name, listener::{ListenerManager, ListenerManagerArgs}, session::{SessionManager, SessionManagerArgs}, subscriber::SubscriberManager, topic::{TopicManager, TopicManagerArgs}, SUBSCRIBE_REQUEST_FAILED_TXT, UNEXPECTED_MESSAGE_STR};
use crate::{BrokerMessage, LISTENER_MANAGER_NAME, SESSION_MANAGER_NAME, SUBSCRIBER_MANAGER_NAME, TOPIC_MANAGER_NAME};
use ractor::{async_trait, registry::where_is, rpc::call_and_forward, Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};


// ============================== Broker Supervisor Actor Definition ============================== //

pub struct Broker;

//State object containing references to worker actors.
pub struct BrokerState;
/// Would-be configurations for the broker actor itself, as well its workers
#[derive(Clone)]
pub struct BrokerArgs {
    /// Socket address to listen for connections on
    pub bind_addr: String,
    /// The amount of time (in seconds) before a session times out
    /// Should be between 10 and 300 seconds
    pub session_timeout: Option<u64>,
    pub server_cert_file: String,
    pub private_key_file: String,
    pub ca_cert_file: String
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
        
        let listener_manager_args =  ListenerManagerArgs {
            bind_addr: args.bind_addr,
            server_cert_file: args.server_cert_file,
            private_key_file: args.private_key_file,
            ca_cert_file: args.ca_cert_file
        };

        Actor::spawn_linked(Some(LISTENER_MANAGER_NAME.to_owned()), ListenerManager, listener_manager_args, myself.clone().into()).await.expect("Expected to start Listener Manager");


        //set default timeout for sessions, or use args
        let mut session_timeout: u64 = 90;
        if let Some(timeout) = args.session_timeout {
            session_timeout = timeout;
        }

        Actor::spawn_linked(Some(SESSION_MANAGER_NAME.to_string()), SessionManager, SessionManagerArgs { session_timeout: session_timeout }, myself.clone().into()).await.expect("Expected Session Manager to start");

        //TODO: Read some topics from configuration based on services we want to observer/consumer messages for
        let topic_mgr_args = TopicManagerArgs {topics: None};

        Actor::spawn_linked(Some(TOPIC_MANAGER_NAME.to_owned()), TopicManager, topic_mgr_args, myself.clone().into()).await.expect("Expected to start Topic Manager");    

        Actor::spawn_linked(Some(SUBSCRIBER_MANAGER_NAME.to_string()), SubscriberManager, (), myself.clone().into()).await.expect("Expected to start Subscriber Manager");

        let state = BrokerState;
        Ok(state)
    }

    async fn handle_supervisor_evt(&self, _myself: ActorRef<Self::Msg>, msg: SupervisionEvent, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
        match msg {
            SupervisionEvent::ActorStarted(actor_cell) => info!("Worker agent: {0:?}:{1:?} started", actor_cell.get_name(), actor_cell.get_id()),
            
            SupervisionEvent::ActorTerminated(actor_cell, ..) => info!("Worker {0:?}:{1:?} terminated, restarting..", actor_cell.get_name(), actor_cell.get_id()),
            
            SupervisionEvent::ActorFailed(actor_cell, e) => {
                warn!("Worker agent: {0:?}:{1:?} failed! {e}", actor_cell.get_name(), actor_cell.get_id());
                //determine type of actor that failed and restart
                //NOTE: "Remote" actors can't have their types checked? But they do send serializable messages
                // If we can deserialize them to a datatype here, that may be another acceptable means of determining type
                //TODO: Figure out what panics/failures we can/can't recover from
                // Missing certificate files and the inability to forward some messages count as bad states
                _myself.stop(Some("ACTOR_FAILED".to_string()));

            },
            SupervisionEvent::ProcessGroupChanged(_) => (),
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
        _: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        //TODO: Implement? The master node seems purely responsible for managing actor lifecycles, doesn't really do any message brokering on its own
        match message {
            BrokerMessage::RegistrationRequest { registration_id, client_id } => {                
                match &where_is(SESSION_MANAGER_NAME.to_string())
                {
                    Some(session_mgr) => {
                        session_mgr.send_message(BrokerMessage::RegistrationRequest { registration_id, client_id }).expect("Failed to forward registration response to listener manager");
                    }, 
                    None => todo!()
                }
            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
                // where_is(SUBSCRIBER_MANAGER_NAME.to_owned()).unwrap().send_message(BrokerMessage::SubscribeRequest { registration_id: registration_id.clone(), topic: topic.clone() }).expect("Failed to forward subscribeRequest to subscriber manager");

                //Look for existing subscriber actor.
                
                if let Some(registration_id) = registration_id {
                    
                    match where_is(get_subsciber_name(&registration_id, &topic)) {
                        Some(_) => {
                            //Send success message, session already subscribed
                            match &where_is(registration_id.clone()) {
                                //session's probably dead for one reason or another if we fail here,
                                //log and move on
                                Some(session) => {
                                    session.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id: registration_id.clone(), topic: topic.clone(), result: Ok(()) })
                                    .unwrap_or_else(
                                        |e| {
                                            warn!("Session {registration_id} not found! {e}!");
                                        })
                                }, 
                                None => warn!("Session {registration_id} not found!")
                            }
                        },
                        None => {
                            // If no subscriber exists, forward subscribe request topic actor
                            // forward to topic actor
                            match where_is(topic.clone()) {
                                Some(actor) => {
                                    //forward request
                                    actor.send_message(BrokerMessage::SubscribeRequest { registration_id: Some(registration_id.clone()), topic: topic.clone() })
                                    .unwrap_or_else(|e| {
                                        todo!("What to do if broker can't find the topic actor when subscribing?")
                                        //TODO:What to do if broker can't find the topic actor when subscribing?
                                        // if we "find" the topic actor, but fail to send it a message
                                        // it's possible it either panicked or was killed sometime between the lookup and the send
                                        // We can either outright fail to subscribe in this case and alert the client,
                                        // OR we can start a new topic over again?
                                        

                                        //couldn't send a message to the topic actor, let know
                                        // warn!("Failed to subscribe client to topic \"{topic}\". Topic not found!")
                                        // match &where_is(registration_id.clone()) {
                                        //     Some(session) => {
                                        //         session.send_message(BrokerMessage::SubscribeAcknowledgment { registration_id: registration_id.clone(), topic: topic.clone(), result: Ok(()) })
                                        //         .unwrap_or_else(
                                        //             |e| {
                                        //                 warn!("Failed to subscribe client to topic \"{topic}\". {e}!");
                                        //             })
                                        //     }, 
                                        //     None => warn!("Failed to subscribe client to topic \"{topic}\". Session not found!")
                                        // }

                                        
                                    })
                                },
                                None => {
                                    //no topic actor exists, tell manager to make one
                                    let topic_mgr = where_is(TOPIC_MANAGER_NAME.to_owned());
                                    let sub_mgr = where_is(SUBSCRIBER_MANAGER_NAME.to_owned());

                                    let t = topic.clone();
                                    let id = registration_id.clone();

                                    if topic_mgr.is_some() && sub_mgr.is_some() {
                                        let _ = call_and_forward(&topic_mgr.unwrap(),
                                        |reply| {
                                            BrokerMessage::AddTopic { reply, registration_id: Some(id), topic: t }
                                        },
                                        sub_mgr.unwrap(),
                                        move |result| {
                                            result.map_or_else(|e| {
                                                //failed
                                                // send error to client
                                                todo!("send error to client")
                                            }, |_| {
                                                //succeeded, forward to sub manager
                                                BrokerMessage::SubscribeRequest { registration_id: Some(registration_id), topic}
                                            })
                                        },
                                        None) //TODO: Set timeouut?
                                        .map_err(|e| {
                                            error!("{}", format!("{SUBSCRIBE_REQUEST_FAILED_TXT}: {e}"));
                                        })
                                        .unwrap().await;
                                    } else {
                                        error!("{}", format!("{SUBSCRIBE_REQUEST_FAILED_TXT}: Failed to locate one or more managers!"));
                                    }
                                },
                            }
                        },
                    }
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
                match where_is(SUBSCRIBER_MANAGER_NAME.to_owned()) {
                    Some(actor) => actor.send_message(BrokerMessage::PublishResponse {topic, payload, result }).expect("Failed to forward notification to subscriber manager"),
                    None => tracing::error!("Failed to lookup subscriber manager!")
                } 
                
            },
            BrokerMessage::ErrorMessage { error, .. } => {
                warn!("Error Received: {error}");
            },
            BrokerMessage::DisconnectRequest { client_id, registration_id } => {
                //start cleanup
                info!("Cleaning up session {registration_id:?}");
                if let Some(manager) = where_is(SUBSCRIBER_MANAGER_NAME.to_string()) {
                    manager.send_message(BrokerMessage::DisconnectRequest {
                        client_id: client_id.clone(),
                        registration_id: registration_id.clone(),
                    }).expect("Expected to forward message");
                    
                }
                // Tell listener manager to kill listener, it's not coming back
                if let Some(manager) = where_is(LISTENER_MANAGER_NAME.to_string()) {
                    manager.send_message(BrokerMessage::DisconnectRequest {
                        client_id: client_id.clone(),
                        registration_id,
                    }).expect("Expected to forward message");
                }
            }
            BrokerMessage::TimeoutMessage { client_id, registration_id, error } => {
                //cleanup subscribers
                
                if let Some(manager) = where_is(SUBSCRIBER_MANAGER_NAME.to_string()) {
                    manager.send_message(BrokerMessage::TimeoutMessage {
                        client_id: client_id.clone(),
                        registration_id: registration_id.clone(),
                        error: error.clone()
                    }).expect("Expected to forward message");
                    
                }
            }
            _ => warn!(UNEXPECTED_MESSAGE_STR)
        }
        Ok(())
    }
}   
