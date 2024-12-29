use tokio::{io::{AsyncBufReadExt, AsyncWriteExt}, net::TcpListener, sync::Mutex};
use tracing::{debug, error, info, warn};
use std::{ collections::HashMap, sync::Arc};


use ractor::{registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use async_trait::async_trait;


use common::{BrokerMessage, ClientMessage, BROKER_NAME};

use crate::UNEXPECTED_MESSAGE_STR;

// ============================== Listener Manager ============================== //
/// The actual listener/server process. When clients connect to the server, their stream is split and 
/// given to a worker processes to use to interact with and handle that connection with the client.
pub struct ListenerManager;
pub struct ListenerManagerState {
    listeners: HashMap<String, ActorRef<BrokerMessage>>, //client_ids : listenerAgent mapping
}

pub struct ListenerManagerArgs {
    pub broker_id:  String
    //TODO: Get bind_addr from config and pass in here?
}
#[async_trait]
impl Actor for ListenerManager {
    type Msg = BrokerMessage;
    type State = ListenerManagerState;
    type Arguments = ListenerManagerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: ListenerManagerArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        tracing::info!("ListenerManager: Starting {myself:?}");
        //link with supervisor
        match where_is(BROKER_NAME.to_string()) {
            Some(broker) => myself.link(broker),
            None => warn!("Couldn't link with broker supervisor!")
        }
        //set up state object
        let state = ListenerManagerState { listeners: HashMap::new() };
        info!("ListenerManager: Agent starting");
        Ok(state)
    }

    /// Once a the manager is running as a process, start the server
    /// and listen for incoming connections
    async fn post_start(&self, myself: ActorRef<Self::Msg>, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {


        let bind_addr = "127.0.0.1:8080"; //TODO: replace with value from state after reading it in from args
        let server = TcpListener::bind(bind_addr).await.expect("could not start tcp listener");
        
        info!("ListenerManager: Server running on {}", bind_addr);
        
        let _ = tokio::spawn(async move {
            
               while let Ok((stream, _)) = server.accept().await {
                    
                        // Generate a unique client ID
                        let client_id = uuid::Uuid::new_v4().to_string();
                        
                        // Create and start a new Listener actor for this connection
                        
                        let (reader, writer) = stream.into_split();
                        let writer = tokio::io::BufWriter::new(writer);
                        
                        let listener_args = ListenerArguments {
                            writer: Arc::new(Mutex::new(writer)),
                            reader: Some(reader),
                            client_id: client_id.clone(),
                            registration_id: None
                        };
           
                        //start listener actor to handle connection
                        let _ = Actor::spawn_linked(Some(client_id.clone()), Listener, listener_args, myself.clone().into()).await.expect("Failed to start listener for new connection");
            }
        });

        Ok(())
    }

    async fn handle_supervisor_evt(&self, _: ActorRef<Self::Msg>, msg: SupervisionEvent, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        
        match msg {
            SupervisionEvent::ActorStarted(actor_cell) => {
                info!("Worker agent: {0:?}:{1:?} started", actor_cell.get_name(), actor_cell.get_id());               
                //Finish registration flow here
                let client_id = actor_cell.get_name().clone().unwrap();
                let listener_ref: ActorRef<BrokerMessage> = ActorRef::where_is(client_id.clone()).unwrap();
                state.listeners.insert(client_id.clone(), listener_ref.clone());
                
            }
            SupervisionEvent::ActorTerminated(actor_cell, _, reason) => {
                //TODO: Getting here means a connection died, forward a message to thje broker via the session manager
                
                info!("Worker agent: {0:?}:{1:?} terminated. {reason:?}", actor_cell.get_name(), actor_cell.get_id());
                state.listeners.remove(&actor_cell.get_name().unwrap());
                
            }
            SupervisionEvent::ActorFailed(actor_cell, _) => {
                warn!("Worker agent: {0:?}:{1:?} failed!", actor_cell.get_name(), actor_cell.get_id());
            },
            SupervisionEvent::ProcessGroupChanged(_) => todo!(),
        }

        Ok(())
    }

    async fn handle(
        &self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        _: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            BrokerMessage::RegistrationResponse { registration_id, client_id, success, error } => {
                //forwward registration_id back to listener to signal success
                debug!("Forwarding registration ack to listener: {client_id}");
                where_is(client_id.clone()).unwrap().send_message(BrokerMessage::RegistrationResponse { registration_id, client_id, success, error }).expect("Failed to forward message to client: {client_id}");
                
            },
            BrokerMessage::TimeoutMessage { client_id, ..} => {
                match where_is(client_id.clone()) {
                    Some(listener) => listener.stop(Some("TIMEDOUT".to_string())),
                    None => warn!("Couldn't find listener {client_id}")
                }
                
            }
            _ => {
                todo!()
            }
        }
        Ok(())
    }


}

// ============================== Listener actor ============================== //
/// The Listener is the actor responsible for maintaining services' connection to the broker
/// and interpreting client messages.
/// All comms are forwarded to the broker via the sessionAgent after being validated.
struct Listener;

struct ListenerState {
    writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
    reader: Option<tokio::net::tcp::OwnedReadHalf>, // Use Option to allow taking ownership
    client_id: String,
    registration_id: Option<String>,
}
//TDOD: Establish why we use this vs passing a state obj?
struct ListenerArguments {
    writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
    reader: Option<tokio::net::tcp::OwnedReadHalf>, // Use Option to allow taking ownership
    client_id: String,
    registration_id: Option<String>,
}

impl Listener {

    async fn write(client_id: String, msg: ClientMessage, writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>)  {
        match serde_json::to_string(&msg) {
            Ok(serialized) => {
                tokio::spawn( async move {
                    let mut writer = writer.lock().await;
                    let msg = format!("{serialized}\n"); //add newline
                    if let Err(e) = writer.write_all(msg.as_bytes()).await {
                        warn!("Failed to send message to client {client_id}: {e}");
                    }
                    writer.flush().await.expect("???");
                }).await.expect("Expected write thread to finish");   
            }
            Err(e) => error!("{e}")
        }
    }
}

#[async_trait]
impl Actor for Listener {
    type Msg = BrokerMessage;
    type State = ListenerState;
    type Arguments = ListenerArguments;

    async fn pre_start(
        &self,
        _: ActorRef<Self::Msg>,
        args: ListenerArguments
    ) -> Result<Self::State, ActorProcessingErr> {
        let state: ListenerState = ListenerState {
            writer: args.writer,
            reader: args.reader,
            client_id: args.client_id.clone(),
            registration_id: args.registration_id
        };
        Ok(state)
    }

    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        info!("Listener: Listener started for client_id: {}", state.client_id.clone());
        
        let id= state.client_id.clone(); 

        let reader = state.reader.take().expect("Reader already taken!");
        
        //start listening
        let _ = tokio::spawn(async move {

            let mut buf = String::new();

            let mut buf_reader = tokio::io::BufReader::new(reader);
            loop { 
                buf.clear();
                match buf_reader.read_line(&mut buf).await {
                    Ok(bytes) => {
                        if bytes > 0 {
                            
                            if let Ok(msg) = serde_json::from_str::<ClientMessage>(&buf) {
                                match msg {
                                    ClientMessage::PingMessage => debug!("PING"),
                                    _ => {
                                      debug!("Received message: {msg:?}");
                                      //convert datatype to broker_meessage, fields will be populated during message handling
                                      let converted_msg = BrokerMessage::from_client_message(msg, id.clone(), None);
                                      myself.send_message(converted_msg).expect("Could not forward message to {myself:?}");
                                    }
                                }

                                
                            } else {
                                //bad data
                                warn!("Failed to parse message from client");
                                // todo!("Send message back to client with an error");
                            }
                        }    
                    }, 
                    Err(e) => {
                            // Handle client disconnection, populate state in handler
                            myself.send_message(BrokerMessage::TimeoutMessage { client_id: String::default(), registration_id: None, error: Some(e.to_string()) } ).unwrap();
                            warn!("Client {id} disconnected: {e}");
                    }
                } 
            }
        });
        Ok(())
    }

    async fn post_stop(&self, myself: ActorRef<Self::Msg>, _state: &mut Self::State) -> Result<(), ActorProcessingErr> {

        debug!("Successfully stopped {myself:?}");
        Ok(())
    }
    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            BrokerMessage::RegistrationRequest { registration_id, client_id } => {
                // If we got some registration id here, 
                // try to find the active session and reconnect,

                if registration_id.is_some() && state.registration_id.is_some() {
                    let id_in = registration_id.unwrap();
                    let session_id = &state.registration_id.clone().unwrap();


                    if id_in == session_id.to_owned() {
                        //send message to session that it has a new listener for it to get messages from
                        match where_is(session_id.clone()) {
                            Some(session) => {
                                info!("Resuming session: {session_id}!");
                                session.send_message(
                                    BrokerMessage::RegistrationRequest {
                                    registration_id: Some(session_id.to_owned()), client_id
                                })
                                .expect("Expected to send new client_id to new session");

                            }, None => {error!("Could not find session with id {session_id} for client: {client_id}") }
                        }
                    }
                } else {
                    // Forward to broker to begin creating new session and wait for response.
                    info!("Starting new session!");
                    match where_is(BROKER_NAME.to_string()) {
                        Some(broker) => {
                            broker.send_message(BrokerMessage::RegistrationRequest { registration_id: None, client_id: myself.get_name().unwrap_or_default() })
                            .expect("Expected to forward request to broker");
                        } None => todo!()
                    }
                }

                

            }
            BrokerMessage::RegistrationResponse { registration_id, client_id, success, error } => {
                if success {
                    debug!("Successfully registered with id: {registration_id:?}");
                    state.registration_id = registration_id.clone();
                    
                    Listener::write(client_id.clone(), ClientMessage::RegistrationResponse {
                        registration_id: registration_id.unwrap_or_default(),
                        success: true,
                        error: None 
                    }, Arc::clone(&state.writer)).await;

                } else {
                    error!("Failed to register with broker! {error:?}");
                    //TODO: Determine failure cases and how to respond
                }
                
            },
            BrokerMessage::PublishRequest { topic, payload, .. } => {
                //confirm listener has registered session
                if let Some(id) = &state.registration_id {

                    where_is(id.clone()).map_or_else(|| { error!("Could not forward request to session")},
                    |session| {
                        session.send_message(BrokerMessage::PublishRequest { registration_id: Some(id.to_string()), topic, payload }).expect("Expected to forward message");
                    });
                                                    
                } else {warn!("Received request from unregistered client!!"); }
            }
            BrokerMessage::PublishResponse { topic, payload, .. } => {
                info!("Successfully published message to topic: {topic}");
                let msg = ClientMessage::PublishResponse { topic: topic.clone(), payload: payload.clone(), result: Result::Ok(()) };  
                Listener::write(state.client_id.clone(), msg, Arc::clone(&state.writer)).await;            
            },

            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, .. } => {
                debug!("Agent successfully subscribed to topic: {topic}");
                let response = ClientMessage::SubscribeAcknowledgment {
                    topic, result: Result::Ok(())
                };
                
                
                Listener::write(registration_id.clone(), response, Arc::clone(&state.writer)).await;

            },
            BrokerMessage::SubscribeRequest {topic , ..} => {
                //Got request to subscribe from client, confirm we've been registered
                if let Some(id) = &state.registration_id {

                
                
                    where_is(id.clone()).map_or_else(|| { error!("Could not forward request to session")},
                    |session| {
                        session.send_message(BrokerMessage::SubscribeRequest { registration_id: Some(id.to_string()), topic }).expect("Expected to forward message");
                    });
                
                                
                } else {warn!("Received request from unregistered client!!"); }
               
            },
            BrokerMessage::UnsubscribeRequest { topic, .. } => {
                if let Some(id) = &state.registration_id {    
                    where_is(id.clone()).map_or_else(|| { error!("Could not forward request to session")},
                    |session| {
                        session.send_message(BrokerMessage::UnsubscribeRequest { registration_id: Some(id.to_string()), topic }).expect("Expected to forward message");
                    });
                } else {warn!("Received request from unregistered client!!"); }
            }
            BrokerMessage::UnsubscribeAcknowledgment { registration_id, topic, .. } => {
                //TODO: log client id instead
                debug!("Session {registration_id} successfully unsubscribed from topic: {topic}");
                let response = ClientMessage::UnsubscribeAcknowledgment {
                     topic, result: Result::Ok(())
                };
                
                Listener::write(registration_id.clone(), response, Arc::clone(&state.writer)).await;
            },
            BrokerMessage::DisconnectRequest { client_id } => {
                info!("Received disconnect request from client: {client_id}");
                //if we're registered, propogate to session agent, otherwise, die with honor
                match &state.registration_id {
                    Some(id) => {
                        where_is(id.to_string()).unwrap().send_message(BrokerMessage::DisconnectRequest { client_id }).unwrap();
                    }
                    _ => ()
                }
                myself.kill();

            }
            BrokerMessage::TimeoutMessage {error, .. } => {
                //Client timed out, if we were registered, let session know, otherwise, die with honor
                match &state.registration_id {
                    Some(id) => where_is(id.to_owned()).map_or_else(|| {}, |session| {
                        
                        session.send_message(BrokerMessage::TimeoutMessage { client_id: myself.get_name().unwrap(), registration_id: Some(id.clone()), error: error }).expect("Expected to forward message to session.")
                     }),
                    _ => ()
                }
                //TODO: Do something productive instead of falling on ones sword. Enter reconnect loop that tries 1-3 times before giving up?
                // myself.kill()
            }
            _ => {
                warn!(UNEXPECTED_MESSAGE_STR)
            }
        }
        Ok(())
        
    }
}
