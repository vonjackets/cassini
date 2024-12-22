use tokio::{io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, Interest}, net::{tcp::OwnedReadHalf, TcpListener}, sync::Mutex};
use tracing::{debug, error, info, warn};
use tracing_subscriber::field::debug;
use crate::{broker::{self, Broker}, session::{self, SessionManager, SessionManagerMessage}, topic::{TopicManager, TopicManagerArgs}};
use std::{ collections::HashMap, sync::{mpsc::Receiver, Arc}, thread};


use ractor::{concurrency::Duration, message, registry::where_is, Actor, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};
use async_trait::async_trait;

use serde::{Deserialize, Serialize};

use common::{BrokerMessage, ClientMessage};

// ============================== Listener Manager ============================== //
/// 
pub struct ListenerManager;
pub struct ListenerManagerState {
    listeners: HashMap<String, ActorRef<BrokerMessage>>, //client_ids : listenerAgent mapping
    broker_ref: ActorRef<BrokerMessage>
}

pub struct ListenerManagerArgs {
    pub broker_id:  String
    ////TODO: Get bind_addr from config and pass in here?
}
#[async_trait]
impl Actor for ListenerManager {
    type Msg = BrokerMessage;
    type State = ListenerManagerState;
    type Arguments = ListenerManagerArgs;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        args: ListenerManagerArgs
    ) -> Result<Self::State, ActorProcessingErr> {
        tracing::info!("ListenerManager: Starting {myself:?}");
        
        
        //set up state object
        let state = ListenerManagerState { listeners: HashMap::new(), broker_ref: ActorRef::from(where_is(args.broker_id).unwrap()) };
        info!("ListenerManager: Agent starting");
        Ok(state)
    }

    /// So as to not block the initialization, once a the manager is running as a process, start the server
    /// and listen for incoming connections
    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {

        //link with supervisor
        myself.link(state.broker_ref.get_cell());
        //myself.notify_supervisor(ractor::SupervisionEvent::ActorStarted(myself.get_cell()));


        let bind_addr = "127.0.0.1:8080"; //TODO: replace with value from state after reading it in from args
        let server = TcpListener::bind(bind_addr).await.expect("could not start tcp listener");
        
        info!("ListenerManager: Server running on {}", bind_addr);
        
        //TODO: evaluate whether this is the correct flow
        let handle = tokio::spawn(async move {
            
               while let Ok((stream, _)) = server.accept().await {
                    
                        // Generate a unique client ID
                        let client_id = uuid::Uuid::new_v4().to_string();
                        
                        //TODO: remove in favor of using try_get_supervisor
                        //give listener actor reference ot the manager
                        let mgr_ref = myself.clone();

                        // Create and start a new Listener actor for this connection
                        //TODO: pass startup args to pass the stream, the manager's reference, and the client id in
                        let (reader, writer) = stream.into_split();
                        let writer = tokio::io::BufWriter::new(writer);
                        
                        let listener_args = ListenerArguments {
                            writer: Arc::new(Mutex::new(writer)),
                            reader: Some(reader),
                            supervisor: mgr_ref,
                            client_id: client_id.clone(),
                            registration_id: None
                        };

                        
                        //start listener actor to handle connection
                        let _ = Actor::spawn_linked(Some(client_id.clone()), Listener, listener_args, myself.clone().into()).await.expect("Failed to start listener for new connection");
                        // //state can't be manipulated in this thread, send request to self to finish registration
                        // myself.cast(BrokerMessage::RegistrationRequest { client_id: client_id.clone() }).expect("Failed to send self a Registration Request for new conn");
                        // debug!("forwarded registration request to {myself:?}");

                    
            }
        
       
        });

        //handle.await.expect("Error awaiting handle for tcp server");

        Ok(())
    }

    async fn handle_supervisor_evt(&self, myself: ActorRef<Self::Msg>, msg: SupervisionEvent, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        
        match msg {
            SupervisionEvent::ActorStarted(actor_cell) => {
                info!("Worker agent: {0:?}:{1:?} started", actor_cell.get_name(), actor_cell.get_id());               
                //Finish registration flow here
                let client_id = actor_cell.get_name().clone().unwrap();
                let listener_ref: ActorRef<BrokerMessage> = ActorRef::where_is(client_id.clone()).unwrap();
                // let listener_rpc: RpcReplyPort<ActorRef<BrokerMessage>> = RpcReplyPort::from(listener_ref);
                //check listener is already registered
                //TODO: Ask the session manager for this information by forwarding the request to it
                if state.listeners.contains_key(&client_id.clone()) {
                    warn!("ListenerManager: Listener already exists for client ID {}", client_id);
                    //TODO: Send error message
                } else {
                    info!("Registering client: {client_id}");
                    state.listeners.insert(client_id.clone(), listener_ref.clone());
                    let broker_ref = ActorRef::from(myself.try_get_supervisor().expect("Failed to get ref to listenerManager supervisor"));
                    info!("Forarding message to broker");
                    match broker_ref.send_message(BrokerMessage::RegistrationRequest { client_id: client_id }) {
                        Ok(()) => {
                            debug!("successfully sent message");
                        },
                        Err(e) => todo!()
                    }
                }
            }
            SupervisionEvent::ActorTerminated(actor_cell, _, _) => {
                info!("Worker agent: {0:?}:{1:?} terminated", actor_cell.get_name(), actor_cell.get_id());
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
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            BrokerMessage::RegistrationRequest { client_id } => {

            },
            BrokerMessage::RegistrationResponse { registration_id, client_id, success, error } => {
                //forwward registration_id back to listener to signal success
                debug!("Forwarding registration ack to listener: {client_id}");
                where_is(client_id.clone()).unwrap().send_message(BrokerMessage::RegistrationResponse { registration_id, client_id, success, error }).expect("Failed to forward message to client: {client_id}");
                
            },
            BrokerMessage::DisconnectRequest { registration_id, client_id } => {
                info!("received disconnect request for session: {registration_id}");
                //kill
                let listener_ref = state.listeners.get(&client_id).expect("Failed to find listener agent by id {client_id}");
                listener_ref.kill();
            }
            _ => {
                todo!()
            }
        }
        Ok(())
    }


}

// ============================== Listener actor ============================== //
/// The Listener is the actor responsible for maintaining services' connection to the broker.
/// It is manged by the ListenerManager
struct Listener;

struct ListenerState {
    writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
    reader: Option<tokio::net::tcp::OwnedReadHalf>, // Use Option to allow taking ownership
    supervisor: ActorRef<BrokerMessage>,
    client_id: String,
    registration_id: Option<String>,
}
//TDOD: Establish why we use this vs passing a state obj?
struct ListenerArguments {
    writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
    reader: Option<tokio::net::tcp::OwnedReadHalf>, // Use Option to allow taking ownership
    supervisor: ActorRef<BrokerMessage>,
    client_id: String,
    registration_id: Option<String>,
}
impl Listener {

    async fn write(client_id: String, msg: ClientMessage, writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>)  {
        match serde_json::to_string(&msg) {
            Ok(serialized) => {
                tokio::spawn(async move {
                    let mut writer = writer.lock().await;
                    if let Err(e) = writer.write_all(serialized.as_bytes()).await {
                        warn!("Failed to send message to client {client_id}: {msg:?}");
                    }
                }); 
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
        myself: ActorRef<Self::Msg>,
        args: ListenerArguments
    ) -> Result<Self::State, ActorProcessingErr> {
        let state: ListenerState = ListenerState {
            writer: args.writer,
            reader: args.reader,
            supervisor: args.supervisor,
            client_id: args.client_id.clone(),
            registration_id: args.registration_id
        };
        Ok(state)
    }

    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        info!("Listener: Listener started for client_id: {}", state.client_id.clone());

        let id= state.client_id.clone();
        let mut reader = state.reader.take().expect("Reader already taken!");
        
        //start listening
        let _ = tokio::spawn(async move {

            let mut buf = String::new();

            let mut buf_reader = tokio::io::BufReader::new(reader);
            loop {
                
                // ready can return false positives, wait a moment
                // thread::sleep(Duration::from_secs(1));    
                    match buf_reader.read_line(&mut buf).await {
                        Ok(bytes) => {
                        //debug!("Received {received} bytes");
                            if bytes == 0 {
                                ()
                            } else {
                                
                                if let Ok(msg) = serde_json::from_str::<ClientMessage>(&buf) {
                                    info!("Received message: {msg:?}");
                                    //Not sure if this is conventional, just pipe the message to the handler
                                    let converted_msg = BrokerMessage::from_client_message(msg, id.clone());
                                    myself.cast(converted_msg).expect("Could not forward message to {myself:?}");
                                    
                                } else {
                                    //bad data
                                    warn!("Failed to parse message from client");
                                   // todo!("Send message back to client with an error");
                                }
                            }

                            
                            }, Err(e) => error!("{e}")
                        }
                    
                    
            }
                        

            // Handle client disconnection
            debug!("Client disconnected");
            myself.kill();
            

        });
        Ok(())
    }

    async fn post_stop(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {

        debug!("Successfully stopped {myself:?}");
        Ok(())
    }
    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            BrokerMessage::RegistrationResponse { registration_id, client_id, success, error } => {
                debug!("Client {client_id} successfully registered with id: {registration_id}");
                state.registration_id = Some(registration_id);
            },
            BrokerMessage::PublishResponse { topic, payload, result } => {
                info!("Successfully published message to topic: {topic}");
                let msg = ClientMessage::PublishResponse { topic: topic.clone(), payload: payload.clone(), result: Result::Ok(()) };
                
                Listener::write(state.client_id.clone(), msg, Arc::clone(&state.writer)).await;            
            },

            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result } => {
                debug!("Agent successfully subscribed to topic: {topic}");
                let response = ClientMessage::SubscribeAcknowledgment {
                    topic, result: Result::Ok(())
                };
                //forward to client
                
                Listener::write(registration_id.clone(), response, Arc::clone(&state.writer)).await;

            },
            BrokerMessage::SubscribeRequest {topic, .. } => {
                //Got request to subscribe from client, confirm we've been registered
                match &state.registration_id {
                    Some(registration_id) => {
                        //forward to session
                        where_is(registration_id.to_owned()).unwrap().send_message(BrokerMessage::SubscribeRequest { registration_id: Some(registration_id.to_owned()), topic}).expect("Failed to forward subscribe request to session {registration_id}");
                    }, None => {
                        
                        todo!("Unregistered listener trying to subscribe to topic {topic}, Send 403 type error to client")
                    }
                }
               
            }
            BrokerMessage::UnsubscribeAcknowledgment { registration_id, topic, result } => {
                //TODO: log client id instead
                debug!("Session {registration_id} successfully unsubscribed from topic: {topic}");
                let response = ClientMessage::UnsubscribeAcknowledgment {
                     topic, result: Result::Ok(())
                };
                
                Listener::write(registration_id.clone(), response, Arc::clone(&state.writer)).await;
            },
            BrokerMessage::ErrorMessage { client_id, error } => todo!(),

            _ => {
                todo!()
            }
        }
        Ok(())
        
    }
}
