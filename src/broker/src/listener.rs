use rustls::{pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer}, server::WebPkiClientVerifier, RootCertStore, ServerConfig};
use tokio::{io::{split, AsyncBufReadExt, AsyncWriteExt, BufWriter, ReadHalf, WriteHalf}, net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_rustls::{server::TlsStream, TlsAcceptor};
use tracing::{debug, error, info, warn};
use std::{ collections::HashMap, sync::Arc};

use ractor::{registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use async_trait::async_trait;


use crate::{BrokerMessage, ClientMessage, BROKER_NAME};

use crate::UNEXPECTED_MESSAGE_STR;





// ============================== Listener Manager ============================== //
/// The actual listener/server process. When clients connect to the server, their stream is split and 
/// given to a worker processes to use to interact with and handle that connection with the client.
/// TODO: Implement mTLS for the broker server using provided certificates. 
pub struct ListenerManager;
pub struct ListenerManagerState {
    listeners: HashMap<String, ActorRef<BrokerMessage>>, //client_ids : listenerAgent mapping
    bind_addr: String,
    server_config: Arc<ServerConfig>
}

pub struct ListenerManagerArgs {
    pub bind_addr: String,
    pub server_cert_file: String,
    pub private_key_file: String,
    pub ca_cert_file: String
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

        //link with supervisor
        match where_is(BROKER_NAME.to_string()) {
            Some(broker) => myself.link(broker),
            None => warn!("Couldn't link with broker supervisor!")
        }
                
        let certs = CertificateDer::pem_file_iter(args.server_cert_file)
        .unwrap()
        .map(|cert| cert.unwrap())
        .collect();

        let mut root_store = RootCertStore::empty();
        let root_cert = CertificateDer::from_pem_file(args.ca_cert_file).expect("Expected to read server cert as PEM");
        root_store.add(root_cert).expect("Expected to add root cert to server store");
    
        let verifier = WebPkiClientVerifier::builder(Arc::new(root_store)).build().expect("Expected to build server verifier");

        let private_key = PrivateKeyDer::from_pem_file(args.private_key_file).expect("Expected to load private key from file");

        let server_config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, private_key)
        .expect("bad certificate/key");


        //set up state object
        let state = ListenerManagerState { listeners: HashMap::new(), bind_addr: args.bind_addr, server_config: Arc::new(server_config) };
        info!("ListenerManager: Agent starting");
        Ok(state)
    }

    /// Once a the manager is running as a process, start the server
    /// and listen for incoming connections
    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {

        let bind_addr = state.bind_addr.clone();
        let acceptor = TlsAcceptor::from(Arc::clone(&state.server_config));

        let server = TcpListener::bind(bind_addr.clone()).await.expect("could not start tcp listener");
        
        info!("ListenerManager: Server running on {bind_addr}");

        let _ = tokio::spawn(async move {
            
               while let Ok((stream, _)) = server.accept().await {

                    let acceptor = acceptor.clone();
                    
                    let stream = acceptor.accept(stream).await.expect("Expected to complete tls handshake");

                    // Generate a unique client ID
                    let client_id = uuid::Uuid::new_v4().to_string();
                    
                    // Create and start a new Listener actor for this connection
                    
                    let (reader, writer) = split(stream);

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
                
            }
            SupervisionEvent::ActorTerminated(actor_cell, _, reason) => {                
                info!("Worker agent: {0:?}:{1:?} stopped. {reason:?}", actor_cell.get_name(), actor_cell.get_id());               
                
            }
            SupervisionEvent::ActorFailed(actor_cell, _) => {
                warn!("Worker agent: {0:?}:{1:?} failed!", actor_cell.get_name(), actor_cell.get_id());

            },
            SupervisionEvent::ProcessGroupChanged(_) => (),
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
            BrokerMessage::DisconnectRequest { client_id, .. } => {
                match where_is(client_id.clone()) {
                    Some(listener) => listener.stop(Some("DISCONNECTED".to_string())),
                    None => warn!("Couldn't find listener {client_id}")
                }
            }
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
    writer: Arc<Mutex<BufWriter<WriteHalf<TlsStream<TcpStream>>>>>,
    reader: Option<ReadHalf<TlsStream<TcpStream>>>, 
    client_id: String,
    registration_id: Option<String>,
}

struct ListenerArguments {
    writer: Arc<Mutex<BufWriter<WriteHalf<TlsStream<TcpStream>>>>>,
    reader: Option<ReadHalf<TlsStream<TcpStream>>>, 
    client_id: String,
    registration_id: Option<String>,
}

impl Listener {

    async fn write(client_id: String, msg: ClientMessage, writer: Arc<Mutex<BufWriter<WriteHalf<TlsStream<TcpStream>>>>> )  {
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
        
        Ok(ListenerState {
            writer: args.writer,
            reader: args.reader,
            client_id: args.client_id.clone(),
            registration_id: args.registration_id
        })
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
                                      //convert datatype to broker_meessage, fields will be populated during message handling
                                      let converted_msg = BrokerMessage::from_client_message(msg, id.clone(), None);
                                      debug!("Received message: {converted_msg:?}");
                                      myself.send_message(converted_msg).expect("Could not forward message to handler");
                                    }
                                }
                            } else {
                                //bad data
                                warn!("Failed to parse message from client");
                            }
                        }    
                    }, 
                    Err(e) => {
                        // Handle client disconnection, populate state in handler
                        myself.send_message(BrokerMessage::TimeoutMessage { client_id: String::default(), registration_id: None, error: Some(e.to_string()) } )
                        .expect("Expected to forward timeout to handler");
                        warn!("Client {id} disconnected: {e}");
                        break;
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
                //if we got some session_id
                if let Some(session_id) = registration_id {
                    
                    //send message to session that it has a new listener for it to get messages from
                    match where_is(session_id.clone()) {
                        Some(session) => {
                            info!("Resuming session: {session_id}!");
                            state.registration_id = Some(session_id.clone());
                            session.send_message(
                                BrokerMessage::RegistrationRequest {
                                registration_id: Some(session_id.to_owned()), client_id
                            })
                            .expect("Expected to send new client_id to new session");

                        }, None => { 
                            //if we can't find it, inform the client
                            warn!("Received registration request for invalid session: {session_id:?}");
                            Listener::write(
                                client_id,
                                ClientMessage::RegistrationResponse
                                    { registration_id: session_id, success: false,  error: Some(String::from("Unexpected session id")) },
                                Arc::clone(&state.writer))
                                .await;
                        }
                    }
                } else {
                    // Forward to broker to begin creating new session and wait for response.
                    match where_is(BROKER_NAME.to_string()) {
                        Some(broker) => {
                            broker.send_message(BrokerMessage::RegistrationRequest { registration_id: None, client_id: myself.get_name().unwrap_or_default() })
                            .expect("Expected to forward request to broker");
                        } None => warn!("Couldn't locate broker supervisor!")
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
                    warn!("Failed to register with broker! {error:?}");
                }
                
            },
            BrokerMessage::PublishRequest { topic, payload, registration_id } => {
                //confirm listener has registered session
                if registration_id == state.registration_id && registration_id.is_some() {
                    let id = registration_id.unwrap();
                    where_is(id.clone()).map_or_else(|| { error!("Could not forward request to session")},
                    |session| {
                        session.send_message(BrokerMessage::PublishRequest { registration_id: Some(id), topic, payload }).expect("Expected to forward message");
                    });
                                                    
                } else {
                    warn!("Received bad request, session mismatch: {registration_id:?}");
                    Listener::write(state.client_id.clone(),
                    ClientMessage::PublishResponse {
                        topic,
                        payload,
                        result: Err("Received request from unknown client! {registration_id:?}".to_string())
                    },
                    Arc::clone(&state.writer))
                    .await;
                }
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
            BrokerMessage::SubscribeRequest {registration_id, topic } => {
                //Got request to subscribe from client, confirm we've been registered
                if registration_id == state.registration_id && registration_id.is_some() {
                    let id = registration_id.unwrap();
                    where_is(id.clone()).map_or_else(|| { error!("Could not forward request to session")},
                    |session| {
                        session.send_message(BrokerMessage::SubscribeRequest { registration_id: Some(id), topic }).expect("Expected to forward message");
                    });
                
                                
                } else {warn!("Received request from unregistered client!!"); }
               
            },
            BrokerMessage::UnsubscribeRequest { registration_id,topic } => {
                if registration_id == state.registration_id && registration_id.is_some() {
                    let id = registration_id.unwrap();
                    where_is(id.clone()).map_or_else(|| { error!("Could not forward request to session")},
                    |session| {
                        session.send_message(BrokerMessage::UnsubscribeRequest { registration_id: Some(id), topic }).expect("Expected to forward message");
                    });
                } else {warn!("Received request from unregistered client!!"); }
            }
            BrokerMessage::UnsubscribeAcknowledgment { registration_id, topic, .. } => {

                debug!("Session {registration_id} successfully unsubscribed from topic: {topic}");
                let response = ClientMessage::UnsubscribeAcknowledgment {
                     topic, result: Result::Ok(())
                };
                
                Listener::write(registration_id.clone(), response, Arc::clone(&state.writer)).await;
            },
            BrokerMessage::DisconnectRequest { client_id, registration_id } => {
                info!("Client {client_id} disconnected. Ending Session");
                if registration_id == state.registration_id && registration_id.is_some() {
                    //if we're registered, propogate to session agent
                            let id = registration_id.unwrap();
                            match where_is(id.clone()) {
                                Some(session) => session.send_message(BrokerMessage::DisconnectRequest { client_id, registration_id: Some(id.to_string()) }).expect("Expected to forward message"),
                                None => warn!("Failed to find session: {id}")
                            }
                } else {                                       
                    // Otherwise, tell supervisor this listener is done
                    match myself.try_get_supervisor() {
                        Some(broker) => broker.send_message(
                            BrokerMessage::DisconnectRequest { client_id, registration_id: None })
                            .expect("Expected to forward message"),

                        None => warn!("Failed to find supervisor")
                    }
                }                   
                    
                
            }
            BrokerMessage::TimeoutMessage {error, .. } => {
                //Client timed out, if we were registered, let session know
                match &state.registration_id {
                    Some(id) => where_is(id.to_owned()).map_or_else(|| {}, |session| {
                        warn!("Listener: {myself:?} disconnected unexpectedly!");
                        session.send_message(BrokerMessage::TimeoutMessage { client_id: myself.get_name().unwrap(), registration_id: Some(id.clone()), error: error }).expect("Expected to forward message to session.")
                     }),
                    _ => ()
                }
                myself.stop(Some("TIMEDOUT".to_string()));
                
            }
            _ => {
                warn!(UNEXPECTED_MESSAGE_STR)
            }
        }
        Ok(())
        
    }
}
