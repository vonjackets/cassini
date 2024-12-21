use tokio::{io::{AsyncBufReadExt, AsyncWriteExt}, net::{tcp::OwnedReadHalf, TcpListener}, sync::Mutex};
use tracing::{debug, error, info, warn};
use crate::{broker::Broker, session::{SessionManager, SessionManagerMessage}, topic::{TopicManager, TopicManagerArgs}, BrokerMessage};
use std::{ collections::HashMap, sync::Arc};


use ractor::{registry::where_is, Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use async_trait::async_trait;

use serde::{Deserialize, Serialize};



// ============================== Listener Manager ============================== //
/// 
pub struct ListenerManager;
pub struct ListenerManagerState {
    listeners: HashMap<String, ActorRef<BrokerMessage>>, //client_ids : listenerAgent mapping
    broker_ref: Option<ActorRef<BrokerMessage>>
}

pub struct ListenerManagerArgs {
    pub broker_ref:  Option<ActorRef<BrokerMessage>>
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
        tracing::info!("ListenerManager: Started {myself:?}");
        
        //set up state object
        let state = ListenerManagerState { listeners: HashMap::new(), broker_ref: args.broker_ref};
        info!("ListenerManager: Agent starting");
        Ok(state)
    }

    /// So as to not block the initialization, once a the manager is running as a process, start the server
    /// and listen for incoming connections
    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        let bind_addr = "127.0.0.1:8080"; //TODO: replace with value from state after reading it in from args
        let server = TcpListener::bind(bind_addr).await.expect("could not start tcp listener");
        
        info!("ListenerManager: Server running on {}", bind_addr);
        
        //TODO: evaluate whether this is the correct flow
        let _ = tokio::spawn(async move {
            loop {
                match server.accept().await {
                    Ok((stream,_))=> {
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
                            client_id: client_id.clone()
                        };

                        
                        //start listener actor to handle connection
                        let _ = Actor::spawn_linked(Some(client_id.clone()), Listener, listener_args, myself.clone().into()).await.expect("Failed to start listener for new connection");
                        //state can't be manipulated in this thread, send request to self to finish registration
                        myself.send_message(BrokerMessage::RegistrationRequest { client_id: client_id.clone() }).expect("Failed to send self a Registration Request for new conn");
                        debug!("forwarded registration request to broker");

                    },
                    Err(_) => todo!()
                }
            }
       
        });

        Ok(())
    }

    async fn handle_supervisor_evt(&self, _: ActorRef<Self::Msg>, msg: SupervisionEvent, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
        
        match msg {
            SupervisionEvent::ActorStarted(actor_cell) => {
                info!("Worker agent: {0:?}:{1:?} started", actor_cell.get_name(), actor_cell.get_id());
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
                //Finish registration flow here
                let client_id = client_id.clone();
                let listener_ref: ActorRef<BrokerMessage> = ActorRef::where_is(client_id.clone()).unwrap();

                //check listener is already registered
                //TODO: Ask the session manager for this information by forwarding the request to it
                if state.listeners.contains_key(&client_id.clone()) {
                    warn!("ListenerManager: Listener already exists for client ID {}", client_id);
                    //TODO: Send error message
                } else {
                    state.listeners.insert(client_id.clone(), listener_ref.clone());
                    info!("ListenerManager: Registered Listener for client ID {}", client_id);

                    let broker_ref = myself.try_get_supervisor().expect("Failed to get ref to listenerManager supervisor");
                    broker_ref.send_message(BrokerMessage::RegistrationRequest { client_id: client_id }).expect("Failed to forward registration request to broker");
                }
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
}
//TDOD: Establish why we use this vs passing a state obj?
struct ListenerArguments {
    writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
    reader: Option<tokio::net::tcp::OwnedReadHalf>, // Use Option to allow taking ownership
    supervisor: ActorRef<BrokerMessage>,
    client_id: String,
}
impl Listener {

    async fn write(client_id: String, msg: String, writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>)  {
        tokio::spawn(async move {
            let mut writer = writer.lock().await;
            if let Err(e) = writer.write_all(msg.as_bytes()).await {
                warn!("Failed to send message to client {client_id}: {msg}");
            }
        });  
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
            client_id: args.client_id.clone()
        };
        info!("Listener: Listener started for client_id: {}", state.client_id.clone());

        Ok(state)
    }

    async fn post_start(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        let id= state.client_id.clone();
        let reader = tokio::io::BufReader::new(state.reader.take().expect("Reader already taken!"));
        
        //start listening
        let _ = tokio::spawn(async move {
        
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Ok(msg) = serde_json::from_str::<BrokerMessage>(&line) {
                    //Not sure if this is conventional, just pipe the message to the handler
                    myself.send_message(msg).expect("Could not forward message to {myself:?}");
                    
                } else {
                    //bad data
                    warn!("Failed to parse message from client");
                    todo!("Send message back to client with an error");
                }
            }


            // Handle client disconnection
            

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
                debug!("Received registration ack from manager!");
            },
            BrokerMessage::PublishResponse { topic, payload, result } => {
                info!("Successfully published message to topic: {topic}");
                let msg = BrokerMessage::PublishResponse { topic: topic.clone(), payload: payload.clone(), result: Result::Ok(()) };
                let serialized = serde_json::to_string(&msg).unwrap();
                Listener::write(state.client_id.clone(), serialized, Arc::clone(&state.writer)).await;            
            },

            BrokerMessage::SubscribeAcknowledgment { registration_id, topic, result } => {
                debug!("Agent successfully subscribed to topic: {topic}");
                let response = BrokerMessage::SubscribeAcknowledgment {
                    registration_id: registration_id.clone(), topic, result: Result::Ok(())
                };
                //forward to client
                let serialized = serde_json::to_string(&response).unwrap();
                Listener::write(registration_id.clone(), serialized, Arc::clone(&state.writer)).await;

            },
            BrokerMessage::SubscribeRequest { registration_id, topic } => {
               //TODO: Forward message to session
               
               
            }
            BrokerMessage::UnsubscribeAcknowledgment { registration_id, topic, result } => {
                debug!("Agent successfully unsubscribed from topic: {topic}");
                let response = BrokerMessage::UnsubscribeAcknowledgment {
                    registration_id: registration_id.clone(), topic, result: Result::Ok(())
                };
                //forward to client
                let serialized = serde_json::to_string(&response).unwrap();
                Listener::write(registration_id.clone(), serialized, Arc::clone(&state.writer)).await;
            },
            BrokerMessage::ErrorMessage { client_id, error } => todo!(),

            _ => {
                todo!()
            }
        }
        Ok(())
        
    }
}
