use std::sync::Arc;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use common::ClientMessage;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};


/// Messages handled by the TCP client actor
pub enum TcpClientMessage {
    Send(ClientMessage),
    RegistrationResponse(String),
    ErrorMessage(String),
    GetRegistrationId(RpcReplyPort<String>)
}

/// Actor state for the TCP client
pub struct TcpClientState {
    bind_addr: String,
    writer: Option<Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>>,
    reader: Option<tokio::net::tcp::OwnedReadHalf>, // Use Option to allow taking ownership
    registration_id: Option<String>,
    
}

pub struct TcpClientArgs {
    pub bind_addr: String,
    pub registration_id: Option<String>
}

/// TCP client actor
pub struct TcpClientActor;

#[async_trait]
impl Actor for TcpClientActor {
    type Msg = TcpClientMessage;
    type State = TcpClientState;
    type Arguments = TcpClientArgs;

    async fn pre_start(&self,
        _: ActorRef<Self::Msg>,
        args: TcpClientArgs) -> Result<Self::State, ActorProcessingErr> {
        info!("TCP Client Actor starting...");
                        
        let state = TcpClientState { bind_addr: args.bind_addr, reader: None, writer: None, registration_id: args.registration_id };

        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
        
        let addr = state.bind_addr.clone();
        info!("{myself:?} started. Connecting to {addr} ");
        
        let stream = TcpStream::connect(&addr).await.expect("Failed to connect to {addr}");
                
        let (reader, write_half) = stream.into_split();
        
        let writer = tokio::io::BufWriter::new(write_half);
        
        state.reader = Some(reader);
        state.writer = Some(Arc::new(Mutex::new(writer)));
     
        info!("{myself:?} Listening... ");
        let reader = tokio::io::BufReader::new(state.reader.take().expect("Reader already taken!"));
        //start listening
        let _ = tokio::spawn(async move {
            let mut buf = String::new();

            let mut buf_reader = tokio::io::BufReader::new(reader);

                while let Ok(bytes) = buf_reader.read_line(&mut buf).await {                
                    if bytes == 0 { () } else {
                        if let Ok(msg) = serde_json::from_slice::<ClientMessage>(buf.as_bytes()) {
                            
                            match msg {
                                ClientMessage::RegistrationResponse { registration_id, success, error } => {
                                    if success {
                                        info!("Successfully began session with id: {registration_id}");
                                        myself.send_message(TcpClientMessage::RegistrationResponse(registration_id)).expect("Could not forward message to {myself:?");
                                        
                                    } else {
                                        warn!("Failed to register session with the server. {error:?}");
                                    }
                                },
                                ClientMessage::PublishResponse { topic, payload, result } => {
                                    //new message on topic
                                    if result.is_ok() {
                                        debug!("New message on topic {topic}: {payload}");
                                        //TODO: Forward message to some consumer
                                    } else {
                                        warn!("Failed to publish message to topic: {topic}");
                                    }
                                },

                                ClientMessage::SubscribeAcknowledgment { topic, result } => {
                                    if result.is_ok() {
                                        debug!("Successfully subscribed to topic: {topic}");
                                    } else {
                                        warn!("Failed to subscribe to topic: {topic}");
                                    }
                                },

                                ClientMessage::UnsubscribeAcknowledgment { topic, result } => {
                                    if result.is_ok() {
                                        debug!("Successfully unsubscribed from topic: {topic}");
                                    } else {
                                        warn!("Failed to unsubscribe from topic: {topic}");
                                    }  
                                },
                                _ => {
                                    warn!("Unexpected message {buf}");
                                }
                            }
                        } else {
                            //bad data
                            warn!("Failed to parse message: {buf}");
                        }
                    }
                    buf.clear(); //empty the buffer
                }        
        });
            

        Ok(())
    }

    async fn post_stop(&self, myself: ActorRef<Self::Msg>, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
        debug!("Successfully stopped {myself:?}");
        Ok(())
    }
    
    async fn handle(&self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,) -> Result<(), ActorProcessingErr> {
        match message {
            TcpClientMessage::Send(broker_msg) => {
                let str = serde_json::to_string(&broker_msg).unwrap();
                let msg = format!("{str}\n");
                debug!("{msg}");
                let unwrapped_writer = state.writer.clone().unwrap();
                let mut writer: tokio::sync::MutexGuard<'_, io::BufWriter<tokio::net::tcp::OwnedWriteHalf>> = unwrapped_writer.lock().await;        
                let _: usize = writer.write(msg.as_bytes()).await.expect("Expected bytes to be written");

                writer.flush().await.expect("Expected buffer to get flushed");
            }
            TcpClientMessage::RegistrationResponse(registration_id) => state.registration_id = Some(registration_id),
            TcpClientMessage::GetRegistrationId(reply) => {
                if let Some(registration_id) = &state.registration_id { reply.send(registration_id.to_owned()).expect("Expected to send registration_id to reply port"); } 
                else { reply.send(String::default()).expect("Expected to send default string"); }
            }
            _ => todo!()
                                
        }
        Ok(())
    }

}