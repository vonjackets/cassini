use std::sync::Arc;
use std::time::Duration;
use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use common::ClientMessage;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Messages handled by the TCP client actor
pub enum TcpClientMessage {
    Send(ClientMessage),
    // RegistrationResponse(client_id),
}

impl TcpClientMessage{
    
}

/// Actor state for the TCP client
struct TcpClientState {
    writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>,
    reader: Option<tokio::net::tcp::OwnedReadHalf>, // Use Option to allow taking ownership
}

/// TCP client actor
struct TcpClientActor;

#[async_trait]
impl Actor for TcpClientActor {
    type Msg = TcpClientMessage;
    type State = TcpClientState;
    type Arguments = ();

    async fn pre_start(&self,
        _: ActorRef<Self::Msg>,
        _: ()) -> Result<Self::State, ActorProcessingErr> {
        info!("TCP Client Actor starting...");

        let bind_addr = "127.0.0.1:8080"; //TODO: replace with value from state after reading it in from args
        let stream = TcpStream::connect(bind_addr).await.expect("Failed to connect to {bind_addr}");
                
        let (reader, write_half) = stream.into_split();
        
        let writer = tokio::io::BufWriter::new(write_half);
                        
        let state = TcpClientState { reader: Some(reader), writer: Arc::new(Mutex::new(writer)) };

        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
        info!("{myself:?} Listening... ");
        let reader = tokio::io::BufReader::new(state.reader.take().expect("Reader already taken!"));
        //start listening
        let _ = tokio::spawn(async move {
            let mut buf = String::new();

            let mut buf_reader = tokio::io::BufReader::new(reader);

                while let Ok(bytes) = buf_reader.read_line(&mut buf).await {                
                    if bytes == 0 { () } else {
                        if let Ok(msg) = serde_json::from_slice::<ClientMessage>(buf.as_bytes()) {
                            // handle publish responses containing new data
                            match msg {
                                ClientMessage::PublishResponse { topic, payload, result } => {
                                    //new message on topic
                                    if result.is_ok() {
                                        debug!("New message on topic {topic}: {payload}");
                                        //TODO: Forward message to some consumer
                                    } else {
                                        warn!("Failed to publish message to topic: {topic}");
                                        //TODO: Depending on the error type, should we try again?
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

                                ClientMessage::PongMessage => todo!(),
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
                let mut writer: tokio::sync::MutexGuard<'_, io::BufWriter<tokio::net::tcp::OwnedWriteHalf>> = state.writer.lock().await;        
                let _: usize = writer.write(msg.as_bytes()).await.expect("Expected bytes to be written");

                writer.flush().await.expect("Expected buffer to get flushed");
            }
        }
        Ok(())
    }

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::init_logging();

    let _ = tokio::spawn(async move {
        let (client, handle) = Actor::spawn(None, TcpClientActor, ()).await.expect("Failed to start client actor");

    
        client.send_after( Duration::from_secs(1), || { 
            TcpClientMessage::Send(ClientMessage::SubscribeRequest { topic: "apples".to_owned() })
        });
        client.send_interval(Duration::from_secs(10), || { TcpClientMessage::Send(ClientMessage::PingMessage) });
        
        client.send_interval(Duration::from_secs(3),
        || { TcpClientMessage::Send(ClientMessage::PublishRequest { topic: "apples".to_string(), payload: "Hello apple".to_string() } )}
        );

        client.send_after(Duration::from_secs(5), || {TcpClientMessage::Send(ClientMessage::UnsubscribeRequest { topic: "apples".to_owned() })});
            

        
        handle.await.expect("something happened");
    }).await.expect("Keep actor alive");

    Ok(())

    

    
}