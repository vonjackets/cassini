use std::sync::Arc;
use std::time::Duration;

use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, Message};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use common::{BrokerMessage, ClientMessage};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use tracing_subscriber::field::debug;
use tracing_subscriber::fmt::format;






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
    client_id: Option<String>
}

/// TCP client actor
struct TcpClientActor;

impl TcpClientActor {

    // fn handle_broker_message(msg: ClientMessage, state: &mut TcpClientState) {

    // }

}

#[async_trait]
impl Actor for TcpClientActor {
    type Msg = TcpClientMessage;
    type State = TcpClientState;
    type Arguments = ();

    async fn pre_start(&self,
        myself: ActorRef<Self::Msg>,
        args: ()) -> Result<Self::State, ActorProcessingErr> {
        info!("TCP Client Actor starting...");

        let bind_addr = "127.0.0.1:8080"; //TODO: replace with value from state after reading it in from args
        let stream = TcpStream::connect(bind_addr).await.expect("Failed to connect to {bind_addr}");
        
        //allow stream to become readable
       // stream.readable().await.expect("Stream failed to becomne readable");
        
        let (reader, write_half) = stream.into_split();
        
        let writer = tokio::io::BufWriter::new(write_half);
                        
        let state = TcpClientState { reader: Some(reader), writer: Arc::new(Mutex::new(writer)), client_id: None };

        
        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
        println!("Entering read_loop ");
        let reader = tokio::io::BufReader::new(state.reader.take().expect("Reader already taken!"));
        //start listening
        let _ = tokio::spawn(async move {
            let mut buf = String::new();

            let mut buf_reader = tokio::io::BufReader::new(reader);

                while let Ok(bytes) = buf_reader.read_line(&mut buf).await {
                    buf.clear();
                
                    if bytes == 0 { () } else {
                        
                        if let Ok(msg) = serde_json::from_slice::<ClientMessage>(buf.as_bytes()) {
                            info!("{msg:?}");
                        } else {
                            //bad data
                            warn!("Failed to parse message from client");
                            // todo!("Send message back to client with an error");
                        }
                    }
                }        
        });
            

        Ok(())
    }

    async fn post_stop(&self, myself: ActorRef<Self::Msg>, state: &mut Self::State) -> Result<(), ActorProcessingErr> {

        debug!("Successfully stopped {myself:?}");
        Ok(())
    }

    
    
    async fn handle(&self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,) -> Result<(), ActorProcessingErr> {
        match message {
            TcpClientMessage::Send(broker_msg) => {
                //debug!("Received message: {broker_msg:?}");
                let str = serde_json::to_string(&broker_msg).unwrap();
                let msg = format!("{str}\n");

                println!("Sending message to broker...");
                let mut writer = state.writer.lock().await;        
                let _: usize = writer.write(msg.as_bytes()).await.expect("Expected bytes to be written");

                writer.flush().await.expect("Expected buffer to get flushed");

                // Serialize the BrokerMessage
                //let serialized_msg = owned_clone.as_bytes();
                
                println!("Message sent: {:?}", broker_msg);
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

    
    client.send_after( Duration::from_secs(3), || { 
        tracing::info!("Sending message");
        TcpClientMessage::Send(ClientMessage::SubscribeRequest { topic: "apples".to_owned() })});
    
    client.send_interval(Duration::from_secs(5), || { TcpClientMessage::Send(ClientMessage::PingMessage) });
    
    handle.await.expect("something happened");
    
    }).await.expect("Keep actor alive");

    Ok(())

    

    
}