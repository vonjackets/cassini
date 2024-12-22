use std::sync::Arc;
use std::time::Duration;

use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, Message};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use common::{BrokerMessage, ClientMessage};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use tracing_subscriber::field::debug;






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
    async fn write(msg: String, writer: Arc<Mutex<tokio::io::BufWriter<tokio::net::tcp::OwnedWriteHalf>>>)  {
        println!("Sending message to client: {msg}");
        let mut writer = writer.lock().await;
        writer.write_all(msg.as_bytes()).await.expect("Error writing to client");
        

    }

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
        println!("TCP Client Actor starting...");

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
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                debug!("Reading line: {line}");
                if let Ok(msg) = serde_json::from_str::<ClientMessage>(&line) {
                    info!("Received message: {msg:?}");
                    match msg {
                        ClientMessage::PublishResponse { topic, payload, result } => {},
                        ClientMessage::SubscribeAcknowledgment { topic, result } => {
                            match result {
                                Ok(()) => debug!("Successfully subscribed to topic: {topic}"),
                                Err(msg) => warn!("Failed to subscribe to topic {topic}: {msg}")
                            }

                        },            
                        ClientMessage::PingMessage => todo!(),
                        ClientMessage::PongMessage => todo!(),
                        _ => todo!()
                    }
                } else {
                    //bad data
                    warn!("Failed to parse message from client");
                    todo!("Send message back to client with an error");
                }
            }
            // TODO: Handle client disconnection
            
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
                debug!("Received message: {broker_msg:?}");
                let str = serde_json::to_string(&broker_msg).unwrap();
                let owned_clone = str.clone();

                TcpClientActor::write(owned_clone, Arc::clone(&state.writer)).await;

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

    let (client, handle) = Actor::spawn(None, TcpClientActor, ()).await.expect("Failed to start client actor");

    // handle.await.expect("something happened");

    client.cast(TcpClientMessage::Send(ClientMessage::SubscribeRequest { topic: "apples".to_owned() })).unwrap();
    Ok(())

    

    
}