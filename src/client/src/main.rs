use std::time::Duration;

use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, Message};
use serde::{Deserialize, Serialize};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use common::BrokerMessage;

/// Messages handled by the TCP client actor
pub enum TcpClientMessage {
    Send(BrokerMessage),
}

/// Actor state for the TCP client
struct TcpClientState {
    stream: TcpStream
}

/// TCP client actor
struct TcpClientActor;

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
        stream.readable().await.expect("Stream failed to becomne readable");

        let state = TcpClientState {
            stream: stream
        };

        
        Ok(state)
    }

    async fn post_start(
        &self,
        myself: ActorRef<Self::Msg>,
        state: &mut Self::State ) ->  Result<(), ActorProcessingErr> {
                        
            loop {
                // Creating the buffer **after** the `await` prevents it from
                // being stored in the async task.
                let mut buf = [0; 4096];

                // Try to read data, this may still fail with `WouldBlock`
                // if the readiness event is a false positive.
                match state.stream.try_read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                    println!("read {} bytes", n);
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                    }
                    Err(e) => {
                        
                    }
                }
            }
        Ok(())
    }
    
    
    async fn handle(&self,
        _: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,) -> Result<(), ActorProcessingErr> {
        match message {
            TcpClientMessage::Send(broker_msg) => {
                let str = serde_json::to_string(&broker_msg).unwrap();
                let owned_clone = str.clone();
                // Serialize the BrokerMessage
                let serialized_msg = owned_clone.as_bytes();

                // Send the serialized message
                if let Err(e) = state.stream.write_all(&serialized_msg).await {
                    println!("Failed to send message: {}", e);
                    return Err(e.into());
                }
                println!("Message sent: {:?}", broker_msg);
            }
        }
        Ok(())
    }

}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let (client, handle) = Actor::spawn(None, TcpClientActor, ()).await.expect("Failed to start client actor");

    handle.await.expect("something happened");

    client.send_message(TcpClientMessage::Send(BrokerMessage::SubscribeRequest { registration_id: None, topic: "apples".to_owned() })).unwrap();
    Ok(())

    

    
}