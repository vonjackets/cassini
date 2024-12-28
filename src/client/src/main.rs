use std::time::Duration;
use cassini_client::client::{TcpClientActor, TcpClientMessage, TcpClientArgs};
use ractor::Actor;
use common::ClientMessage;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::init_logging();

    let _ = tokio::spawn(async move {
        //TODO: populate bind_addr from config
        let (client, handle) = Actor::spawn(None, TcpClientActor, TcpClientArgs {bind_addr: "127.0.0.1".to_owned(), registration_id: None}).await.expect("Failed to start client actor");

        //Client needs to register with broker before it can send any messages
        client.send_message(TcpClientMessage::Send(
            ClientMessage::RegistrationRequest { registration_id: None }
        )).unwrap();

        client.send_after( Duration::from_secs(1), || { 
            TcpClientMessage::Send(ClientMessage::SubscribeRequest { topic: "apples".to_owned() })
        });
        client.send_interval(Duration::from_secs(10), || { TcpClientMessage::Send(ClientMessage::PingMessage) });
        
        client.send_interval(Duration::from_secs(3),
        || { TcpClientMessage::Send(ClientMessage::PublishRequest { topic: "apples".to_string(), payload: "Hello apple".to_string() } )}
        );

        client.send_after(Duration::from_secs(10), || {TcpClientMessage::Send(ClientMessage::UnsubscribeRequest { topic: "apples".to_owned() })});
            

        
        handle.await.expect("something happened");
    }).await.expect("Keep actor alive");

    Ok(())

    

    
}