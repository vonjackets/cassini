use std::{env, time::Duration};
use cassini_client::client::{TcpClientActor, TcpClientMessage, TcpClientArgs};
use ractor::Actor;
use common::ClientMessage;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    common::init_logging();

    let client_cert_file = env::var("TLS_CLIENT_CERT").unwrap();
    let private_key_file = env::var("TLS_CLIENT_KEY").unwrap();
    let ca_cert_file = env::var("TLS_CA_CERT").unwrap();

    //TODO: populate bind_addr from config
    let args =  TcpClientArgs {bind_addr:"127.0.0.1:8080".to_owned(),registration_id:None, ca_cert_file, client_cert_file, private_key_file };

    let (client, handle) = Actor::spawn(None, TcpClientActor, args).await.expect("Failed to start client actor");

    //Client needs to register with broker before it can send any messages
    client.send_message(TcpClientMessage::Send(
        ClientMessage::RegistrationRequest { registration_id: None }
    )).unwrap();

    client.send_interval(Duration::from_secs(10), || { TcpClientMessage::Send(ClientMessage::PingMessage) });

    handle.await.expect("something happened");


    Ok(())

    

    
}