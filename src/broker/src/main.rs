#![allow(clippy::incompatible_msrv)]
use cassini_server::broker::{Broker, BrokerArgs};

use common::{init_logging, BROKER_NAME};
use ractor::Actor;



// ============================== Main ============================== //

#[tokio::main]
async fn main() {
    init_logging();
    //start supervisor
    //TODO: Read configurations from somewhere
    let args = BrokerArgs { bind_addr: String::from("127.0.0.1:8080"), session_timeout: None };
    let (_broker, handle) = Actor::spawn(Some(BROKER_NAME.to_string()), Broker, args)
        .await
        .expect("Failed to start Broker");
    //CAUTION: Don't touch
    handle.await.expect("Something went wrong");
}
