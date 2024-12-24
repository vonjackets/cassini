#![allow(clippy::incompatible_msrv)]
use cassini_server::{init_logging, broker::Broker};

use ractor::Actor;



// ============================== Main ============================== //

#[tokio::main]
async fn main() {
    init_logging();
    //start supervisor
    let (broker, handle) = Actor::spawn(Some("BrokerSupervisor".to_string()), Broker, ())
        .await
        .expect("Failed to start Broker");
    //CAUTION: Don't touch
    handle.await.expect("Something went wrong");
}
