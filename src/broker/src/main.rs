#![allow(clippy::incompatible_msrv)]
use cassini_server::broker::Broker;

use common::{init_logging, BROKER_NAME};
use ractor::Actor;



// ============================== Main ============================== //

#[tokio::main]
async fn main() {
    init_logging();
    //start supervisor
    let (broker, handle) = Actor::spawn(Some(BROKER_NAME.to_string()), Broker, ())
        .await
        .expect("Failed to start Broker");
    //CAUTION: Don't touch
    handle.await.expect("Something went wrong");
}
