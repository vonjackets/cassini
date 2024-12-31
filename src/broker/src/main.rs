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
    let server_cert_file = String::from("/Users/vacoates/projects/cassini/src/broker/certs/server_polar_certificate_chain.pem");
    let private_key_file = String::from("/Users/vacoates/projects/cassini/src/broker/certs/server_polar_key.pem");
    let ca_cert_file = String::from("/Users/vacoates/projects/cassini/ca_certificates/ca_certificate.pem");

    let args = BrokerArgs { bind_addr: String::from("127.0.0.1:8080"), session_timeout: None, server_cert_file , private_key_file, ca_cert_file  };
    let (_broker, handle) = Actor::spawn(Some(BROKER_NAME.to_string()), Broker, args)
        .await
        .expect("Failed to start Broker");
    //CAUTION: Don't touch
    handle.await.expect("Something went wrong");
}
