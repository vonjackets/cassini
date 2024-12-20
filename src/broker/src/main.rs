// Copyright (c) Sean Lawlor
//
// This source code is licensed under both the MIT license found in the
// LICENSE-MIT file in the root directory of this source tree.

//! An example supervisory tree of actors
//!
//! Execute with
//!
//! ```text
//! cargo run --example supervisor
//! ```

#![allow(clippy::incompatible_msrv)]
use cassini_server::{init_logging, broker::Broker};

use ractor::Actor;



// ============================== Main ============================== //

#[tokio::main]
async fn main() {
    init_logging();
    //start supervisor
    let (broker, _) = Actor::spawn(Some("broker".to_string()), Broker, ())
        .await
        .expect("Failed to start Broker");

}
