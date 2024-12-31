# Cassini Integration Tests

This folder contains a small suite of integration tests for how a client may interact with cassini over a network.

Each one represents a a case of client/broker interaction, such as basic connecting, registration, timeouts etc.

Because of this, it's ideal that each test be ran one after the other. For example

`cargo test tests::test_tcp_client_connect`

`cargo test test_client_can_reconnect_after_connection_drop`


## mtls
This prototype client and server uses tokio rustls to implement mTLS for secure comms between actors.
You can generate certs yourself or use rabbitmq's `tls-gen` project.

NOTE: when you make the certificates, set the `CN` variable to `polar`, a better CN will be chosen later for production level testing