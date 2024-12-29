# Cassini Integration Tests

This folder contains a small suite of integration tests for how a client may interact with cassini over a network.

Each one represents a a case of client/broker interaction, such as basic connecting, registration, timeouts etc.

Because of this, it's ideal that each test be ran one after the other. For example

`cargo test tests::test_tcp_client_connect`

`cargo test test_client_can_reconnect_after_connection_drop`

