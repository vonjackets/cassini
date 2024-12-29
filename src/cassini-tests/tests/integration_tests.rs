
#[cfg(test)]
mod tests {

    use core::panic;
    use cassini_client::client::TcpClientArgs;
    use cassini_server::broker::Broker;
    use common::BROKER_NAME;
    use ractor::{async_trait, ActorProcessingErr, ActorRef};
    use ractor::{concurrency::Duration, Actor};
    use cassini_client::client::{TcpClientActor, TcpClientMessage};
    use common::ClientMessage;

    pub struct MockSupervisorState;
    pub struct MockSupervisor;

    #[async_trait]
    impl Actor for MockSupervisor {
        type Msg = ();
        type State = MockSupervisorState;
        type Arguments = ();

        async fn pre_start(
            &self,
            _: ActorRef<Self::Msg>,
            _: (),
        ) -> Result<Self::State, ActorProcessingErr> {

            Ok(MockSupervisorState)
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            _: Self::Msg,
            _: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            Ok(())
        }
    }

        // fn get_id_msg() -> TcpClientMessage {
        //     return TcpClientMessage::GetRegistrationId(("Arg".to_string(), OutputPort::default()
        // }

        #[tokio::test]
        async fn test_tcp_client_connect() {
            //start listener manager to listen for incoming connections
            common::init_logging();
            
            let _ = tokio::spawn(async {
                let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

                let broker_supervisor = supervisor.clone();

                let _ = tokio::spawn(async move {
                    //start supervisor
                    let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, (), broker_supervisor.clone().into())
                        .await
                        .expect("Failed to start Broker");
                    
                    
                    
                    handle.await.expect("Something went wrong");
                });

                tokio::time::sleep(Duration::from_secs(1)).await;

                let _ = tokio::spawn(async {
                    let (client, handle) = Actor::spawn(Some("test_client".to_owned()), TcpClientActor, TcpClientArgs {
                        bind_addr: "127.0.0.1:8080".to_string(),
                        registration_id: None
                    }).await.expect("Failed to start client actor");    
                
                    //disconnect
                    let client_id = client.get_name().unwrap();
                    let _ = client.send_message(TcpClientMessage::Send(ClientMessage::DisconnectRequest(client_id))).map_err(|e| panic!("Failed to send message to client actor {e}"));
                    let _ = client.kill_after(Duration::from_secs(1)).await;
                    handle.await.expect("expected to start actor");
                    
                }).await;
                
            }).await.expect("Expected test to pass");

        }
        #[tokio::test]
        async fn test_client_registers_successfully() {
            common::init_logging();
            let _ = tokio::spawn(async {
                let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

                let broker_supervisor = supervisor.clone();

                let _ = tokio::spawn(async move {
                    //start supervisor
                    let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, (), broker_supervisor.clone().into())
                        .await
                        .expect("Failed to start Broker");
                    
                    
                    
                    handle.await.expect("Something went wrong");
                });

                tokio::time::sleep(Duration::from_secs(1)).await;
                
                let client_supervisor: ActorRef<()> = supervisor.clone();

                let client_handle = tokio::spawn(async move{
                    let (client, handle) = Actor::spawn_linked(Some("test_client".to_owned()),
                    TcpClientActor,
                    TcpClientArgs {
                        bind_addr: "127.0.0.1:8080".to_string(),
                        registration_id: None
                    },
                    client_supervisor.clone().into()).await.expect("Failed to start client actor");    

                    client.send_message(TcpClientMessage::Send(
                        ClientMessage::RegistrationRequest { registration_id: None }
                    )).unwrap();

                    tokio::time::sleep(Duration::from_secs(1)).await;
                    
                    let session_id = client
                    .call(TcpClientMessage::GetRegistrationId, Some(Duration::from_secs(10)))
                    .await.unwrap().unwrap();

                    assert_ne!(session_id, String::default());
                    
                    //disconnect
                    client.send_message(TcpClientMessage::Send(
                        ClientMessage::DisconnectRequest(client.get_name().unwrap())
                    )).expect("Expected to foward msg");
                    client.kill();

                    handle.await.expect("expected to start actor");
                    
                });

            let _ = client_handle.await;

            }).await.expect("Expected test to pass");
        }

        /// Test scenario where a client disconnects unexpectedly,
        /// When the listener's connection fails the session manager should wait for a 
        /// pre-configured amount of time before cleaning up the session and removing all subscriptions for
        /// that session.
        #[tokio::test]
        async fn test_session_timeout() {
            common::init_logging();
            let _ = tokio::spawn(async {
                let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

                let broker_supervisor = supervisor.clone();

                let _ = tokio::spawn(async move {
                    let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, (), broker_supervisor.clone().into())
                        .await
                        .expect("Failed to start Broker");
                    
                    
                    
                    handle.await.expect("Something went wrong");
                });

                tokio::time::sleep(Duration::from_secs(1)).await;
                
                let client_supervisor: ActorRef<()> = supervisor.clone();

                let client_handle = tokio::spawn(async move{
                    let (client, handle) = Actor::spawn_linked(Some("test_client".to_owned()),
                    TcpClientActor,
                    TcpClientArgs {
                        bind_addr: "127.0.0.1:8080".to_string(),
                        registration_id: None
                    },
                    client_supervisor.clone().into()).await.expect("Failed to start client actor");    

                    client.send_message(TcpClientMessage::Send(
                        ClientMessage::RegistrationRequest { registration_id: None }
                    )).unwrap();

                    tokio::time::sleep(Duration::from_secs(1)).await;

                    let session_id = client
                    .call(TcpClientMessage::GetRegistrationId, Some(Duration::from_secs(10)))
                    .await.unwrap().unwrap();

                    assert_ne!(session_id, String::default());
                    

                    tokio::time::sleep(Duration::from_secs(1)).await;

                    client.send_message(TcpClientMessage::Send(
                        ClientMessage::SubscribeRequest { topic: "Apples".to_string() }
                    )).unwrap();

                    tokio::time::sleep(Duration::from_secs(1)).await;

                    //disconnect
                    client.send_message(TcpClientMessage::Send(
                        ClientMessage::TimeoutMessage(client.get_name().unwrap())
                    )).expect("Expected to foward msg");

                    client.kill_after(Duration::from_secs(5));

                    handle.await.expect("expected to start actor");
                    
                });

            let _ = client_handle.await;

            }).await.expect("Expected test to pass");
        }

        ///Confirms clients that get disconnected unexpectedly can resume their session and keep subscriptions
        /// Start broker
        /// , start new
        /// Start some client,
        /// 
        #[tokio::test]
        async fn test_client_can_reconnect_after_connection_drop() {
            common::init_logging();
            let _ = tokio::spawn(async {
                let (supervisor, supervisor_handle) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

                let broker_supervisor = supervisor.clone();

                let _ = tokio::spawn(async move {
                    //start supervisor
                    let (broker, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, (), broker_supervisor.clone().into())
                        .await
                        .expect("Failed to start Broker");
                    
                    broker.kill_after(Duration::from_secs(10));
                    
                    handle.await.expect("Something went wrong");
                });

                tokio::time::sleep(Duration::from_secs(1)).await;
                
                let client_supervisor: ActorRef<()> = supervisor.clone();

                let client_handle = tokio::spawn(async move{
                    let (client, handle) = Actor::spawn_linked(Some("test_client".to_owned()),
                    TcpClientActor,
                    TcpClientArgs {
                        bind_addr: "127.0.0.1:8080".to_string(),
                        registration_id: None
                    },
                    client_supervisor.clone().into()).await.expect("Failed to start client actor");    

                    client.send_message(TcpClientMessage::Send(
                        ClientMessage::RegistrationRequest { registration_id: None }
                    )).unwrap();

                    tokio::time::sleep(Duration::from_secs(1)).await;

                    
                    // let session_id = ractor::call!(client,TcpClientMessage::GetRegistrationId).unwrap();
                    let session_id = client
                    .call(TcpClientMessage::GetRegistrationId, Some(Duration::from_secs(10)))
                    .await.unwrap().unwrap();
                        
                    
                    //disconnect
                    client.send_message(TcpClientMessage::Send(
                        ClientMessage::TimeoutMessage(client.get_name().unwrap())
                    )).expect("Expected to foward msg");

                    
                    // send new registration request with same registration_id to resume session
                    let _ = client.send_after(Duration::from_secs(3), || {
                        TcpClientMessage::Send(
                            ClientMessage::RegistrationRequest { registration_id: Some(session_id) }
                        )
                    } ).await.expect("Expected to send re registration request");


                    let _ = client.send_message( TcpClientMessage::Send(ClientMessage::PublishRequest { topic: "apples".to_string(), payload: "Hello apple".to_string()}));

                    handle.await.expect("expected client not to panic");
                    
                });


                let _ = client_handle.await; 
                let _ = supervisor_handle.await;


            }).await.expect("Expected test to pass");
        }

}
