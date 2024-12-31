
#[cfg(test)]
mod tests {

    use core::panic;
    use cassini_client::client::TcpClientArgs;
    use cassini_server::broker::{Broker, BrokerArgs};
    use common::BROKER_NAME;
    use ractor::{async_trait, ActorProcessingErr, ActorRef, SupervisionEvent};
    use ractor::{concurrency::Duration, Actor};
    use cassini_client::client::{TcpClientActor, TcpClientMessage};
    use common::ClientMessage;

    //Bind to some other port if desired
    pub const BIND_ADDR: &str = "127.0.0.1:8080";

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

        async fn handle_supervisor_evt(&self, _: ActorRef<Self::Msg>, msg: SupervisionEvent, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
            
            match msg {
                SupervisionEvent::ActorStarted(actor_cell) => (),
                SupervisionEvent::ActorTerminated(actor_cell, boxed_state, reason) => {
                    println!("Session: {0:?}:{1:?} terminated. {reason:?}", actor_cell.get_name(), actor_cell.get_id());
                },
                SupervisionEvent::ActorFailed(actor_cell, error) => {
                    println!("Error: actor {0:?}:{1:?} Should not have failed", actor_cell.get_name(), actor_cell.get_id());
                    panic!()
                },
                SupervisionEvent::ProcessGroupChanged(group_change_message) => todo!(),
            }    
            
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_tcp_client_connect() {
        //start listener manager to listen for incoming connections
        common::init_logging();
        
        let _ = tokio::spawn(async {
            let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

            let broker_supervisor = supervisor.clone();

            let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: None };

            let _ = tokio::spawn(async move {
                //start supervisor
                let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
                    .await
                    .expect("Failed to start Broker");
                
                
                
                handle.await.expect("Something went wrong");
            });

            tokio::time::sleep(Duration::from_secs(1)).await;

            let _ = tokio::spawn(async {
                let (client, handle) = Actor::spawn(Some("test_client".to_owned()), TcpClientActor, TcpClientArgs {
                    bind_addr: BIND_ADDR.to_string(),
                    registration_id: None
                }).await.expect("Failed to start client actor");    
            

                let _ = client.send_message(TcpClientMessage::Send(ClientMessage::DisconnectRequest(None))).map_err(|e| panic!("Failed to send message to client actor {e}"));
                let _ = client.kill_after(Duration::from_secs(5));
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
            let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: None };
            let _ = tokio::spawn(async move {
                //start supervisor
                let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
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
                    bind_addr: BIND_ADDR.to_string(),
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
                
                client.kill();

                handle.await.expect("expected to start actor");
                
            });

        let _ = client_handle.await;

        }).await.expect("Expected test to pass");
    }

    #[tokio::test]
    async fn test_registered_client_disconnect() {
        common::init_logging();
        let _ = tokio::spawn(async {
            let (supervisor, supervisor_handle) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

            let broker_supervisor = supervisor.clone();
            let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: None };
            let _ = tokio::spawn(async move {
                //start supervisor
                let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
                    .await
                    .expect("Failed to start Broker");
                
                
                
                handle.await.expect("Something went wrong");
            });

            tokio::time::sleep(Duration::from_secs(1)).await;
            
            let client_supervisor: ActorRef<()> = supervisor.clone();

            let client_handle = tokio::spawn(async move {

                let (client, _) = Actor::spawn_linked(Some("test_client".to_owned()),
                TcpClientActor,
                TcpClientArgs {
                    bind_addr: BIND_ADDR.to_string(),
                    registration_id: None
                },
                client_supervisor.clone().into()).await.expect("Failed to start client actor");    

                client.send_message(TcpClientMessage::Send(
                    ClientMessage::RegistrationRequest { registration_id: None }
                )).unwrap();

                tokio::time::sleep(Duration::from_secs(1)).await;
                
                //disconnect

                let session_id = client
                .call(TcpClientMessage::GetRegistrationId, Some(Duration::from_secs(3)))
                .await.unwrap().unwrap();

                assert_ne!(session_id, String::default());

                client.send_message(TcpClientMessage::Send(
                    ClientMessage::DisconnectRequest(Some(session_id))
                )).expect("Expected to forward msg");
                
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
            let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: Some(10) };
            let _ = tokio::spawn(async move {
                let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
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
                    bind_addr: BIND_ADDR.to_string(),
                    registration_id: None
                },
                client_supervisor.clone().into()).await.expect("Failed to start client actor");    

                client.send_message(TcpClientMessage::Send(
                    ClientMessage::RegistrationRequest { registration_id: None }
                )).unwrap();

                tokio::time::sleep(Duration::from_secs(1)).await;

                let session_id = client
                .call(TcpClientMessage::GetRegistrationId,
                    Some(Duration::from_secs(3)))
                .await.unwrap().unwrap();

                assert_ne!(session_id, String::default());
                

                tokio::time::sleep(Duration::from_secs(1)).await;
                
                //create some subscription
                client.send_message(TcpClientMessage::Send(
                    ClientMessage::SubscribeRequest { 
                        registration_id: Some(session_id.clone()),
                        topic: String::from("Apples")
                    }
                )).unwrap();

                tokio::time::sleep(Duration::from_secs(1)).await;

                //disconnect
                client.send_message(TcpClientMessage::Send(
                    ClientMessage::TimeoutMessage(Some(session_id))
                )).expect("Expected to foward msg");

                //wait a moment before ending test
                client.kill_after(Duration::from_secs(15));

                handle.await.expect("expected to start actor");
                
            });

        let _ = client_handle.await;

        }).await.expect("Expected test to pass");
    }

    /// Confirms clients that get disconnected unexpectedly can resume their session and keep subscriptions
    #[tokio::test]
    async fn test_client_reconnect_after_timeout() {
        common::init_logging();
        let _ = tokio::spawn(async {
            let (supervisor, supervisor_handle) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

            let broker_supervisor = supervisor.clone();

            let _ = tokio::spawn(async move {
                let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: Some(10) };
                //start supervisor
                let (broker, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
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
                    bind_addr: BIND_ADDR.to_string(),
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

                //subscribe to topic
                let topic = String::from("Apples");
                client.send_message(TcpClientMessage::Send(
                    ClientMessage::SubscribeRequest { topic: topic.clone(), registration_id: Some(session_id.clone())}
                )).expect("Expected to foward msg");

                tokio::time::sleep(Duration::from_secs(1)).await;

                // Force timeout
                client.send_message(TcpClientMessage::Send(
                    ClientMessage::TimeoutMessage(Some(session_id.clone()))
                )).expect("Expected to foward msg");

                let cloned_id = session_id.clone();
                // send new registration request with same registration_id to resume session
                let _ = client.send_after(Duration::from_secs(3), || {
                    TcpClientMessage::Send(
                        ClientMessage::RegistrationRequest { registration_id: Some(cloned_id) }
                    )
                } ).await.expect("Expected to send re registration request");
                
                //Publish messsage
                let _ = client.send_message( TcpClientMessage::Send(ClientMessage::PublishRequest { topic, payload: "Hello apple".to_string(), registration_id: Some(session_id.clone())}));

                client.kill();

                handle.await.expect("expected client not to panic");
                
            });


            let _ = client_handle.await; 
            let _ = supervisor_handle.await;


        }).await.expect("Expected test to pass");
    }

}
