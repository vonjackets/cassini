
#[cfg(test)]
mod tests {

    use core::panic;
    use std::env;
    use cassini_client::client::{self, TcpClientArgs};
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

        async fn handle_supervisor_evt(&self, _: ActorRef<Self::Msg>, msg: SupervisionEvent, _: &mut Self::State) -> Result<(), ActorProcessingErr> {
            
            match msg {
                SupervisionEvent::ActorStarted(_) => (),
                SupervisionEvent::ActorTerminated(actor_cell, _, reason) => {
                    println!("Session: {0:?}:{1:?} terminated. {reason:?}", actor_cell.get_name(), actor_cell.get_id());
                },
                SupervisionEvent::ActorFailed(actor_cell, _) => {
                    println!("Error: actor {0:?}:{1:?} Should not have failed", actor_cell.get_name(), actor_cell.get_id());
                    panic!()
                },
                SupervisionEvent::ProcessGroupChanged(..) => todo!(),
            }    
            
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_tcp_client_connect() {
        //start listener manager to listen for incoming connections
        common::init_logging();
        
        
            let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

            let broker_supervisor = supervisor.clone();

            let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: None, server_cert_file: env::var("TLS_SERVER_CERT_CHAIN").unwrap(), private_key_file: env::var("TLS_SERVER_KEY").unwrap(), ca_cert_file: env::var("TLS_CA_CERT").unwrap() };

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
                    registration_id: None,
                    client_cert_file: env::var("TLS_CLIENT_CERT").unwrap(),
                    private_key_file: env::var("TLS_CLIENT_KEY").unwrap(),
                    ca_cert_file: env::var("TLS_CA_CERT").unwrap(),
                }).await.expect("Failed to start client actor");    
            

                let _ = client.send_message(TcpClientMessage::Send(ClientMessage::DisconnectRequest(None))).map_err(|e| panic!("Failed to send message to client actor {e}"));
                let _ = client.kill_after(Duration::from_secs(5));
                handle.await.expect("expected to start actor");
                
            }).await;
            
        

    }
    #[tokio::test]
    async fn test_client_registers_successfully() {
        common::init_logging();
        let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

        let broker_supervisor = supervisor.clone();
        let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: None, server_cert_file: env::var("TLS_SERVER_CERT_CHAIN").unwrap(), private_key_file: env::var("TLS_SERVER_KEY").unwrap(), ca_cert_file: env::var("TLS_CA_CERT").unwrap() };
        let _ = tokio::spawn(async move {
            //start supervisor
            let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
                .await
                .expect("Failed to start Broker");
            
            
            
            handle.await.expect("Something went wrong");
        });

        tokio::time::sleep(Duration::from_secs(1)).await;
        
        let client_supervisor: ActorRef<()> = supervisor.clone();

        tokio::spawn(async move{
            let (client, handle) = Actor::spawn_linked(Some("test_client".to_owned()),
            TcpClientActor,
            TcpClientArgs {
                bind_addr: BIND_ADDR.to_string(),
                registration_id: None,
                client_cert_file: env::var("TLS_CLIENT_CERT").unwrap(),
                private_key_file: env::var("TLS_CLIENT_KEY").unwrap(),
                ca_cert_file: env::var("TLS_CA_CERT").unwrap(),

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

            handle.await.expect("expected to start actor");
            
        }).await.expect("Expected client to register successfully");

        
        
    }

    #[tokio::test]
    async fn test_registered_client_disconnect() {
        common::init_logging();
    
        let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

        let broker_supervisor = supervisor.clone();
        let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: None, server_cert_file: env::var("TLS_SERVER_CERT_CHAIN").unwrap(), private_key_file: env::var("TLS_SERVER_KEY").unwrap(), ca_cert_file: env::var("TLS_CA_CERT").unwrap() };
        let _ = tokio::spawn(async move {
            //start supervisor
            let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
                .await
                .expect("Failed to start Broker");
            
            
            
            handle.await.expect("Something went wrong");
        });

        tokio::time::sleep(Duration::from_secs(1)).await;
        
        let client_supervisor: ActorRef<()> = supervisor.clone();

        tokio::spawn(async move {

            let (client, _) = Actor::spawn_linked(Some("test_client".to_owned()),
            TcpClientActor,
            TcpClientArgs {
                bind_addr: BIND_ADDR.to_string(),
                registration_id: None,
                client_cert_file: env::var("TLS_CLIENT_CERT").unwrap(),
                private_key_file: env::var("TLS_CLIENT_KEY").unwrap(),
                ca_cert_file: env::var("TLS_CA_CERT").unwrap(),
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

            tokio::time::sleep(Duration::from_secs(1)).await;


            
        }).await.expect("Expected client to register with broker, then disconnect gracefully");

    
        
        
    }


    /// Test scenario where a client disconnects unexpectedly,
    /// When the listener's connection fails the session manager should wait for a 
    /// pre-configured amount of time before cleaning up the session and removing all subscriptions for
    /// that session.
    #[tokio::test]
    async fn test_session_timeout() {
        common::init_logging();
        
        let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

        let broker_supervisor = supervisor.clone();

        let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: Some(3), server_cert_file: env::var("TLS_SERVER_CERT_CHAIN").unwrap(), private_key_file: env::var("TLS_SERVER_KEY").unwrap(), ca_cert_file: env::var("TLS_CA_CERT").unwrap() };

        let _ = tokio::spawn(async move {
            let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
                .await
                .expect("Failed to start Broker");
            
            handle.await.expect("Something went wrong");
        });

        tokio::time::sleep(Duration::from_secs(2)).await;
        
        let client_supervisor: ActorRef<()> = supervisor.clone();

        let _ = tokio::spawn(async move {
            let (client, client_handle) = Actor::spawn_linked(Some("test_client".to_owned()),
            TcpClientActor,
            TcpClientArgs {
                bind_addr: BIND_ADDR.to_string(),
                registration_id: None,
                client_cert_file: env::var("TLS_CLIENT_CERT").unwrap(),
                private_key_file: env::var("TLS_CLIENT_KEY").unwrap(),
                ca_cert_file: env::var("TLS_CA_CERT").unwrap(),
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

            client.kill_after(Duration::from_secs(8)).await.expect("Client should've died");
            
        }).await.expect("Expected client to send messages");
        
    }

    /// Confirms clients that get disconnected unexpectedly can resume their session and keep subscriptions
    #[tokio::test]
    async fn test_client_reconnect_after_timeout() {
        common::init_logging();
    
        let (supervisor, supervisor_handle) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

        let broker_supervisor = supervisor.clone();

        let _ = tokio::spawn( async move {
            let broker_args = BrokerArgs { bind_addr: String::from(BIND_ADDR), session_timeout: None, server_cert_file: env::var("TLS_SERVER_CERT_CHAIN").unwrap(), private_key_file: env::var("TLS_SERVER_KEY").unwrap(), ca_cert_file: env::var("TLS_CA_CERT").unwrap() };
            //start supervisor
            let (_, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, broker_args, broker_supervisor.clone().into())
                .await
                .expect("Failed to start Broker");
            handle.await.expect("Expected Broker to run");
        });

        tokio::time::sleep(Duration::from_secs(1)).await;
        
        let client_supervisor: ActorRef<()> = supervisor.clone();

        let _ = tokio::spawn(async move{
            let (client, _) = Actor::spawn_linked(Some("test_client".to_owned()),
            TcpClientActor,
            TcpClientArgs {
                bind_addr: BIND_ADDR.to_string(),
                registration_id: None,
                client_cert_file: env::var("TLS_CLIENT_CERT").unwrap(),
                private_key_file: env::var("TLS_CLIENT_KEY").unwrap(),
                ca_cert_file: env::var("TLS_CA_CERT").unwrap(),
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

            // wait a moment, then force timeout and kill first client
            tokio::time::sleep(Duration::from_secs(3)).await;
            
            client.send_message(TcpClientMessage::Send(
                ClientMessage::TimeoutMessage(Some(session_id.clone()))
            )).expect("Expected to foward msg");

            //create new client, connect and send registration request with same session_id
            let (new_client, _) = Actor::spawn_linked(Some("new_client".to_owned()),
            TcpClientActor,
            TcpClientArgs {
                bind_addr: BIND_ADDR.to_string(),
                registration_id: Some(session_id.clone()),
                client_cert_file: env::var("TLS_CLIENT_CERT").unwrap(),
                private_key_file: env::var("TLS_CLIENT_KEY").unwrap(),
                ca_cert_file: env::var("TLS_CA_CERT").unwrap(),
            },
            client_supervisor.clone().into()).await.expect("Failed to start client actor");    

            let cloned_id = session_id.clone();
            // send new registration request with same registration_id to resume session
            let _ = new_client.send_after(Duration::from_secs(1), || {
                TcpClientMessage::Send(
                    ClientMessage::RegistrationRequest { registration_id: Some(cloned_id) }
                )
            } ).await.expect("Expected to send re registration request");
            
            //Publish messsage
            let _ = new_client.send_message( TcpClientMessage::Send(ClientMessage::PublishRequest { topic, payload: "Hello apple".to_string(), registration_id: Some(session_id.clone())}));
            
        }).await;
        
        tokio::time::sleep(Duration::from_secs(2)).await;

        supervisor.stop_children(None);
        let _ = supervisor.stop_and_wait(None, Some(Duration::from_secs(1))).await;
        
    }

}
