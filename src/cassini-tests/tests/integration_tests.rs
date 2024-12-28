
#[cfg(test)]
mod tests {



use cassini_client::client::TcpClientArgs;
use cassini_server::broker::Broker;
use common::BROKER_NAME;
use ractor::{async_trait, ActorProcessingErr, ActorRef, SupervisionEvent};
use ractor::{concurrency::Duration, Actor};

use cassini_client::client::{TcpClientActor, TcpClientMessage};
use common::ClientMessage;
pub struct MockSupervisorState;
// pub enum MockSupervisorMessage;
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

    async fn handle_supervisor_evt(&self, myself: ActorRef<Self::Msg>, msg: SupervisionEvent, state: &mut Self::State) -> Result<(), ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
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

            let _ = tokio::spawn(async move {
                //start supervisor
                let (broker, handle) = Actor::spawn_linked(Some(BROKER_NAME.to_string()), Broker, (), broker_supervisor.clone().into())
                    .await
                    .expect("Failed to start Broker");
                
                broker.kill_after(Duration::from_secs(10));
                
                handle.await.expect("Something went wrong");
            });

            tokio::time::sleep(Duration::from_secs(3)).await;

            let _ = tokio::spawn(async {
                let (client, handle) = Actor::spawn(Some("test_client".to_owned()), TcpClientActor, TcpClientArgs {
                    bind_addr: "127.0.0.1:8080".to_string(),
                    registration_id: None
                }).await.expect("Failed to start client actor");    
               
                //disconnect
                let client_id = client.get_name().unwrap();
                let _ = client.send_message(TcpClientMessage::Send(ClientMessage::DisconnectRequest(client_id))).map_err(|e| panic!("Failed to send message to client actor {e}"));
                client.kill();
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
            
                
                //disconnect
                client.send_message(TcpClientMessage::Send(
                     ClientMessage::DisconnectRequest(client.get_name().unwrap())
                )).expect("Expected to foward msg");
                client.kill();

                handle.await.expect("expected to start actor");
                
            });

        client_handle.await;

        }).await.expect("Expected test to pass");
    }

    //TODO: Finish this test to confirm network drops don't interrupt session
    #[tokio::test]
    async fn test_client_can_reconnect_after_connection_drop() {
        common::init_logging();
        let _ = tokio::spawn(async {
            let (supervisor, _) = Actor::spawn(None, MockSupervisor, ()).await.unwrap();

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
            
                
                //disconnect
                client.send_message(TcpClientMessage::Send(
                     ClientMessage::DisconnectRequest(client.get_name().unwrap())
                )).expect("Expected to foward msg");
                client.kill();

                handle.await.expect("expected to start actor");
                
            });

        client_handle.await;

        }).await.expect("Expected test to pass");
    }

}