//! Decline discovery before a stdio connection enters rmcp's inline lifecycle.
//! HTTP discovery is already request-scoped; its rejection stays in the handler.
use rmcp::{
    model::{
        ClientJsonRpcMessage, ClientRequest, DiscoverRequestMethod, ErrorData, ServerJsonRpcMessage,
    },
    transport::{IntoTransport, Transport},
    RoleServer,
};

/// Keep discovery probes outside rmcp's connection lifecycle. In rmcp 3,
/// first-request discovery permanently requires inline metadata, even when the
/// handler rejects it and the client subsequently initializes on the same pipe.
pub fn classic_stdio_transport<T, E, A>(transport: T) -> impl Transport<RoleServer, Error = E>
where
    T: IntoTransport<RoleServer, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    ClassicStdioTransport(transport.into_transport())
}

struct ClassicStdioTransport<T>(T);

impl<T: Transport<RoleServer>> Transport<RoleServer> for ClassicStdioTransport<T> {
    type Error = T::Error;

    fn send(
        &mut self,
        message: ServerJsonRpcMessage,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'static {
        self.0.send(message)
    }

    async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
        loop {
            let message = self.0.receive().await?;
            if let ClientJsonRpcMessage::Request(request) = &message {
                if matches!(request.request, ClientRequest::DiscoverRequest(_)) {
                    let response = ServerJsonRpcMessage::error(
                        ErrorData::method_not_found::<DiscoverRequestMethod>(),
                        Some(request.id.clone()),
                    );
                    if let Err(error) = self.0.send(response).await {
                        tracing::warn!(%error, "failed to decline MCP discovery");
                        return None;
                    }
                    continue;
                }
            }
            return Some(message);
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.0.close().await
    }
}
