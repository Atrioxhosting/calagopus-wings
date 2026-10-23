use super::State;
use crate::{
    response::{ApiResponse, ApiResponseResult},
    routes::{GetState, api::servers::_server_::GetServer},
};
use utoipa_axum::{router::OpenApiRouter, routes};

#[derive(serde::Serialize, serde::Deserialize)]
struct Operation {
    address: String,
    was_running: bool,
}

#[derive(serde::Deserialize, utoipa::ToSchema)]
struct Request {
    address: String,
}
#[derive(serde::Serialize, utoipa::ToSchema)]
struct Response {
    address: String,
    running: bool,
}

#[utoipa::path(post, path = "/", request_body = Request, responses((status = OK, body = Response)))]
async fn apply(
    state: GetState,
    server: GetServer,
    crate::Payload(request): crate::Payload<Request>,
) -> ApiResponseResult {
    let result: Result<Response, anyhow::Error> = async {
        let _operation = server.network_operation.lock().await;
        let config = state.config.client.server(server.uuid).await?;
        anyhow::ensure!(
            config
                .settings
                .allocations
                .ipv6
                .as_ref()
                .is_some_and(|ip| ip.address == request.address),
            "IPv6 request is stale"
        );
        server
            .update_configuration(config.settings, config.process_configuration, false)
            .await;
        let directory = state
            .config
            .resolve_as_path(|config| &config.system.root_directory)
            .join("network-operations");
        tokio::fs::create_dir_all(&directory).await?;
        let journal = directory.join(format!("{}.json", server.uuid));
        let previous: Option<Operation> = match tokio::fs::read(&journal).await {
            Ok(bytes) => Some(serde_json::from_slice(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(
            previous
                .as_ref()
                .is_none_or(|op| op.address == request.address),
            "A different network operation is still journaled"
        );
        let was_running = previous
            .as_ref()
            .map(|op| op.was_running)
            .unwrap_or(server.state.get_state() != crate::server::state::ServerState::Offline);
        let operation = Operation {
            address: request.address.clone(),
            was_running,
        };
        let temporary = journal.with_extension("json.new");
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &serde_json::to_vec(&operation)?).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&temporary, &journal).await?;
        tokio::fs::File::open(&directory).await?.sync_all().await?;
        let endpoints = state.executor.game_network_state(server.uuid).await?;
        if endpoints.len() == 1
            && endpoints.first().is_some_and(|endpoint| {
                endpoint.running && endpoint.addresses == [request.address.clone()]
            })
        {
            server.sync_container().await?;
            tokio::fs::remove_file(&journal).await?;
            return Ok(Response {
                address: request.address,
                running: true,
            });
        }
        let running = server.state.get_state() != crate::server::state::ServerState::Offline;
        if running {
            anyhow::ensure!(
                server.locked_state().is_none(),
                "Server is locked; network change is pending"
            );
            server
                .stop_with_kill_timeout(std::time::Duration::from_secs(30), true)
                .await?;
            tokio::time::timeout(std::time::Duration::from_secs(45), async {
                while server.state.get_state() != crate::server::state::ServerState::Offline {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            })
            .await?;
        }
        server.destroy_container().await;
        anyhow::ensure!(
            state
                .executor
                .game_network_state(server.uuid)
                .await?
                .is_empty(),
            "Old container endpoint has not been removed"
        );
        if was_running {
            server
                .start(Some(std::time::Duration::from_secs(10)), true)
                .await?;
            tokio::time::timeout(std::time::Duration::from_secs(90), async {
                loop {
                    let endpoints = state.executor.game_network_state(server.uuid).await?;
                    if endpoints.len() == 1
                        && endpoints.first().is_some_and(|endpoint| {
                            endpoint.running && endpoint.addresses == [request.address.clone()]
                        })
                    {
                        return Ok::<(), anyhow::Error>(());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
            })
            .await??;
        }
        tokio::fs::remove_file(&journal).await?;
        Ok(Response {
            address: request.address,
            running: was_running,
        })
    }
    .await;
    ApiResponse::new_serialized(result?).ok()
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(apply))
        .with_state(state.clone())
}
