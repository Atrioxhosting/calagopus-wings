use super::State;
use crate::{
    response::{ApiResponse, ApiResponseResult},
    routes::GetState,
};
use utoipa_axum::{router::OpenApiRouter, routes};
#[utoipa::path(get, path="/{server}", responses((status=OK,body=Vec<crate::server::game_ipv6::Endpoint>)))]
async fn get(
    state: GetState,
    axum::extract::Path(server): axum::extract::Path<uuid::Uuid>,
) -> ApiResponseResult {
    ApiResponse::new_serialized(state.executor.game_network_state(server).await?).ok()
}
pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get))
        .with_state(state.clone())
}
