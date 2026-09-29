use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::servers::_server_::GetServer,
    };

    #[utoipa::path(get, path = "/", responses((status = OK, body = serde_json::Value)))]
    pub async fn route(server: GetServer) -> ApiResponseResult {
        let mut data = serde_json::to_value(server.bandwidth.snapshot())?;
        data["bandwidth_per_gib"] =
            serde_json::json!(server.resource_usage().bandwidth.bandwidth_per_gib);
        ApiResponse::new_serialized(data).ok()
    }
}

mod correction {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::servers::_server_::GetServer,
    };
    use axum::http::StatusCode;
    use serde::Deserialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Payload {
        id: uuid::Uuid,
        actor: String,
        delta_bytes: i64,
        generation: u64,
    }

    #[utoipa::path(post, path = "/correction", responses((status = OK, body = serde_json::Value)), request_body = inline(Payload))]
    pub async fn route(
        server: GetServer,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        if data.actor.trim().is_empty() || data.actor.len() > 255 {
            return ApiResponse::error("invalid actor")
                .with_status(StatusCode::BAD_REQUEST)
                .ok();
        }
        match server
            .bandwidth
            .correct(data.id, data.actor, data.delta_bytes, data.generation)
        {
            Ok(_) => {
                server.reevaluate_bandwidth().await;
                ApiResponse::new_serialized(server.bandwidth.snapshot()).ok()
            }
            Err(err) => ApiResponse::error(&err.to_string())
                .with_status(StatusCode::CONFLICT)
                .ok(),
        }
    }
}

mod handoff {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::servers::_server_::GetServer,
    };
    use axum::http::StatusCode;
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Payload {
        expected_generation: u64,
    }

    #[utoipa::path(post, path = "/handoff", responses((status = OK, body = serde_json::Value)), request_body = serde_json::Value)]
    pub async fn route(
        server: GetServer,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        // Transfer has already stopped the normal process before Panel requests ownership.
        if server.state.get_state() != crate::server::state::ServerState::Offline {
            return ApiResponse::error("source service is still running")
                .with_status(StatusCode::CONFLICT)
                .ok();
        }
        server.bandwidth.observe_container(
            server.resource_usage().network.rx_bytes,
            server.resource_usage().network.tx_bytes,
        );
        if let Some((rx, tx)) = server.poll_tundra_traffic().await {
            server.bandwidth.observe_tundra(rx, tx);
        }
        match server.bandwidth.freeze(data.expected_generation) {
            Ok(state) => ApiResponse::new_serialized(state).ok(),
            Err(err) => ApiResponse::error(&err.to_string())
                .with_status(StatusCode::CONFLICT)
                .ok(),
        }
    }
}

mod unfreeze {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::servers::_server_::GetServer,
    };
    use axum::http::StatusCode;
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Payload {
        expected_generation: u64,
    }

    #[utoipa::path(post, path = "/unfreeze", responses((status = OK, body = serde_json::Value)), request_body = serde_json::Value)]
    pub async fn route(
        server: GetServer,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        match server.bandwidth.unfreeze(data.expected_generation) {
            Ok(()) => ApiResponse::new_serialized(serde_json::json!({"unfrozen": true})).ok(),
            Err(err) => ApiResponse::error(&err.to_string())
                .with_status(StatusCode::CONFLICT)
                .ok(),
        }
    }
}

mod accept {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::servers::_server_::GetServer,
        server::bandwidth::LedgerState,
    };
    use axum::http::StatusCode;
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Payload {
        expected_generation: u64,
        ledger: LedgerState,
    }

    #[utoipa::path(post, path = "/accept", responses((status = OK, body = serde_json::Value)), request_body = serde_json::Value)]
    pub async fn route(
        server: GetServer,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        if !server.is_transferring() {
            return ApiResponse::error("destination service is not transferring")
                .with_status(StatusCode::CONFLICT)
                .ok();
        }
        let tundra_baseline = server.poll_tundra_traffic().await;
        match server
            .bandwidth
            .accept(data.ledger, data.expected_generation, tundra_baseline)
        {
            Ok(()) => {
                server.reevaluate_bandwidth().await;
                ApiResponse::new_serialized(server.bandwidth.snapshot()).ok()
            }
            Err(err) => ApiResponse::error(&err.to_string())
                .with_status(StatusCode::CONFLICT)
                .ok(),
        }
    }
}

mod abort_import {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::api::servers::_server_::GetServer,
    };
    use axum::http::StatusCode;
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct Payload {
        generation: u64,
    }

    #[utoipa::path(post, path = "/abort-import", responses((status = OK, body = serde_json::Value)), request_body = serde_json::Value)]
    pub async fn route(
        server: GetServer,
        crate::Payload(data): crate::Payload<Payload>,
    ) -> ApiResponseResult {
        match server.bandwidth.abort_import(data.generation) {
            Ok(()) => ApiResponse::new_serialized(serde_json::json!({"aborted": true})).ok(),
            Err(err) => ApiResponse::error(&err.to_string())
                .with_status(StatusCode::CONFLICT)
                .ok(),
        }
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .routes(routes!(correction::route))
        .routes(routes!(handoff::route))
        .routes(routes!(unfreeze::route))
        .routes(routes!(accept::route))
        .routes(routes!(abort_import::route))
        .with_state(state.clone())
}
