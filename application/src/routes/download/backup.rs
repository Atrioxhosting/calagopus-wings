use super::State;
use utoipa_axum::{router::OpenApiRouter, routes};

mod get {
    use crate::{
        response::{ApiResponse, ApiResponseResult},
        routes::GetState,
        server::filesystem::{archive::StreamableArchiveFormat, virtualfs::ByteRange},
    };
    use axum::{
        extract::Query,
        http::{HeaderMap, StatusCode},
    };
    use futures::StreamExt;
    use serde::Deserialize;
    use utoipa::ToSchema;

    #[derive(ToSchema, Deserialize)]
    pub struct Params {
        token: String,

        #[serde(default)]
        archive_format: StreamableArchiveFormat,
    }

    #[derive(Deserialize)]
    pub struct BackupJwtPayload {
        #[serde(flatten)]
        pub base: crate::remote::jwt::BasePayload,

        pub server_uuid: Option<uuid::Uuid>,
        pub backup_uuid: uuid::Uuid,
        pub unique_id: compact_str::CompactString,
        #[serde(default)]
        pub database: bool,
    }

    impl crate::routes::token::TokenPayload for BackupJwtPayload {
        #[inline]
        fn base(&self) -> &crate::remote::jwt::BasePayload {
            &self.base
        }
    }

    #[utoipa::path(get, path = "/", responses(
        (status = OK, body = String),
        (status = UNAUTHORIZED, body = String),
        (status = NOT_FOUND, body = String),
        (status = EXPECTATION_FAILED, body = String),
    ), params(
        (
            "token" = String, Query,
            description = "The JWT token to use for authentication",
        ),
    ))]
    pub async fn route(
        state: GetState,
        headers: HeaderMap,
        Query(data): Query<Params>,
    ) -> ApiResponseResult {
        let payload: BackupJwtPayload =
            crate::routes::token::verify(&state, &data.token, "backup-download")?;

        crate::routes::token::consume(&state, &payload.unique_id)?;

        let backup = match state
            .backup_manager
            .find(&state, payload.backup_uuid)
            .await?
        {
            Some(backup) => backup,
            None => {
                return ApiResponse::error("backup not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            }
        };

        let service = if matches!(&*backup, crate::server::backup::Backup::Wings(_)) {
            let Some(server_uuid) = payload.server_uuid else {
                return ApiResponse::error("backup has no service owner")
                    .with_status(StatusCode::EXPECTATION_FAILED)
                    .ok();
            };
            let Some(server) = state.server_manager.get_server(server_uuid).await else {
                return ApiResponse::error("server not found")
                    .with_status(StatusCode::NOT_FOUND)
                    .ok();
            };
            if server.bandwidth.blocked() {
                return ApiResponse::error("bandwidth quota reached")
                    .with_status(StatusCode::FORBIDDEN)
                    .ok();
            }
            Some(server)
        } else {
            None
        };

        let download = if payload.database {
            backup.download_database(&state).await
        } else {
            backup
                .download(
                    &state,
                    data.archive_format,
                    ByteRange::from_headers(&headers),
                )
                .await
        };

        let mut response = match download {
            Ok(response) => response,
            Err(err) => {
                tracing::error!("failed to download backup: {:?}", err);

                ApiResponse::error("failed to download backup")
                    .with_status(StatusCode::EXPECTATION_FAILED)
            }
        };
        if let Some(server) = service {
            response.body = axum::body::Body::from_stream(response.body.into_data_stream().map(
                move |result| {
                    if server.bandwidth.blocked() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "bandwidth quota reached",
                        ));
                    }
                    result
                        .map(|chunk| {
                            server.bandwidth.record_stream(0, chunk.len() as u64);
                            chunk
                        })
                        .map_err(std::io::Error::other)
                },
            ));
        }
        response.ok()
    }
}

pub fn router(state: &State) -> OpenApiRouter<State> {
    OpenApiRouter::new()
        .routes(routes!(get::route))
        .with_state(state.clone())
}
