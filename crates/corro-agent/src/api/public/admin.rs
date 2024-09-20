use std::net::SocketAddr;
use axum::Extension;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use bytes::{BufMut, BytesMut};
use tokio::sync::mpsc::channel;
use tracing::{debug, error, trace};
use corro_types::actor::ClusterId;
use corro_types::agent::Agent;
use corro_types::api::Statement;
use crate::api::public::build_query_rows_response;

enum Op {
    Join(ClusterId, SocketAddr),
    Leave,
    GetId,
    SetId(ClusterId),
}

pub async fn api_v1_admin(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let (mut tx, body) = hyper::Body::channel();

    // TODO: timeout on data send instead of infinitely waiting for channel space.
    let (data_tx, mut data_rx) = channel(512);

    tokio::spawn(async move {
        let mut buf = BytesMut::new();

        while let Some(row_res) = data_rx.recv().await {
            {
                let mut writer = (&mut buf).writer();
                if let Err(e) = serde_json::to_writer(&mut writer, &row_res) {
                    _ = tx
                        .send_data(
                            serde_json::to_vec(&serde_json::json!(QueryEvent::Error(
                                e.to_compact_string()
                            )))
                                .expect("could not serialize error json")
                                .into(),
                        )
                        .await;
                    return;
                }
            }

            buf.extend_from_slice(b"\n");

            if let Err(e) = tx.send_data(buf.split().freeze()).await {
                error!("could not send data through body's channel: {e}");
                return;
            }
        }
        debug!("query body channel done");
    });

    trace!("building query rows response...");

    match build_query_rows_response(&agent, data_tx, stmt).await {
        Ok(_) => {
            #[allow(clippy::needless_return)]
            return hyper::Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .expect("could not build query response body");
        }
        Err((status, res)) => {
            #[allow(clippy::needless_return)]
            return hyper::Response::builder()
                .status(status)
                .body(
                    serde_json::to_vec(&res)
                        .expect("could not serialize query error response")
                        .into(),
                )
                .expect("could not build query response body");
        }
    }
}
