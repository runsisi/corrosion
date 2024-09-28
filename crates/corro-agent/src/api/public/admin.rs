use std::net::SocketAddr;
use std::time::Duration;
use axum::Extension;
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::oneshot;
use tokio::task::block_in_place;
use tracing::{debug, info, warn};

use corro_types::actor::ClusterId;
use corro_types::agent::{Agent, FocaState};
use corro_types::broadcast::{FocaCmd, FocaInput};


#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum AdminOp {
    Join { cluster_id: u64, addr: String },
    Leave,
    GetId,
    SetId { cluster_id: u64 },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    error: String,
    data: serde_json::Value,
}

#[derive(Debug, thiserror::Error)]
pub enum AdminError {
    #[error("join timeout")]
    JoinTimeout,
}

use AdminOp::*;

async fn set_cluster_id(agent: &Agent, cluster_id: ClusterId) -> eyre::Result<()> {
    let mut conn = agent.pool().write_priority().await?;

    block_in_place(|| {
        let tx = conn.transaction()?;

        tx.execute("INSERT OR REPLACE INTO __corro_state (key, value) VALUES ('cluster_id', ?)", [cluster_id])?;

        let (cb_tx, cb_rx) = oneshot::channel();

        agent
            .tx_foca()
            .blocking_send(FocaInput::Cmd(FocaCmd::ChangeIdentity(
                agent.actor(cluster_id),
                cb_tx,
            )))?;

        cb_rx.blocking_recv()??;

        tx.commit()?;

        agent.set_cluster_id(cluster_id);

        Ok(())
    })
}

async fn handle_admin(agent: Agent, op: AdminOp) -> eyre::Result<serde_json::Value> {
    match op {
        Join { cluster_id, addr } => {
            let mut new_cluster = false;

            let mut cluster_id = ClusterId(cluster_id);
            let addr: SocketAddr = addr.parse()?;

            if cluster_id == ClusterId(0) {
                // the initial member
                new_cluster = true;

                let ts = agent.clock().new_timestamp().get_time().as_u64();
                cluster_id = ClusterId(ts);
            }

            if agent.cluster_id() != cluster_id {
                set_cluster_id(&agent, cluster_id).await?;
            }

            if !new_cluster {
                let (cb_tx, cb_rx) = oneshot::channel();

                agent
                    .tx_foca()
                    .send(FocaInput::Cmd(FocaCmd::Join(addr.into(), cb_tx)))
                    .await?;

                cb_rx.await??;

                let mut rounds = 0;

                // wait for 50 * 100ms = 5s
                for _ in 0..50 {
                    match agent.foca_state() {
                        FocaState::Active => {
                            info!("{}", format!("Joined cluster {cluster_id} successfully"));
                            return Ok(serde_json::Value::Null);
                        }
                        _ => {
                            rounds += 1;
                            if rounds % 10 == 0 {
                                debug!("{}", format!("Waiting on join cluster {cluster_id}"));
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                warn!("{}", format!("Waiting on join cluster {cluster_id} timeout"));

                return Err(AdminError::JoinTimeout.into());
            }

            Ok(serde_json::Value::Null)
        }
        Leave => {
            if agent.cluster_id() != ClusterId(0) {
                let ts = agent.clock().new_timestamp().get_time().as_u64();
                let cluster_id = ClusterId(ts);

                set_cluster_id(&agent, cluster_id).await?;
            }

            Ok(serde_json::Value::Null)
        }
        GetId => {
            let cluster_id = agent.cluster_id();

            Ok(json!({
                "cluster_id": cluster_id.0,
            }))
        }
        SetId {cluster_id} => {
            let cluster_id = ClusterId(cluster_id);

            set_cluster_id(&agent, cluster_id).await?;

            Ok(serde_json::Value::Null)
        }
    }
}

pub async fn api_v1_admin(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(op): axum::extract::Json<AdminOp>,
) -> (StatusCode, axum::Json<Response>) {
    match handle_admin(agent, op).await {
        Ok(data) => (
            StatusCode::OK,
            axum::Json(Response {
                error: "".into(),
                data: serde_json::to_value(data).unwrap(),
            }),
        ),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(Response {
                error: err.to_string(),
                data: serde_json::Value::Null,
            }),
        ),
    }
}
