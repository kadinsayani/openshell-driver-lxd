// SPDX-License-Identifier: AGPL-3.0-or-later

//! Waiting for asynchronous LXD operations to complete.

use std::time::Duration;

use futures::StreamExt;

use crate::client::LxdClient;
use crate::error::LxdError;
use crate::types::Operation;

impl LxdClient {
    /// `GET /1.0/operations/<id>` — fetch the current state of an operation
    /// without blocking.
    ///
    /// `id` is the bare UUID from [`Operation::id`], not the full path.
    pub async fn get_operation(&self, id: &str) -> Result<Operation, LxdError> {
        self.get::<Operation>(&format!("/1.0/operations/{id}"))
            .await?
            .into_metadata()
    }

    /// Block until the operation identified by `id` reaches a terminal state.
    ///
    /// Subscribes to LXD's WebSocket event stream (`/1.0/events?type=operation`)
    /// and immediately reconciles the current operation state to close the race
    /// window between subscribe and check. Falls back to the REST long-poll
    /// (`/1.0/operations/<uuid>/wait?timeout=-1`) if the WebSocket handshake
    /// fails (e.g. an older LXD that does not support WebSocket events).
    ///
    /// Callers that need a deadline should wrap this with
    /// [`tokio::time::timeout`]:
    ///
    /// ```ignore
    /// tokio::time::timeout(Duration::from_secs(60), lxd.wait_operation(id)).await??;
    /// ```
    ///
    /// `id` is the bare UUID from [`Operation::id`], not the full path.
    pub async fn wait_operation(&self, id: &str) -> Result<Operation, LxdError> {
        let full_path = format!("/1.0/operations/{id}");

        loop {
            // Open the event subscription BEFORE the reconcile check to close
            // the race where the operation completes between check and subscribe.
            let mut events = match self.subscribe_events(&["operation"]).await {
                Ok(s) => s,
                Err(LxdError::Io(_) | LxdError::WebSocket(_)) => {
                    // WebSocket unavailable — fall back to REST long-poll.
                    return self.wait_operation_rest(id).await;
                }
                Err(e) => return Err(e),
            };

            // Reconcile: the operation may have reached a terminal state while
            // we were connecting the WebSocket.
            if let Some(outcome) = self.reconcile_operation(id).await? {
                return outcome;
            }

            // Drain the event stream until we see a terminal event for this operation.
            'stream: while let Some(result) = events.next().await {
                match result {
                    Ok(event) if event.type_ == "operation" => {
                        // LXD emits the id as either a bare UUID or the full path.
                        let event_id = event.metadata["id"].as_str().unwrap_or("");
                        if event_id != id && event_id != full_path {
                            continue;
                        }
                        let status = event.metadata["status"].as_str().unwrap_or("");
                        if status == "Success" {
                            return serde_json::from_value::<Operation>(event.metadata)
                                .map_err(LxdError::Json);
                        }
                        if status == "Failure" || status == "Cancelled" {
                            return Err(LxdError::OperationFailed {
                                description: event.metadata["description"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                                err: event.metadata["err"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                            });
                        }
                    }
                    Ok(_) => {}
                    // Io/WebSocket: reconnect. Json: skip the bad frame and keep
                    // draining — a single malformed frame should not abort the wait.
                    Err(LxdError::Io(_) | LxdError::WebSocket(_) | LxdError::Json(_)) => {
                        break 'stream;
                    }
                    Err(e) => return Err(e),
                }
            }

            // Stream ended (cleanly or with an error); reconcile before retrying.
            if let Some(outcome) = self.reconcile_operation(id).await? {
                return outcome;
            }

            // Always sleep before re-subscribing to avoid a busy-loop when LXD
            // closes the WebSocket cleanly (e.g. a server-side idle timeout).
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Fetches the current operation state and maps it to a terminal outcome.
    ///
    /// Returns:
    /// - `Ok(Some(Ok(op)))` — terminal success
    /// - `Ok(Some(Err(e)))` — terminal failure or cancellation
    /// - `Ok(None)` — still running; caller should keep waiting
    ///
    /// Transient transport errors (`Io`, `Hyper`) are treated as "still running"
    /// so the outer loop retries.
    async fn reconcile_operation(
        &self,
        id: &str,
    ) -> Result<Option<Result<Operation, LxdError>>, LxdError> {
        match self.get_operation(id).await {
            Ok(op) if op.status == "Success" => Ok(Some(Ok(op))),
            Ok(op) if op.status == "Failure" || op.status == "Cancelled" => {
                Ok(Some(Err(LxdError::OperationFailed {
                    description: op.description,
                    err: op.err,
                })))
            }
            Ok(_) => Ok(None),
            Err(LxdError::Io(_) | LxdError::Hyper(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// REST long-poll fallback for [`Self::wait_operation`].
    ///
    /// Used when the WebSocket event subscription is unavailable.
    async fn wait_operation_rest(&self, id: &str) -> Result<Operation, LxdError> {
        loop {
            match self
                .get::<Operation>(&format!("/1.0/operations/{id}/wait?timeout=-1"))
                .await
            {
                Ok(resp) => {
                    let op = resp.into_metadata()?;
                    if op.status == "Failure" || op.status == "Cancelled" || !op.err.is_empty() {
                        return Err(LxdError::OperationFailed {
                            description: op.description,
                            err: op.err,
                        });
                    }
                    return Ok(op);
                }
                Err(LxdError::Io(_) | LxdError::Hyper(_)) => {
                    match self.get_operation(id).await {
                        Ok(op) if op.status == "Success" => return Ok(op),
                        Ok(op) if op.status == "Failure" || op.status == "Cancelled" => {
                            return Err(LxdError::OperationFailed {
                                description: op.description,
                                err: op.err,
                            });
                        }
                        Ok(_) => {}
                        Err(LxdError::Io(_) | LxdError::Hyper(_)) => {}
                        Err(LxdError::Api { status_code: 404, .. }) => {}
                        Err(e) => return Err(e),
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
