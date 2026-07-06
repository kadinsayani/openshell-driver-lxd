// SPDX-License-Identifier: AGPL-3.0-or-later

//! Network ACL management.

use serde_json::json;

use crate::client::LxdClient;
use crate::error::LxdError;

/// A single LXD Network ACL rule.
#[derive(Debug, Clone)]
pub struct LxdNetworkAclRule {
    /// Rule action: `"allow"` or `"drop"`.
    pub action: String,
    /// Destination CIDR (e.g. `"10.0.0.0/8"`).
    pub destination: String,
    /// Destination port or range (e.g. `"8080"`, `"8080-8090"`).
    pub destination_port: String,
    /// IP protocol: `"tcp"`, `"udp"`, or `"icmp"`.
    pub protocol: String,
    /// Rule state: `"enabled"` or `"disabled"`.
    pub state: String,
}

impl LxdNetworkAclRule {
    /// Allow egress TCP to a specific destination CIDR and port.
    pub fn allow_egress_tcp(dest_cidr: &str, dest_port: u16) -> Self {
        Self {
            action: "allow".to_string(),
            destination: dest_cidr.to_string(),
            destination_port: dest_port.to_string(),
            protocol: "tcp".to_string(),
            state: "enabled".to_string(),
        }
    }
}

impl LxdClient {
    /// Ensures a named Network ACL exists with the given egress rules.
    ///
    /// Creates the ACL if it does not exist (`POST /1.0/network-acls`), or
    /// replaces the ruleset if it does (`PUT /1.0/network-acls/<name>`).
    /// Idempotent.
    pub async fn ensure_network_acl(
        &self,
        name: &str,
        egress: Vec<LxdNetworkAclRule>,
    ) -> Result<(), LxdError> {
        let egress_json: Vec<serde_json::Value> = egress
            .iter()
            .map(|r| {
                json!({
                    "action": r.action,
                    "destination": r.destination,
                    "destination_port": r.destination_port,
                    "protocol": r.protocol,
                    "state": r.state,
                })
            })
            .collect();

        let body = json!({
            "name": name,
            "description": "OpenShell sandbox egress policy",
            "egress": egress_json,
            "ingress": [],
            "config": {},
        });

        match self
            .get::<serde_json::Value>(&format!("/1.0/network-acls/{name}"))
            .await
        {
            Ok(_) => {
                self.put::<serde_json::Value>(&format!("/1.0/network-acls/{name}"), body)
                    .await?;
            }
            Err(LxdError::Api {
                status_code: 404, ..
            }) => {
                match self
                    .post::<serde_json::Value>("/1.0/network-acls", body.clone())
                    .await
                {
                    Ok(_) => {}
                    // A concurrent caller created the ACL between our GET and POST.
                    Err(LxdError::Api {
                        status_code: 409, ..
                    }) => {
                        self.put::<serde_json::Value>(
                            &format!("/1.0/network-acls/{name}"),
                            body,
                        )
                        .await?;
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    /// Deletes a named Network ACL. A 404 response is treated as success.
    pub async fn delete_network_acl(&self, name: &str) -> Result<(), LxdError> {
        match self
            .delete::<serde_json::Value>(&format!("/1.0/network-acls/{name}"))
            .await
        {
            Ok(_)
            | Err(LxdError::Api {
                status_code: 404, ..
            }) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
