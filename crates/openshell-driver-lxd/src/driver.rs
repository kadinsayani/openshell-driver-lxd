// SPDX-License-Identifier: AGPL-3.0-or-later

//! Core LXD compute driver logic, independent of the gRPC transport.

use computev1::pb::{DriverSandbox, GetCapabilitiesResponse};
use lxd_client::{LxdClient, LxdError};

use crate::config::Config;
use crate::error::DriverError;
use crate::mapping;

const DRIVER_NAME: &str = "lxd";

/// LXD compute driver.
#[derive(Debug, Clone)]
pub struct LxdComputeDriver {
    config: Config,
    lxd: LxdClient,
}

impl LxdComputeDriver {
    #[must_use]
    pub fn new(config: Config, lxd: LxdClient) -> Self {
        Self { config, lxd }
    }

    /// Report driver capabilities and defaults.
    #[must_use]
    pub fn capabilities(&self) -> GetCapabilitiesResponse {
        GetCapabilitiesResponse {
            driver_name: DRIVER_NAME.to_string(),
            driver_version: env!("CARGO_PKG_VERSION").to_string(),
            default_image: self.config.default_image.clone(),
        }
    }

    pub async fn validate_sandbox_create(
        &self,
        sandbox: &DriverSandbox,
    ) -> Result<(), DriverError> {
        if sandbox.name.is_empty() {
            return Err(DriverError::InvalidArgument("sandbox.name is required".into()));
        }
        if sandbox.id.is_empty() {
            return Err(DriverError::InvalidArgument("sandbox.id is required".into()));
        }
        if sandbox.spec.is_none() {
            return Err(DriverError::InvalidArgument("sandbox.spec is required".into()));
        }
        if sandbox
            .spec
            .as_ref()
            .and_then(|s| s.template.as_ref())
            .is_none()
        {
            return Err(DriverError::InvalidArgument(
                "sandbox.spec.template is required".into(),
            ));
        }
        Ok(())
    }

    pub async fn get_sandbox(&self, name: &str) -> Result<DriverSandbox, DriverError> {
        let instance = self.lxd.get_instance(name).await?;
        Ok(mapping::instance_to_driver_sandbox(&instance))
    }

    pub async fn list_sandboxes(&self) -> Result<Vec<DriverSandbox>, DriverError> {
        let instances = self.lxd.list_instances().await?;
        Ok(instances
            .iter()
            .filter(|i| i.config.contains_key(mapping::KEY_SANDBOX_ID))
            .map(mapping::instance_to_driver_sandbox)
            .collect())
    }

    pub async fn create_sandbox(&self, sandbox: &DriverSandbox) -> Result<(), DriverError> {
        let spec = sandbox.spec.as_ref().ok_or_else(|| {
            DriverError::InvalidArgument("sandbox.spec is required".into())
        })?;
        let template = spec.template.as_ref().ok_or_else(|| {
            DriverError::InvalidArgument("sandbox.spec.template is required".into())
        })?;

        let has_token = !spec.sandbox_token.is_empty();
        let config = mapping::build_create_config(sandbox, spec, template, "", has_token)?;
        let devices = mapping::build_create_devices(template, false);
        let profiles = mapping::build_profiles(template);

        let image = if template.image.is_empty() {
            &self.config.default_image
        } else {
            &template.image
        };

        // Create the instance stopped so we can push the token file before the
        // supervisor starts — avoids a race where the supervisor reads
        // OPENSHELL_SANDBOX_TOKEN_FILE before it has been written.
        let op = self
            .lxd
            .create_instance(&sandbox.name, image, config, devices, profiles, false)
            .await?;
        self.lxd.wait_operation(&op.id).await?;

        if has_token {
            self.lxd
                .push_file_into_instance(
                    &sandbox.name,
                    mapping::GUEST_SANDBOX_TOKEN_PATH,
                    spec.sandbox_token.as_bytes(),
                )
                .await?;
        }

        let op = self.lxd.start_instance(&sandbox.name).await?;
        self.lxd.wait_operation(&op.id).await?;

        Ok(())
    }

    pub async fn stop_sandbox(&self, name: &str) -> Result<(), DriverError> {
        let op = match self.lxd.stop_instance(name, false).await {
            // LXD returns 400 with "not running" when the instance is already stopped.
            Err(LxdError::Api { status_code: 400, ref message })
                if message.contains("not running") || message.contains("already stopped") =>
            {
                return Ok(())
            }
            other => other?,
        };
        self.lxd.wait_operation(&op.id).await?;
        Ok(())
    }

    /// Returns `Some(sandbox_id)` if the sandbox was deleted (the value is the
    /// `user.openshell.sandbox_id` from the instance config, used by the gRPC
    /// layer to broadcast a WatchSandboxes Deleted event), or `None` if the
    /// sandbox was not found (idempotent — the caller may retry safely).
    pub async fn delete_sandbox(&self, name: &str) -> Result<Option<String>, DriverError> {
        let instance = match self.lxd.get_instance(name).await {
            Ok(i) => i,
            Err(LxdError::Api { status_code: 404, .. }) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let sandbox_id = instance
            .config
            .get(mapping::KEY_SANDBOX_ID)
            .cloned()
            .unwrap_or_default();

        // Force-stop before deleting; LXD rejects deletion of running instances.
        match self.lxd.stop_instance(name, true).await {
            Ok(op) => {
                self.lxd.wait_operation(&op.id).await?;
            }
            Err(LxdError::Api { status_code: 400, .. }) => {} // already stopped
            Err(e) => return Err(e.into()),
        }

        let op = match self.lxd.delete_instance(name).await {
            Err(LxdError::Api { status_code: 404, .. }) => return Ok(None),
            other => other?,
        };
        self.lxd.wait_operation(&op.id).await?;
        Ok(Some(sandbox_id))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;
    use lxd_client::{LxdClient, LxdEndpoint};

    use super::*;

    #[test]
    fn capabilities_reports_static_fields() {
        let config = Config::parse_from(["openshell-driver-lxd"]);
        let lxd = LxdClient::new(LxdEndpoint::UnixSocket(PathBuf::from(
            "/var/snap/lxd/common/lxd/unix.socket",
        )))
        .unwrap();
        let driver = LxdComputeDriver::new(config, lxd);

        let response = driver.capabilities();

        assert_eq!(response.driver_name, "lxd");
        assert_eq!(response.driver_version, env!("CARGO_PKG_VERSION"));
        assert_eq!(response.default_image, "openshell-sandbox");
    }
}
