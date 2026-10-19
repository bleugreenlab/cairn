//! Deployments: a recipe placed on named machines.

use serde::{Deserialize, Serialize};

use crate::machine::MachineName;
use crate::pool::RecordSource;
use crate::recipe::RecipeId;

crate::string_id!(
    /// A deployment's id, unique within the inventory.
    DeploymentId
);

/// A recipe placed on named machines: a gang that runs on all of them or on
/// none. The API base and health URL are derived from the head machine's
/// address; no address is stored here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deployment {
    pub id: DeploymentId,
    pub source: RecordSource,
    pub recipe: RecipeId,
    /// The machines it occupies, in placement order.
    pub machines: Vec<MachineName>,
    /// The machine whose address serves the API; one of `machines`.
    pub head: MachineName,
    pub api_port: u16,
    /// The API base's path, such as `/v1`.
    pub api_path: String,
    /// The health check's path, such as `/v1/models`.
    pub health_path: String,
}

impl Deployment {
    /// The OpenAI-compatible API base, given the head machine's address.
    pub fn api_base(&self, address: &str) -> String {
        format!("{}{}", origin(address, self.api_port), self.api_path)
    }

    /// The health URL, given the head machine's address.
    pub fn health_url(&self, address: &str) -> String {
        format!("{}{}", origin(address, self.api_port), self.health_path)
    }
}

fn origin(address: &str, port: u16) -> String {
    if address.contains(':') && !address.starts_with('[') {
        format!("http://[{address}]:{port}")
    } else {
        format!("http://{address}:{port}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_derive_from_the_head_address() {
        let deployment = Deployment {
            id: "d".into(),
            source: RecordSource::Cairn,
            recipe: "r".into(),
            machines: vec!["sp1".into()],
            head: "sp1".into(),
            api_port: 8001,
            api_path: "/v1".into(),
            health_path: "/v1/models".into(),
        };
        assert_eq!(
            deployment.api_base("192.0.2.40"),
            "http://192.0.2.40:8001/v1"
        );
        assert_eq!(
            deployment.health_url("192.0.2.40"),
            "http://192.0.2.40:8001/v1/models"
        );
        assert_eq!(
            deployment.api_base("2001:db8::1"),
            "http://[2001:db8::1]:8001/v1"
        );
    }
}
