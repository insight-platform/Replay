use crate::job_writer::SinkConfiguration;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use twelf::config;

#[derive(Debug, Serialize, Deserialize)]
pub enum TopicPrefixSpec {
    SourceId(String),
    Prefix(String),
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceConfiguration {
    pub(crate) url: String,
    pub(crate) receive_timeout: Duration,
    pub(crate) receive_hwm: usize,
    pub(crate) topic_prefix_spec: TopicPrefixSpec,
    pub(crate) routing_ids_cache_size: usize,
    pub(crate) fix_ipc_permissions: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Storage {
    RocksDB {
        path: String,
        data_expiration_ttl: Duration,
    },
}

#[config]
#[derive(Debug, Serialize)]
pub struct ServiceConfiguration {
    management_port: u16,
    in_stream: SourceConfiguration,
    out_stream: SinkConfiguration,
    storage: Storage,
}

#[cfg(test)]
mod tests {
    use crate::service_configuration::ServiceConfiguration;
    use anyhow::Result;
    use std::env::set_var;
    use twelf::Layer;

    #[test]
    fn test_configuration() -> Result<()> {
        // set env SOCKET_PATH=in
        set_var("SOCKET_PATH_IN", "in");
        set_var("SOCKET_PATH_OUT", "out");
        let config = ServiceConfiguration::with_layers(&[
            Layer::Json("./assets/rocksdb.json".into()),
            Layer::Env(None),
        ])?;
        assert_eq!(config.management_port, 8080);
        assert_eq!(config.in_stream.url, "router+bind:ipc:///tmp/in");
        assert_eq!(config.out_stream.url, "dealer+connect:ipc:///tmp/out");
        Ok(())
    }
}
