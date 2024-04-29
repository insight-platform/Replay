use crate::job_writer::SinkConfiguration;
use crate::store::rocksdb::RocksStore;
use crate::stream_processor::RocksDbStreamProcessor;
use anyhow::{bail, Result};
use savant_core::transport::zeromq::{NonBlockingReader, NonBlockingWriter, ReaderConfigBuilder};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use twelf::config;

#[derive(Debug, Serialize, Deserialize)]
pub enum TopicPrefixSpec {
    #[serde(rename = "source_id")]
    SourceId(String),
    #[serde(rename = "prefix")]
    Prefix(String),
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceConfiguration {
    pub(crate) url: String,
    pub(crate) receive_timeout: Duration,
    pub(crate) receive_hwm: usize,
    pub(crate) topic_prefix_spec: TopicPrefixSpec,
    pub(crate) source_cache_size: usize,
    pub(crate) fix_ipc_permissions: Option<u32>,
    pub(crate) inflight_ops: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Storage {
    #[serde(rename = "rocksdb")]
    RocksDB {
        path: String,
        data_expiration_ttl: Duration,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommonConfiguration {
    pub(crate) management_port: u16,
    pub(crate) stats_period: Duration,
    pub(crate) pass_metadata_only: bool,
}

#[config]
#[derive(Debug, Serialize)]
pub struct ServiceConfiguration {
    common: CommonConfiguration,
    in_stream: SourceConfiguration,
    out_stream: SinkConfiguration,
    storage: Storage,
}

impl ServiceConfiguration {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.common.management_port <= 1024 {
            bail!("Management port must be set to a value greater than 1024!");
        }
        Ok(())
    }
}

impl From<&TopicPrefixSpec> for savant_core::transport::zeromq::TopicPrefixSpec {
    fn from(value: &TopicPrefixSpec) -> Self {
        match value {
            TopicPrefixSpec::SourceId(value) => Self::SourceId(value.clone()),
            TopicPrefixSpec::Prefix(value) => Self::Prefix(value.clone()),
            TopicPrefixSpec::None => Self::None,
        }
    }
}

impl TryFrom<&SourceConfiguration> for NonBlockingReader {
    type Error = anyhow::Error;

    fn try_from(source_conf: &SourceConfiguration) -> Result<NonBlockingReader, Self::Error> {
        let conf = ReaderConfigBuilder::default()
            .url(&source_conf.url)?
            .with_receive_timeout(source_conf.receive_timeout.as_millis() as i32)?
            .with_receive_hwm(source_conf.receive_hwm as i32)?
            .with_topic_prefix_spec((&source_conf.topic_prefix_spec).into())?
            .with_routing_cache_size(source_conf.source_cache_size)?
            .with_fix_ipc_permissions(source_conf.fix_ipc_permissions)?
            .build()?;
        Ok(NonBlockingReader::new(&conf, source_conf.inflight_ops)?)
    }
}

impl TryFrom<ServiceConfiguration> for RocksDbStreamProcessor {
    type Error = anyhow::Error;

    fn try_from(conf: ServiceConfiguration) -> Result<Self, Self::Error> {
        conf.validate()?;
        let storage = match conf.storage {
            Storage::RocksDB {
                path,
                data_expiration_ttl,
            } => {
                let in_stream = NonBlockingReader::try_from(&conf.in_stream)?;
                let out_stream = NonBlockingWriter::try_from(&conf.out_stream)?;
                let path = Path::new(&path);
                let storage = RocksStore::new(path, data_expiration_ttl)?;
                RocksDbStreamProcessor::new(
                    Arc::new(Mutex::new(storage)),
                    in_stream,
                    out_stream,
                    conf.common.stats_period,
                    conf.common.pass_metadata_only,
                )
            }
        };
        Ok(storage)
    }
}

#[cfg(test)]
mod tests {
    use crate::service_configuration::ServiceConfiguration;
    use crate::stream_processor::RocksDbStreamProcessor;
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
        assert_eq!(config.common.management_port, 8080);
        assert_eq!(config.in_stream.url, "router+bind:ipc:///tmp/in");
        assert_eq!(config.out_stream.url, "dealer+connect:ipc:///tmp/out");
        let _ = RocksDbStreamProcessor::try_from(config)?;
        Ok(())
    }
}
