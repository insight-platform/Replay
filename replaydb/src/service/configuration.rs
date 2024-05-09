use crate::job_writer::SinkConfiguration;
use anyhow::{bail, Result};
use savant_core::transport::zeromq::{NonBlockingReader, ReaderConfigBuilder};
use serde::{Deserialize, Serialize};
use std::result;
use std::time::Duration;
use twelf::{config, Layer};

#[derive(Debug, Serialize, Deserialize)]
pub enum TopicPrefixSpec {
    #[serde(rename = "source_id")]
    SourceId(String),
    #[serde(rename = "prefix")]
    Prefix(String),
    #[serde(rename = "none")]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    pub(crate) job_writer_cache_max_capacity: u64,
    pub(crate) job_writer_cache_ttl: Duration,
}

#[config]
#[derive(Debug, Serialize)]
pub struct ServiceConfiguration {
    pub common: CommonConfiguration,
    pub in_stream: SourceConfiguration,
    pub out_stream: Option<SinkConfiguration>,
    pub storage: Storage,
}

impl ServiceConfiguration {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.common.management_port <= 1024 {
            bail!("Management port must be set to a value greater than 1024!");
        }
        Ok(())
    }

    pub fn new(path: &str) -> Result<Self> {
        let conf = Self::with_layers(&[Layer::Json(path.into())])?;
        conf.validate()?;
        Ok(conf)
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

    fn try_from(
        source_conf: &SourceConfiguration,
    ) -> result::Result<NonBlockingReader, Self::Error> {
        let conf = ReaderConfigBuilder::default()
            .url(&source_conf.url)?
            .with_receive_timeout(source_conf.receive_timeout.as_millis() as i32)?
            .with_receive_hwm(source_conf.receive_hwm as i32)?
            .with_topic_prefix_spec((&source_conf.topic_prefix_spec).into())?
            .with_routing_cache_size(source_conf.source_cache_size)?
            .with_fix_ipc_permissions(source_conf.fix_ipc_permissions)?
            .build()?;
        NonBlockingReader::new(&conf, source_conf.inflight_ops)
    }
}