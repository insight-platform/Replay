use crate::job::configuration::JobConfiguration;
use crate::job::factory::RocksDbJobFactory;
use crate::job::SyncJobStopCondition;
use crate::service::configuration::{ServiceConfiguration, Storage};
use crate::store::rocksdb::RocksDbStore;
use crate::store::SyncRocksDbStore;
use crate::stream_processor::RocksDbStreamProcessor;
use anyhow::Result;
use hashbrown::HashMap;
use savant_core::transport::zeromq::{NonBlockingReader, NonBlockingWriter};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct RocksDbService {
    stream_processor: Arc<Mutex<RocksDbStreamProcessor>>,
    job_factory: RocksDbJobFactory,
    job_map: HashMap<Uuid, (JoinHandle<()>, JobConfiguration, SyncJobStopCondition)>,
}

impl TryFrom<&ServiceConfiguration> for SyncRocksDbStore {
    type Error = anyhow::Error;

    fn try_from(configuration: &ServiceConfiguration) -> Result<Self> {
        let Storage::RocksDB {
            path,
            data_expiration_ttl,
        } = configuration.storage.clone();

        let path = PathBuf::from(path);
        let store = RocksDbStore::new(&path, data_expiration_ttl)?;
        let sync_store = Arc::new(Mutex::new(store));
        Ok(sync_store)
    }
}

impl TryFrom<&ServiceConfiguration> for RocksDbStreamProcessor {
    type Error = anyhow::Error;

    fn try_from(configuration: &ServiceConfiguration) -> Result<Self> {
        let store = SyncRocksDbStore::try_from(configuration)?;
        let input = NonBlockingReader::try_from(&configuration.in_stream)?;
        let output = if let Some(out_stream) = &configuration.out_stream {
            Some(NonBlockingWriter::try_from(out_stream)?)
        } else {
            None
        };
        Ok(RocksDbStreamProcessor::new(
            store,
            input,
            output,
            configuration.common.stats_period,
            configuration.common.pass_metadata_only,
        ))
    }
}

impl RocksDbService {
    pub fn new(config: &ServiceConfiguration) -> Result<Self> {
        let stream_processor = RocksDbStreamProcessor::try_from(config)?;
        let store = stream_processor.store();
        let job_factory = RocksDbJobFactory::new(
            store,
            config.common.job_writer_cache_max_capacity.try_into()?,
            config.common.job_writer_cache_ttl,
        )?;

        Ok(Self {
            stream_processor: Arc::new(Mutex::new(stream_processor)),
            job_factory,
            job_map: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::service::configuration::ServiceConfiguration;
    use crate::service::rocksdb_service::RocksDbService;
    use std::env::set_var;

    #[test]
    fn test_rocksdb_service() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir().unwrap();
        set_var("DB_PATH", tmp_dir.path().to_str().unwrap());
        set_var("SOCKET_PATH_IN", "in");
        set_var("SOCKET_PATH_OUT", "out");
        let config = ServiceConfiguration::new("assets/rocksdb.json")?;
        let _service = RocksDbService::new(&config)?;
        Ok(())
    }

    #[test]
    fn test_rockdb_serice_opt_writer() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir().unwrap();
        set_var("DB_PATH", tmp_dir.path().to_str().unwrap());
        set_var("SOCKET_PATH_IN", "in");
        let config = ServiceConfiguration::new("assets/rocksdb_opt_out.json")?;
        let _service = RocksDbService::new(&config)?;
        Ok(())
    }
}
