use crate::job::configuration::JobConfiguration;
use crate::job::factory::RocksDbJobFactory;
use crate::job::query::JobQuery;
use crate::job::stop_condition::JobStopCondition;
use crate::job::SyncJobStopCondition;
use crate::service::configuration::{ServiceConfiguration, Storage};
use crate::service::JobManager;
use crate::store::rocksdb::RocksDbStore;
use crate::store::SyncRocksDbStore;
use crate::stream_processor::RocksDbStreamProcessor;
use anyhow::{bail, Result};
use hashbrown::HashMap;
use log::{info, warn};
use savant_core::transport::zeromq::{NonBlockingReader, NonBlockingWriter};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct RocksDbService {
    stream_processor_job_handle: Option<JoinHandle<Result<()>>>,
    job_factory: RocksDbJobFactory,
    job_map: HashMap<
        Uuid,
        (
            JoinHandle<Result<()>>,
            JobConfiguration,
            Arc<SyncJobStopCondition>,
        ),
    >,
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
        let mut stream_processor = RocksDbStreamProcessor::try_from(config)?;
        let store = stream_processor.store();
        let job_factory = RocksDbJobFactory::new(
            store,
            config.common.job_writer_cache_max_capacity.try_into()?,
            config.common.job_writer_cache_ttl,
        )?;

        let stream_processor_job_handle =
            Some(tokio::spawn(async move { stream_processor.run().await }));

        Ok(Self {
            stream_processor_job_handle,
            job_factory,
            job_map: HashMap::new(),
        })
    }
}

impl JobManager for RocksDbService {
    async fn add_job(&mut self, job_query: JobQuery) -> Result<()> {
        let configuration = job_query.configuration.clone();
        let mut job = self.job_factory.create_job(job_query).await?;
        let job_id = job.get_id();
        let stop_condition = job.get_stop_condition_ref();
        let job_handle = tokio::spawn(async move { job.run_until_complete().await });
        self.job_map
            .insert(job_id, (job_handle, configuration, stop_condition));
        Ok(())
    }

    fn stop_job(&mut self, job_id: Uuid) -> Result<()> {
        if let Some((job_handle, _, _)) = self.job_map.remove(&job_id) {
            job_handle.abort();
        }
        Ok(())
    }

    fn update_stop_condition(
        &mut self,
        job_id: Uuid,
        stop_condition: JobStopCondition,
    ) -> Result<()> {
        if let Some((_, _, stop_condition_ref)) = self.job_map.get_mut(&job_id) {
            let mut sc = stop_condition_ref.0.lock();
            *sc = stop_condition;
        }
        Ok(())
    }

    fn list_running_jobs(&self) -> Vec<Uuid> {
        self.job_map.keys().cloned().collect()
    }

    async fn check_stream_processor_finished(&mut self) -> Result<bool> {
        if self.stream_processor_job_handle.is_none() {
            bail!("Stream processor job handle is none. No longer functional");
        }

        if self
            .stream_processor_job_handle
            .as_ref()
            .unwrap()
            .is_finished()
        {
            let sp = self.stream_processor_job_handle.take().unwrap();
            warn!("Stream processor job is finished. Stopping all jobs");
            sp.await??;
            for (uuid, (job_handle, _, _)) in self.job_map.drain() {
                info!("Stopping job: {}", uuid);
                job_handle.abort();
                info!("Job: {} stopped", uuid);
            }
            return Ok(true);
        }
        Ok(false)
    }

    async fn shutdown(&mut self) -> Result<()> {
        if let Some(sp) = self.stream_processor_job_handle.take() {
            info!("Stopping stream processor");
            sp.abort();
            let _ = sp.await;
            info!("Stream processor stopped");
        }
        for (uuid, (job_handle, _, _)) in self.job_map.drain() {
            info!("Stopping job: {}", uuid);
            job_handle.abort();
            let _ = job_handle.await;
            info!("Job: {} stopped", uuid);
        }
        Ok(())
    }

    async fn clean_stopped_jobs(&mut self) -> Result<()> {
        let mut to_remove = vec![];
        for (uuid, (job_handle, _, _)) in self.job_map.iter() {
            if job_handle.is_finished() {
                to_remove.push(*uuid);
            }
        }
        for uuid in to_remove {
            let (job_handle, _, _) = self.job_map.remove(&uuid).unwrap();
            let res = job_handle.await?;
            if let Err(e) = res {
                warn!("Job: {} failed with error: {}", uuid, e);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::service::configuration::ServiceConfiguration;
    use crate::service::rocksdb_service::RocksDbService;
    use crate::service::JobManager;
    use std::env::set_var;

    #[tokio::test]
    async fn test_rocksdb_service() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir().unwrap();
        set_var("DB_PATH", tmp_dir.path().to_str().unwrap());
        set_var("SOCKET_PATH_IN", "in");
        set_var("SOCKET_PATH_OUT", "out");
        let config = ServiceConfiguration::new("assets/rocksdb.json")?;
        let mut service = RocksDbService::new(&config)?;
        service.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_rockdb_service_opt_writer() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir().unwrap();
        set_var("DB_PATH", tmp_dir.path().to_str().unwrap());
        set_var("SOCKET_PATH_IN", "in");
        let config = ServiceConfiguration::new("assets/rocksdb_opt_out.json")?;
        let mut service = RocksDbService::new(&config)?;
        service.shutdown().await?;
        Ok(())
    }
}
