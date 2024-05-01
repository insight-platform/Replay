use hashbrown::HashMap;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::job::configuration::JobConfiguration;
use crate::job::factory::RocksDbJobFactory;
use crate::job::SyncJobStopCondition;
use anyhow::Result;
use crate::job::stop_condition::JobStopCondition;

trait JobRepository {
    fn add_job(&mut self, job: JobConfiguration, stop_condition: SyncJobStopCondition) -> Result<()>;
    fn stop_job(&mut self, job_id: Uuid) -> Result<()>;
    fn update_stop_condition(&mut self, job_id: Uuid, stop_condition: JobStopCondition) -> Result<()>;
    fn list_running_jobs(&self) -> Vec<Uuid>;
}

pub struct RocksDBJobRepository {
    factory: RocksDbJobFactory,
    job_map: HashMap<Uuid, (JoinHandle<()>, JobConfiguration, SyncJobStopCondition)>
}