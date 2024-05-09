use crate::job::configuration::JobConfiguration;
use crate::job::stop_condition::JobStopCondition;
use crate::job::SyncJobStopCondition;
use uuid::Uuid;

pub mod configuration;
pub mod rocksdb_service;

trait JobRepository {
    fn add_job(
        &mut self,
        job: JobConfiguration,
        stop_condition: SyncJobStopCondition,
    ) -> anyhow::Result<()>;
    fn stop_job(&mut self, job_id: Uuid) -> anyhow::Result<()>;
    fn update_stop_condition(
        &mut self,
        job_id: Uuid,
        stop_condition: JobStopCondition,
    ) -> anyhow::Result<()>;
    fn list_running_jobs(&self) -> Vec<Uuid>;
}
