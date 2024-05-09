use crate::job::query::JobQuery;
use crate::job::stop_condition::JobStopCondition;
use uuid::Uuid;

pub mod configuration;
pub mod rocksdb_service;

pub(crate) trait JobManager {
    async fn add_job(&mut self, job: JobQuery) -> anyhow::Result<()>;
    fn stop_job(&mut self, job_id: Uuid) -> anyhow::Result<()>;
    fn update_stop_condition(
        &mut self,
        job_id: Uuid,
        stop_condition: JobStopCondition,
    ) -> anyhow::Result<()>;
    fn list_running_jobs(&self) -> Vec<Uuid>;
    async fn check_stream_processor_finished(&mut self) -> anyhow::Result<()>;
    async fn shutdown(&mut self) -> anyhow::Result<()>;
    async fn clean_stopped_jobs(&mut self) -> anyhow::Result<()>;
}
