use crate::job::query::JobQuery;
use crate::job::{Job, RocksDbJob};
use crate::job_writer::cache::JobWriterCache;
use crate::store::rocksdb::RocksStore;
use crate::store::Store;
use anyhow::Result;
use savant_core::primitives::frame_update::VideoFrameUpdate;
use savant_core::utils::uuid_v7::incremental_uuid_v7;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct RocksDbJobFactory(JobFactory<RocksStore>);

impl RocksDbJobFactory {
    pub fn new(
        path: &Path,
        max_capacity: NonZeroU64,
        data_ttl: Duration,
        writer_cache_ttl: Duration,
    ) -> Result<Self> {
        Ok(Self(JobFactory::new(
            RocksStore::new(path, data_ttl)?,
            max_capacity,
            writer_cache_ttl,
        )))
    }

    pub async fn create_job(&mut self, job_configuration: JobQuery) -> Result<RocksDbJob> {
        Ok(RocksDbJob(self.0.create_job(job_configuration).await?))
    }
}

pub(crate) struct JobFactory<S: Store> {
    store: Arc<Mutex<S>>,
    writer_cache: JobWriterCache,
}

impl<S> JobFactory<S>
where
    S: Store,
{
    pub fn new(store: S, max_capacity: NonZeroU64, ttl: Duration) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            writer_cache: JobWriterCache::new(max_capacity, ttl),
        }
    }
    pub async fn create_job(&mut self, job_configuration: JobQuery) -> Result<Job<S>> {
        let writer = self.writer_cache.get(&job_configuration.sink)?;
        let job_id = incremental_uuid_v7().as_u128();
        let anchor_uuid = uuid::Uuid::parse_str(&job_configuration.anchor_keyframe)?;

        let pos = self
            .store
            .lock()
            .await
            .get_first(
                &job_configuration.configuration.stored_source_id,
                anchor_uuid,
                job_configuration.offset.clone(),
            )
            .await?;

        if pos.is_none() {
            return Err(anyhow::anyhow!(
                "No frame position found for: {}",
                job_configuration.json()?
            ));
        }

        let mut update = VideoFrameUpdate::default();
        for attribute in job_configuration.attributes.iter() {
            update.add_frame_attribute(attribute.clone());
        }

        Job::new(
            self.store.clone(),
            writer,
            job_id,
            pos.unwrap(),
            job_configuration.stop_condition,
            job_configuration.configuration,
            Some(update),
        )
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    #[tokio::test]
    async fn test_create_rocksdb_job() -> Result<()> {
        //let store =
        Ok(())
    }
}
