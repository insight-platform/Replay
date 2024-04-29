use crate::job::query::JobQuery;
use crate::job::{Job, RocksDbJob};
use crate::job_writer::cache::JobWriterCache;
use crate::store::rocksdb::RocksStore;
use crate::store::Store;
use anyhow::Result;
use savant_core::primitives::frame_update::VideoFrameUpdate;
use savant_core::utils::uuid_v7::incremental_uuid_v7;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct RocksDbJobFactory(JobFactory<RocksStore>);

impl RocksDbJobFactory {
    pub fn new(
        store: Arc<Mutex<RocksStore>>,
        max_capacity: NonZeroU64,
        writer_cache_ttl: Duration,
    ) -> Result<Self> {
        Ok(Self(JobFactory::new(store, max_capacity, writer_cache_ttl)))
    }

    pub fn store(&self) -> Arc<Mutex<RocksStore>> {
        self.0.store.clone()
    }

    pub async fn create_job(&mut self, query: JobQuery) -> Result<RocksDbJob> {
        Ok(RocksDbJob(self.0.create_job(query).await?))
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
    pub fn new(store: Arc<Mutex<S>>, max_capacity: NonZeroU64, ttl: Duration) -> Self {
        Self {
            store,
            writer_cache: JobWriterCache::new(max_capacity, ttl),
        }
    }
    pub async fn create_job(&mut self, query: JobQuery) -> Result<Job<S>> {
        let writer = self.writer_cache.get(&query.sink)?;
        let job_id = incremental_uuid_v7().as_u128();
        let anchor_uuid = uuid::Uuid::parse_str(&query.anchor_keyframe)?;

        let pos = self
            .store
            .lock()
            .await
            .get_first(
                &query.configuration.stored_source_id,
                anchor_uuid,
                query.offset.clone(),
            )
            .await?;

        if pos.is_none() {
            return Err(anyhow::anyhow!(
                "No frame position found for: {}",
                query.json()?
            ));
        }

        let mut update = VideoFrameUpdate::default();
        for attribute in query.attributes.iter() {
            update.add_frame_attribute(attribute.clone());
        }

        Job::new(
            self.store.clone(),
            writer,
            job_id,
            pos.unwrap(),
            query.stop_condition,
            query.configuration,
            Some(update),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::job::configuration::JobConfigurationBuilder;
    use crate::job::factory::RocksDbJobFactory;
    use crate::job::query::JobQuery;
    use crate::job::stop_condition::JobStopCondition;
    use crate::job_writer::SinkConfiguration;
    use crate::store::rocksdb::RocksStore;
    use crate::store::{gen_properly_filled_frame, JobOffset, Store};
    use anyhow::Result;
    use savant_core::primitives::attribute_value::AttributeValue;
    use savant_core::primitives::Attribute;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_create_rocksdb_job() -> Result<()> {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path();
        let store = Arc::new(Mutex::new(RocksStore::new(path, Duration::from_secs(60))?));

        let mut factory =
            RocksDbJobFactory::new(store.clone(), 1024u64.try_into()?, Duration::from_secs(30))?;
        let f = gen_properly_filled_frame();
        let source_id = f.get_source_id();
        store
            .lock()
            .await
            .add_message(&f.to_message(), &[], &[])
            .await?;
        let f = gen_properly_filled_frame();
        store
            .lock()
            .await
            .add_message(&f.to_message(), &[], &[])
            .await?;

        let configuration = JobConfigurationBuilder::default()
            .min_duration(Duration::from_millis(7))
            .max_duration(Duration::from_millis(10))
            .stored_source_id(source_id)
            .resulting_source_id("resulting_source_id".to_string())
            .build()
            .unwrap();
        let stop_condition = JobStopCondition::frame_count(1);
        let offset = JobOffset::Blocks(1);
        let job_query = JobQuery::new(
            SinkConfiguration::test_dealer_connect_sink(),
            configuration,
            stop_condition,
            f.get_uuid(),
            offset,
            vec![Attribute::persistent(
                "key",
                "value",
                vec![
                    AttributeValue::integer(1, Some(0.5)),
                    AttributeValue::float_vector(vec![1.0, 2.0, 3.0], None),
                ],
                &None,
                false,
            )],
        );
        let _job = factory.create_job(job_query).await?;
        Ok(())
    }
}