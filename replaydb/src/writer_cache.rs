//use std::time::Duration;
// use mini_moka::sync::CacheBuilder;
// use mini_moka::unsync::Cache;
//use savant_core::transport::zeromq::{NonBlockingWriter, WriterConfig, WriterSocketType};
//use serde::{Deserialize, Serialize};
// use savant_core::utils::default_once::DefaultOnceCell;
// use std::num::NonZeroU64;
// use std::sync::Arc;
// use std::time::Duration;
// use tokio::sync::Mutex;

use crate::job_writer::{JobWriter, WriterConfiguration};
use anyhow::Result;
use mini_moka::unsync::{Cache, CacheBuilder};
use savant_core::transport::zeromq::NonBlockingWriter;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct WriterCache {
    cache: Cache<WriterConfiguration, Arc<Mutex<JobWriter>>>,
}

impl WriterCache {
    pub fn new(max_capacity: NonZeroU64, ttl: Duration) -> Self {
        Self {
            cache: CacheBuilder::new(max_capacity.get())
                .time_to_live(ttl)
                .build(),
        }
    }

    pub fn get(&mut self, configuration: &WriterConfiguration) -> Result<Arc<Mutex<JobWriter>>> {
        if let Some(w) = self.cache.get(configuration) {
            return Ok(w.clone());
        } else {
            let writer = Arc::new(Mutex::new(
                NonBlockingWriter::try_from(configuration)?.into(),
            ));
            self.cache.insert(configuration.clone(), writer.clone());
            Ok(writer)
        }
    }
}
