use crate::store::Store;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobStopCondition {
    LastKeyFrame(u128),
    FrameCount(usize),
    KeyFrameCount(usize),
    PTSDelta(f64),
    RealTimeDelta(f64),
}

#[derive(Serialize)]
pub struct Job {
    #[serde(skip)]
    store: Arc<Mutex<dyn Store>>,
    id: u128,
    pts_sync: bool,
    source_id: String,
    send_eos: bool,
    stop_condition: JobStopCondition,
    idle_timeout: Duration,
    position: usize,
}

impl Debug for Job {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("pts_sync", &self.pts_sync)
            .field("source_id", &self.source_id)
            .field("send_eos", &self.send_eos)
            .field("stop_condition", &self.stop_condition)
            .field("idle_timeout", &self.idle_timeout)
            .field("position", &self.position)
            .finish()
    }
}

impl Job {
    pub fn new(
        store: Arc<Mutex<dyn Store>>,
        id: u128,
        position: usize,
        pts_sync: bool,
        source_id: String,
        send_eos: bool,
        stop_condition: JobStopCondition,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            store,
            id,
            pts_sync,
            source_id,
            send_eos,
            position,
            stop_condition,
            idle_timeout,
        }
    }
}
