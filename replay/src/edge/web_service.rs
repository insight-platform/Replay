use replaydb::service::rocksdb_service::RocksDbService;
use tokio::sync::Mutex;

pub mod find_keyframes;
pub mod list_jobs;
pub mod shutdown;
pub mod status;

pub struct JobService {
    pub service: Mutex<RocksDbService>,
    pub shutdown: Mutex<bool>,
}
