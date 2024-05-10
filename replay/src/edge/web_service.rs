use replaydb::service::rocksdb_service::RocksDbService;
use tokio::sync::Mutex;

pub mod shutdown;
pub mod status;

pub struct JobService {
    pub service: Mutex<RocksDbService>,
    pub shutdown: Mutex<bool>,
}
