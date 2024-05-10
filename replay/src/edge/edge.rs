mod web_service;

use crate::web_service::find_keyframes::find_keyframes;
use crate::web_service::shutdown::shutdown;
use crate::web_service::status::status;
use crate::web_service::JobService;
use actix_web::{web, App, HttpServer};
use anyhow::{anyhow, Result};
use log::{debug, info};
use replaydb::service::configuration::ServiceConfiguration;
use replaydb::service::rocksdb_service::RocksDbService;
use replaydb::service::JobManager;
use std::env::args;
use tokio::sync::Mutex;

#[actix_web::main]
async fn main() -> Result<()> {
    env_logger::init();
    let conf_arg = args()
        .nth(1)
        .ok_or_else(|| anyhow!("missing configuration argument"))?;
    info!("Configuration: {}", conf_arg);
    let conf = ServiceConfiguration::new(&conf_arg)?;
    debug!("Configuration: {:?}", conf);
    let rocksdb_service = RocksDbService::new(&conf)?;
    debug!("RocksDbService initialized");
    let job_service = web::Data::new(JobService {
        service: Mutex::new(rocksdb_service),
        shutdown: Mutex::new(false),
    });

    let http_job_service = job_service.clone();
    let job = tokio::spawn(
        HttpServer::new(move || {
            App::new()
                .app_data(http_job_service.clone())
                .service(status)
                .service(shutdown)
                .service(find_keyframes)
        })
        .bind(("127.0.0.1", 8080))?
        .run(),
    );

    let signal_job_service = job_service.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Ctrl-C received");
        let mut js_bind = signal_job_service.shutdown.lock().await;
        *js_bind = true;
    });

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if *job_service.shutdown.lock().await {
            job.abort();
            let _ = job.await;
            job_service.service.lock().await.shutdown().await?;
            break;
        }
    }
    Ok(())
}
