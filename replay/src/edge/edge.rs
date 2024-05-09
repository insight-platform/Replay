use anyhow::{anyhow, Result};
use replaydb::service::configuration::ServiceConfiguration;
use replaydb::stream_processor::RocksDbStreamProcessor;
use std::env::args;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let conf_arg = args()
        .nth(1)
        .ok_or_else(|| anyhow!("missing configuration argument"))?;
    let _conf = ServiceConfiguration::new(&conf_arg)?;
    // let mut stream_processor = RocksDbStreamProcessor::try_from(&conf)?;
    // let job = tokio::spawn(async move {
    //     let _ = stream_processor.run().await;
    // });
    // tokio::join!(job).0?;
    //let _ = job.await?;
    Ok(())
}
