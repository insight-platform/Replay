use crate::store::rocksdb::RocksStore;
use crate::store::{to_hex_string, Store};
use crate::{ZmqReader, ZmqWriter};
use anyhow::{bail, Result};
use parking_lot::Mutex;
use savant_core::transport::zeromq::ReaderResult;
use std::sync::Arc;

struct StreamProcessor<T: Store> {
    db: Arc<Mutex<T>>,
    input: ZmqReader,
    output: ZmqWriter,
}

impl<T> StreamProcessor<T>
where
    T: Store,
{
    pub fn new(db: Arc<Mutex<T>>, input: ZmqReader, output: ZmqWriter) -> Self {
        Self { db, input, output }
    }

    pub async fn run_once(&mut self) -> Result<()> {
        let message = self.input.receive();
        match message {
            Ok(m) => match m {
                ReaderResult::Message {
                    message,
                    topic,
                    routing_id,
                    data,
                } => {
                    log::debug!(
                        "Received message: topic: {:?}, routing_id: {:?}, message: {:?}",
                        to_hex_string(&topic),
                        to_hex_string(routing_id.as_ref().unwrap_or(&Vec::new())),
                        message
                    );
                    if message.is_video_frame()
                        || message.is_user_data()
                        || message.is_end_of_stream()
                    {
                        self.db.lock().add_message(&message, &topic, &data).await?;
                    }
                    let data_slice = data.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>();
                    self.output.send_message(
                        std::str::from_utf8(&topic)?,
                        &message,
                        &data_slice,
                    )?;
                }
                ReaderResult::Timeout => {
                    log::info!("Timeout receiving message, waiting for next message.");
                }
                ReaderResult::PrefixMismatch { topic, routing_id } => {
                    log::warn!(
                        "Received message with mismatched prefix: topic: {:?}, routing_id: {:?}",
                        to_hex_string(&topic),
                        to_hex_string(routing_id.as_ref().unwrap_or(&Vec::new()))
                    );
                }
                ReaderResult::RoutingIdMismatch { topic, routing_id } => {
                    log::warn!(
                            "Received message with mismatched routing_id: topic: {:?}, routing_id: {:?}",
                            to_hex_string(&topic),
                            to_hex_string(routing_id.as_ref().unwrap_or(&Vec::new()))
                        );
                }
                ReaderResult::TooShort(m) => {
                    log::warn!("Received message that was too short: {:?}", m);
                }
            },
            Err(e) => {
                bail!("Error receiving message: {:?}", e);
            }
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            self.run_once().await?
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::store::{gen_properly_filled_frame, Store};
    use crate::{ZmqReader, ZmqWriter};
    use anyhow::Result;
    use parking_lot::Mutex;
    use savant_core::transport::zeromq::{ReaderConfig, ReaderResult, WriterConfig};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_stream_processor() -> Result<()> {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap();
        let db = crate::store::rocksdb::RocksStore::new(path, Duration::from_secs(60)).unwrap();

        let in_reader = ZmqReader::new(
            &ReaderConfig::new()
                .url(&format!("router+bind:ipc://{}/in", path))?
                .with_fix_ipc_permissions(Some(0o777))?
                .build()?,
        )?;

        let mut in_writer = ZmqWriter::new(
            &WriterConfig::new()
                .url(&format!("dealer+connect:ipc://{}/in", path))?
                .build()?,
        )?;

        let mut out_reader = ZmqReader::new(
            &ReaderConfig::new()
                .url(&format!("router+bind:ipc://{}/out", path))?
                .with_fix_ipc_permissions(Some(0o777))?
                .build()?,
        )?;

        let out_writer = ZmqWriter::new(
            &WriterConfig::new()
                .url(&format!("dealer+connect:ipc://{}/out", path))?
                .build()?,
        )?;
        let db = Arc::new(Mutex::new(db));
        let mut processor =
            crate::stream_processor::StreamProcessor::new(db.clone(), in_reader, out_writer);

        let f = gen_properly_filled_frame();
        let uuid = f.get_uuid_u128();
        let m1 = f.to_message();
        in_writer.send_message("test", &m1, &[&[0x01]])?;
        processor.run_once().await?;
        let res = out_reader.receive()?;
        let (m2, _, _) = db.lock().get_message("test", 0).await?.unwrap();
        assert_eq!(uuid, m2.as_video_frame().unwrap().get_uuid_u128());
        match res {
            ReaderResult::Message {
                message,
                topic,
                routing_id: _,
                data,
            } => {
                assert_eq!(message.as_video_frame().unwrap().get_uuid_u128(), uuid);
                assert_eq!(topic, b"test");
                assert_eq!(data, vec![vec![0x01]]);
            }
            ReaderResult::Timeout => {
                panic!("Timeout");
            }
            ReaderResult::PrefixMismatch { .. } => {
                panic!("Prefix mismatch");
            }
            ReaderResult::RoutingIdMismatch { .. } => {
                panic!("Routing ID mismatch");
            }
            ReaderResult::TooShort(_) => {
                panic!("Too short");
            }
        }
        std::fs::remove_dir_all(path).unwrap_or_default();
        Ok(())
    }
}

pub struct RocksDbStreamProcessor(StreamProcessor<RocksStore>);

impl RocksDbStreamProcessor {
    pub fn new(db: Arc<Mutex<RocksStore>>, input: ZmqReader, output: ZmqWriter) -> Self {
        Self(StreamProcessor::new(db, input, output))
    }

    pub async fn run_once(&mut self) -> Result<()> {
        self.0.run_once().await
    }

    pub async fn run(&mut self) -> Result<()> {
        self.0.run().await
    }
}
