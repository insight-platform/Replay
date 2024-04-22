use crate::store::rocksdb::RocksStore;
use crate::store::Store;
use crate::topic_to_string;
use anyhow::{bail, Result};
use savant_core::message::Message;
use savant_core::transport::zeromq::{
    NonBlockingReader, NonBlockingWriter, ReaderResult, WriterResult,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Debug)]
struct StreamStats {
    packet_counter: u64,
    byte_counter: u64,
}

struct StreamProcessor<T: Store> {
    db: Arc<Mutex<T>>,
    input: NonBlockingReader,
    output: NonBlockingWriter,
    stats: StreamStats,
    last_stats: Instant,
    stats_period: Duration,
}

impl<T> StreamProcessor<T>
where
    T: Store,
{
    pub fn new(
        db: Arc<Mutex<T>>,
        input: NonBlockingReader,
        output: NonBlockingWriter,
        stats_period: Duration,
    ) -> Self {
        Self {
            db,
            input,
            output,
            stats: StreamStats {
                packet_counter: 0,
                byte_counter: 0,
            },
            stats_period,
            last_stats: Instant::now(),
        }
    }

    async fn receive_message(&mut self) -> Result<ReaderResult> {
        loop {
            if Instant::now() - self.last_stats > self.stats_period {
                log::info!(
                    target: "replay::db::stream_processor::receive_message",
                    "Stats: packets: {}, bytes: {}",
                    self.stats.packet_counter,
                    self.stats.byte_counter
                );
                self.last_stats = Instant::now();
            }
            let message = self.input.try_receive();
            if message.is_none() {
                tokio_timerfd::sleep(Duration::from_micros(100)).await?;
                continue;
            }
            return message.unwrap();
        }
    }

    async fn send_message(&mut self, topic: &str, message: &Message, data: &[&[u8]]) -> Result<()> {
        loop {
            let res = self.output.send_message(topic, message, data)?;
            loop {
                let send_res = res.try_get()?;
                if send_res.is_none() {
                    tokio_timerfd::sleep(Duration::from_micros(100)).await?;
                    continue;
                }
                let send_res = send_res.unwrap()?;
                match send_res {
                    WriterResult::SendTimeout => {
                        log::warn!("Send timeout, retrying.");
                        break;
                    }
                    WriterResult::AckTimeout(_) => {
                        log::warn!("Ack timeout, retrying.");
                        break;
                    }
                    WriterResult::Ack { .. } => {
                        return Ok(());
                    }
                    WriterResult::Success { .. } => {
                        return Ok(());
                    }
                }
            }
        }
    }

    pub async fn run_once(&mut self) -> Result<()> {
        let message = self.receive_message().await;
        match message {
            Ok(m) => match m {
                ReaderResult::Blacklisted(topic) => {
                    log::debug!("Received blacklisted message: {}", topic_to_string(&topic));
                }
                ReaderResult::Message {
                    message,
                    topic,
                    routing_id,
                    data,
                } => {
                    log::debug!(
                        "Received message: topic: {}, routing_id: {}, message: {:?}",
                        topic_to_string(&topic),
                        topic_to_string(routing_id.as_ref().unwrap_or(&Vec::new())),
                        message
                    );
                    if message.is_video_frame()
                        || message.is_user_data()
                        || message.is_end_of_stream()
                    {
                        self.db
                            .lock()
                            .await
                            .add_message(&message, &topic, &data)
                            .await?;
                    }
                    let data_slice = data.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>();
                    self.send_message(std::str::from_utf8(&topic)?, &message, &data_slice)
                        .await?;
                }
                ReaderResult::Timeout => {
                    log::info!("Timeout receiving message, waiting for next message.");
                }
                ReaderResult::PrefixMismatch { topic, routing_id } => {
                    log::warn!(
                        "Received message with mismatched prefix: topic: {:?}, routing_id: {:?}",
                        topic_to_string(&topic),
                        topic_to_string(routing_id.as_ref().unwrap_or(&Vec::new()))
                    );
                }
                ReaderResult::RoutingIdMismatch { topic, routing_id } => {
                    log::warn!(
                            "Received message with mismatched routing_id: topic: {:?}, routing_id: {:?}",
                            topic_to_string(&topic),
                            topic_to_string(routing_id.as_ref().unwrap_or(&Vec::new()))
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
    use anyhow::Result;
    use savant_core::transport::zeromq::{
        NonBlockingReader, NonBlockingWriter, ReaderConfig, ReaderResult, WriterConfig,
    };
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_stream_processor() -> Result<()> {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap();
        let db = crate::store::rocksdb::RocksStore::new(path, Duration::from_secs(60)).unwrap();

        let mut in_reader = NonBlockingReader::new(
            &ReaderConfig::new()
                .url(&format!("router+bind:ipc://{}/in", path))?
                .with_fix_ipc_permissions(Some(0o777))?
                .build()?,
            100,
        )?;
        in_reader.start()?;
        tokio_timerfd::sleep(Duration::from_millis(100)).await?;

        let mut in_writer = NonBlockingWriter::new(
            &WriterConfig::new()
                .url(&format!("dealer+connect:ipc://{}/in", path))?
                .build()?,
            100,
        )?;
        in_writer.start()?;
        tokio_timerfd::sleep(Duration::from_millis(100)).await?;

        let mut out_reader = NonBlockingReader::new(
            &ReaderConfig::new()
                .url(&format!("router+bind:ipc://{}/out", path))?
                .with_fix_ipc_permissions(Some(0o777))?
                .build()?,
            100,
        )?;
        out_reader.start()?;
        tokio_timerfd::sleep(Duration::from_millis(100)).await?;

        let mut out_writer = NonBlockingWriter::new(
            &WriterConfig::new()
                .url(&format!("dealer+connect:ipc://{}/out", path))?
                .build()?,
            100,
        )?;
        out_writer.start()?;
        tokio_timerfd::sleep(Duration::from_millis(100)).await?;

        let db = Arc::new(Mutex::new(db));
        let mut processor = crate::stream_processor::StreamProcessor::new(
            db.clone(),
            in_reader,
            out_writer,
            Duration::from_secs(30),
        );

        let f = gen_properly_filled_frame();
        let uuid = f.get_uuid_u128();
        let m1 = f.to_message();
        in_writer.send_message("test", &m1, &[&[0x01]])?;
        processor.run_once().await?;
        let res = out_reader.receive()?;
        let (m2, _, _) = db.lock().await.get_message("test", 0).await?.unwrap();
        assert_eq!(uuid, m2.as_video_frame().unwrap().get_uuid_u128());
        match res {
            ReaderResult::Blacklisted(topic) => {
                panic!("Blacklisted message: {:?}", topic);
            }
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
        Ok(())
    }
}

pub struct RocksDbStreamProcessor(StreamProcessor<RocksStore>);

impl RocksDbStreamProcessor {
    pub fn new(
        db: Arc<Mutex<RocksStore>>,
        input: NonBlockingReader,
        output: NonBlockingWriter,
        stats_period: Duration,
    ) -> Self {
        Self(StreamProcessor::new(db, input, output, stats_period))
    }

    pub async fn run_once(&mut self) -> Result<()> {
        self.0.run_once().await
    }

    pub async fn run(&mut self) -> Result<()> {
        self.0.run().await
    }
}
