use crate::store::Store;
use crate::ZmqWriter;
use anyhow::Result;
use parking_lot::Mutex;
use savant_core::message::Message;
use savant_core::transport::zeromq::WriterResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobStopCondition {
    LastKeyFrame(u128),
    FrameCount(usize),
    KeyFrameCount(usize),
    PTSDelta(f64),
    RealTimeDelta(f64),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RoutingLabelsUpdateStrategy {
    Keep,
    Replace(Vec<String>),
    Append(Vec<String>),
}

#[derive(Serialize)]
pub struct Job {
    #[serde(skip)]
    store: Arc<Mutex<dyn Store>>,
    #[serde(skip)]
    writer: Arc<Mutex<ZmqWriter>>,
    id: u128,
    pts_sync: bool,
    db_source_id: String,
    resulting_source_id: String,
    routing_labels: RoutingLabelsUpdateStrategy,
    send_eos: bool,
    stop_condition: JobStopCondition,
    idle_timeout: Duration,
    accumulated_idle_timeout: Duration,
    position: usize,
    last_pts: Option<u128>,
    last_elapsed: Duration,
    last_invocation: u128,
    next_step: u128,
}

impl Debug for Job {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("pts_sync", &self.pts_sync)
            .field("source_id", &self.db_source_id)
            .field("resulting_source_id", &self.resulting_source_id)
            .field("routing_labels", &self.routing_labels)
            .field("send_eos", &self.send_eos)
            .field("stop_condition", &self.stop_condition)
            .field("idle_timeout", &self.idle_timeout)
            .field("accumulated_idle_timeout", &self.accumulated_idle_timeout)
            .field("position", &self.position)
            .field("next_step", &self.next_step)
            .finish()
    }
}

impl Job {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<Mutex<dyn Store>>,
        writer: Arc<Mutex<ZmqWriter>>,
        id: u128,
        position: usize,
        pts_sync: bool,
        db_source_id: String,
        resulting_source_id: String,
        routing_labels: RoutingLabelsUpdateStrategy,
        send_eos: bool,
        stop_condition: JobStopCondition,
        idle_timeout: Duration,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self {
            store,
            writer,
            id,
            pts_sync,
            db_source_id,
            resulting_source_id,
            routing_labels,
            send_eos,
            position,
            stop_condition,
            idle_timeout,
            last_pts: None,
            accumulated_idle_timeout: Duration::from_secs(0),
            last_invocation: 0,
            last_elapsed: Duration::from_secs(0),
            next_step: now,
        }
    }

    pub fn get_current_message_pts(&self, m: &Message) -> Option<u128> {
        if m.is_video_frame() {
            Some(m.as_video_frame().unwrap().get_creation_timestamp_ns())
        } else {
            None
        }
    }

    pub fn prepare_message(&self, m: Message) -> Message {
        let mut message = if m.is_end_of_stream() {
            let mut eos = m.as_end_of_stream().unwrap().clone();
            eos.source_id = self.resulting_source_id.clone();
            Message::end_of_stream(eos)
        } else if m.is_video_frame() {
            let mut f = m.as_video_frame().unwrap().clone();
            f.set_source_id(&self.resulting_source_id);
            f.to_message()
        } else {
            m
        };

        match &self.routing_labels {
            RoutingLabelsUpdateStrategy::Keep => {}
            RoutingLabelsUpdateStrategy::Replace(labels) => {
                message.meta_mut().routing_labels = labels.clone();
            }
            RoutingLabelsUpdateStrategy::Append(labels) => {
                message.meta_mut().routing_labels.extend(labels.clone());
            }
        }

        message
    }

    pub fn send_message(
        &mut self,
        message: Message,
        data: Vec<Vec<u8>>,
    ) -> Result<Option<Duration>> {
        let ts = Instant::now();
        let sliced_data = data.iter().map(|d| d.as_slice()).collect::<Vec<_>>();
        dbg!(&sliced_data);

        let result =
            self.writer
                .lock()
                .send_message(&self.resulting_source_id, &message, &sliced_data)?;

        match result {
            WriterResult::SendTimeout => {
                log::warn!("Job {} send timeout", self.id);
                Ok(None)
            }
            WriterResult::AckTimeout(timeout) => {
                log::warn!("Job {} ack timeout after {} ms", self.id, timeout);
                Ok(None)
            }
            WriterResult::Ack {
                send_retries_spent,
                receive_retries_spent,
                time_spent,
            } => {
                log::debug!(
                    "Job {} sent message {} in {} ms with {} send retries and {} receive retries",
                    self.id,
                    self.position,
                    time_spent,
                    send_retries_spent,
                    receive_retries_spent
                );

                self.position += 1;
                Ok(Some(ts.elapsed()))
            }
            WriterResult::Success {
                retries_spent,
                time_spent,
            } => {
                log::debug!(
                    "Job {} sent message {} in {} ms with {} retries",
                    self.id,
                    self.position,
                    time_spent,
                    retries_spent
                );

                self.position += 1;
                Ok(Some(ts.elapsed()))
            }
        }
    }

    pub fn advance(&mut self) -> Result<u128> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        if self.next_step > now {
            return Ok(self.next_step - now);
        }

        // ready
        let res = self
            .store
            .lock()
            .get_message(&self.db_source_id, self.position)?;

        if let Some((message, _, data)) = res {
            self.accumulated_idle_timeout = Duration::from_secs(0);
            let message_pts = self.get_current_message_pts(&message);
            self.next_step = self.calculate_next_step(message_pts, Some(self.last_elapsed));
            if self.next_step > now {
                return Ok(self.next_step - now);
            }
            let prepared_message = self.prepare_message(message);
            let elapsed = self.send_message(prepared_message, data)?;
            if elapsed.is_none() {
                return Ok(0);
            }
            self.last_elapsed = elapsed.unwrap();
            if let Some(mp) = message_pts {
                self.last_pts = Some(mp);
            }
            self.last_invocation = now;
        } else {
            self.accumulated_idle_timeout +=
                Duration::from_nanos((now - self.last_invocation) as u64);
        }
        Ok(self.next_step - now)
    }
    fn calculate_next_step(
        &mut self,
        pts_sec: Option<u128>,
        last_op_duration: Option<Duration>,
    ) -> u128 {
        if !self.pts_sync {
            self.last_invocation
        } else if self.last_pts.is_none() {
            self.last_invocation
        } else if pts_sec.is_none() || last_op_duration.is_none() {
            self.last_invocation
        } else {
            let pts_sec = pts_sec.unwrap();
            let last_op_duration = last_op_duration.unwrap();

            let pts_delta = pts_sec.saturating_sub(self.last_pts.unwrap());
            let next_pts_delta_sec = pts_delta.saturating_sub(last_op_duration.as_nanos());

            self.last_invocation + next_pts_delta_sec
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{gen_properly_filled_frame, Offset};
    use crate::ZmqReader;
    use savant_core::primitives::eos::EndOfStream;
    use savant_core::transport::zeromq::{ReaderConfig, ReaderResult, WriterConfig};
    use uuid::Uuid;

    struct MockStore {
        pub payload: Option<(Message, Vec<u8>, Vec<Vec<u8>>)>,
    }

    impl Store for MockStore {
        fn add_message(
            &mut self,
            message: &Message,
            _topic: &[u8],
            _data: &[Vec<u8>],
        ) -> Result<usize> {
            Ok(0)
        }

        fn get_message(
            &mut self,
            _: &str,
            id: usize,
        ) -> Result<Option<(Message, Vec<u8>, Vec<Vec<u8>>)>> {
            Ok(self.payload.take())
        }

        fn get_first(
            &mut self,
            _: &str,
            keyframe_uuid: Uuid,
            before: Offset,
        ) -> Result<Option<usize>> {
            todo!("")
        }
    }

    #[test]
    fn test_unsynchronized_advancing() -> Result<()> {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap();
        let db = crate::store::rocksdb::RocksStore::new(path, Duration::from_secs(60)).unwrap();

        let mut in_reader = ZmqReader::new(
            &ReaderConfig::new()
                .url(&format!("router+bind:ipc://{}/in", path))?
                .with_fix_ipc_permissions(Some(0o777))?
                .build()?,
        )?;

        let in_writer = ZmqWriter::new(
            &WriterConfig::new()
                .url(&format!("dealer+connect:ipc://{}/in", path))?
                .build()?,
        )?;

        let store = Arc::new(Mutex::new(MockStore {
            payload: Some((
                Message::end_of_stream(EndOfStream::new(String::from("source"))),
                vec![],
                vec![],
            )),
        }));

        let mut job = Job::new(
            store.clone(),
            Arc::new(Mutex::new(in_writer)),
            0,
            0,
            false,
            "source".to_string(),
            "result".to_string(),
            RoutingLabelsUpdateStrategy::Keep,
            false,
            JobStopCondition::LastKeyFrame(0),
            Duration::from_secs(0),
        );

        let res = job.advance();
        assert!(matches!(res, Ok(0)));
        let m = in_reader.receive().unwrap();
        if let ReaderResult::Message {
            message,
            topic,
            routing_id,
            data,
        } = m
        {
            assert_eq!(message.as_end_of_stream().unwrap().source_id, "result");
            assert_eq!(topic, b"result");
            assert!(data.is_empty());
        } else {
            panic!("Unexpected message type");
        }

        store.lock().payload = Some((
            gen_properly_filled_frame().to_message(),
            vec![],
            vec![vec![0x0]],
        ));

        let res = job.advance();
        assert!(matches!(res, Ok(0)));
        let m = in_reader.receive().unwrap();
        if let ReaderResult::Message {
            message,
            topic,
            routing_id,
            data,
        } = m
        {
            assert!(message.is_video_frame());
            assert_eq!(topic, b"result");
            assert_eq!(data, vec![vec![0x0]]);
        } else {
            panic!("Unexpected message type");
        }

        Ok(())
    }
}
