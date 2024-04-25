use anyhow::{bail, Result};
use savant_core::message::Message;
use savant_core::primitives::frame_update::VideoFrameUpdate;
use savant_core::transport::zeromq::WriterResult;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::job_writer::JobWriter;
use crate::store::rocksdb::RocksStore;
use configuration::JobConfiguration;
use stop_condition::JobStopCondition;

use crate::store::Store;

pub mod configuration;
pub mod factory;
pub mod query;
pub mod stop_condition;

const STD_FPS: f64 = 30.0;

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub enum RoutingLabelsUpdateStrategy {
    #[default]
    Bypass,
    Replace(Vec<String>),
    Append(Vec<String>),
}

pub enum SendEither<'a> {
    Message(&'a Message, &'a [&'a [u8]]),
    EOS,
}

pub struct RocksDbJob(Job<RocksStore>);

impl RocksDbJob {
    pub fn new(
        store: Arc<Mutex<RocksStore>>,
        writer: Arc<Mutex<JobWriter>>,
        id: u128,
        position: usize,
        stop_condition: JobStopCondition,
        configuration: JobConfiguration,
        update: Option<VideoFrameUpdate>,
    ) -> Result<Self> {
        Ok(Self(Job::new(
            store,
            writer,
            id,
            position,
            stop_condition,
            configuration,
            update,
        )?))
    }

    pub async fn run_until_complete(&mut self) -> Result<()> {
        self.0.run_until_complete().await
    }

    pub fn json(&self) -> Result<String> {
        self.0.json()
    }
}

#[derive(Serialize)]
pub(crate) struct Job<S: Store> {
    #[serde(skip)]
    store: Arc<Mutex<S>>,
    #[serde(skip)]
    writer: Arc<Mutex<JobWriter>>,
    id: u128,
    stop_condition: JobStopCondition,
    position: usize,
    configuration: JobConfiguration,
    last_pts: Option<i64>,
    update: Option<VideoFrameUpdate>,
}

impl<S> Debug for Job<S>
where
    S: Store,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id)
            .field("stop_condition", &self.stop_condition)
            .field("position", &self.position)
            .field("configuration", &self.configuration)
            .field("update", &self.update)
            .finish()
    }
}

impl<S> Job<S>
where
    S: Store,
{
    pub fn json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(Into::into)
    }

    pub fn new(
        store: Arc<Mutex<S>>,
        writer: Arc<Mutex<JobWriter>>,
        id: u128,
        position: usize,
        stop_condition: JobStopCondition,
        configuration: JobConfiguration,
        update: Option<VideoFrameUpdate>,
    ) -> Result<Self> {
        if configuration.min_duration > configuration.max_duration {
            bail!("Min PTS delta is greater than max PTS delta!");
        }
        if configuration.stored_source_id.is_empty() || configuration.resulting_source_id.is_empty()
        {
            bail!("Stored source id or resulting source id is empty!");
        }

        Ok(Self {
            store,
            writer,
            id,
            position,
            stop_condition,
            configuration,
            last_pts: None,
            update,
        })
    }

    async fn read_message(&self) -> Result<(Message, Vec<Vec<u8>>)> {
        let now = Instant::now();
        loop {
            let message = self
                .store
                .lock()
                .await
                .get_message(&self.configuration.stored_source_id, self.position)
                .await?;
            if now.elapsed() > self.configuration.max_idle_duration {
                let log_message = format!(
                    "No message received during the configured {} idle time (ms). Job Id: {} will be finished!",
                    self.configuration.max_idle_duration.as_millis(),
                    self.id
                );
                log::warn!(target: "replay::db::job::read_message", "{}", &log_message);
                bail!("{}", log_message);
            }
            match message {
                Some((m, _, data)) => {
                    return Ok((m, data));
                }
                None => {
                    tokio_timerfd::sleep(Duration::from_millis(1)).await?;
                }
            }
        }
    }

    pub(self) fn prepare_message(&self, m: Message) -> Result<Option<Message>> {
        let message = if m.is_end_of_stream() {
            if self.configuration.skip_intermediary_eos {
                None
            } else {
                let mut eos = m.as_end_of_stream().unwrap().clone();
                eos.source_id
                    .clone_from(&self.configuration.resulting_source_id);
                Some(Message::end_of_stream(eos))
            }
        } else if m.is_video_frame() {
            let mut f = m.as_video_frame().unwrap().clone();
            f.set_source_id(&self.configuration.resulting_source_id);
            if let Some(u) = &self.update {
                f.update(u)?;
            }
            Some(f.to_message())
        } else {
            None
        };

        if message.is_none() {
            return Ok(None);
        }

        let mut message = message.unwrap();

        match &self.configuration.routing_labels {
            RoutingLabelsUpdateStrategy::Bypass => {}
            RoutingLabelsUpdateStrategy::Replace(labels) => {
                message.meta_mut().routing_labels.clone_from(labels);
            }
            RoutingLabelsUpdateStrategy::Append(labels) => {
                message.meta_mut().routing_labels.extend(labels.clone());
            }
        }

        Ok(Some(message))
    }

    fn check_pts_decrease(&mut self, message: &Message) -> Result<bool> {
        if !message.is_video_frame() {
            return Ok(false);
        }
        let message = message.as_video_frame().unwrap();
        if self.last_pts.is_none() {
            self.last_pts = Some(message.get_pts());
            return Ok(false);
        }
        let pts = message.get_pts();
        let last_pts = self.last_pts.unwrap();
        self.last_pts = Some(pts);

        if pts < last_pts {
            let message = format!(
                "PTS discrepancy detected in job {}: {} < {}!",
                self.id, pts, last_pts
            );
            log::warn!(target: "replay::db::job::handle_discrepant_pts", "{}", &message);
            if self.configuration.stop_on_incorrect_pts {
                log::warn!("Job will be finished due to a discrepant pts!");
                bail!("{}", message);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn send_either(&self, one_of: SendEither<'_>) -> Result<()> {
        let now = Instant::now();
        loop {
            let send_res = match one_of {
                SendEither::Message(m, data) => self.writer.lock().await.send_message(
                    &self.configuration.resulting_source_id,
                    m,
                    if self.configuration.send_metadata_only {
                        &[]
                    } else {
                        data
                    },
                )?,
                SendEither::EOS => self
                    .writer
                    .lock()
                    .await
                    .send_eos(&self.configuration.resulting_source_id)?,
            };
            loop {
                if now.elapsed() > self.configuration.max_delivery_duration {
                    let message = format!(
                        "Message delivery timeout occurred in job {}. Job will be finished!",
                        self.id
                    );
                    log::warn!(target: "replay::db::job::send_either", "{}", &message);
                    bail!("{}", message);
                }

                let res = send_res.try_get()?;
                if res.is_none() {
                    tokio_timerfd::sleep(Duration::from_micros(50)).await?;
                    continue;
                }
                let res = res.unwrap()?;
                match res {
                    WriterResult::SendTimeout => {
                        log::warn!("Send timeout occurred in job {}, retrying ...", self.id);
                        break;
                    }
                    WriterResult::AckTimeout(t) => {
                        let message = format!(
                            "Ack timeout ({}) occurred in job {}, retrying ...",
                            t, self.id
                        );
                        log::warn!(target: "replay::db::job::send_either", "{}", &message);
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

    pub async fn run_until_complete(&mut self) -> Result<()> {
        if self.configuration.pts_sync {
            self.run_pts_synchronized_until_complete().await?;
        } else {
            self.run_fast_until_complete().await?;
        }

        if self.configuration.send_eos {
            self.send_either(SendEither::EOS).await?;
        }
        Ok(())
    }

    async fn run_fast_until_complete(&mut self) -> Result<()> {
        loop {
            let (m, data) = self.read_message().await?;
            let m = self.prepare_message(m)?;
            if m.is_none() {
                continue;
            }
            let m = m.unwrap();
            self.check_pts_decrease(&m)?;

            let sliced_data = data.iter().map(|d| d.as_slice()).collect::<Vec<_>>();
            self.send_either(SendEither::Message(&m, &sliced_data))
                .await?;

            self.position += 1;
            if self.stop_condition.check(&m)? {
                log::info!("Job Id: {} has been finished by stop condition!", self.id);
                break;
            }
        }
        Ok(())
    }

    async fn run_pts_synchronized_until_complete(&mut self) -> Result<()> {
        let mut last_video_frame_sent = Instant::now();
        let (prev_message, data) = self.read_message().await?;
        if !prev_message.is_video_frame() {
            let message = format!(
                "First message in job {} is not a video frame, job will be finished!",
                self.id
            );
            log::warn!(target: "replay::db::job", "{}", &message);
            bail!("{}", message);
        }

        let mut prev_frame_message = Some(prev_message);
        let mut first_run_data = Some(data);
        let mut loop_time = Instant::now();
        let mut last_skew = Duration::from_secs(0);
        loop {
            log::debug!(target: "replay::db::job", "Loop time: {:?}", loop_time.elapsed());
            loop_time = Instant::now();
            let (message, data) = if first_run_data.is_none() {
                self.read_message().await?
            } else {
                (
                    prev_frame_message.as_ref().unwrap().clone(),
                    first_run_data.take().unwrap(),
                )
            };

            let message = self.prepare_message(message)?;
            if message.is_none() {
                continue;
            }
            // calculate pause
            let message = message.unwrap();

            let delay = if !message.is_video_frame() {
                Duration::from_secs(0)
            } else {
                let videoframe = message.as_video_frame().unwrap();
                let prev_video_frame = prev_frame_message
                    .as_ref()
                    .unwrap()
                    .as_video_frame()
                    .unwrap();
                if self.check_pts_decrease(&message)? {
                    self.configuration.pts_discrepancy_fix_duration
                } else {
                    let pts_diff = videoframe.get_pts() - prev_video_frame.get_pts();
                    let pts_diff = pts_diff.max(0) as f64;
                    let (time_base_num, time_base_den) = videoframe.get_time_base();
                    let pts_diff = pts_diff * time_base_num as f64 / time_base_den as f64;
                    Duration::from_secs_f64(pts_diff)
                }
            };

            if message.is_video_frame() {
                let delay = if delay > self.configuration.max_duration {
                    let message = format!(
                        "PTS discrepancy delay is greater than the configured max delay in job {}. The job will use configured delay for the next frame!",
                        self.id
                    );
                    log::debug!(target: "replay::db::job", "{}", &message);
                    self.configuration.max_duration
                } else if delay < self.configuration.min_duration {
                    let message = format!(
                        "PTS discrepancy delay is less than the configured min delay in job {}. The job will use configured delay for the next frame!",
                        self.id
                    );
                    log::debug!(target: "replay::db::job", "{}", &message);
                    self.configuration.min_duration
                } else {
                    delay
                };
                let corrected_delay = delay
                    .checked_sub(last_video_frame_sent.elapsed())
                    .unwrap_or_else(|| Duration::from_secs(0));

                let corrected_delay = corrected_delay
                    .checked_sub(last_skew)
                    .unwrap_or_else(|| Duration::from_secs(0));

                //dbg!(corrected_delay);
                log::debug!(target: "replay::db::job", "Corrected delay: {:?}", corrected_delay);
                let skew = Instant::now();
                tokio_timerfd::sleep(corrected_delay).await?;
                last_skew = skew
                    .elapsed()
                    .checked_sub(corrected_delay)
                    .unwrap_or_else(|| Duration::from_secs(0));
                log::debug!(target: "replay::db::job", "Last timer skew: {:?}", last_skew);
                last_video_frame_sent = Instant::now();
            }

            let sliced_data = data.iter().map(|d| d.as_slice()).collect::<Vec<_>>();

            //let send_measurement = Instant::now();
            self.send_either(SendEither::Message(&message, &sliced_data))
                .await?;
            //dbg!(send_measurement.elapsed());

            self.position += 1;

            if self.stop_condition.check(&message)? {
                log::info!("Job Id: {} has been finished by stop condition!", self.id);
                break;
            }

            if message.is_video_frame() {
                prev_frame_message = Some(message);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;
    use savant_core::message::Message;
    use savant_core::primitives::eos::EndOfStream;
    use savant_core::primitives::frame_update::{AttributeUpdatePolicy, VideoFrameUpdate};
    use savant_core::primitives::{Attribute, WithAttributes};
    use savant_core::transport::zeromq::{
        NonBlockingReader, NonBlockingWriter, ReaderConfig, ReaderResult, WriterConfig,
    };
    use savant_core::utils::uuid_v7::incremental_uuid_v7;
    use tokio::sync::Mutex;
    use tokio_timerfd::sleep;
    use uuid::Uuid;

    use crate::job::configuration::JobConfigurationBuilder;
    use crate::job::stop_condition::JobStopCondition;
    use crate::job::{Job, RoutingLabelsUpdateStrategy, SendEither};
    use crate::job_writer::JobWriter;
    use crate::store::{gen_properly_filled_frame, JobOffset, Store};

    struct MockStore {
        pub messages: Vec<(Option<(Message, Vec<u8>, Vec<Vec<u8>>)>, Duration)>,
    }

    impl Store for MockStore {
        async fn add_message(
            &mut self,
            _message: &Message,
            _topic: &[u8],
            _data: &[Vec<u8>],
        ) -> Result<usize> {
            unreachable!("MockStore::add_message")
        }

        async fn get_message(
            &mut self,
            _source_id: &str,
            _id: usize,
        ) -> Result<Option<(Message, Vec<u8>, Vec<Vec<u8>>)>> {
            let (m, d) = self.messages.remove(0);
            tokio_timerfd::sleep(d).await?;
            //dbg!("get_message", self.messages.len(), systime_ms());
            Ok(m)
        }

        async fn get_first(
            &mut self,
            _source_id: &str,
            _keyframe_uuid: Uuid,
            _before: JobOffset,
        ) -> Result<Option<usize>> {
            unreachable!("MockStore::get_first")
        }
    }

    #[test]
    fn test_configuration_builder() -> Result<()> {
        let _ = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .build_and_validate()?;
        Ok(())
    }

    async fn get_channel() -> Result<(NonBlockingReader, Arc<Mutex<JobWriter>>)> {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap();
        let mut reader = NonBlockingReader::new(
            &ReaderConfig::new()
                .url(&format!("router+bind:ipc://{}/in", path))?
                .with_fix_ipc_permissions(Some(0o777))?
                .with_receive_timeout(1000)?
                .build()?,
            500,
        )?;
        reader.start()?;
        sleep(Duration::from_millis(100)).await?;
        let mut writer = NonBlockingWriter::new(
            &WriterConfig::new()
                .url(&format!("dealer+connect:ipc://{}/in", path))?
                .build()?,
            500,
        )?;
        writer.start()?;
        sleep(Duration::from_millis(100)).await?;

        Ok((reader, Arc::new(Mutex::new(writer.into()))))
    }

    async fn shutdown_channel(mut r: NonBlockingReader, w: Mutex<JobWriter>) -> Result<()> {
        if !r.is_shutdown() {
            r.shutdown()?;
        }

        let m = w.lock().await;
        drop(m);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_message() -> Result<()> {
        let (r, w) = get_channel().await?;

        let store = MockStore {
            messages: vec![
                (
                    Some((gen_properly_filled_frame().to_message(), vec![], vec![])),
                    Duration::from_millis(10),
                ),
                (
                    Some((gen_properly_filled_frame().to_message(), vec![], vec![])),
                    Duration::from_millis(100),
                ),
                (None, Duration::from_millis(1)),
            ],
        };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .build_and_validate()?;
        let job = Job::new(
            store,
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            None,
        )?;
        let m = job.read_message().await?;
        assert_eq!(m.0.is_video_frame(), true);
        let m = job.read_message().await;
        assert!(m.is_err());

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_read_no_data() -> Result<()> {
        let (r, w) = get_channel().await?;

        let store = MockStore {
            messages: vec![
                (None, Duration::from_millis(10)),
                (None, Duration::from_millis(10)),
                (
                    Some((gen_properly_filled_frame().to_message(), vec![], vec![])),
                    Duration::from_millis(10),
                ),
                (None, Duration::from_millis(1)),
            ],
        };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .build_and_validate()?;

        let job = Job::new(
            store,
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            None,
        )?;
        let now = tokio::time::Instant::now();
        let m = job.read_message().await?;
        assert_eq!(m.0.is_video_frame(), true);
        assert!(now.elapsed() > Duration::from_millis(30));

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_prepare_message() -> Result<()> {
        let (r, w) = get_channel().await?;

        let store = MockStore { messages: vec![] };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .routing_labels(RoutingLabelsUpdateStrategy::Replace(vec![
                "label-1".to_string(),
                "label-2".to_string(),
            ]))
            .build_and_validate()?;

        let job = Job::new(
            store.clone(),
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            None,
        )?;

        let m = job.prepare_message(gen_properly_filled_frame().to_message())?;
        assert!(m.is_some());
        let m = m.unwrap();
        assert_eq!(
            m.get_labels(),
            vec!["label-1".to_string(), "label-2".to_string()]
        );
        let m = m.as_video_frame().unwrap();
        assert_eq!(m.get_source_id(), "resulting_id".to_string());
        let m = job.prepare_message(Message::end_of_stream(EndOfStream::new(
            "source_id".to_string(),
        )))?;
        assert!(m.is_some());
        let m = m.unwrap();
        assert_eq!(
            m.get_labels(),
            vec!["label-1".to_string(), "label-2".to_string()]
        );
        let eos = m.as_end_of_stream().unwrap();
        assert_eq!(eos.source_id, "resulting_id".to_string());

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_prepare_message_skip_intermediary_eos() -> Result<()> {
        let (r, w) = get_channel().await?;

        let store = MockStore { messages: vec![] };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .skip_intermediary_eos(true)
            .build_and_validate()?;

        let job = Job::new(
            store.clone(),
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            None,
        )?;

        let m = job.prepare_message(gen_properly_filled_frame().to_message())?;
        assert!(m.is_some());
        let m = m.unwrap().as_video_frame().unwrap();
        assert_eq!(m.get_source_id(), "resulting_id".to_string());

        let m = job.prepare_message(Message::end_of_stream(EndOfStream::new(
            "source_id".to_string(),
        )))?;
        assert!(m.is_none());

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_check_discrepant_pts() -> Result<()> {
        let (r, w) = get_channel().await?;

        let store = MockStore { messages: vec![] };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .build_and_validate()?;

        let mut job = Job::new(
            store.clone(),
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            None,
        )?;

        let eos = Message::end_of_stream(EndOfStream::new("source_id".to_string()));
        let res = job.check_pts_decrease(&eos)?;
        assert_eq!(res, false);

        let first = gen_properly_filled_frame().to_message();

        tokio_timerfd::sleep(Duration::from_millis(1)).await?;
        let second = gen_properly_filled_frame().to_message();

        tokio_timerfd::sleep(Duration::from_millis(1)).await?;
        let third = gen_properly_filled_frame().to_message();

        let res = job.check_pts_decrease(&first)?;
        assert_eq!(res, false);

        let res = job.check_pts_decrease(&third)?;
        assert_eq!(res, false);

        let res = job.check_pts_decrease(&second)?;
        assert_eq!(res, true);
        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_check_discrepant_pts_stop_when_incorrect() -> Result<()> {
        let (r, w) = get_channel().await?;

        let store = MockStore { messages: vec![] };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .stop_on_incorrect_pts(true)
            .build_and_validate()?;

        let mut job = Job::new(
            store.clone(),
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            None,
        )?;

        let first = gen_properly_filled_frame().to_message();

        tokio_timerfd::sleep(Duration::from_millis(1)).await?;
        let second = gen_properly_filled_frame().to_message();

        let res = job.check_pts_decrease(&second)?;
        assert_eq!(res, false);

        let res = job.check_pts_decrease(&first);
        assert!(res.is_err());
        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_send_either() -> Result<()> {
        let (r, w) = get_channel().await?;

        let store = MockStore { messages: vec![] };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .build_and_validate()?;

        let job = Job::new(
            store.clone(),
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            None,
        )?;
        job.send_either(SendEither::EOS).await?;
        let res = r.receive()?;
        assert!(matches!(
            res,
            ReaderResult::Message {
                message,
                topic,
                routing_id: _,
                data: _
            } if message.is_end_of_stream() && topic == b"resulting_id"
        ));

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_send_either_timeout() -> Result<()> {
        let (mut r, w) = get_channel().await?;

        let store = MockStore { messages: vec![] };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .stop_on_incorrect_pts(true)
            .max_delivery_duration(Duration::from_millis(300))
            .build_and_validate()?;

        let job = Job::new(
            store.clone(),
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            None,
        )?;
        r.shutdown()?;
        let res = job.send_either(SendEither::EOS).await;
        assert!(res.is_err());

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_run_fast_until_complete() -> Result<()> {
        let (r, w) = get_channel().await?;

        let frames = vec![
            gen_properly_filled_frame(),
            gen_properly_filled_frame(),
            gen_properly_filled_frame(),
        ];

        let store = MockStore {
            messages: frames
                .iter()
                .map(|f| {
                    (
                        Some((f.to_message(), vec![], vec![])),
                        Duration::from_millis(10),
                    )
                })
                .collect(),
        };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .build_and_validate()?;

        let mut job = Job::new(
            store,
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(frames.last().as_ref().unwrap().get_uuid_u128()),
            job_conf,
            None,
        )?;
        job.run_fast_until_complete().await?;
        for f in frames {
            let res = r.receive()?;
            assert!(matches!(
                res,
                ReaderResult::Message {
                    message,
                    topic,
                    routing_id: _,
                    data: _
                } if message.is_video_frame() && topic == b"resulting_id" && message.as_video_frame().unwrap().get_uuid_u128() == f.get_uuid_u128()
            ));
        }

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;

        Ok(())
    }

    async fn basic_sync_delivery(id: u128) -> Result<(NonBlockingReader, Mutex<JobWriter>)> {
        let (r, w) = get_channel().await?;
        let mut frames = vec![];
        let n = 20;
        for _ in 0..n {
            let f = gen_properly_filled_frame();
            frames.push(f);
            tokio_timerfd::sleep(Duration::from_millis(30)).await?;
        }
        //dbg!("frames filled", thread_now.elapsed(),);
        let store = MockStore {
            messages: frames
                .iter()
                .map(|f| {
                    (
                        Some((f.to_message(), vec![], vec![])),
                        Duration::from_millis(0),
                    )
                })
                .collect(),
        };
        let store = Arc::new(Mutex::new(store));
        //dbg!("store created", thread_now.elapsed());
        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .build_and_validate()?;

        let mut job = Job::new(
            store,
            w.clone(),
            id,
            0,
            JobStopCondition::last_frame(frames.last().as_ref().unwrap().get_uuid_u128()),
            job_conf,
            None,
        )?;
        let now = tokio::time::Instant::now();
        job.run_pts_synchronized_until_complete().await?;
        assert!(
            now.elapsed() > Duration::from_millis(n * 33)
                && now.elapsed() < Duration::from_millis((n + 1) * 33)
        );
        for f in frames {
            let res = r.receive()?;
            assert!(matches!(
                res,
                ReaderResult::Message {
                    message,
                    topic,
                    routing_id: _,
                    data: _
                } if message.is_video_frame() && topic == b"resulting_id" && message.as_video_frame().unwrap().get_uuid_u128() == f.get_uuid_u128()
            ));
        }

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;

        Ok((r, w))
    }

    #[tokio::test]
    async fn test_run_sync_basic_until_complete() -> Result<()> {
        let (r, w) = basic_sync_delivery(0).await?;
        shutdown_channel(r, w).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_run_sync_basic_until_complete_with_eos() -> Result<()> {
        let (r, w) = get_channel().await?;

        let last_uuid: u128;
        let store = MockStore {
            messages: vec![
                (
                    Some((gen_properly_filled_frame().to_message(), vec![], vec![])),
                    Duration::from_millis(0),
                ),
                (
                    Some((
                        Message::end_of_stream(EndOfStream::new("source_id".to_string())),
                        vec![],
                        vec![],
                    )),
                    Duration::from_millis(0),
                ),
                (
                    Some((gen_properly_filled_frame().to_message(), vec![], vec![])),
                    Duration::from_millis(0),
                ),
                (
                    Some((
                        Message::end_of_stream(EndOfStream::new("source_id".to_string())),
                        vec![],
                        vec![],
                    )),
                    Duration::from_millis(0),
                ),
                (
                    Some((
                        {
                            let f = gen_properly_filled_frame();
                            last_uuid = f.get_uuid_u128();
                            f
                        }
                        .to_message(),
                        vec![],
                        vec![],
                    )),
                    Duration::from_millis(0),
                ),
            ],
        };
        let message_count = store
            .messages
            .iter()
            .filter(|(m, _)| {
                m.as_ref()
                    .map(|(m, _, _)| m.is_video_frame())
                    .unwrap_or(false)
            })
            .count() as u64;

        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .skip_intermediary_eos(false)
            .build_and_validate()?;

        let mut job = Job::new(
            store,
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(last_uuid),
            job_conf,
            None,
        )?;

        let now = tokio::time::Instant::now();
        job.run_pts_synchronized_until_complete().await?;
        assert!(
            now.elapsed() > Duration::from_millis((message_count - 1) * 33)
                && now.elapsed() < Duration::from_millis((message_count + 1) * 33)
        );

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_run_sync_basic_until_complete_min_duration() -> Result<()> {
        let (r, w) = get_channel().await?;

        let mut frames = vec![];

        let n = 20;
        for _ in 0..n {
            let f = gen_properly_filled_frame();
            frames.push(f);
            tokio_timerfd::sleep(Duration::from_millis(10)).await?;
        }

        let store = MockStore {
            messages: frames
                .iter()
                .map(|f| {
                    (
                        Some((f.to_message(), vec![], vec![])),
                        Duration::from_millis(0),
                    )
                })
                .collect(),
        };
        let store = Arc::new(Mutex::new(store));
        let frame_duration = 20;

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .min_duration(Duration::from_millis(frame_duration))
            .build_and_validate()?;

        let mut job = Job::new(
            store,
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(frames.last().as_ref().unwrap().get_uuid_u128()),
            job_conf,
            None,
        )?;
        let now = tokio::time::Instant::now();
        job.run_pts_synchronized_until_complete().await?;
        //dbg!(now.elapsed());
        assert!(
            now.elapsed() > Duration::from_millis(n * frame_duration)
                && now.elapsed() < Duration::from_millis((n + 1) * frame_duration)
        );
        for f in frames {
            let res = r.receive()?;
            assert!(matches!(
                res,
                ReaderResult::Message {
                    message,
                    topic,
                    routing_id: _,
                    data: _
                } if message.is_video_frame() && topic == b"resulting_id" && message.as_video_frame().unwrap().get_uuid_u128() == f.get_uuid_u128()
            ));
        }

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_run_sync_basic_until_complete_max_duration() -> Result<()> {
        let (r, w) = get_channel().await?;

        let mut frames = vec![];

        let n = 20;
        for _ in 0..n {
            let f = gen_properly_filled_frame();
            frames.push(f);
            tokio_timerfd::sleep(Duration::from_millis(30)).await?;
        }

        let store = MockStore {
            messages: frames
                .iter()
                .map(|f| {
                    (
                        Some((f.to_message(), vec![], vec![])),
                        Duration::from_millis(0),
                    )
                })
                .collect(),
        };
        let store = Arc::new(Mutex::new(store));
        let frame_duration = 20;

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .min_duration(Duration::from_millis(1))
            .max_duration(Duration::from_millis(frame_duration))
            .build_and_validate()?;

        let mut job = Job::new(
            store,
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(frames.last().as_ref().unwrap().get_uuid_u128()),
            job_conf,
            None,
        )?;
        let now = tokio::time::Instant::now();
        job.run_pts_synchronized_until_complete().await?;
        //dbg!(now.elapsed());
        // the first frame is sent instantly, that is why we have n - 1 here
        assert!(
            now.elapsed() > Duration::from_millis((n - 1) * frame_duration)
                && now.elapsed() < Duration::from_millis(n * frame_duration)
        );
        for f in frames {
            let res = r.receive()?;
            assert!(matches!(
                res,
                ReaderResult::Message {
                    message,
                    topic,
                    routing_id: _,
                    data: _
                } if message.is_video_frame() && topic == b"resulting_id" && message.as_video_frame().unwrap().get_uuid_u128() == f.get_uuid_u128()
            ));
        }

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;

        Ok(())
    }

    #[tokio::test]
    async fn concurrent_jobs() -> Result<()> {
        let t = tokio::spawn(basic_sync_delivery(0));
        let t2 = tokio::spawn(basic_sync_delivery(1));
        sleep(Duration::from_secs(1)).await?;
        let (r1, w1) = t.await??;
        let (r2, w2) = t2.await??;
        shutdown_channel(r1, w1).await?;
        shutdown_channel(r2, w2).await?;
        Ok(())
    }

    #[tokio::test]
    async fn job_with_update() -> Result<()> {
        let (r, w) = get_channel().await?;

        let frames = vec![
            gen_properly_filled_frame(),
            gen_properly_filled_frame(),
            gen_properly_filled_frame(),
        ];

        let store = MockStore {
            messages: frames
                .iter()
                .map(|f| {
                    (
                        Some((f.to_message(), vec![], vec![])),
                        Duration::from_millis(10),
                    )
                })
                .collect(),
        };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .build_and_validate()?;

        let mut u = VideoFrameUpdate::default();
        u.add_frame_attribute(Attribute::persistent("new", "value", vec![], &None, false));
        u.set_frame_attribute_policy(AttributeUpdatePolicy::Error);

        let mut job = Job::new(
            store,
            w.clone(),
            0,
            0,
            JobStopCondition::last_frame(frames.last().as_ref().unwrap().get_uuid_u128()),
            job_conf,
            Some(u),
        )?;
        job.run_fast_until_complete().await?;
        for f in frames {
            let res = r.receive()?;
            assert!(matches!(
                res,
                ReaderResult::Message {
                    message,
                    topic,
                    routing_id: _,
                    data: _
                } if message.is_video_frame() && topic == b"resulting_id" && {
                    let m = message.as_video_frame().unwrap();
                    m.get_uuid_u128() == f.get_uuid_u128() && m.contains_attribute("new", "value")
                }
            ));
        }

        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_json() -> Result<()> {
        let (r, w) = get_channel().await?;

        let store = MockStore { messages: vec![] };
        let store = Arc::new(Mutex::new(store));

        let job_conf = JobConfigurationBuilder::default()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .max_idle_duration(Duration::from_millis(50))
            .build_and_validate()?;

        let mut u = VideoFrameUpdate::default();
        u.add_frame_attribute(Attribute::persistent("new", "value", vec![], &None, false));
        u.set_frame_attribute_policy(AttributeUpdatePolicy::Error);

        let job = Job::new(
            store.clone(),
            w.clone(),
            incremental_uuid_v7().as_u128(),
            0,
            JobStopCondition::last_frame(incremental_uuid_v7().as_u128()),
            job_conf,
            Some(u),
        )?;
        let _ = job.json()?;
        drop(job);
        let w = Arc::try_unwrap(w).or(Err(anyhow::anyhow!("Arc unwrapping failed")))?;
        shutdown_channel(r, w).await?;

        Ok(())
    }
}
