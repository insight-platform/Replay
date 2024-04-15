use crate::store::rocksdb::RocksStore;
use crate::store::Store;
use anyhow::{bail, Result};
use derive_builder::Builder;
use parking_lot::Mutex;
use savant_core::message::Message;
use savant_core::transport::zeromq::{NonBlockingWriter, WriterResult};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobStopCondition {
    LastKeyFrame(u128),
    FrameCount(usize),
    KeyFrameCount(usize),
    PTSDelta(f64),
    RealTimeDelta(f64),
}

#[derive(Debug, Serialize, Deserialize, Clone, Builder)]
pub struct JobConfiguration {
    #[builder(default)]
    pts_sync: bool,
    #[builder(default)]
    skip_intermediary_eos: bool,
    #[builder(default)]
    send_eos: bool,
    #[builder(default)]
    pts_discrepancy_fix_duration: Duration,
    #[builder(default)]
    min_duration: Duration,
    #[builder(default)]
    max_duration: Duration,
    #[builder(default)]
    stored_source_id: String,
    #[builder(default)]
    resulting_source_id: String,
    #[builder(default)]
    routing_labels: RoutingLabelsUpdateStrategy,
    #[builder(default)]
    max_idle_duration: Duration,
    #[builder(default)]
    max_delivery_duration: Duration,
}

impl JobConfigurationBuilder {
    pub fn build_and_validate(&mut self) -> Result<JobConfiguration> {
        let c = self.build()?;
        if c.min_duration > c.max_duration {
            bail!("Min PTS delta is greater than max PTS delta!");
        }
        if c.stored_source_id.is_empty() || c.resulting_source_id.is_empty() {
            bail!("Stored source id or resulting source id is empty!");
        }
        Ok(c)
    }
}

impl Default for JobConfiguration {
    fn default() -> Self {
        Self {
            pts_sync: false,
            skip_intermediary_eos: false,
            send_eos: false,
            pts_discrepancy_fix_duration: Duration::from_secs_f64(1_f64 / 33_f64),
            min_duration: Duration::from_secs_f64(1_f64 / 33_f64),
            max_duration: Duration::from_secs_f64(1_f64 / 33_f64),
            stored_source_id: String::new(),
            resulting_source_id: String::new(),
            routing_labels: RoutingLabelsUpdateStrategy::Bypass,
            max_idle_duration: Duration::from_secs(10),
            max_delivery_duration: Duration::from_secs(10),
        }
    }
}

pub struct JobConditionStopState;

impl JobConditionStopState {
    pub fn check(
        &mut self,
        _position: usize,
        _condition: &JobStopCondition,
        _message: &Message,
    ) -> bool {
        false
    }
}

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
        writer: Arc<NonBlockingWriter>,
        id: u128,
        position: usize,
        stop_condition: JobStopCondition,
        configuration: JobConfiguration,
    ) -> Result<Self> {
        Ok(Self(Job::new(
            store,
            writer,
            id,
            position,
            stop_condition,
            configuration,
        )?))
    }

    pub async fn run_until_complete(&mut self) -> Result<()> {
        self.0.run_until_complete().await
    }
}

#[derive(Serialize)]
pub(crate) struct Job<S: Store> {
    #[serde(skip)]
    store: Arc<Mutex<S>>,
    #[serde(skip)]
    writer: Arc<NonBlockingWriter>,
    id: u128,
    stop_condition: JobStopCondition,
    position: usize,
    configuration: JobConfiguration,
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
            .finish()
    }
}

impl<S> Job<S>
where
    S: Store,
{
    pub fn new(
        store: Arc<Mutex<S>>,
        writer: Arc<NonBlockingWriter>,
        id: u128,
        position: usize,
        stop_condition: JobStopCondition,
        configuration: JobConfiguration,
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
        })
    }

    async fn read_message(&self) -> Result<(Message, Vec<Vec<u8>>)> {
        let now = Instant::now();
        loop {
            let message = self
                .store
                .lock()
                .get_message(&self.configuration.stored_source_id, self.position)
                .await?;
            match message {
                Some((m, _, data)) => {
                    return Ok((m, data));
                }
                None => {
                    if now.elapsed() > self.configuration.max_idle_duration {
                        let log_message = format!(
                                "No message received during the configured {} idle time (seconds). Job Id: {} will be finished!",
                                self.configuration.max_idle_duration.as_secs(),
                                self.id
                            );
                        log::warn!(target: "replay::db::job::read_message", "{}", &log_message);
                        bail!("{}", log_message);
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
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

    fn prepare_message(&self, m: Message) -> Option<Message> {
        let message = if m.is_end_of_stream() {
            if self.configuration.skip_intermediary_eos {
                None
            } else {
                let mut eos = m.as_end_of_stream().unwrap().clone();
                eos.source_id = self.configuration.resulting_source_id.clone();
                Some(Message::end_of_stream(eos))
            }
        } else if m.is_video_frame() {
            let mut f = m.as_video_frame().unwrap().clone();
            f.set_source_id(&self.configuration.resulting_source_id);
            Some(f.to_message())
        } else {
            None
        };

        if message.is_none() {
            return None;
        }
        let mut message = message.unwrap();

        match &self.configuration.routing_labels {
            RoutingLabelsUpdateStrategy::Bypass => {}
            RoutingLabelsUpdateStrategy::Replace(labels) => {
                message.meta_mut().routing_labels = labels.clone();
            }
            RoutingLabelsUpdateStrategy::Append(labels) => {
                message.meta_mut().routing_labels.extend(labels.clone());
            }
        }

        Some(message)
    }

    async fn send_either(&self, one_of: SendEither<'_>) -> Result<()> {
        let now = Instant::now();
        loop {
            let res = match one_of {
                SendEither::Message(m, data) => {
                    self.writer
                        .send_message(&self.configuration.resulting_source_id, m, data)?
                }
                SendEither::EOS => self
                    .writer
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

                let res = res.try_get()?;
                if res.is_none() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    break;
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

    async fn run_fast_until_complete(&mut self) -> Result<()> {
        let mut stop_state = JobConditionStopState;
        loop {
            let (m, data) = self.read_message().await?;
            let m = self.prepare_message(m);
            if m.is_none() {
                continue;
            }
            let m = m.unwrap();
            let sliced_data = data.iter().map(|d| d.as_slice()).collect::<Vec<_>>();
            self.send_either(SendEither::Message(&m, &sliced_data))
                .await?;

            self.position += 1;
            if stop_state.check(self.position, &self.stop_condition, &m) {
                log::info!("Job Id: {} has been finished by stop condition!", self.id);
                break;
            }
        }
        Ok(())
    }

    async fn run_pts_synchronized_until_complete(&mut self) -> Result<()> {
        let mut stop_state = JobConditionStopState;
        let mut last_sent = Instant::now();
        let (prev_message, data) = self.read_message().await?;
        if !prev_message.is_video_frame() {
            let message = format!(
                "First message in job {} is not a video frame, job will be finished!",
                self.id
            );
            log::warn!(target: "replay::db::job", "{}", &message);
            bail!("{}", message);
        }

        let mut prev_message = Some(prev_message);
        let mut first_run_data = Some(data);
        let mut time_spent = Duration::from_secs(0);
        loop {
            // avoid double read for the first iteration
            let (message, data) = if first_run_data.is_none() {
                self.read_message().await?
            } else {
                (
                    prev_message.as_ref().unwrap().clone(),
                    first_run_data.take().unwrap(),
                )
            };

            let message = self.prepare_message(message);
            if message.is_none() {
                continue;
            }
            // calculate pause
            let message = message.unwrap();

            let mut delay = if !message.is_video_frame() {
                Duration::from_secs(0)
            } else {
                let videoframe = message.as_video_frame().unwrap();
                let prev_video_frame = prev_message.as_ref().unwrap().as_video_frame().unwrap();
                if prev_video_frame.get_pts() > videoframe.get_pts() {
                    let message = format!(
                        "PTS discrepancy detected in job {}. The job will use configured delay for the next frame!",
                        self.id
                    );
                    log::warn!(target: "replay::db::job", "{}", &message);
                    self.configuration.pts_discrepancy_fix_duration
                } else {
                    let pts_diff = videoframe.get_pts() - prev_video_frame.get_pts();
                    let pts_diff = pts_diff.max(0) as f64;
                    let (time_base_num, time_base_den) = videoframe.get_time_base();
                    let pts_diff = pts_diff * time_base_num as f64 / time_base_den as f64;
                    Duration::from_secs_f64(pts_diff)
                }
            };

            if delay > self.configuration.max_duration {
                let message = format!(
                    "PTS discrepancy delay is greater than the configured max delay in job {}. The job will use configured delay for the next frame!",
                    self.id
                );
                log::debug!(target: "replay::db::job", "{}", &message);
                delay = self.configuration.max_duration;
            }

            if delay < self.configuration.min_duration {
                let message = format!(
                    "PTS discrepancy delay is less than the configured min delay in job {}. The job will use configured delay for the next frame!",
                    self.id
                );
                log::debug!(target: "replay::db::job", "{}", &message);
                delay = self.configuration.min_duration;
            }

            let delay = delay
                .checked_sub(time_spent)
                .unwrap_or_else(|| Duration::from_secs(0));

            tokio::time::sleep(delay).await;

            let sliced_data = data.iter().map(|d| d.as_slice()).collect::<Vec<_>>();
            self.send_either(SendEither::Message(&message, &sliced_data))
                .await?;

            if message.is_video_frame() {
                time_spent = last_sent.elapsed();
                last_sent = Instant::now();
            }
            self.position += 1;

            if stop_state.check(self.position, &self.stop_condition, &message) {
                log::info!("Job Id: {} has been finished by stop condition!", self.id);
                break;
            }

            prev_message = Some(message);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::job::{JobConfigurationBuilder, RoutingLabelsUpdateStrategy};
    use anyhow::Result;

    #[test]
    fn test_configuration_builder() -> Result<()> {
        let c = JobConfigurationBuilder::create_empty()
            .routing_labels(RoutingLabelsUpdateStrategy::Bypass)
            .stored_source_id("source_id".to_string())
            .resulting_source_id("resulting_id".to_string())
            .build_and_validate()?;
        dbg!(c);
        Ok(())
    }
}
