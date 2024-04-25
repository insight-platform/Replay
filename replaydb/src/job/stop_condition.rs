use anyhow::Result;
use savant_core::message::Message;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobStopCondition {
    LastFrame {
        uuid: String,
        #[serde(skip)]
        uuid_u128: Option<u128>,
    },
    FrameCount(usize),
    KeyFrameCount(usize),
    PTSDeltaSec {
        max_delta_sec: f64,
        #[serde(skip)]
        first_pts: Option<i64>,
    },
    RealTimeDelta {
        configured_delta_ms: u64,
        #[serde(skip)]
        initial_ts: Option<Instant>,
    },
}

impl JobStopCondition {
    pub fn last_frame(uuid: u128) -> Self {
        JobStopCondition::LastFrame {
            uuid: Uuid::from_u128(uuid).to_string(),
            uuid_u128: Some(uuid),
        }
    }

    pub fn frame_count(count: usize) -> Self {
        JobStopCondition::FrameCount(count)
    }

    pub fn key_frame_count(count: usize) -> Self {
        JobStopCondition::KeyFrameCount(count)
    }

    pub fn pts_delta_sec(max_delta: f64) -> Self {
        JobStopCondition::PTSDeltaSec {
            max_delta_sec: max_delta,
            first_pts: None,
        }
    }

    pub fn real_time_delta_ms(configured_delta: u64) -> Self {
        JobStopCondition::RealTimeDelta {
            configured_delta_ms: configured_delta,
            initial_ts: Some(Instant::now()),
        }
    }

    pub fn check(&mut self, message: &Message) -> Result<bool> {
        if !message.is_video_frame() {
            return Ok(false);
        }
        let message = message.as_video_frame().unwrap();
        match self {
            JobStopCondition::LastFrame { uuid, uuid_u128 } => {
                if uuid_u128.is_none() {
                    *uuid_u128 = Some(Uuid::parse_str(uuid)?.as_u128());
                }
                Ok(message.get_uuid_u128() >= uuid_u128.unwrap())
            }
            JobStopCondition::FrameCount(fc) => {
                if *fc == 1 {
                    return Ok(true);
                }
                *fc -= 1;
                Ok(false)
            }
            JobStopCondition::KeyFrameCount(kfc) => {
                if let Some(true) = message.get_keyframe() {
                    if *kfc == 1 {
                        return Ok(true);
                    }
                    *kfc -= 1;
                }
                Ok(false)
            }
            JobStopCondition::PTSDeltaSec {
                max_delta_sec,
                first_pts,
            } => {
                if first_pts.is_none() {
                    *first_pts = Some(message.get_pts());
                    return Ok(false);
                }
                let pts = message.get_pts();
                let prev_pts = first_pts.unwrap();
                let pts_delta = pts.saturating_sub(prev_pts);
                let (time_base_n, time_base_d) = message.get_time_base();
                let pts_delta = pts_delta as f64 * time_base_n as f64 / time_base_d as f64;
                if pts_delta > *max_delta_sec {
                    return Ok(true);
                }
                Ok(false)
            }
            JobStopCondition::RealTimeDelta {
                configured_delta_ms,
                initial_ts,
            } => {
                if initial_ts.is_none() {
                    *initial_ts = Some(Instant::now());
                    return Ok(false);
                }
                let elapsed = initial_ts.map(|i| i.elapsed().as_millis()).unwrap();
                if elapsed > *configured_delta_ms as u128 {
                    return Ok(true);
                }
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobStopCondition;
    use crate::store::gen_properly_filled_frame;
    use anyhow::Result;
    use savant_core::utils::uuid_v7::incremental_uuid_v7;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_last_frame_stop_condition() -> Result<()> {
        let frame_before = gen_properly_filled_frame();
        thread::sleep(Duration::from_millis(1));
        let mut stop_condition = JobStopCondition::last_frame(incremental_uuid_v7().as_u128());
        assert!(!stop_condition.check(&frame_before.to_message())?);
        thread::sleep(Duration::from_millis(1));
        let frame_after = gen_properly_filled_frame();
        assert!(stop_condition.check(&frame_after.to_message())?);
        Ok(())
    }

    #[test]
    fn test_frame_count_stop_condition() -> Result<()> {
        let frame = gen_properly_filled_frame();
        let mut stop_condition = JobStopCondition::frame_count(2);
        assert!(!stop_condition.check(&frame.to_message())?);
        assert!(stop_condition.check(&frame.to_message())?);
        Ok(())
    }

    #[test]
    fn test_key_frame_count_stop_condition() -> Result<()> {
        let mut frame = gen_properly_filled_frame();
        frame.set_keyframe(Some(true));
        let mut stop_condition = JobStopCondition::key_frame_count(2);
        assert!(!stop_condition.check(&frame.to_message())?);
        frame.set_keyframe(Some(false));
        assert!(!stop_condition.check(&frame.to_message())?);
        let key_frame = gen_properly_filled_frame();
        assert!(stop_condition.check(&key_frame.to_message())?);
        Ok(())
    }

    #[test]
    fn test_pts_delta_stop_condition() -> Result<()> {
        let mut frame = gen_properly_filled_frame();
        frame.set_time_base((1, 1_000_000));
        frame.set_pts(1_000_000);
        let mut stop_condition = JobStopCondition::pts_delta_sec(1.0);
        assert!(!stop_condition.check(&frame.to_message())?);
        frame.set_pts(1_700_000);
        assert!(!stop_condition.check(&frame.to_message())?);
        frame.set_pts(2_100_000);
        assert!(stop_condition.check(&frame.to_message())?);
        Ok(())
    }

    #[test]
    fn test_real_time_delta_stop_condition() -> Result<()> {
        let frame = gen_properly_filled_frame();
        let mut stop_condition = JobStopCondition::real_time_delta_ms(500);
        assert!(!stop_condition.check(&frame.to_message())?);
        thread::sleep(Duration::from_millis(600));
        assert!(stop_condition.check(&frame.to_message())?);
        Ok(())
    }
}
