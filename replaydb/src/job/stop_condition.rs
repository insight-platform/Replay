use savant_core::message::Message;
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobStopCondition {
    LastFrame(u128),
    FrameCount(usize),
    KeyFrameCount(usize),
    PTSDeltaSec {
        max_delta_sec: f64,
        #[serde(skip)]
        first_pts: Option<i64>,
    },
    RealTimeDelta {
        configured_delta_ms: u128,
        #[serde(skip)]
        initial_ts: Option<Instant>,
    },
}

impl JobStopCondition {
    pub fn last_frame(uuid: u128) -> Self {
        JobStopCondition::LastFrame(uuid)
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

    pub fn real_time_delta_ms(configured_delta: u128) -> Self {
        JobStopCondition::RealTimeDelta {
            configured_delta_ms: configured_delta,
            initial_ts: Some(Instant::now()),
        }
    }

    pub fn check(&mut self, message: &Message) -> bool {
        if !message.is_video_frame() {
            return false;
        }
        let message = message.as_video_frame().unwrap();
        match self {
            JobStopCondition::LastFrame(uuid) => message.get_uuid_u128() >= *uuid,
            JobStopCondition::FrameCount(fc) => {
                if *fc == 1 {
                    return true;
                }
                *fc -= 1;
                false
            }
            JobStopCondition::KeyFrameCount(kfc) => {
                if let Some(true) = message.get_keyframe() {
                    if *kfc == 1 {
                        return true;
                    }
                    *kfc -= 1;
                }
                false
            }
            JobStopCondition::PTSDeltaSec {
                max_delta_sec,
                first_pts,
            } => {
                if first_pts.is_none() {
                    *first_pts = Some(message.get_pts());
                    return false;
                }
                let pts = message.get_pts();
                let prev_pts = first_pts.unwrap();
                let pts_delta = pts.saturating_sub(prev_pts);
                let (time_base_n, time_base_d) = message.get_time_base();
                let pts_delta = pts_delta as f64 * time_base_n as f64 / time_base_d as f64;
                if pts_delta > *max_delta_sec {
                    return true;
                }
                false
            }
            JobStopCondition::RealTimeDelta {
                configured_delta_ms,
                initial_ts,
            } => {
                if initial_ts.is_none() {
                    *initial_ts = Some(Instant::now());
                    return false;
                }
                let elapsed = initial_ts.map(|i| i.elapsed().as_millis()).unwrap();
                if elapsed > *configured_delta_ms {
                    return true;
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobStopCondition;
    use crate::store::gen_properly_filled_frame;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_last_frame_stop_condition() {
        let frame_before = gen_properly_filled_frame();
        thread::sleep(Duration::from_millis(1));
        let mut stop_condition = JobStopCondition::last_frame(uuid::Uuid::now_v7().as_u128());
        assert!(!stop_condition.check(&frame_before.to_message()));
        thread::sleep(Duration::from_millis(1));
        let frame_after = gen_properly_filled_frame();
        assert!(stop_condition.check(&frame_after.to_message()));
    }

    #[test]
    fn test_frame_count_stop_condition() {
        let frame = gen_properly_filled_frame();
        let mut stop_condition = JobStopCondition::frame_count(2);
        assert!(!stop_condition.check(&frame.to_message()));
        assert!(stop_condition.check(&frame.to_message()));
    }

    #[test]
    fn test_key_frame_count_stop_condition() {
        let mut frame = gen_properly_filled_frame();
        frame.set_keyframe(Some(true));
        let mut stop_condition = JobStopCondition::key_frame_count(2);
        assert!(!stop_condition.check(&frame.to_message()));
        frame.set_keyframe(Some(false));
        assert!(!stop_condition.check(&frame.to_message()));
        let key_frame = gen_properly_filled_frame();
        assert!(stop_condition.check(&key_frame.to_message()));
    }

    #[test]
    fn test_pts_delta_stop_condition() {
        let mut frame = gen_properly_filled_frame();
        frame.set_time_base((1, 1_000_000));
        frame.set_pts(1_000_000);
        let mut stop_condition = JobStopCondition::pts_delta_sec(1.0);
        assert!(!stop_condition.check(&frame.to_message()));
        frame.set_pts(1_700_000);
        assert!(!stop_condition.check(&frame.to_message()));
        frame.set_pts(2_100_000);
        assert!(stop_condition.check(&frame.to_message()));
    }

    #[test]
    fn test_real_time_delta_stop_condition() {
        let frame = gen_properly_filled_frame();
        let mut stop_condition = JobStopCondition::real_time_delta_ms(500);
        assert!(!stop_condition.check(&frame.to_message()));
        thread::sleep(Duration::from_millis(600));
        assert!(stop_condition.check(&frame.to_message()));
    }
}
