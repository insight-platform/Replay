pub mod rocksdb;

use anyhow::Result;
use savant_core::message::Message;
use savant_core::primitives::frame::VideoFrameProxy;
use savant_core::test::gen_frame;
use std::time::SystemTime;
use uuid::Uuid;

pub enum BeforeOffset {
    Blocks(usize),
    Seconds(f64),
}

pub trait Store {
    fn add_message(&mut self, message: &Message, topic: &[u8], data: &[Vec<u8>]) -> Result<usize>;

    fn get_message(
        &mut self,
        source_id: &str,
        id: usize,
    ) -> Result<Option<(Message, Vec<u8>, Vec<Vec<u8>>)>>;

    fn get_first(
        &mut self,
        source_id: &str,
        keyframe_uuid: Uuid,
        before: BeforeOffset,
    ) -> Result<Option<usize>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use savant_core::primitives::eos::EndOfStream;

    struct SampleStore {
        keyframes: Vec<(Uuid, usize)>,
        messages: Vec<Message>,
    }

    impl Store for SampleStore {
        fn add_message(
            &mut self,
            message: &Message,
            _topic: &[u8],
            _data: &[Vec<u8>],
        ) -> Result<usize> {
            let current_len = self.messages.len();
            if message.is_video_frame() {
                let f = message.as_video_frame().unwrap();
                if let Some(true) = f.get_keyframe() {
                    self.keyframes.push((f.get_uuid(), current_len));
                }
            }
            self.messages.push(message.clone());
            Ok(current_len)
        }

        fn get_message(
            &mut self,
            _: &str,
            id: usize,
        ) -> Result<Option<(Message, Vec<u8>, Vec<Vec<u8>>)>> {
            Ok(Some((self.messages[id].clone(), vec![], vec![])))
        }

        fn get_first(
            &mut self,
            _: &str,
            keyframe_uuid: Uuid,
            before: BeforeOffset,
        ) -> Result<Option<usize>> {
            let idx = self.keyframes.iter().position(|(u, _)| u == &keyframe_uuid);
            if idx.is_none() {
                return Ok(None);
            }
            let idx = idx.unwrap();

            Ok(Some(match before {
                BeforeOffset::Blocks(blocks_before) => {
                    if idx < blocks_before {
                        self.keyframes[0].1
                    } else {
                        self.keyframes[idx - blocks_before].1
                    }
                }
                BeforeOffset::Seconds(seconds_before) => {
                    let frame = self.messages[self.keyframes[idx].1]
                        .as_video_frame()
                        .unwrap();
                    let current_pts = frame.get_pts() as u64;
                    let time_base = frame.get_time_base();
                    let before_scaled =
                        (seconds_before * time_base.1 as f64 / time_base.0 as f64) as u64;
                    let mut i = self.keyframes[idx].1 - 1;
                    while i > 0 {
                        if self.messages[i].is_video_frame() {
                            let f = self.messages[i].as_video_frame().unwrap();
                            if let Some(true) = f.get_keyframe() {
                                let pts = f.get_pts() as u64;
                                if current_pts - pts > before_scaled {
                                    break;
                                }
                            }
                        }
                        i -= 1;
                    }
                    i
                }
            }))
        }
    }

    #[test]
    fn test_sample_store() -> Result<()> {
        let mut store = SampleStore {
            keyframes: Vec::new(),
            messages: Vec::new(),
        };

        let mut f = gen_frame();
        f.set_keyframe(Some(true));
        f.set_time_base((1, 1));
        f.set_pts(0);
        store.add_message(&f.to_message(), &[], &[])?;
        store.add_message(
            &Message::end_of_stream(EndOfStream::new(String::from(""))),
            &[],
            &[],
        )?;
        let mut f = gen_frame();
        f.set_keyframe(Some(false));
        f.set_time_base((1, 1));
        f.set_pts(1);
        store.add_message(&f.to_message(), &[], &[])?;

        let mut f = gen_frame();
        f.set_keyframe(Some(false));
        f.set_time_base((1, 1));
        f.set_pts(2);
        store.add_message(&f.to_message(), &[], &[])?;

        let mut f = gen_frame();
        f.set_keyframe(Some(true));
        f.set_time_base((1, 1));
        f.set_pts(3);
        store.add_message(&f.to_message(), &[], &[])?;
        store.add_message(
            &Message::end_of_stream(EndOfStream::new(String::from(""))),
            &[],
            &[],
        )?;
        let mut f = gen_frame();
        f.set_keyframe(Some(false));
        f.set_time_base((1, 1));
        f.set_pts(4);
        store.add_message(&f.to_message(), &[], &[])?;

        let mut f = gen_frame();
        let u = f.get_uuid();
        f.set_keyframe(Some(true));
        f.set_time_base((1, 1));
        f.set_pts(5);
        store.add_message(&f.to_message(), &[], &[])?;

        let mut f = gen_frame();
        f.set_keyframe(Some(false));
        f.set_time_base((1, 1));
        f.set_pts(6);
        store.add_message(&f.to_message(), &[], &[])?;

        let first = store.get_first("", u, BeforeOffset::Blocks(1))?;
        assert_eq!(first, Some(4));
        let (m, _, _) = store.get_message("", first.unwrap())?.unwrap();
        assert!(matches!(
            m.as_video_frame().unwrap().get_keyframe(),
            Some(true)
        ));

        let first_ts = store.get_first("", u, BeforeOffset::Seconds(5.0))?;
        assert_eq!(first_ts, Some(0));
        let (m, _, _) = store.get_message("", first_ts.unwrap())?.unwrap();

        assert!(matches!(
            m.as_video_frame().unwrap().get_keyframe(),
            Some(true)
        ));

        Ok(())
    }
}

pub fn to_hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{:02x}", byte)).collect()
}

#[cfg(test)]
pub fn gen_properly_filled_frame() -> VideoFrameProxy {
    let mut f = gen_frame();
    let (tbn, tbd) = (1, 1000_000);
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros() as f64
        / 1_000_000.0;
    let pts = (now * tbd as f64 / tbn as f64) as i64;
    f.set_pts(pts);
    f.set_time_base((tbn, tbd));
    f.set_keyframe(Some(true));
    f
}
