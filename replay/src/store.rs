mod mongodb;
mod rocksdb;

use anyhow::Result;
use savant_core::message::Message;
use uuid::Uuid;

pub enum BeforeOffset {
    Blocks(usize),
    Seconds(u64),
}

trait Store {
    fn add_message(&mut self, message: Message) -> Result<usize>;
    fn get_message(&self, source_id: &str, id: usize) -> Result<Option<Message>>;
    fn get_first(
        &self,
        source_id: &str,
        keyframe_uuid: Uuid,
        before: BeforeOffset,
    ) -> Result<Option<usize>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use savant_core::primitives::eos::EndOfStream;
    use savant_core::test::gen_frame;

    struct SampleStore {
        keyframes: Vec<(Uuid, usize)>,
        messages: Vec<Message>,
    }

    impl Store for SampleStore {
        fn add_message(&mut self, message: Message) -> Result<usize> {
            let current_len = self.messages.len();
            if message.is_video_frame() {
                let f = message.as_video_frame().unwrap();
                if let Some(true) = f.get_keyframe() {
                    self.keyframes.push((f.get_uuid(), current_len));
                }
            }
            self.messages.push(message);
            Ok(current_len)
        }

        fn get_message(&self, _: &str, id: usize) -> Result<Option<Message>> {
            Ok(Some(self.messages[id].clone()))
        }

        fn get_first(
            &self,
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
                    let before_scaled = seconds_before * time_base.1 as u64 / time_base.0 as u64;
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
        store.add_message(f.to_message())?;
        store.add_message(Message::end_of_stream(EndOfStream::new(String::from(""))))?;
        let mut f = gen_frame();
        f.set_keyframe(Some(false));
        f.set_time_base((1, 1));
        f.set_pts(1);
        store.add_message(f.to_message())?;

        let mut f = gen_frame();
        f.set_keyframe(Some(false));
        f.set_time_base((1, 1));
        f.set_pts(2);
        store.add_message(f.to_message())?;

        let mut f = gen_frame();
        f.set_keyframe(Some(true));
        f.set_time_base((1, 1));
        f.set_pts(3);
        store.add_message(f.to_message())?;
        store.add_message(Message::end_of_stream(EndOfStream::new(String::from(""))))?;
        let mut f = gen_frame();
        f.set_keyframe(Some(false));
        f.set_time_base((1, 1));
        f.set_pts(4);
        store.add_message(f.to_message())?;

        let mut f = gen_frame();
        let u = f.get_uuid();
        f.set_keyframe(Some(true));
        f.set_time_base((1, 1));
        f.set_pts(5);
        store.add_message(f.to_message())?;

        let mut f = gen_frame();
        f.set_keyframe(Some(false));
        f.set_time_base((1, 1));
        f.set_pts(6);
        store.add_message(f.to_message())?;

        let first = store.get_first("", u, BeforeOffset::Blocks(1))?;
        assert_eq!(first, Some(4));
        let m = store.get_message("", first.unwrap())?;
        assert!(matches!(
            m.unwrap().as_video_frame().unwrap().get_keyframe(),
            Some(true)
        ));

        let first_ts = store.get_first("", u, BeforeOffset::Seconds(5))?;
        assert_eq!(first_ts, Some(0));
        let m = store.get_message("", first_ts.unwrap())?;

        assert!(matches!(
            m.unwrap().as_video_frame().unwrap().get_keyframe(),
            Some(true)
        ));

        Ok(())
    }
}
