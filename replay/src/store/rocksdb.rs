use crate::store::BeforeOffset;
use anyhow::{bail, Result};
use bincode::{Decode, Encode};
use hashbrown::HashMap;
use md5::{Digest, Md5};
use rocksdb::{ColumnFamilyDescriptor, Options, SliceTransform, DB};
use savant_core::message::{load_message, save_message, Message};
use std::mem::size_of;
use std::path::Path;
use std::time::Duration;
use std::{fs, io};
use uuid::Uuid;

const CF_MESSAGE_DB: &str = "message_db";
const CF_KEYFRAME_DB: &str = "keyframes_db";
const CF_INDEX_DB: &str = "index_db";

pub(crate) fn dir_size(path: &str) -> io::Result<usize> {
    fn dir_size(mut dir: fs::ReadDir) -> io::Result<usize> {
        dir.try_fold(0, |acc, file| {
            let file = file?;
            let size = match file.metadata()? {
                data if data.is_dir() => dir_size(fs::read_dir(file.path())?)?,
                data => data.len() as usize,
            };
            Ok(acc + size)
        })
    }
    dir_size(fs::read_dir(Path::new(path))?)
}

type MessageKeyIndex = usize;
#[derive(Encode, Decode, PartialEq, Debug)]
struct MessageKey {
    pub(crate) source_md5: [u8; 16],
    pub(crate) index: MessageKeyIndex,
}

#[derive(Encode, Decode, PartialEq, Debug)]
struct IndexKey {
    pub(crate) source_md5: [u8; 16],
}

#[derive(Encode, Decode, PartialEq, Debug)]
struct IndexValue {
    pub(crate) index: usize,
}

type KeyFrameUUID = u128;
#[derive(Encode, Decode, PartialEq, Debug)]
struct KeyframeKey {
    pub(crate) source_md5: [u8; 16],
    pub(crate) keyframe_uuid: KeyFrameUUID,
}

#[derive(Encode, Decode, PartialEq, Debug)]
struct KeyframeValue {
    pub(crate) index: usize,
    pub(crate) pts: i64,
}

pub struct RocksStore {
    db: DB,
    source_id_hashes: HashMap<String, [u8; 16]>,
    resident_index_values: HashMap<String, usize>,
}

impl RocksStore {
    pub(crate) fn get_source_hash(&mut self, source_id: &str) -> Result<[u8; 16]> {
        if let Some(hash) = self.source_id_hashes.get(source_id) {
            return Ok(*hash);
        }

        let mut hasher = Md5::new();
        hasher.update(source_id.as_bytes());
        let hash = hasher.finalize();
        let mut hash_bytes = [0; 16];
        hash_bytes.copy_from_slice(&hash[..]);
        self.source_id_hashes
            .insert(source_id.to_string(), hash_bytes);
        Ok(hash_bytes)
    }

    pub(crate) fn current_index_value(&mut self, source_id: &str) -> Result<usize> {
        if let Some(index) = self.resident_index_values.get(source_id) {
            return Ok(*index);
        }

        let key = IndexKey {
            source_md5: self.get_source_hash(source_id)?,
        };

        let key_bytes = bincode::encode_to_vec(&key, bincode::config::standard())?;
        let cf = self.db.cf_handle(CF_INDEX_DB).unwrap();
        let value_bytes = self.db.get_cf(cf, &key_bytes)?;
        let value = if let Some(value_bytes) = value_bytes {
            let value: IndexValue =
                bincode::decode_from_slice(&value_bytes, bincode::config::standard())?.0;
            value.index
        } else {
            0
        };

        self.resident_index_values
            .insert(source_id.to_string(), value);
        Ok(value)
    }
    pub fn new(path: &str, ttl: Duration) -> Result<Self> {
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);
        cf_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(
            size_of::<MessageKey>() - size_of::<MessageKeyIndex>(),
        ));
        let cf_message = ColumnFamilyDescriptor::new(CF_MESSAGE_DB, cf_opts);

        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);
        cf_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(
            size_of::<KeyframeKey>() - size_of::<KeyFrameUUID>(),
        ));

        let cf_keyframe = ColumnFamilyDescriptor::new(CF_KEYFRAME_DB, cf_opts);

        let cf_opts = Options::default();
        let cf_index = ColumnFamilyDescriptor::new(CF_INDEX_DB, cf_opts);

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = DB::open_cf_descriptors_with_ttl(
            &db_opts,
            &path,
            vec![cf_message, cf_keyframe, cf_index],
            ttl,
        )?;

        Ok(Self {
            db,
            source_id_hashes: HashMap::new(),
            resident_index_values: HashMap::new(),
        })
    }

    pub fn remove_db(path: &str) -> Result<()> {
        Ok(DB::destroy(&Options::default(), path)?)
    }

    pub fn disk_size(&self, path: &str) -> Result<usize> {
        Ok(dir_size(path)?)
    }
}

impl super::Store for RocksStore {
    fn add_message(&mut self, message: Message) -> Result<usize> {
        let mut frame_opt = None;
        let source_id = if message.is_video_frame() {
            let frame = message.as_video_frame().unwrap();
            let source_id = frame.get_source_id();
            if matches!(frame.get_keyframe(), Some(true)) {
                frame_opt = Some(frame);
            }
            source_id
        } else if message.is_end_of_stream() {
            let eos = message.as_end_of_stream().unwrap();
            eos.source_id.clone()
        } else if message.is_user_data() {
            let user_data = message.as_user_data().unwrap();
            user_data.get_source_id().to_string()
        } else {
            bail!("Unsupported message type. Only video frames, end of stream, and user data are supported.");
        };
        let source_hash = self.get_source_hash(&source_id)?;
        let index = self.current_index_value(&source_id)?;
        let mut batch = rocksdb::WriteBatch::default();

        let key = MessageKey {
            source_md5: source_hash,
            index,
        };
        let key_bytes = bincode::encode_to_vec(&key, bincode::config::standard())?;
        let value_bytes = save_message(&message)?;
        let cf = self
            .db
            .cf_handle(CF_MESSAGE_DB)
            .expect("CF_MESSAGE_DB not found");

        if let Some(f) = frame_opt {
            let key = KeyframeKey {
                source_md5: source_hash,
                keyframe_uuid: f.get_uuid_u128(),
            };
            let key_bytes = bincode::encode_to_vec(&key, bincode::config::standard())?;
            let value = KeyframeValue {
                index,
                pts: f.get_pts(),
            };
            let value_bytes = bincode::encode_to_vec(&value, bincode::config::standard())?;
            let cf = self
                .db
                .cf_handle(CF_KEYFRAME_DB)
                .expect("CF_KEYFRAME_DB not found");
            batch.put_cf(cf, &key_bytes, &value_bytes);
        }

        batch.put_cf(cf, &key_bytes, &value_bytes);

        let index_key = IndexKey {
            source_md5: source_hash,
        };

        let index_value = IndexValue { index: index + 1 };

        let index_key_bytes = bincode::encode_to_vec(&index_key, bincode::config::standard())?;
        let index_value_bytes = bincode::encode_to_vec(&index_value, bincode::config::standard())?;
        let cf = self
            .db
            .cf_handle(CF_INDEX_DB)
            .expect("CF_INDEX_DB not found");
        batch.put_cf(cf, &index_key_bytes, &index_value_bytes);

        self.db.write(batch)?;

        Ok(index)
    }

    fn get_message(&mut self, source_id: &str, id: usize) -> Result<Option<Message>> {
        let source_hash = self.get_source_hash(source_id)?;
        let key = MessageKey {
            source_md5: source_hash,
            index: id,
        };
        let key_bytes = bincode::encode_to_vec(&key, bincode::config::standard())?;
        let cf = self
            .db
            .cf_handle(CF_MESSAGE_DB)
            .expect("CF_MESSAGE_DB not found");
        let value_bytes = self.db.get_cf(cf, &key_bytes)?;
        if let Some(value_bytes) = value_bytes {
            let message = load_message(&value_bytes);
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    fn get_first(
        &mut self,
        _source_id: &str,
        _keyframe_uuid: Uuid,
        _before: BeforeOffset,
    ) -> Result<Option<usize>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use savant_core::test::gen_frame;

    #[test]
    fn test_rocksdb_init() -> Result<()> {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap();
        let mut db = RocksStore::new(path, Duration::from_secs(60))?;

        let source_id1 = "test_source_id-1";
        let source_id2 = "test_source_id-2";
        let source_hash1 = db.get_source_hash(source_id1)?;
        let source_hash2 = db.get_source_hash(source_id2)?;
        assert_ne!(source_hash1, source_hash2);
        let source_hash1_1 = db.get_source_hash(source_id1)?;
        assert_eq!(source_hash1, source_hash1_1);

        let index = db.current_index_value(source_id1)?;
        assert_eq!(index, 0);
        let index = db.current_index_value(source_id1)?;
        assert_eq!(index, 0);

        let disk_size = db.disk_size(path)?;
        assert!(disk_size > 0);

        let _ = RocksStore::remove_db(path);
        Ok(())
    }

    #[test]
    fn test_load_message() -> Result<()> {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap();
        let frame = gen_frame();
        let source_id = frame.get_source_id();
        {
            let mut db = RocksStore::new(path, Duration::from_secs(60))?;
            let m = frame.to_message();
            let id = db.add_message(m.clone())?;
            let m2 = db.get_message(&source_id, id)?;
            assert_eq!(
                m.as_video_frame().unwrap().get_uuid_u128(),
                m2.unwrap().as_video_frame().unwrap().get_uuid_u128()
            );
        }
        {
            let mut db = RocksStore::new(path, Duration::from_secs(60))?;
            let m = db.get_message(&source_id, 0)?;
            assert!(m.is_some());
            assert_eq!(db.current_index_value(&source_id)?, 1);
            let m = db.get_message(&source_id, 1)?;
            assert!(m.is_none());
        }
        let _ = RocksStore::remove_db(path);
        Ok(())
    }
}
