#![feature(test)]

extern crate test;

use parking_lot::Mutex;
use replaydb::store::gen_properly_filled_frame;
use replaydb::store::rocksdb::RocksStore;
use replaydb::stream_processor::StreamProcessor;
use replaydb::{ZmqReader, ZmqWriter};
use savant_core::transport::zeromq::{ReaderConfig, WriterConfig};
use std::sync::Arc;
use std::time::Duration;
use test::Bencher;

#[bench]
fn bench_0016k(b: &mut Bencher) -> anyhow::Result<()> {
    let data = vec![0u8; 16 * 1024];
    bench_bandwidth(b, data)
}

#[bench]
fn bench_0128k(b: &mut Bencher) -> anyhow::Result<()> {
    let data = vec![0u8; 128 * 1024];
    bench_bandwidth(b, data)
}

#[bench]
fn bench_1024k(b: &mut Bencher) -> anyhow::Result<()> {
    let data = vec![0u8; 1024 * 1024];
    bench_bandwidth(b, data)
}

fn bench_bandwidth(b: &mut Bencher, data: Vec<u8>) -> anyhow::Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap();
    let db = RocksStore::new(path, Duration::from_secs(60)).unwrap();

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
    let out_writer = ZmqWriter::new(
        &WriterConfig::new()
            .url(&format!("pub+bind:ipc://{}/out", path))?
            .build()?,
    )?;
    let db = Arc::new(Mutex::new(db));
    let mut processor = StreamProcessor::new(db.clone(), in_reader, out_writer);

    b.iter(|| {
        let f = gen_properly_filled_frame();
        in_writer
            .send_message("test", &f.to_message(), &[&data])
            .unwrap();
        processor.run_once().unwrap();
    });

    std::fs::remove_dir_all(path).unwrap_or_default();
    Ok(())
}
