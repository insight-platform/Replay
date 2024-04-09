use crate::store::Store;
use anyhow::{bail, Result};
use parking_lot::RwLock;
use savant_core::transport::zeromq::{ReaderResult, SyncReader, SyncWriter};
use std::sync::Arc;

struct StreamProcessor {
    db: Arc<RwLock<dyn Store>>,
    input: SyncReader,
    output: SyncWriter,
}

impl StreamProcessor {
    pub fn new(db: Arc<RwLock<dyn Store>>, input: SyncReader, output: SyncWriter) -> Self {
        Self { db, input, output }
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            let message = self.input.receive();
            let message = match message {
                Ok(m) => match m {
                    ReaderResult::Message {
                        message,
                        topic,
                        routing_id,
                        data,
                    } => {}
                    ReaderResult::Timeout => {
                        log::info!("Timeout receiving message, waiting for next message.");
                    }
                    ReaderResult::PrefixMismatch { topic, routing_id } => {
                        log::warn!(
                            "Received message with mismatched prefix: topic: {:?}, routing_id: {:?}",
                            topic,
                            routing_id
                        );
                    }
                    ReaderResult::RoutingIdMismatch { topic, routing_id } => {
                        log::warn!(
                            "Received message with mismatched routing_id: topic: {:?}, routing_id: {:?}",
                            topic,
                            routing_id
                        );
                    }
                    ReaderResult::TooShort(m) => {
                        log::warn!("Received message that was too short: {:?}", m);
                    }
                },
                Err(e) => {
                    bail!("Error receiving message: {:?}", e);
                }
            };
        }
    }
}
