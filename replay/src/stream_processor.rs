use crate::store::{to_hex_string, Store};
use anyhow::{bail, Result};
use parking_lot::RwLock;
use savant_core::transport::zeromq::{ReaderResult, SyncReader, SyncWriter};
use std::sync::Arc;

pub struct StreamProcessor {
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
            match message {
                Ok(m) => match m {
                    ReaderResult::Message {
                        message,
                        topic,
                        routing_id,
                        data,
                    } => {
                        log::debug!(
                            "Received message: topic: {:?}, routing_id: {:?}, message: {:?}",
                            to_hex_string(&topic),
                            to_hex_string(&routing_id.as_ref().unwrap_or(&Vec::new())),
                            message
                        );
                        self.db.write().add_message(&message, &topic, &data)?;
                        let data_slice = data.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>();
                        self.output.send_message(
                            &String::from_utf8(topic)?,
                            &message,
                            &data_slice,
                        )?;
                    }
                    ReaderResult::Timeout => {
                        log::info!("Timeout receiving message, waiting for next message.");
                    }
                    ReaderResult::PrefixMismatch { topic, routing_id } => {
                        log::warn!(
                            "Received message with mismatched prefix: topic: {:?}, routing_id: {:?}",
                            to_hex_string(&topic),
                            to_hex_string(&routing_id.as_ref().unwrap_or(&Vec::new()))
                        );
                    }
                    ReaderResult::RoutingIdMismatch { topic, routing_id } => {
                        log::warn!(
                            "Received message with mismatched routing_id: topic: {:?}, routing_id: {:?}",
                            to_hex_string(&topic),
                            to_hex_string(&routing_id.as_ref().unwrap_or(&Vec::new()))
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
