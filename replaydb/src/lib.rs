use savant_core::utils::bytes_to_hex_string;
use std::str::from_utf8;

//pub mod job_orig;
pub mod job;
pub mod store;
pub mod stream_processor;

pub fn topic_to_string(topic: &[u8]) -> String {
    from_utf8(topic)
        .map(|s| String::from(s))
        .unwrap_or(bytes_to_hex_string(topic))
}
