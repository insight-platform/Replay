use savant_core::transport::zeromq::{NoopResponder, Reader, Writer, ZmqSocketProvider};

pub mod job;
pub mod store;
pub mod stream_processor;

pub type ZmqWriter = Writer<NoopResponder, ZmqSocketProvider>;

pub type ZmqReader = Reader<NoopResponder, ZmqSocketProvider>;
