use crate::job::query::JobQuery;
use savant_core::transport::zeromq::NonBlockingReader;
use uuid::Uuid;

pub trait ServiceRuntime {
    fn run_job(&mut self, query: JobQuery) -> anyhow::Result<()>;
    fn update_job(&mut self, job_id: Uuid) -> anyhow::Result<()>;
}

// impl TryFrom<&ServiceConfiguration> for RocksDbStreamProcessor {
//     type Error = anyhow::Error;
//
//     fn try_from(conf: &ServiceConfiguration) -> Result<Self, Self::Error> {
//         let stream_processor = match &conf.storage {
//             Storage::RocksDB {
//                 path,
//                 data_expiration_ttl,
//             } => {
//                 let mut in_stream = NonBlockingReader::try_from(&conf.in_stream)?;
//                 in_stream.start()?;
//
//                 let out_stream = if let Some(oc) = &conf.out_stream {
//                     Some(NonBlockingWriter::try_from(oc)?)
//                 } else {
//                     None
//                 };
//                 let path = Path::new(&path);
//                 let storage = RocksStore::new(path, *data_expiration_ttl)?;
//                 RocksDbStreamProcessor::new(
//                     Arc::new(Mutex::new(storage)),
//                     in_stream,
//                     out_stream,
//                     conf.common.stats_period,
//                     conf.common.pass_metadata_only,
//                 )
//             }
//         };
//         Ok(stream_processor)
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::service_configuration::ServiceConfiguration;
//     use crate::stream_processor::RocksDbStreamProcessor;
//     use anyhow::Result;
//     use std::env::set_var;
//     use twelf::Layer;
//
//     #[test]
//     fn test_configuration() -> Result<()> {
//         // set env SOCKET_PATH=in
//         set_var("SOCKET_PATH_IN", "in");
//         set_var("SOCKET_PATH_OUT", "out");
//
//         let config =
//             ServiceConfiguration::with_layers(&[Layer::Json("assets/rocksdb.json".into())])?;
//         assert_eq!(config.common.management_port, 8080);
//         assert_eq!(config.in_stream.url, "router+bind:ipc:///tmp/in");
//         let os = config.out_stream.as_ref().unwrap();
//         assert_eq!(os.url, "dealer+connect:ipc:///tmp/out");
//         let _ = RocksDbStreamProcessor::try_from(&config)?;
//         Ok(())
//     }
//
//     #[test]
//     fn test_configuration_opt_out() -> Result<()> {
//         let config = ServiceConfiguration::with_layers(&[Layer::Json(
//             "assets/rocksdb_opt_out.json".into(),
//         )])?;
//         let _ = RocksDbStreamProcessor::try_from(&config)?;
//         Ok(())
//     }
// }
