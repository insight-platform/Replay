use crate::job::configuration::JobConfiguration;
use crate::job::stop_condition::JobStopCondition;
use crate::job_writer::WriterConfiguration;
use crate::store::JobOffset;
use anyhow::Result;
use savant_core::primitives::Attribute;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct JobQuery {
    pub socket: WriterConfiguration,
    pub configuration: JobConfiguration,
    pub stop_condition: JobStopCondition,
    pub offset: JobOffset,
    pub attributes: Vec<Attribute>,
}

impl JobQuery {
    pub fn new(
        socket: WriterConfiguration,
        configuration: JobConfiguration,
        stop_condition: JobStopCondition,
        offset: JobOffset,
        attributes: Vec<Attribute>,
    ) -> Self {
        Self {
            socket,
            configuration,
            stop_condition,
            offset,
            attributes,
        }
    }

    pub fn json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub fn json_pretty(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }
}

#[cfg(test)]
mod tests {
    use crate::job::configuration::JobConfigurationBuilder;
    use crate::job::stop_condition::JobStopCondition;
    use crate::job_writer::WriterConfiguration;
    use crate::query::JobQuery;
    use crate::store::JobOffset;
    use savant_core::primitives::attribute_value::AttributeValue;
    use savant_core::primitives::Attribute;
    use std::time::Duration;

    #[test]
    fn test_job_query() {
        let configuration = JobConfigurationBuilder::default()
            .min_duration(Duration::from_millis(700))
            .max_duration(Duration::from_secs_f64(1_f64 / 30_f64))
            .stored_source_id("stored_source_id".to_string())
            .resulting_source_id("resulting_source_id".to_string())
            .build()
            .unwrap();
        let stop_condition = JobStopCondition::frame_count(1);
        let offset = JobOffset::Blocks(0);
        let job_query = JobQuery::new(
            WriterConfiguration::default(),
            configuration,
            stop_condition,
            offset,
            vec![Attribute::persistent(
                "key",
                "value",
                vec![
                    AttributeValue::integer(1, Some(0.5)),
                    AttributeValue::float_vector(vec![1.0, 2.0, 3.0], None),
                ],
                &None,
                false,
            )],
        );
        let json = job_query.json_pretty().unwrap();
        println!("{}", json);
        let _ = JobQuery::from_json(&json).unwrap();
    }
}
