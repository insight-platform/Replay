use actix_web::{delete, Responder, web};
use serde::Deserialize;
use uuid::Uuid;

use replaydb::service::JobManager;

use crate::web_service::{JobService, ResponseMessage};

#[derive(Deserialize)]
struct JobFilter {
    job: String,
}

#[delete("/job")]
async fn delete_job(js: web::Data<JobService>, q: web::Query<JobFilter>) -> impl Responder {
    let mut js_bind = js.service.lock().await;

    let cleanup = js_bind.clean_stopped_jobs().await;
    if let Err(e) = cleanup {
        return ResponseMessage::Error(e.to_string());
    }

    let job_uuid = Uuid::try_from(q.job.as_str());
    if let Err(e) = job_uuid {
        return ResponseMessage::Error(e.to_string());
    }
    let job_uuid = job_uuid.unwrap();
    let res = js_bind.stop_job(job_uuid);
    match res {
        Ok(_) => ResponseMessage::Ok,
        Err(e) => ResponseMessage::Error(e.to_string()),
    }
}
