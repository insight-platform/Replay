use actix_web::{get, web, Responder};
use log::info;
use serde::Deserialize;

use replaydb::service::JobManager;

use crate::web_service::{JobService, ResponseMessage};

#[derive(Deserialize)]
struct JobFilter {
    job: Option<String>,
}

#[get("/job")]
async fn list_jobs(js: web::Data<JobService>, q: web::Query<JobFilter>) -> impl Responder {
    let mut js_bind = js.service.lock().await;

    let cleanup = js_bind.clean_stopped_jobs().await;
    if let Err(e) = cleanup {
        return ResponseMessage::Error(e.to_string());
    }

    let jobs = js_bind
        .list_running_jobs()
        .into_iter()
        .map(|(uuid, c, s)| (uuid.to_string(), c, s))
        .collect::<Vec<_>>();
    let jobs = if let Some(job) = &q.job {
        info!("Listing job: {}", job);
        jobs.into_iter()
            .filter(|(uuid, _, _)| uuid == job)
            .collect()
    } else {
        info!("Listing all currently running jobs");
        jobs
    };
    ResponseMessage::ListJobs(jobs)
}
