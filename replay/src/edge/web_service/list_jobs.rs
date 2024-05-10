use crate::web_service::JobService;
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::{get, web, HttpResponse, Responder};
use replaydb::job::configuration::JobConfiguration;
use replaydb::job::stop_condition::JobStopCondition;
use replaydb::service::JobManager;
use serde::Serialize;

#[derive(Debug, Serialize)]
enum Response {
    Ok(Vec<(String, JobConfiguration, JobStopCondition)>),
    Err(String),
}

impl Responder for Response {
    type Body = BoxBody;
    fn respond_to(self, _req: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(&self).unwrap();
        HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(body)
    }
}

#[get("/jobs/list")]
async fn list_jobs(js: web::Data<JobService>) -> impl Responder {
    let mut js_bind = js.service.lock().await;

    let cleanup = js_bind.clean_stopped_jobs().await;
    if let Err(e) = cleanup {
        return Response::Err(e.to_string());
    }

    let jobs = js_bind
        .list_running_jobs()
        .into_iter()
        .map(|(uuid, c, s)| (uuid.to_string(), c, s))
        .collect();
    Response::Ok(jobs)
}
