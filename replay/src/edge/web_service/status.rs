use crate::web_service::JobService;
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::{get, web, HttpResponse, Responder};
use log::error;
use replaydb::service::JobManager;
use serde::Serialize;

#[derive(Debug, Serialize)]
enum Status {
    Running,
    Finished,
    Error(String),
}

impl Responder for Status {
    type Body = BoxBody;
    fn respond_to(self, _req: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(&self).unwrap();
        let mut resp = match self {
            Status::Running => HttpResponse::Ok(),
            Status::Finished => HttpResponse::Ok(),
            Status::Error(_) => HttpResponse::InternalServerError(),
        };
        resp.content_type(ContentType::json()).body(body)
    }
}

#[get("/status")]
async fn status(js: web::Data<JobService>) -> impl Responder {
    let mut js_bind = js.service.lock().await;
    match js_bind.check_stream_processor_finished().await {
        Ok(finished) => {
            if finished {
                Status::Finished
            } else {
                Status::Running
            }
        }
        Err(e) => {
            error!("Stream processor finished with error: {}", e);
            Status::Error(e.to_string())
        }
    }
}
