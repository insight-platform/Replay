use crate::web_service::JobService;
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::{post, web, HttpResponse, Responder};
use log::info;
use serde::Serialize;

#[derive(Debug, Serialize)]
enum Status {
    Ok,
}

impl Responder for Status {
    type Body = BoxBody;
    fn respond_to(self, _req: &actix_web::HttpRequest) -> HttpResponse<Self::Body> {
        let body = serde_json::to_string(&self).unwrap();
        HttpResponse::Ok()
            .content_type(ContentType::json())
            .body(body)
    }
}

#[post("/shutdown")]
async fn shutdown(js: web::Data<JobService>) -> impl Responder {
    let mut js_bind = js.shutdown.lock().await;
    info!("Shutting down");
    *js_bind = true;
    Status::Ok
}
