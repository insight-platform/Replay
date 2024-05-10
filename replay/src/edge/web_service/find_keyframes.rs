use crate::web_service::JobService;
use actix_web::body::BoxBody;
use actix_web::http::header::ContentType;
use actix_web::{post, web, HttpResponse, Responder};
use log::debug;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
enum Response {
    Ok(String, Vec<String>),
    Error(String),
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

#[derive(Debug, Serialize, Deserialize)]
struct FindKeyframesQuery {
    source_id: String,
    from: Option<u64>,
    to: Option<u64>,
    limit: usize,
}

#[post("/keyframes/find")]
async fn find_keyframes(
    js: web::Data<JobService>,
    query: web::Json<FindKeyframesQuery>,
) -> impl Responder {
    let mut js_bind = js.service.lock().await;
    let uuids_res = js_bind
        .find_keyframes(&query.source_id, query.from, query.to, query.limit)
        .await;
    debug!(
        "Received Keyframe Lookup Query: {}",
        serde_json::to_string(&query).unwrap()
    );
    match uuids_res {
        Ok(uuids) => Response::Ok(
            query.source_id.clone(),
            uuids.into_iter().map(String::from).collect(),
        ),
        Err(e) => Response::Error(e.to_string()),
    }
}
