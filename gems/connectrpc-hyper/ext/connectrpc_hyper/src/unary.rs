//! The Connect unary fast path: connect or reuse, send, head and the whole
//! body under the response limit as one operation, so a call releases the
//! GVL once.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;

use crate::body::CallBody;
use crate::call::send;
use crate::failure::Failure;
use crate::pool::{DriverCore, RequestSpec};

pub struct Unary {
    pub status: u16,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

fn content_length(headers: &http::HeaderMap) -> Option<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

/// Retries once, on a fresh connection and with a refreshed wire timeout: a
/// request hyper hands back unsent, or a stream the server refused with
/// REFUSED_STREAM before response headers. Nothing else is replayed.
/// `dispatched` is true while a request is queued: neither of those reached
/// a handler, so a pending retry is not dispatched.
pub async fn unary(
    core: Arc<DriverCore>,
    spec: RequestSpec,
    payload: Option<Bytes>,
    limit: usize,
    deadline: Option<Instant>,
    dispatched: &AtomicBool,
) -> Result<Unary, Failure> {
    let mut exclude = None;
    loop {
        let mut lease = core.acquire(&spec.origin, false, exclude, deadline).await?;
        let body = match &payload {
            Some(bytes) => CallBody::Full(Some(bytes.clone())),
            None => CallBody::Empty,
        };
        let request = spec.build(body, deadline, lease.is_h2())?;
        dispatched.store(true, Ordering::SeqCst);
        let response = match send(&mut lease, request).await {
            Ok(response) => response,
            Err(mut error) => {
                let unsent = error.take_message().is_some();
                let replayable = unsent || Failure::refused_stream(error.error());
                if replayable && exclude.is_none() {
                    exclude = Some(lease.id());
                    dispatched.store(false, Ordering::SeqCst);
                    continue;
                }
                return Err(Failure::from_hyper(error.error()));
            }
        };
        let (parts, mut incoming) = response.into_parts();
        if content_length(&parts.headers).is_some_and(|length| length > limit as u64) {
            return Err(Failure::new(
                "response_limit",
                "response exceeds byte limit",
            ));
        }
        let mut body = BytesMut::new();
        while let Some(frame) = incoming.frame().await {
            let frame = frame.map_err(|error| Failure::from_hyper(&error))?;
            if let Ok(data) = frame.into_data() {
                if body.len() + data.len() > limit {
                    return Err(Failure::new(
                        "response_limit",
                        "response exceeds byte limit",
                    ));
                }
                body.extend_from_slice(&data);
            }
        }
        core.release_h1(&mut lease);
        return Ok(Unary {
            status: parts.status.as_u16(),
            headers: parts.headers,
            body: body.freeze(),
        });
    }
}
