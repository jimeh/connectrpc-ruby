//! The Connect unary fast path: connect or reuse, send, head and the whole
//! body under the response limit as one operation, so a call releases the
//! GVL once.
use std::sync::Arc;
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
/// request hyper hands back unsent, or, before response headers, a stream the
/// server refused with REFUSED_STREAM or that its GOAWAY says it never
/// processed (a stream id above the last stream id). Nothing else is replayed.
pub async fn unary(
    core: Arc<DriverCore>,
    spec: RequestSpec,
    payload: Option<Bytes>,
    limit: usize,
    deadline: Option<Instant>,
) -> Result<Unary, Failure> {
    let mut exclude = None;
    loop {
        let mut lease = core.acquire(&spec.origin, false, exclude, deadline).await?;
        let body = match &payload {
            Some(bytes) => CallBody::full(bytes.clone()),
            None => CallBody::empty(),
        };
        let request = spec.build(body, deadline, lease.is_h2())?;
        // Spec hold point: parks the call before it hands its request to
        // hyper, while the runtime keeps driving the connection.
        core.holds.reach("request:send").await;
        let (sent, mark) = send(&mut lease, request);
        // Spec hold point: armed with a zero bound, it counts the requests
        // handed to hyper.
        core.holds.block("request:queued");
        let response = match sent.await {
            Ok(response) => response,
            Err(mut error) => {
                let unsent = error.take_message().is_some();
                if unsent {
                    // Spec hold point: armed with a zero bound, it counts the
                    // requests hyper handed back unsent.
                    core.holds.reach("retry:unsent").await;
                }
                // Its stream is above a GOAWAY's last stream id and either h2
                // failed it for that GOAWAY or it never reached the socket.
                let unprocessed = !unsent
                    && mark.as_ref().is_some_and(|mark| {
                        mark.unprocessed()
                            && (Failure::remote_go_away(error.error()) || mark.unsent())
                    });
                let replayable = unsent || unprocessed || Failure::refused_stream(error.error());
                if replayable && exclude.is_none() {
                    if unprocessed {
                        // Spec hold point: armed with a zero bound, it counts
                        // the requests replayed after a GOAWAY.
                        core.holds.reach("retry:unprocessed").await;
                    }
                    exclude = Some(lease.id());
                    continue;
                }
                return Err(lease.failure(error.error()));
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
            let frame = frame.map_err(|error| lease.failure(&error))?;
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
