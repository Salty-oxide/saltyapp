//! The HTTP half: talking to a ksqlDB server.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use error_stack::Report;
use futures_util::StreamExt;
use salty_core::{AppError, Result};

use crate::protocol::{
    parse_header, parse_row, parse_server_error, parse_streams, stream_for_topic, KsqlQueryHeader,
    KsqlRow,
};

/// Where a ksqlDB server is, and how to authenticate to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KsqlEndpoint {
    /// Base URL, e.g. `http://localhost:8088`.
    pub url: String,
    /// Basic auth as `user:password`, matching how the schema registry's
    /// credentials are already stored — one shape for both services rather
    /// than two.
    pub basic_auth: Option<String>,
}

impl KsqlEndpoint {
    /// Joins a path onto the base URL without doubling or dropping the slash.
    fn endpoint(&self, path: &str) -> String {
        format!("{}/{}", self.url.trim_end_matches('/'), path.trim_start_matches('/'))
    }
}

/// What a streaming query produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KsqlStreamOutcome {
    pub header: Option<KsqlQueryHeader>,
    /// True when the caller's cancellation flag ended the read rather than the
    /// server closing the stream. The UI says "stopped" rather than "finished".
    pub cancelled: bool,
}

/// A ksqlDB client for one endpoint.
pub struct KsqlClient {
    http: reqwest::Client,
    endpoint: KsqlEndpoint,
}

/// How long to wait for the server to start responding.
///
/// Applied to the *connection and headers*, never to the body: a push query's
/// body stays open indefinitely by design, and a read timeout would kill every
/// live tail on a quiet topic.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

impl KsqlClient {
    pub fn new(endpoint: KsqlEndpoint) -> Result<Self, AppError> {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|err| {
                Report::new(AppError::Validation).attach(format!("could not build an HTTP client: {err}"))
            })?;
        Ok(KsqlClient { http, endpoint })
    }

    fn with_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.endpoint.basic_auth.as_deref() {
            Some(credentials) => {
                let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
                request.basic_auth(user, Some(password))
            }
            None => request,
        }
    }

    /// Turns a non-2xx response into an error carrying the server's own
    /// message.
    ///
    /// ksqlDB explains its failures in the body — "Line 1:8: mismatched input"
    /// is actionable, "HTTP 400" is not.
    async fn server_error(what: &str, response: reqwest::Response) -> Report<AppError> {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let detail = parse_server_error(&body)
            .map(|error| error.message)
            .unwrap_or_else(|| {
                if body.trim().is_empty() {
                    format!("ksqlDB returned {status}")
                } else {
                    body.chars().take(400).collect()
                }
            });

        // Deliberately `AppError::SchemaRegistry`-adjacent reasoning: this is
        // *not* `AppError::Authentication`, whatever the status code. ksqlDB is
        // a different service from the broker, and a 401 here says nothing
        // about whether the Kafka credentials work. Classifying it as an auth
        // failure would feed the credential circuit breaker and take a working
        // cluster offline inside the app over an unrelated service.
        Report::new(AppError::Kafka).attach(format!("{what}: {detail}"))
    }

    /// Runs a non-streaming statement against `/ksql` and returns the raw
    /// response body.
    pub async fn statement(&self, sql: &str) -> Result<String, AppError> {
        let response = self
            .with_auth(self.http.post(self.endpoint.endpoint("ksql")))
            .json(&serde_json::json!({ "ksql": sql, "streamsProperties": {} }))
            .send()
            .await
            .map_err(|err| {
                Report::new(AppError::Kafka).attach(format!("could not reach ksqlDB: {err}"))
            })?;

        if !response.status().is_success() {
            return Err(Self::server_error("ksqlDB rejected the statement", response).await);
        }
        response
            .text()
            .await
            .map_err(|err| Report::new(AppError::Kafka).attach(format!("could not read the ksqlDB response: {err}")))
    }

    /// The stream registered over `topic`, if any.
    ///
    /// Backs the topic tab, which cannot open on a query until it knows the
    /// stream's name — ksqlDB cannot `SELECT` from a raw topic.
    pub async fn stream_for_topic(&self, topic: &str) -> Result<Option<String>, AppError> {
        let body = self.statement("LIST STREAMS;").await?;
        Ok(stream_for_topic(&parse_streams(&body), topic))
    }

    /// Runs a streaming query, handing each row to `on_row` as it arrives.
    ///
    /// Returns when the server closes the stream or `cancelled` is set,
    /// whichever comes first. The body is read incrementally — a push query
    /// never ends, so buffering it would mean never returning at all.
    /// `on_header` fires the moment the column frame is read, **before** any
    /// row. That ordering is not a nicety: a push query does not end until it
    /// is stopped, so a caller that waited for the return value to learn the
    /// columns would never draw a grid at all.
    pub async fn query_stream<H, F>(
        &self,
        sql: &str,
        cancelled: Arc<AtomicBool>,
        mut on_header: H,
        mut on_row: F,
    ) -> Result<KsqlStreamOutcome, AppError>
    where
        H: FnMut(&KsqlQueryHeader) + Send,
        F: FnMut(KsqlRow) + Send,
    {
        let response = self
            .with_auth(self.http.post(self.endpoint.endpoint("query-stream")))
            .json(&serde_json::json!({ "sql": sql, "properties": {} }))
            .send()
            .await
            .map_err(|err| {
                Report::new(AppError::Kafka).attach(format!("could not reach ksqlDB: {err}"))
            })?;

        if !response.status().is_success() {
            return Err(Self::server_error("ksqlDB rejected the query", response).await);
        }

        let mut header: Option<KsqlQueryHeader> = None;
        // Bytes, not a `String`. A chunk boundary can fall in the middle of a
        // multi-byte character, and decoding each chunk as it arrives would
        // turn that character into a replacement char before the rest of it
        // was even read — silently corrupting any non-ASCII value in a result.
        // Buffering bytes and decoding whole lines avoids it: a complete line
        // is valid UTF-8, because it is JSON.
        let mut pending: Vec<u8> = Vec::new();
        let mut stream = response.bytes_stream();
        let mut was_cancelled = false;

        while let Some(chunk) = stream.next().await {
            // Checked per chunk rather than per row: a quiet topic can go
            // minutes between rows, and Stop has to take effect when the user
            // presses it, not when the next record happens to arrive.
            if cancelled.load(Ordering::Relaxed) {
                was_cancelled = true;
                break;
            }

            let chunk = chunk.map_err(|err| {
                Report::new(AppError::Kafka).attach(format!("the ksqlDB stream failed: {err}"))
            })?;
            pending.extend_from_slice(&chunk);

            // Frames are newline-delimited, but a chunk can split one in half,
            // so only whole lines are consumed and the remainder is carried
            // into the next chunk.
            //
            // Walked with a cursor and drained **once** per chunk rather than
            // once per line. Draining per line shifts the whole unread tail
            // each time, which is quadratic in the number of lines a chunk
            // carries — and a chunk here is tens of kilobytes of ~50-byte
            // rows, so that is hundreds of lines and megabytes of pointless
            // memmove per chunk on a fast stream.
            let mut consumed = 0;
            while let Some(offset) = pending[consumed..].iter().position(|byte| *byte == b'\n') {
                let end = consumed + offset;
                {
                    let decoded = String::from_utf8_lossy(&pending[consumed..end]);
                    let line = decoded.trim();
                    if !line.is_empty() {
                        let mut handled = false;
                        if header.is_none() {
                            if let Some(parsed) = parse_header(line) {
                                on_header(&parsed);
                                header = Some(parsed);
                                handled = true;
                            }
                        }
                        if !handled && let Some(row) = parse_row(line) {
                            on_row(row);
                        }
                    }
                }
                consumed = end + 1;
            }
            if consumed > 0 {
                pending.drain(..consumed);
            }
        }

        // The tail, for a server that closed without a final newline.
        if !was_cancelled {
            let tail = String::from_utf8_lossy(&pending);
            let line = tail.trim();
            if !line.is_empty() {
                if header.is_none() {
                    header = parse_header(line);
                    if let Some(parsed) = header.as_ref() {
                        on_header(parsed);
                    }
                }
                if header.is_some()
                    && let Some(row) = parse_row(line)
                {
                    on_row(row);
                }
            }
        }

        Ok(KsqlStreamOutcome { header, cancelled: was_cancelled })
    }

    /// Asks the server to release a push query.
    ///
    /// Called alongside dropping the response rather than instead of it.
    /// Dropping the socket usually terminates the query, but "usually" is not
    /// good enough when being wrong leaves a query running on someone's shared
    /// server. Failure here is reported to the caller and never escalated: the
    /// rows have already been delivered, and the query is very likely gone
    /// anyway.
    pub async fn close_query(&self, query_id: &str) -> Result<(), AppError> {
        let response = self
            .with_auth(self.http.post(self.endpoint.endpoint("close-query")))
            .json(&serde_json::json!({ "queryId": query_id }))
            .send()
            .await
            .map_err(|err| {
                Report::new(AppError::Kafka).attach(format!("could not reach ksqlDB to close the query: {err}"))
            })?;

        if !response.status().is_success() {
            return Err(Self::server_error("ksqlDB refused to close the query", response).await);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(url: &str) -> KsqlEndpoint {
        KsqlEndpoint { url: url.into(), basic_auth: None }
    }

    #[test]
    fn joins_a_path_onto_the_base_url() {
        assert_eq!(
            endpoint("http://localhost:8088").endpoint("query-stream"),
            "http://localhost:8088/query-stream"
        );
    }

    // A pasted URL routinely has a trailing slash, and the path constants have
    // a leading one. Neither should produce `//`.
    #[test]
    fn does_not_double_the_slash() {
        assert_eq!(
            endpoint("http://localhost:8088/").endpoint("/ksql"),
            "http://localhost:8088/ksql"
        );
    }

    #[test]
    fn keeps_a_base_path_in_the_url() {
        assert_eq!(
            endpoint("https://example.com/ksqldb").endpoint("ksql"),
            "https://example.com/ksqldb/ksql"
        );
    }

    #[test]
    fn builds_a_client_for_a_valid_endpoint() {
        assert!(KsqlClient::new(endpoint("http://localhost:8088")).is_ok());
    }

    // The failure the user sees most: the server is simply not running. It has
    // to arrive as an error they can read, and never as an auth failure — see
    // `server_error`.
    #[tokio::test]
    async fn reports_an_unreachable_server_without_blaming_credentials() {
        let client = KsqlClient::new(endpoint("http://127.0.0.1:1")).expect("client");

        let error = client.statement("SHOW STREAMS;").await.expect_err("should fail");

        assert!(matches!(error.current_context(), AppError::Kafka));
    }

    #[tokio::test]
    async fn reports_an_unreachable_server_for_a_streaming_query_too() {
        let client = KsqlClient::new(endpoint("http://127.0.0.1:1")).expect("client");
        let cancelled = Arc::new(AtomicBool::new(false));

        let error = client
            .query_stream("SELECT 1;", cancelled, |_| {}, |_| {})
            .await
            .expect_err("should fail");

        assert!(matches!(error.current_context(), AppError::Kafka));
    }

    // Cancellation is checked before the first chunk is awaited, so a query
    // stopped the instant it starts never reaches the server's body at all.
    #[tokio::test]
    async fn an_already_cancelled_query_does_not_hang() {
        let client = KsqlClient::new(endpoint("http://127.0.0.1:1")).expect("client");
        let cancelled = Arc::new(AtomicBool::new(true));

        // Still an error, because the connection itself fails — the point is
        // that it returns rather than blocking.
        assert!(client.query_stream("SELECT 1;", cancelled, |_| {}, |_| {}).await.is_err());
    }
}
