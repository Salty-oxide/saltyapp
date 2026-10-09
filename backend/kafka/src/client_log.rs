//! Log lines for the broker clients this app opens and closes.
//!
//! The Kafka crate has no handle on the app's log panel, so events go through
//! a process-wide sink that `src-tauri` installs at startup (the same shape as
//! [`crate::set_app_version`]). Without a sink — plain unit tests, the e2e
//! suite — events are dropped, so nothing here can fail a request.
//!
//! Every line leads with the connection's name in brackets, because the same
//! client id (`salty/<version>`) is shared by every client and says nothing
//! about *which* cluster a connection belongs to.

use std::sync::OnceLock;

/// Which of the app's clients an event is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientKind {
    /// The pooled metadata client behind the tree, status dot and listings.
    Metadata,
    /// The pooled admin client (configs, ACLs).
    Admin,
    /// A short-lived consumer built for one message fetch.
    Fetch,
    /// A short-lived producer built for one publish.
    Producer,
}

impl ClientKind {
    fn label(self) -> &'static str {
        match self {
            ClientKind::Metadata => "metadata",
            ClientKind::Admin => "admin",
            ClientKind::Fetch => "fetch consumer",
            ClientKind::Producer => "producer",
        }
    }
}

/// What happened to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientEvent {
    Opened,
    /// Several clients opened at once — a sharded fetch's consumers.
    OpenedMany(usize),
    /// Swapped for a new one because the old connection looked dead.
    Replaced,
    /// Closed after sitting unused for the idle TTL.
    ClosedIdle,
}

/// `[name] Opened metadata client to host:9092 as salty/1.5.4`.
///
/// Credentials are never part of the line: only the bootstrap servers and the
/// client id, which the broker logs anyway.
pub fn format_client_event(
    connection_name: &str,
    kind: ClientKind,
    event: &ClientEvent,
    bootstrap_servers: &str,
    client_id: &str,
) -> String {
    let label = kind.label();
    let what = match event {
        ClientEvent::Opened => {
            format!("Opened {label} client to {bootstrap_servers} as {client_id}")
        }
        ClientEvent::OpenedMany(count) => {
            format!("Opened {count} {label} clients to {bootstrap_servers} as {client_id}")
        }
        ClientEvent::Replaced => format!(
            "Replaced {label} client to {bootstrap_servers}: the old connection looked dead"
        ),
        ClientEvent::ClosedIdle => format!("Closed idle {label} client to {bootstrap_servers}"),
    };
    format!("[{connection_name}] {what}")
}

type Sink = Box<dyn Fn(&str, String) + Send + Sync>;

static SINK: OnceLock<Sink> = OnceLock::new();

/// Installs the receiver for client events — `(level, message)`. First call
/// wins; later ones are ignored.
pub fn set_client_log_sink(sink: impl Fn(&str, String) + Send + Sync + 'static) {
    let _ = SINK.set(Box::new(sink));
}

pub(crate) fn log_client_event(
    connection: &salty_core::Connection,
    kind: ClientKind,
    event: ClientEvent,
) {
    log_client_event_named(&connection.name, &connection.bootstrap_servers, kind, event);
}

pub(crate) fn log_client_event_named(
    connection_name: &str,
    bootstrap_servers: &str,
    kind: ClientKind,
    event: ClientEvent,
) {
    if let Some(sink) = SINK.get() {
        sink(
            "info",
            format_client_event(
                connection_name,
                kind,
                &event,
                bootstrap_servers,
                crate::config::broker_client_id(),
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(kind: ClientKind, event: ClientEvent) -> String {
        format_client_event("prod-eu", kind, &event, "b1:9092", "salty/1.5.4")
    }

    #[test]
    fn every_line_starts_with_the_connection_name_in_brackets() {
        for kind in [
            ClientKind::Metadata,
            ClientKind::Admin,
            ClientKind::Fetch,
            ClientKind::Producer,
        ] {
            assert!(line(kind, ClientEvent::Opened).starts_with("[prod-eu] "));
        }
    }

    #[test]
    fn an_opened_client_names_its_kind_cluster_and_client_id() {
        assert_eq!(
            line(ClientKind::Metadata, ClientEvent::Opened),
            "[prod-eu] Opened metadata client to b1:9092 as salty/1.5.4"
        );
    }

    #[test]
    fn a_sharded_fetch_reports_how_many_consumers_it_opened() {
        assert_eq!(
            line(ClientKind::Fetch, ClientEvent::OpenedMany(4)),
            "[prod-eu] Opened 4 fetch consumer clients to b1:9092 as salty/1.5.4"
        );
    }

    #[test]
    fn replaced_and_idle_closed_clients_are_distinguished() {
        assert!(line(ClientKind::Admin, ClientEvent::Replaced).contains("Replaced admin client"));
        assert_eq!(
            line(ClientKind::Admin, ClientEvent::ClosedIdle),
            "[prod-eu] Closed idle admin client to b1:9092"
        );
    }

    #[test]
    fn logging_without_a_sink_is_a_no_op() {
        // No sink is installed in this crate's unit tests.
        log_client_event_named("x", "h:1", ClientKind::Metadata, ClientEvent::Opened);
    }
}
