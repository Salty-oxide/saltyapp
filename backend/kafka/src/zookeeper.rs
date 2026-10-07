use async_trait::async_trait;
use salty_core::ConnectionStatus;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Pings a Zookeeper ensemble member. This is a plain TCP reachability
/// check (no Zookeeper wire protocol handshake) — enough to answer "is
/// something listening on host:port", which is what the New Connection
/// modal's Zookeeper ping button needs. `timeout_ms` is the user's
/// configured General settings > Zookeeper > Timeout value.
#[async_trait]
pub trait ZookeeperClient: Send + Sync {
    async fn ping(&self, host: &str, port: u16, timeout_ms: u64) -> ConnectionStatus;
}

pub struct TcpZookeeperClient;

#[async_trait]
impl ZookeeperClient for TcpZookeeperClient {
    async fn ping(&self, host: &str, port: u16, timeout_ms: u64) -> ConnectionStatus {
        let addr = format!("{host}:{port}");
        match timeout(Duration::from_millis(timeout_ms), TcpStream::connect(&addr)).await {
            Ok(Ok(_)) => ConnectionStatus::Reachable,
            _ => ConnectionStatus::Unreachable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn reports_reachable_when_something_is_listening() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let client = TcpZookeeperClient;
        let status = client.ping("127.0.0.1", port, 10_000).await;

        assert_eq!(status, ConnectionStatus::Reachable);
    }

    #[tokio::test]
    async fn reports_unreachable_for_a_closed_port() {
        let client = TcpZookeeperClient;
        let status = client.ping("127.0.0.1", 1, 10_000).await;
        assert_eq!(status, ConnectionStatus::Unreachable);
    }

    #[tokio::test]
    async fn reports_unreachable_for_an_unresolvable_host() {
        let client = TcpZookeeperClient;
        let status = client
            .ping("this-host-does-not-resolve.invalid", 2181, 10_000)
            .await;
        assert_eq!(status, ConnectionStatus::Unreachable);
    }

    #[tokio::test]
    async fn respects_a_short_configured_timeout_against_a_non_routable_address() {
        // 10.255.255.1 is a non-routable address commonly used in tests to
        // simulate a connection attempt that hangs rather than failing fast
        // (unlike a closed port, which fails immediately with connection
        // refused). This proves `timeout_ms` is actually threaded through to
        // the TCP connect call, not just accepted and ignored.
        let client = TcpZookeeperClient;
        let started = std::time::Instant::now();
        let status = client.ping("10.255.255.1", 2181, 200).await;
        assert_eq!(status, ConnectionStatus::Unreachable);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "expected the 200ms timeout to be honored, took {:?}",
            started.elapsed()
        );
    }
}
