//! The bounded TCP listener: the accept seam that enforces
//! `RuntimeConfig::max_connections` ([ADR 0010]).
//!
//! [ADR 0010]: ../../docs/decisions/0010-transport-edge-in-flight-body-budget.md

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::serve::Listener;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;

/// A [`TcpListener`] whose accept seam admits at most `max_connections`
/// concurrent connections ([ADR 0010]): each accepted connection holds an
/// owned semaphore permit for exactly as long as its socket lives, and a
/// connection is only accepted once a permit is available. When the cap is
/// exhausted, `accept` pends — sockets queue in the kernel backlog instead
/// of being served past the cap.
pub struct BoundedListener {
    inner: TcpListener,
    connections: Arc<Semaphore>,
}

impl BoundedListener {
    /// Wraps `inner`, admitting at most `max_connections` connections at a
    /// time.
    #[must_use]
    pub fn new(inner: TcpListener, max_connections: usize) -> Self {
        Self {
            inner,
            connections: Arc::new(Semaphore::new(max_connections)),
        }
    }
}

impl Listener for BoundedListener {
    type Io = PermittedStream;
    type Addr = SocketAddr;

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }

    fn accept(&mut self) -> impl Future<Output = (Self::Io, Self::Addr)> + Send {
        let inner = &self.inner;
        let connections = &self.connections;
        async move {
            loop {
                // The owned permit is acquired BEFORE the kernel accept:
                // when the cap is exhausted this loop pends here, so
                // sockets stay in the kernel backlog instead of being
                // admitted past the cap. The permit lives in the returned
                // connection and frees exactly when that connection ends.
                let Ok(permit) = connections.clone().acquire_owned().await else {
                    // The semaphore closes only when every Arc is dropped,
                    // which cannot happen while this listener is served —
                    // the listener itself holds one for its whole life.
                    unreachable!("the connect semaphore outlives the listener");
                };
                match inner.accept().await {
                    Ok((stream, addr)) => {
                        return (
                            PermittedStream {
                                inner: stream,
                                _permit: permit,
                            },
                            addr,
                        );
                    }
                    Err(error) => {
                        // The axum trait contract: accept errors are
                        // handled here, not returned. The failed attempt
                        // never became a connection, so its permit is
                        // released before retrying, mirroring axum's own
                        // policy: transient connection errors retry
                        // immediately, permanent ones (e.g. EMFILE) log
                        // and back off.
                        drop(permit);
                        if matches!(
                            error.kind(),
                            io::ErrorKind::ConnectionRefused
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::ConnectionReset
                        ) {
                            continue;
                        }
                        tracing::error!("accept error: {error}");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

/// A [`TcpStream`] that holds the permit which admitted it: dropping the
/// connection releases its permit, so a connection's share of the cap
/// lasts exactly as long as the connection itself.
pub struct PermittedStream {
    inner: TcpStream,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl AsyncRead for PermittedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for PermittedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
