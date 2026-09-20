//! Shutdown signalling shared by both servers.
//!
//! Without this, `axum::serve(...)` returns an error when the listener closes on
//! SIGTERM and the `.expect("server error")` panics: every rollout or scale-down
//! left an `Error` pod behind, which looks like a crash in `kubectl get pods`
//! even though the workload was healthy. Resolving the signal lets the server
//! stop accepting, finish in-flight requests, and exit zero.

/// Resolve when the process is asked to stop (SIGTERM or Ctrl-C).
///
/// Kubernetes sends SIGTERM on pod termination, so this is the difference
/// between a clean `Completed` and a misleading `Error` on every restart.
pub async fn signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            // No SIGTERM available: fall back to Ctrl-C only.
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn signal_helper_is_callable_as_a_future() {
        // Construction only: sending a real signal would end the test process.
        // This pins the public surface used by both binaries.
        let future = super::signal();
        drop(future);
    }
}
