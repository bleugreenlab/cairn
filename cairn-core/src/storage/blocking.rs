use std::future::Future;

pub(crate) fn run_db_blocking<T, F, Fut>(make_future: F) -> Result<T, String>
where
    T: Send,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, String>>,
{
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Failed to start database runtime: {e}"))?
                    .block_on(make_future())
            })
            .join()
            .map_err(|_| "Database task panicked".to_string())?
    })
}
