use tokio_util::sync::CancellationToken;
pub struct CancellationGuard {
    token: CancellationToken,
}

impl CancellationGuard {
    pub fn new(token: CancellationToken) -> Self {
        Self { token }
    }
}

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        self.token.cancel()
    }
}
