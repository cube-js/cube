use tokio_util::sync::CancellationToken;
pub struct CancellationGuard<'a> {
    token: &'a CancellationToken,
}

impl<'a> CancellationGuard<'a> {
    pub fn new(token: &'a CancellationToken) -> Self {
        Self { token }
    }
}

impl<'a> Drop for CancellationGuard<'a> {
    fn drop(&mut self) {
        self.token.cancel()
    }
}
