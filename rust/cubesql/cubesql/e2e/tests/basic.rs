use std::fmt::Debug;

use async_trait::async_trait;

#[derive(Debug)]
pub enum RunError {
    Other(String),
}

impl<T: std::error::Error> From<T> for RunError {
    fn from(e: T) -> Self {
        RunError::Other(e.to_string())
    }
}

pub type RunResult<R> = Result<R, RunError>;

#[async_trait]
pub trait AsyncTestSuite: Debug {
    async fn after_all(&mut self) -> RunResult<()>;

    async fn run(&mut self) -> RunResult<()>;
}

pub enum AsyncTestConstructorResult {
    Success(Box<dyn AsyncTestSuite>),
    Skipped(String),
}
