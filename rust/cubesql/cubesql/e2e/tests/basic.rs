use std::fmt::Debug;

use async_trait::async_trait;

pub type RunResult = Result<(), ()>;

#[async_trait]
pub trait AsyncTestSuite: Debug {
    async fn after_all(&mut self) -> RunResult;

    async fn run(&mut self) -> RunResult;
}

pub enum AsyncTestConstructorResult {
    Sucess(Box<dyn AsyncTestSuite>),
    Skipped(String),
}
