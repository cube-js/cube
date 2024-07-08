use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait JsObject {
    async fn call(&self, method: &str, args: &[Arc<dyn JsObject>]) -> Arc<dyn JsObject>;
    fn value(&self) -> serde_json::Value;
}
