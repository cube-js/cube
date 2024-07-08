use async_trait::async_trait;
use cubenativeutils::wrappers::object::{NativeObject, ResultFromJsValue};
use cubenativeutils::CubeError;
use neon::prelude::*;
use std::sync::Arc;

#[async_trait]
pub trait CubeEvaluator {
    async fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError>;
}

pub struct NeonCubeEvaluator {
    native_object: NativeObject,
}

impl NeonCubeEvaluator {
    pub fn new(native_object: NativeObject) -> Self {
        Self { native_object }
    }
}

#[async_trait]
impl CubeEvaluator for NeonCubeEvaluator {
    async fn parse_path(&self, path_type: String, path: String) -> Result<Vec<String>, CubeError> {
        self.native_object
            .call(
                "parsePath",
                Box::new(|holder| {
                    holder.add(path_type)?;
                    holder.add(path)?;

                    Ok(())
                }),
            )
            .await
    }
}
