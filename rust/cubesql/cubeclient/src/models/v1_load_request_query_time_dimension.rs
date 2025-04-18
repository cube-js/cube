/*
 * Cube.js
 *
 * Cube.js Swagger Schema
 *
 * The version of the OpenAPI document: 1.0.0
 *
 * Generated by: https://openapi-generator.tech
 */

use crate::models;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct V1LoadRequestQueryTimeDimension {
    #[serde(rename = "dimension")]
    pub dimension: String,
    #[serde(rename = "granularity", skip_serializing_if = "Option::is_none")]
    pub granularity: Option<String>,
    #[serde(rename = "dateRange", skip_serializing_if = "Option::is_none")]
    pub date_range: Option<serde_json::Value>,
}

impl V1LoadRequestQueryTimeDimension {
    pub fn new(dimension: String) -> V1LoadRequestQueryTimeDimension {
        V1LoadRequestQueryTimeDimension {
            dimension,
            granularity: None,
            date_range: None,
        }
    }
}
