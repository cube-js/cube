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
pub struct V1LoadResponse {
    #[serde(rename = "pivotQuery", skip_serializing_if = "Option::is_none")]
    pub pivot_query: Option<serde_json::Value>,
    #[serde(rename = "slowQuery", skip_serializing_if = "Option::is_none")]
    pub slow_query: Option<bool>,
    #[serde(rename = "queryType", skip_serializing_if = "Option::is_none")]
    pub query_type: Option<String>,
    #[serde(rename = "results")]
    pub results: Vec<models::V1LoadResult>,
}

impl V1LoadResponse {
    pub fn new(results: Vec<models::V1LoadResult>) -> V1LoadResponse {
        V1LoadResponse {
            pivot_query: None,
            slow_query: None,
            query_type: None,
            results,
        }
    }
}
