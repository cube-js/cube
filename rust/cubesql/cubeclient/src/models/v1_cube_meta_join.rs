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
pub struct V1CubeMetaJoin {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "relationship")]
    pub relationship: String,
}

impl V1CubeMetaJoin {
    pub fn new(name: String, relationship: String) -> V1CubeMetaJoin {
        V1CubeMetaJoin { name, relationship }
    }
}
