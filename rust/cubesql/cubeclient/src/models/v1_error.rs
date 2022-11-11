/*
 * Cube.js
 *
 * Cube.js Swagger Schema
 *
 * The version of the OpenAPI document: 1.0.0
 *
 * Generated by: https://openapi-generator.tech
 */

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1Error {
    #[serde(rename = "error")]
    pub error: String,
}

impl V1Error {
    pub fn new(error: String) -> V1Error {
        V1Error { error }
    }
}
