/*
 * Cube.js
 *
 * Cube.js Swagger Schema
 *
 * The version of the OpenAPI document: 1.0.0
 *
 * Hand-maintained: openapi-generator does not emit a Rust type for top-level
 * array schemas, so we mirror the `V1LoadResultDataRow` schema here as a type
 * alias. This is the default payload type used by `load_v1` — it's a streamed
 * `Vec<serde_json::Value>` that avoids the buffered re-parse cost of the
 * untagged `V1LoadResultData` enum on the row-oriented hot path.
 */

pub type V1LoadResultDataRow = Vec<serde_json::Value>;
