use std::collections::BTreeSet;

use anyhow::{bail, Result};
use serde_json::{json, Map, Value};

use crate::{output, Ctx};

/// The HTTP verbs an OpenAPI path item can hold. Anything else under a path
/// (`parameters`, `summary`, `$ref`, extensions) is not an operation.
const METHODS: [&str; 8] = [
    "get", "put", "post", "patch", "delete", "options", "head", "trace",
];

const SCHEMA_REF_PREFIX: &str = "#/components/schemas/";
const COMPONENT_REF_PREFIX: &str = "#/components/";

/// Fetch the API's own OpenAPI document, so every endpoint, parameter and
/// schema can be discovered at runtime rather than guessed.
///
/// Without `--json` this prints an index of operations, which is what a human
/// scanning for an endpoint wants. With `--json` it prints OpenAPI: the whole
/// document when unfiltered, or — when a pattern is given — a valid but much
/// smaller document containing only the matching operations plus the transitive
/// closure of the schemas they reference. That closure is the point of the
/// filter: an agent asking "what does this endpoint take?" needs the request
/// body's schema resolved, not a dangling `$ref` that forces it to pull the
/// whole spec anyway.
#[derive(clap::Args)]
pub struct Args {
    /// Show only operations whose method, path, summary or operationId contains
    /// this text (case-insensitive)
    pattern: Option<String>,
}

/// One operation, flattened out of the nested `paths` → method structure.
struct Operation<'a> {
    path: &'a str,
    method: &'a str,
    op: &'a Value,
}

impl Operation<'_> {
    fn summary(&self) -> &str {
        self.op
            .get("summary")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    fn operation_id(&self) -> &str {
        self.op
            .get("operationId")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    fn matches(&self, needle: &str) -> bool {
        let haystack = format!(
            "{} {} {} {}",
            self.method,
            self.path,
            self.summary(),
            self.operation_id()
        )
        .to_lowercase();
        haystack.contains(needle)
    }
}

fn operations(spec: &Value) -> Vec<Operation<'_>> {
    let Some(paths) = spec.get("paths").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (path, item) in paths {
        let Some(item) = item.as_object() else {
            continue;
        };
        // Iterate METHODS rather than the object's keys so operations always
        // come out in verb order, not whatever order the server serialized.
        for method in METHODS {
            if let Some(op) = item.get(method) {
                out.push(Operation { path, method, op });
            }
        }
    }
    out
}

/// Every `#/components/schemas/...` name referenced anywhere inside `value`.
fn collect_refs(value: &Value, out: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "$ref" {
                    if let Some(name) = child
                        .as_str()
                        .and_then(|r| r.strip_prefix(SCHEMA_REF_PREFIX))
                    {
                        out.insert(name.to_string());
                    }
                }
                collect_refs(child, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_refs(item, out);
            }
        }
        _ => {}
    }
}

/// Expand a set of schema names to include everything they reference, however
/// deeply. Schemas are mutually recursive in places, so this runs to a fixpoint
/// over a visited set rather than recursing through the graph.
fn schema_closure(spec: &Value, seeds: BTreeSet<String>) -> BTreeSet<String> {
    let all = spec
        .get("components")
        .and_then(|c| c.get("schemas"))
        .and_then(Value::as_object);
    let Some(all) = all else {
        return BTreeSet::new();
    };

    let mut resolved: BTreeSet<String> = BTreeSet::new();
    let mut pending: Vec<String> = seeds.into_iter().collect();
    while let Some(name) = pending.pop() {
        if !resolved.insert(name.clone()) {
            continue;
        }
        if let Some(schema) = all.get(&name) {
            let mut refs = BTreeSet::new();
            collect_refs(schema, &mut refs);
            pending.extend(refs.into_iter().filter(|r| !resolved.contains(r)));
        }
    }
    resolved
}

/// Build a valid OpenAPI document holding only `matched`, carrying over the
/// document-level fields a client needs to make sense of it (version, servers,
/// security) plus just the schemas those operations reach.
fn filtered_document(spec: &Value, matched: &[&Operation<'_>]) -> Value {
    let mut paths = Map::new();
    let mut seeds = BTreeSet::new();
    for op in matched {
        collect_refs(op.op, &mut seeds);
        let item = paths
            .entry(op.path.to_string())
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .expect("path item is an object");
        item.insert(op.method.to_string(), op.op.clone());

        // A path item may declare `parameters` that apply to every operation
        // under it. Dropping them would silently shrink the parameter list —
        // the one thing this command exists to report — so they come along and
        // their refs are seeded too.
        if let Some(shared) = spec
            .get("paths")
            .and_then(|p| p.get(op.path))
            .and_then(|i| i.get("parameters"))
        {
            collect_refs(shared, &mut seeds);
            item.insert("parameters".into(), shared.clone());
        }
    }

    let names = schema_closure(spec, seeds);
    let mut schemas = Map::new();
    if let Some(all) = spec
        .get("components")
        .and_then(|c| c.get("schemas"))
        .and_then(Value::as_object)
    {
        for name in &names {
            if let Some(schema) = all.get(name) {
                schemas.insert(name.clone(), schema.clone());
            }
        }
    }

    let mut components = Map::new();
    components.insert("schemas".into(), Value::Object(schemas));
    // Security schemes are tiny and tell the reader how to authenticate the
    // operations it just asked about, so they ride along.
    if let Some(security_schemes) = spec
        .get("components")
        .and_then(|c| c.get("securitySchemes"))
    {
        components.insert("securitySchemes".into(), security_schemes.clone());
    }

    let mut doc = Map::new();
    for key in ["openapi", "info", "servers", "security"] {
        if let Some(value) = spec.get(key) {
            doc.insert(key.to_string(), value.clone());
        }
    }
    doc.insert("paths".into(), Value::Object(paths));
    doc.insert("components".into(), Value::Object(components));
    Value::Object(doc)
}

/// Every `#/components/<bucket>/<name>` ref in `value`, as (bucket, name).
fn collect_component_refs(value: &Value, out: &mut BTreeSet<(String, String)>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if key == "$ref" {
                    if let Some(rest) = child
                        .as_str()
                        .and_then(|r| r.strip_prefix(COMPONENT_REF_PREFIX))
                    {
                        if let Some((bucket, name)) = rest.split_once('/') {
                            out.insert((bucket.to_string(), name.to_string()));
                        }
                    }
                }
                collect_component_refs(child, out);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_component_refs(item, out);
            }
        }
        _ => {}
    }
}

/// Refuse to emit a document whose `$ref`s don't resolve inside it.
///
/// The closure only follows `#/components/schemas/`, which is everything the
/// current spec uses. If an operation ever references another bucket
/// (`parameters`, `responses`, `requestBodies`, …), the filtered document would
/// still carry the `$ref` but not its target: a validator rejects it and an
/// agent resolves it to nothing. Since this output is meant to be consumed
/// unattended, fail loudly rather than hand back something quietly wrong.
fn check_no_dangling_refs(doc: &Value) -> Result<()> {
    let mut refs = BTreeSet::new();
    collect_component_refs(doc, &mut refs);

    let dangling: Vec<String> = refs
        .into_iter()
        .filter(|(bucket, name)| {
            doc.get("components")
                .and_then(|c| c.get(bucket))
                .and_then(|b| b.get(name))
                .is_none()
        })
        .map(|(bucket, name)| format!("#/components/{bucket}/{name}"))
        .collect();

    if !dangling.is_empty() {
        bail!(
            "internal error building the filtered spec: unresolved {} — \
             re-run without a pattern to get the whole document",
            dangling.join(", ")
        );
    }
    Ok(())
}

pub async fn command(args: Args, ctx: &Ctx) -> Result<()> {
    let spec = ctx.api()?.get("/api/v1/spec", &Vec::new()).await?;

    // Unfiltered JSON is the raw document — no reshaping, so it can be piped
    // straight into a generator or a validator.
    let Some(pattern) = args.pattern.as_deref() else {
        if ctx.json {
            output::print_json(&spec);
        } else {
            print_index(&operations(&spec).iter().collect::<Vec<_>>());
        }
        return Ok(());
    };

    let needle = pattern.to_lowercase();
    let all = operations(&spec);
    let matched: Vec<&Operation<'_>> = all.iter().filter(|op| op.matches(&needle)).collect();

    if matched.is_empty() {
        bail!("no operation matches `{pattern}` — run `cube spec` to list them all");
    }

    if ctx.json {
        let doc = filtered_document(&spec, &matched);
        check_no_dangling_refs(&doc)?;
        output::print_json(&doc);
    } else {
        print_index(&matched);
    }
    Ok(())
}

fn print_index(operations: &[&Operation<'_>]) {
    let rows = operations
        .iter()
        .map(|op| {
            vec![
                op.method.to_uppercase(),
                op.path.to_string(),
                op.summary().to_string(),
            ]
        })
        .collect();
    output::table(&["METHOD", "PATH", "SUMMARY"], rows);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> Value {
        json!({
            "openapi": "3.1.0",
            "info": { "title": "t", "version": "1" },
            "paths": {
                "/api/v1/deployments/{id}/settings": {
                    "get": { "operationId": "getSettings", "summary": "Get deployment settings" },
                    "put": {
                        "operationId": "updateSettings",
                        "summary": "Update deployment settings",
                        "requestBody": { "content": { "application/json": {
                            "schema": { "$ref": "#/components/schemas/UpdateDeploymentInput" } } } }
                    },
                    "parameters": [{ "name": "id", "in": "path" }]
                },
                "/api/v1/regions": { "get": { "operationId": "listRegions", "summary": "List regions" } }
            },
            "components": {
                "securitySchemes": { "bearerAuth": { "type": "http" } },
                "schemas": {
                    "UpdateDeploymentInput": { "properties": {
                        "cspsConfig": { "$ref": "#/components/schemas/CspsConfig" } } },
                    "CspsConfig": { "properties": {
                        "self": { "$ref": "#/components/schemas/CspsConfig" } } },
                    "Unrelated": { "type": "object" }
                }
            }
        })
    }

    #[test]
    fn lists_operations_in_verb_order_ignoring_non_operation_keys() {
        let spec = spec();
        let ops = operations(&spec);
        assert_eq!(ops.len(), 3);
        // `parameters` on the path item is not an operation.
        assert!(ops.iter().all(|o| METHODS.contains(&o.method)));
        let settings: Vec<&str> = ops
            .iter()
            .filter(|o| o.path.ends_with("/settings"))
            .map(|o| o.method)
            .collect();
        assert_eq!(settings, vec!["get", "put"]);
    }

    #[test]
    fn matches_on_path_summary_and_operation_id() {
        let spec = spec();
        let ops = operations(&spec);
        assert_eq!(ops.iter().filter(|o| o.matches("settings")).count(), 2);
        assert_eq!(ops.iter().filter(|o| o.matches("listregions")).count(), 1);
        // Case-insensitive, and the method is part of the haystack.
        assert_eq!(ops.iter().filter(|o| o.matches("put")).count(), 1);
        assert_eq!(ops.iter().filter(|o| o.matches("nope")).count(), 0);
    }

    #[test]
    fn filtered_document_carries_the_reachable_schemas_only() {
        let spec = spec();
        let ops = operations(&spec);
        let matched: Vec<&Operation<'_>> = ops.iter().filter(|o| o.matches("settings")).collect();
        let doc = filtered_document(&spec, &matched);

        let paths = doc["paths"].as_object().unwrap();
        assert_eq!(paths.len(), 1);
        let item = paths["/api/v1/deployments/{id}/settings"]
            .as_object()
            .unwrap();
        assert!(item.contains_key("get") && item.contains_key("put"));

        let schemas = doc["components"]["schemas"].as_object().unwrap();
        // Reached through the request body, and through CspsConfig's self-ref
        // (which must terminate rather than spin).
        assert!(schemas.contains_key("UpdateDeploymentInput"));
        assert!(schemas.contains_key("CspsConfig"));
        assert!(!schemas.contains_key("Unrelated"));
        // Still a usable document: version, info and auth survive.
        assert_eq!(doc["openapi"], "3.1.0");
        assert!(doc["info"].is_object());
        assert!(doc["components"]["securitySchemes"]["bearerAuth"].is_object());
        // Nothing points outside the document it just built.
        check_no_dangling_refs(&doc).unwrap();
    }

    #[test]
    fn filtered_document_keeps_path_level_parameters() {
        let spec = spec();
        let ops = operations(&spec);
        let matched: Vec<&Operation<'_>> = ops.iter().filter(|o| o.matches("settings")).collect();
        let doc = filtered_document(&spec, &matched);

        // `id` is declared once on the path item, not per operation. Losing it
        // would understate the endpoint's parameters — the exact thing this
        // command is supposed to report.
        let params = doc["paths"]["/api/v1/deployments/{id}/settings"]["parameters"]
            .as_array()
            .expect("path-level parameters survive filtering");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0]["name"], "id");
    }

    #[test]
    fn dangling_component_refs_are_rejected() {
        // A ref into a bucket the closure doesn't follow: the document keeps the
        // `$ref` but not its target, so it must be refused rather than emitted.
        let doc = json!({
            "openapi": "3.1.0",
            "paths": { "/x": { "get": {
                "parameters": [{ "$ref": "#/components/parameters/Missing" }] } } },
            "components": { "schemas": {} }
        });
        let err = check_no_dangling_refs(&doc).unwrap_err().to_string();
        assert!(err.contains("#/components/parameters/Missing"), "{err}");

        // A resolvable ref in a non-schema bucket is fine.
        let ok = json!({
            "openapi": "3.1.0",
            "paths": { "/x": { "get": {
                "parameters": [{ "$ref": "#/components/parameters/Present" }] } } },
            "components": { "parameters": { "Present": { "name": "p", "in": "query" } } }
        });
        check_no_dangling_refs(&ok).unwrap();
    }
}
