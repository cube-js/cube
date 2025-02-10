use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use swc_core::ecma::{
    ast::Program,
    transforms::testing::test_inline,
    visit::{visit_mut_pass, VisitMut},
};
use swc_core::plugin::{plugin_transform, proxies::TransformPluginProgramMetadata};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransformVisitor {
    cube_names: HashSet<String>,
    cube_symbols: HashMap<String, HashMap<String, bool>>,
}

impl VisitMut for TransformVisitor {
    // Implement necessary visit_mut_* methods for actual custom transform.
    // A comprehensive list of possible visitor methods can be found here:
    // https://rustdoc.swc.rs/swc_ecma_visit/trait.VisitMut.html
}

/// An example plugin function with macro support.
/// `plugin_transform` macro interop pointers into deserialized structs, as well
/// as returning ptr back to host.
///
/// It is possible to opt out from macro by writing transform fn manually
/// if plugin need to handle low-level ptr directly via
/// `__transform_plugin_process_impl(
///     ast_ptr: *const u8, ast_ptr_len: i32,
///     unresolved_mark: u32, should_enable_comments_proxy: i32) ->
///     i32 /*  0 for success, fail otherwise.
///             Note this is only for internal pointer interop result,
///             not actual transform result */`
///
/// This requires manual handling of serialization / deserialization from ptrs.
/// Refer swc_plugin_macro to see how does it work internally.
#[plugin_transform]
pub fn process_transform(program: Program, metadata: TransformPluginProgramMetadata) -> Program {
    let config_str = metadata.get_transform_plugin_config().unwrap_or_default();

    let visitor: TransformVisitor =
        serde_json::from_str(&config_str).expect("Incorrect plugin configuration");

    program.apply(&mut visit_mut_pass(visitor))
}

// An example to test plugin transform.
// Recommended strategy to test plugin's transform is verify
// the Visitor's behavior, instead of trying to run `process_transform` with mocks
// unless explicitly required to do so.
test_inline!(
    Default::default(),
    |_| visit_mut_pass(TransformVisitor {
        cube_names: HashSet::new(),
        cube_symbols: HashMap::new(),
    }),
    boo,
    // Input codes
    r#"console.log("transform");"#,
    // Output codes after transformed with plugin
    r#"console.log("transform");"#
);
