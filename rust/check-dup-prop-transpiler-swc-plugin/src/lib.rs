use std::collections::HashSet;

use swc_core::common::{BytePos, Span, DUMMY_SP};
use swc_core::ecma::visit::VisitMutWith;
use swc_core::plugin::errors::HANDLER;
use swc_core::plugin::{plugin_transform, proxies::TransformPluginProgramMetadata};
use swc_core::{
    ecma::{
        ast::*,
        visit::{visit_mut_pass, VisitMut},
    },
    plugin::proxies::PluginSourceMapProxy,
};

pub struct TransformVisitor {
    source_map: Option<PluginSourceMapProxy>,
}

impl TransformVisitor {
    fn emit_error(&self, span: Span, message: &str) {
        HANDLER.with(|handler| {
            handler
                .struct_span_err(span, &self.format_msg(span, message))
                .emit();
        });
    }

    fn format_msg(&self, span: Span, message: &str) -> String {
        if let Some(ref sm) = self.source_map {
            if let Some(source_file) = sm.source_file.get() {
                let loc = source_file.lookup_line(span.lo()).unwrap_or(0);
                let column = span.lo() - source_file.line_begin_pos(BytePos(loc as u32));
                format!(
                    "{}. Found in {}:{}:{}",
                    message,
                    source_file.name,
                    loc + 1,
                    column.0,
                )
            } else {
                message.to_string()
            }
        } else {
            message.to_string()
        }
    }

    fn compile_key(&self, key: &PropName) -> Option<String> {
        match key {
            PropName::Ident(ident) => Some(ident.sym.to_string()),
            PropName::Str(s) => Some(s.value.to_string()),
            _ => None,
        }
    }

    fn check_object_expr(&mut self, obj: &ObjectLit) {
        let mut unique = HashSet::new();

        for prop_or_spread in obj.props.iter() {
            if let PropOrSpread::Prop(prop_box) = prop_or_spread {
                if let Prop::KeyValue(kv) = &**prop_box {
                    if let Expr::Object(ref inner_obj) = *kv.value {
                        self.check_object_expr(inner_obj);
                    }
                    if let Some(key_name) = self.compile_key(&kv.key) {
                        if unique.contains(&key_name) {
                            let span = match &kv.key {
                                PropName::Ident(ident) => ident.span,
                                PropName::Str(s) => s.span,
                                _ => DUMMY_SP,
                            };
                            self.emit_error(span, &format!("Duplicate property <{}>", key_name));
                        } else {
                            unique.insert(key_name);
                        }
                    }
                }
            }
        }
    }
}

impl VisitMut for TransformVisitor {
    // Implement necessary visit_mut_* methods for actual custom transform.
    // A comprehensive list of possible visitor methods can be found here:
    // https://rustdoc.swc.rs/swc_ecma_visit/trait.VisitMut.html

    fn visit_mut_call_expr(&mut self, call_expr: &mut CallExpr) {
        if let Callee::Expr(callee_expr) = &call_expr.callee {
            if let Expr::Ident(ident) = &**callee_expr {
                if ident.sym == *"cube" || ident.sym == *"view" {
                    for arg in call_expr.args.iter() {
                        if let Expr::Object(ref obj_lit) = *arg.expr {
                            self.check_object_expr(obj_lit);
                        }
                    }
                }
            }
        }
        call_expr.visit_mut_children_with(self)
    }
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
    program.apply(&mut visit_mut_pass(TransformVisitor {
        source_map: Some(metadata.source_map),
    }))
}

// An example to test plugin transform.
// Recommended strategy to test plugin's transform is verify
// the Visitor's behavior, instead of trying to run `process_transform` with mocks
// unless explicitly required to do so.
test_inline!(
    Default::default(),
    |_| visit_mut_pass(TransformVisitor {}),
    boo,
    // Input codes
    r#"console.log("transform");"#,
    // Output codes after transformed with plugin
    r#"console.log("transform");"#
);
