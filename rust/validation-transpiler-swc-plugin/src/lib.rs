use swc_core::common::Span;
use swc_core::common::{errors::HANDLER, BytePos};
use swc_core::ecma::visit::noop_visit_mut_type;
use swc_core::plugin::proxies::PluginSourceMapProxy;
use swc_core::plugin::{plugin_transform, proxies::TransformPluginProgramMetadata};
use swc_core::{
    atoms::JsWord,
    ecma::{
        ast::*,
        visit::{visit_mut_pass, VisitMut},
    },
};

pub struct TransformVisitor {
    source_map: Option<PluginSourceMapProxy>,
}

impl TransformVisitor {
    fn emit_warn(&self, span: Span, message: &str) {
        HANDLER.with(|handler| {
            handler
                .struct_span_warn(span, &self.format_msg(span, message))
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
}

impl VisitMut for TransformVisitor {
    // Implement necessary visit_mut_* methods for actual custom transform.
    // A comprehensive list of possible visitor methods can be found here:
    // https://rustdoc.swc.rs/swc_ecma_visit/trait.VisitMut.html

    // We don't need to do anything besides checking identifiers here
    noop_visit_mut_type!();

    fn visit_mut_ident(&mut self, ident: &mut Ident) {
        let uc_id: JsWord = "USER_CONTEXT".into();
        if ident.sym == uc_id {
            self.emit_warn(
                ident.span,
                "Support for USER_CONTEXT was removed, please migrate to SECURITY_CONTEXT",
            );
            // TODO: How to report the errors?
            // @see https://rustdoc.swc.rs/swc_common/errors/struct.Handler.html
        }
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

#[cfg(test)]
mod tests {
    // Recommended strategy to test plugin's transform is verify
    // the Visitor's behavior, instead of trying to run `process_transform` with mocks
    // unless explicitly required to do so.

    use std::sync::{Arc, Mutex};

    use super::*;
    use swc_core::ecma::ast::{EsVersion, Program};
    use swc_core::{
        common::{
            errors::{DiagnosticBuilder, Emitter, Handler, HandlerFlags},
            sync::Lrc,
            FileName, Globals, SourceMap,
        },
        ecma::visit::VisitMutWith,
    };
    use swc_ecma_parser::{lexer::Lexer, Parser, StringInput, Syntax};

    struct TestEmitter {
        diagnostics: Arc<Mutex<Vec<String>>>,
    }

    impl Emitter for TestEmitter {
        fn emit(&mut self, diagnostic: &DiagnosticBuilder) {
            let mut diags = self.diagnostics.lock().unwrap();
            diags.push(diagnostic.message());
        }
    }

    #[test]
    fn test_warning_for_user_context() {
        let globals = Globals::new();
        let cm: Lrc<SourceMap> = Default::default();
        let diagnostics = Arc::new(Mutex::new(Vec::new()));
        let emitter = Box::new(TestEmitter {
            diagnostics: diagnostics.clone(),
        });
        let handler = Handler::with_emitter_and_flags(
            emitter,
            HandlerFlags {
                can_emit_warnings: true,
                ..Default::default()
            },
        );

        let js_code = "USER_CONTEXT.something;";

        swc_core::common::GLOBALS.set(&globals, || {
            HANDLER.set(&handler, || {
                let fm = cm.new_source_file(
                    Arc::new(FileName::Custom("input.js".into())),
                    js_code.into(),
                );
                let lexer = Lexer::new(
                    Syntax::Es(Default::default()),
                    EsVersion::Es2020,
                    StringInput::from(&*fm),
                    None,
                );
                let mut parser = Parser::new_from(lexer);
                let mut program: Program =
                    parser.parse_program().expect("Failed to parse the JS code");

                let mut visitor = TransformVisitor { source_map: None };
                program.visit_mut_with(&mut visitor);
            });
        });

        let diags = diagnostics.lock().unwrap();
        assert!(
            diags
                .iter()
                .any(|msg| msg.contains("Support for USER_CONTEXT was removed")),
            "Should emit warning",
        );
    }

    #[test]
    fn test_no_warnings() {
        let globals = Globals::new();
        let cm: Lrc<SourceMap> = Default::default();
        let diagnostics = Arc::new(Mutex::new(Vec::new()));
        let emitter = Box::new(TestEmitter {
            diagnostics: diagnostics.clone(),
        });
        let handler = Handler::with_emitter_and_flags(
            emitter,
            HandlerFlags {
                can_emit_warnings: true,
                ..Default::default()
            },
        );

        let js_code = "SECURITY_CONTEXT.something; let someOtherVar = 5;";

        swc_core::common::GLOBALS.set(&globals, || {
            HANDLER.set(&handler, || {
                let fm = cm.new_source_file(
                    Arc::new(FileName::Custom("input.js".into())),
                    js_code.into(),
                );
                let lexer = Lexer::new(
                    Syntax::Es(Default::default()),
                    EsVersion::Es2020,
                    StringInput::from(&*fm),
                    None,
                );
                let mut parser = Parser::new_from(lexer);
                let mut program: Program =
                    parser.parse_program().expect("Failed to parse the JS code");

                let mut visitor = TransformVisitor { source_map: None };
                program.visit_mut_with(&mut visitor);
            });
        });

        let diags = diagnostics.lock().unwrap();
        assert!(diags.is_empty(), "Should not emit warning",);
    }
}
