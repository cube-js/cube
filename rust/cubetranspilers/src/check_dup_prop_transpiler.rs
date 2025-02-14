use std::collections::HashSet;

use swc_core::common::{BytePos, Span, DUMMY_SP};
use swc_core::ecma::visit::VisitMutWith;
use swc_core::plugin::errors::HANDLER;
use swc_core::plugin::proxies::TransformPluginProgramMetadata;
use swc_core::{
    ecma::{
        ast::*,
        visit::{visit_mut_pass, VisitMut},
    },
    plugin::proxies::PluginSourceMapProxy,
};

pub struct CheckDupPropTransformVisitor {
    pub(crate) source_map: Option<PluginSourceMapProxy>,
}

impl CheckDupPropTransformVisitor {
    pub fn new(source_map: Option<PluginSourceMapProxy>) -> Self {
        CheckDupPropTransformVisitor { source_map }
    }

    fn emit_error(&mut self, span: Span, message: &str) {
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
                            self.emit_error(
                                span,
                                &format!("Duplicate property parsing {}", key_name),
                            );
                        } else {
                            unique.insert(key_name);
                        }
                    }
                }
            }
        }
    }
}

impl VisitMut for CheckDupPropTransformVisitor {
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

pub fn process_transform(program: Program, metadata: TransformPluginProgramMetadata) -> Program {
    program.apply(&mut visit_mut_pass(CheckDupPropTransformVisitor {
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
    fn test_errors_for_duplicates_first_level() {
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

        let js_code = r#"
            cube(`cube1`, {
                sql: `SELECT * FROM table`,

                dimensions: {
                    id: {
                        sql: `id`,
                        type: `number`,
                        primary_key: true,
                    },
                    created_at: {
                        sql: `created_at`,
                        type: `time`,
                    },
                    dim1Number: {
                        sql: `dim1Number`,
                        type: `number`,
                    },
                    dim2Number: {
                        sql: `dim2Number`,
                        type: `number`,
                    },
                },

                dimensions: {
                    dim2Number: {
                        sql: `dim2Number`,
                        type: `number`,
                    },
                },

                measures: {
                    count: {
                        type: `count`,
                        sql: `id`,
                    },
                    measureDim1: {
                        sql: `dim1Number`,
                        type:
                            `max`,
                    },
                    measureDim2: {
                        sql: `dim1Number`,
                        type: `min`,
                    },
                },
            });
            "#;

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

                let mut visitor = CheckDupPropTransformVisitor { source_map: None };
                program.visit_mut_with(&mut visitor);
            });
        });

        let diags = diagnostics.lock().unwrap();
        let msgs: Vec<_> = diags
            .iter()
            .filter(|msg| msg.contains("Duplicate property"))
            .collect();
        assert!(msgs.len() == 1, "Should emit errors",);
    }

    #[test]
    fn test_errors_for_duplicates_deep_level() {
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

        let js_code = r#"
            cube(`cube1`, {
                sql: `SELECT * FROM table`,

                dimensions: {
                    id: {
                        sql: `id`,
                        type: `number`,
                        primary_key: true,
                    },
                    created_at: {
                        sql: `created_at`,
                        type: `time`,
                    },
                    dim1Number: {
                        sql: `dim1Number`,
                        type: `number`,
                    },
                    dim1Number: {
                        sql: `dim2Number`,
                        type: `number`,
                    },
                },

                dimensions: {
                    dim2Number: {
                        sql: `dim2Number`,
                        type: `number`,
                    },
                },

                measures: {
                    count: {
                        type: `count`,
                        sql: `id`,
                    },
                    measureDim1: {
                        sql: `dim1Number`,
                        type:
                            `max`,
                    },
                    measureDim1: {
                        sql: `dim1Number`,
                        type: `min`,
                    },
                },
            });
            "#;

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

                let mut visitor = CheckDupPropTransformVisitor { source_map: None };
                program.visit_mut_with(&mut visitor);
            });
        });

        let diags = diagnostics.lock().unwrap();
        let msgs: Vec<_> = diags
            .iter()
            .filter(|msg| msg.contains("Duplicate property"))
            .collect();
        assert!(msgs.len() == 3, "Should emit errors",);
    }

    #[test]
    fn test_no_errors() {
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

        let js_code = r#"
            cube(`cube1`, {
                sql: `SELECT * FROM table`,

                dimensions: {
                    id: {
                        sql: `id`,
                        type: `number`,
                        primary_key: true,
                    },
                    created_at: {
                        sql: `created_at`,
                        type: `time`,
                    },
                    dim1Number: {
                        sql: `dim1Number`,
                        type: `number`,
                    },
                    dim2Number: {
                        sql: `dim2Number`,
                        type: `number`,
                    },
                },

                measures: {
                    count: {
                        type: `count`,
                        sql: `id`,
                    },
                    measureDim1: {
                        sql: `dim1Number`,
                        type:
                            `max`,
                    },
                    measureDim2: {
                        sql: `dim1Number`,
                        type: `min`,
                    },
                },
            });
            "#;

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

                let mut visitor = CheckDupPropTransformVisitor { source_map: None };
                program.visit_mut_with(&mut visitor);
            });
        });

        let diags = diagnostics.lock().unwrap();
        assert!(diags.is_empty(), "Should not emit errors",);
    }
}
