use swc_core::common::errors::Handler;
use swc_core::common::BytePos;
use swc_core::common::{Span, SyntaxContext, DUMMY_SP};
use swc_core::ecma::visit::{noop_visit_mut_type, VisitMutWith};
use swc_core::{
    ecma::{ast::*, visit::VisitMut},
    plugin::proxies::PluginSourceMapProxy,
};

pub struct ImportExportTransformVisitor<'a> {
    pub(crate) source_map: Option<PluginSourceMapProxy>,
    handler: &'a Handler,
}

impl<'a> ImportExportTransformVisitor<'a> {
    pub fn new(source_map: Option<PluginSourceMapProxy>, handler: &'a Handler) -> Self {
        ImportExportTransformVisitor {
            source_map,
            handler,
        }
    }

    fn emit_error(&self, span: Span, message: &str) {
        self.handler
            .struct_span_err(span, &self.format_msg(span, message))
            .emit();
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

impl VisitMut for ImportExportTransformVisitor<'_> {
    // Implement necessary visit_mut_* methods for actual custom transform.
    // A comprehensive list of possible visitor methods can be found here:
    // https://rustdoc.swc.rs/swc_ecma_visit/trait.VisitMut.html

    // We don't need to do anything besides imports here
    noop_visit_mut_type!();

    // Can't use visit_mut_module_item for cases when we need to replace
    // the item with multiple statements
    fn visit_mut_module(&mut self, module: &mut Module) {
        let mut new_body = Vec::with_capacity(module.body.len());

        for mut item in module.body.drain(..) {
            self.visit_mut_module_item(&mut item);

            match &item {
                ModuleItem::ModuleDecl(ModuleDecl::ExportDecl(export_decl)) => {
                    let decl = export_decl.decl.clone();
                    let stmt_decl = ModuleItem::Stmt(Stmt::Decl(decl.clone()));

                    let mut ids = vec![];

                    match decl {
                        Decl::Var(var_decl) => {
                            for var_declarator in var_decl.decls.iter() {
                                if let Pat::Ident(BindingIdent { id, .. }) = &var_declarator.name {
                                    ids.push(id.clone());
                                }
                            }
                        }
                        Decl::Fn(fn_decl) => {
                            ids.push(fn_decl.ident.clone());
                        }
                        Decl::Class(class_decl) => {
                            ids.push(class_decl.ident.clone());
                        }
                        _ => {}
                    }

                    let props: Vec<PropOrSpread> = ids
                        .iter()
                        .map(|ident| {
                            PropOrSpread::Prop(Box::new(Prop::KeyValue(KeyValueProp {
                                key: PropName::Ident(IdentName::from(ident.sym.clone())),
                                value: Box::new(Expr::Ident(ident.clone())),
                            })))
                        })
                        .collect();

                    let add_export_call = Expr::Call(CallExpr {
                        span: DUMMY_SP,
                        callee: Callee::Expr(Box::new(Expr::Ident(Ident::new(
                            "addExport".into(),
                            DUMMY_SP,
                            SyntaxContext::empty(),
                        )))),
                        args: vec![ExprOrSpread {
                            spread: None,
                            expr: Box::new(Expr::Object(ObjectLit {
                                span: DUMMY_SP,
                                props,
                            })),
                        }],
                        type_args: None,
                        ctxt: SyntaxContext::empty(),
                    });

                    let stmt_add_export = ModuleItem::Stmt(Stmt::Expr(ExprStmt {
                        span: DUMMY_SP,
                        expr: Box::new(add_export_call),
                    }));

                    new_body.extend(vec![stmt_decl, stmt_add_export]);
                }
                _ => new_body.push(item),
            }
        }

        module.body = new_body;
    }

    fn visit_mut_module_item(&mut self, item: &mut ModuleItem) {
        if let ModuleItem::ModuleDecl(decl) = item {
            match decl {
                ModuleDecl::Import(import_decl) => {
                    let require_call = Expr::Call(CallExpr {
                        span: DUMMY_SP,
                        callee: Callee::Expr(Box::new(Expr::Ident(Ident::new(
                            "require".into(),
                            DUMMY_SP,
                            SyntaxContext::empty(),
                        )))),
                        args: vec![ExprOrSpread {
                            spread: None,
                            expr: Box::new(Expr::Lit(Lit::Str(*import_decl.src.clone()))),
                        }],
                        type_args: None,
                        ctxt: SyntaxContext::empty(),
                    });

                    let mut var_decls = Vec::with_capacity(import_decl.specifiers.len());

                    for spec in &import_decl.specifiers {
                        match spec {
                            ImportSpecifier::Named(named) => {
                                let local_ident = named.local.clone();
                                let init_expr = if let Some(imported) = &named.imported {
                                    match imported {
                                        ModuleExportName::Ident(id) => Expr::Member(MemberExpr {
                                            span: DUMMY_SP,
                                            obj: Box::new(require_call.clone()),
                                            prop: MemberProp::Ident(IdentName {
                                                span: DUMMY_SP,
                                                sym: id.sym.clone(),
                                            }),
                                        }),
                                        ModuleExportName::Str(s) => Expr::Member(MemberExpr {
                                            span: DUMMY_SP,
                                            obj: Box::new(require_call.clone()),
                                            prop: MemberProp::Computed(ComputedPropName {
                                                span: DUMMY_SP,
                                                expr: Box::new(Expr::Lit(Lit::Str(s.clone()))),
                                            }),
                                        }),
                                    }
                                } else {
                                    Expr::Member(MemberExpr {
                                        span: DUMMY_SP,
                                        obj: Box::new(require_call.clone()),
                                        prop: MemberProp::Ident(IdentName {
                                            span: DUMMY_SP,
                                            sym: local_ident.sym.clone(),
                                        }),
                                    })
                                };

                                let var_decl = VarDeclarator {
                                    span: DUMMY_SP,
                                    name: Pat::Ident(BindingIdent {
                                        id: local_ident,
                                        type_ann: None,
                                    }),
                                    init: Some(Box::new(init_expr)),
                                    definite: false,
                                };
                                var_decls.push(var_decl);
                            }
                            ImportSpecifier::Default(default) => {
                                let local_ident = default.local.clone();
                                let var_decl = VarDeclarator {
                                    span: DUMMY_SP,
                                    name: Pat::Ident(BindingIdent {
                                        id: local_ident,
                                        type_ann: None,
                                    }),
                                    init: Some(Box::new(require_call.clone())),
                                    definite: false,
                                };
                                var_decls.push(var_decl);
                            }
                            ImportSpecifier::Namespace(star_as) => {
                                self.emit_error(star_as.span, "Namespace import not supported");
                            }
                        }
                    }

                    *item = ModuleItem::Stmt(Stmt::Decl(Decl::Var(Box::new(VarDecl {
                        span: DUMMY_SP,
                        kind: VarDeclKind::Const,
                        declare: false,
                        decls: var_decls,
                        ctxt: SyntaxContext::empty(),
                    }))));
                }
                ModuleDecl::ExportNamed(export_named) => {
                    // For named exports we collect object properties for each specifier
                    let mut props = Vec::with_capacity(export_named.specifiers.len());
                    for spec in &export_named.specifiers {
                        match spec {
                            ExportSpecifier::Named(named_spec) => {
                                // Cases like `export { foo as bar }`
                                let key = if let Some(exported) = &named_spec.exported {
                                    match exported {
                                        ModuleExportName::Ident(id) => PropName::Ident(IdentName {
                                            span: DUMMY_SP,
                                            sym: id.sym.clone(),
                                        }),
                                        ModuleExportName::Str(s) => PropName::Str(s.clone()),
                                    }
                                } else {
                                    match &named_spec.orig {
                                        ModuleExportName::Ident(id) => PropName::Ident(IdentName {
                                            span: DUMMY_SP,
                                            sym: id.sym.clone(),
                                        }),
                                        ModuleExportName::Str(s) => PropName::Str(s.clone()),
                                    }
                                };
                                let value_expr = match &named_spec.orig {
                                    ModuleExportName::Ident(id) => Expr::Ident(id.clone()),
                                    ModuleExportName::Str(s) => Expr::Lit(Lit::Str(s.clone())),
                                };
                                let prop =
                                    PropOrSpread::Prop(Box::new(Prop::KeyValue(KeyValueProp {
                                        key,
                                        value: Box::new(value_expr),
                                    })));
                                props.push(prop);
                            }
                            ExportSpecifier::Namespace(_export_namespace_specifier) => {
                                self.emit_error(
                                    export_named.span,
                                    "Unsupported export specifier: Named Namespace",
                                );
                            }
                            ExportSpecifier::Default(_export_default_specifier) => {
                                self.emit_error(
                                    export_named.span,
                                    "Unsupported export specifier: Named Default",
                                );
                            }
                        }
                    }
                    let obj_expr = Expr::Object(ObjectLit {
                        span: DUMMY_SP,
                        props,
                    });
                    let add_export_call = Expr::Call(CallExpr {
                        span: DUMMY_SP,
                        callee: Callee::Expr(Box::new(Expr::Ident(Ident::new(
                            "addExport".into(),
                            DUMMY_SP,
                            SyntaxContext::empty(),
                        )))),
                        args: vec![ExprOrSpread {
                            spread: None,
                            expr: Box::new(obj_expr),
                        }],
                        type_args: None,
                        ctxt: SyntaxContext::empty(),
                    });

                    *item = ModuleItem::Stmt(Stmt::Expr(ExprStmt {
                        span: DUMMY_SP,
                        expr: Box::new(add_export_call),
                    }));
                }
                ModuleDecl::ExportDefaultDecl(export_default) => {
                    let decl_expr: Expr = match &export_default.decl {
                        DefaultDecl::Fn(expr) => Expr::Fn(FnExpr::from(expr.function.clone())),
                        DefaultDecl::Class(expr) => {
                            Expr::Class(ClassExpr::from(expr.class.clone()))
                        }
                        DefaultDecl::TsInterfaceDecl(tsdecl) => {
                            self.emit_error(tsdecl.span, "Unsupported default export declaration");
                            // Return null as fallback
                            Expr::Lit(Lit::Null(Null { span: DUMMY_SP }))
                        }
                    };
                    let set_export_call = Expr::Call(CallExpr {
                        span: DUMMY_SP,
                        callee: Callee::Expr(Box::new(Expr::Ident(Ident::new(
                            "setExport".into(),
                            DUMMY_SP,
                            SyntaxContext::empty(),
                        )))),
                        args: vec![ExprOrSpread {
                            spread: None,
                            expr: Box::new(decl_expr),
                        }],
                        type_args: None,
                        ctxt: SyntaxContext::empty(),
                    });
                    *item = ModuleItem::Stmt(Stmt::Expr(ExprStmt {
                        span: DUMMY_SP,
                        expr: Box::new(set_export_call),
                    }));
                }
                ModuleDecl::ExportDefaultExpr(export_default) => {
                    let decl_expr: Expr = *export_default.expr.clone();
                    let set_export_call = Expr::Call(CallExpr {
                        span: DUMMY_SP,
                        callee: Callee::Expr(Box::new(Expr::Ident(Ident::new(
                            "setExport".into(),
                            DUMMY_SP,
                            SyntaxContext::empty(),
                        )))),
                        args: vec![ExprOrSpread {
                            spread: None,
                            expr: Box::new(decl_expr),
                        }],
                        type_args: None,
                        ctxt: SyntaxContext::empty(),
                    });
                    *item = ModuleItem::Stmt(Stmt::Expr(ExprStmt {
                        span: DUMMY_SP,
                        expr: Box::new(set_export_call),
                    }));
                }
                _ => {}
            }
        }

        item.visit_mut_children_with(self)
    }
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
            FileName, SourceMap,
        },
        ecma::visit::VisitMutWith,
    };
    use swc_ecma_codegen::{text_writer::JsWriter, Config, Emitter as CodeEmitter};
    use swc_ecma_parser::{lexer::Lexer, Parser, StringInput, Syntax};

    fn generate_code(program: &Program, cm: &Lrc<SourceMap>) -> String {
        let mut buf = vec![];
        {
            let mut emitter = CodeEmitter {
                cfg: Config::default(),
                comments: None,
                wr: JsWriter::new(cm.clone(), "\n", &mut buf, None),
                cm: cm.clone(),
            };
            emitter
                .emit_program(program)
                .expect("Failed to generate code");
        }
        String::from_utf8(buf).expect("Invalid UTF8")
    }

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
    fn test_export_default_declaration() {
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
            export default function exp() { console.log('exported function'); };
        "#;

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
        let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

        let mut visitor = ImportExportTransformVisitor::new(None, &handler);
        program.visit_mut_with(&mut visitor);

        let output_code = generate_code(&program, &cm);

        assert!(
            output_code.contains("setExport(function()"),
            "Output code should contain setExport call, got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("console.log('exported function')"),
            "Output code should contain func body, got:\n{}",
            output_code
        );
        let diags = diagnostics.lock().unwrap();
        assert!(
            diags.is_empty(),
            "Should not emit errors, got: {:?}",
            *diags
        );
    }

    #[test]
    fn test_export_default_expression() {
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
            let myVar = 5;
            export default myVar;
        "#;

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
        let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

        let mut visitor = ImportExportTransformVisitor::new(None, &handler);
        program.visit_mut_with(&mut visitor);

        let output_code = generate_code(&program, &cm);

        assert!(
            output_code.contains("setExport(myVar)"),
            "Output code should contain setExport call, got:\n{}",
            output_code
        );
        let diags = diagnostics.lock().unwrap();
        assert!(
            diags.is_empty(),
            "Should not emit errors, got: {:?}",
            *diags
        );
    }

    #[test]
    fn test_export_const_expression() {
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
            export const sql = (input) => intput + 5;
            export const a1 = 5, a2 = ()=>111, a3 = (inputA3)=>inputA3+"Done";
        "#;

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
        let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

        let mut visitor = ImportExportTransformVisitor::new(None, &handler);
        program.visit_mut_with(&mut visitor);

        let output_code = generate_code(&program, &cm);

        assert!(
            output_code.contains("const sql = (input)=>intput + 5;"),
            "Output code should contain original single const definition, got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("addExport({\n    sql: sql\n})"),
            "Output code should contain addExport call, got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("const a1 = 5, a2 = ()=>111, a3 = (inputA3)=>inputA3 + \"Done\""),
            "Output code should contain original multiple const definitions, got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("addExport({\n    a1: a1,\n    a2: a2,\n    a3: a3\n})"),
            "Output code should contain addExport call, got:\n{}",
            output_code
        );

        let diags = diagnostics.lock().unwrap();
        assert!(
            diags.is_empty(),
            "Should not emit errors, got: {:?}",
            *diags
        );
    }

    #[test]
    fn test_import_named_default() {
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
            import def, { foo, bar as baz } from "module";
        "#;
        // Should generate
        // const def = require("module"), foo = require("module").foo, baz = require("module").bar;
        //

        let fm = cm.new_source_file(
            Arc::new(FileName::Custom("import.js".into())),
            js_code.into(),
        );
        let lexer = Lexer::new(
            Syntax::Es(Default::default()),
            EsVersion::Es2020,
            StringInput::from(&*fm),
            None,
        );
        let mut parser = Parser::new_from(lexer);
        let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

        let mut visitor = ImportExportTransformVisitor::new(None, &handler);
        program.visit_mut_with(&mut visitor);

        let output_code = generate_code(&program, &cm);
        assert!(
            output_code.contains("const"),
            "Output code should contain a const declaration, got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("def = require(\"module\")"),
            "Output code should contain 'def', got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("foo = require(\"module\").foo"),
            "Output code should contain 'foo', got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("baz = require(\"module\").bar"),
            "Output code should contain 'baz', got:\n{}",
            output_code
        );
        let diags = diagnostics.lock().unwrap();
        assert!(
            diags.is_empty(),
            "Should not emit errors, got: {:?}",
            *diags
        );
    }

    #[test]
    fn test_namespace_import() {
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
            import * as ns from "module";
        "#;

        let fm = cm.new_source_file(
            Arc::new(FileName::Custom("ns_import.js".into())),
            js_code.into(),
        );
        let lexer = Lexer::new(
            Syntax::Es(Default::default()),
            EsVersion::Es2020,
            StringInput::from(&*fm),
            None,
        );
        let mut parser = Parser::new_from(lexer);
        let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

        let mut visitor = ImportExportTransformVisitor::new(None, &handler);
        program.visit_mut_with(&mut visitor);

        let diags = diagnostics.lock().unwrap();
        let errors: Vec<_> = diags
            .iter()
            .filter(|msg| msg.contains("Namespace import not supported"))
            .collect();
        assert!(
            !errors.is_empty(),
            "Expected error for namespace import, got diagnostics: {:?}",
            *diags
        );
    }

    #[test]
    fn test_export_named() {
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
            export { foo, bar as baz };
        "#;
        // Should generate:
        // addExport({
        //     foo: foo,
        //     baz: bar
        // });

        let fm = cm.new_source_file(
            Arc::new(FileName::Custom("export_named.js".into())),
            js_code.into(),
        );
        let lexer = Lexer::new(
            Syntax::Es(Default::default()),
            EsVersion::Es2020,
            StringInput::from(&*fm),
            None,
        );
        let mut parser = Parser::new_from(lexer);
        let mut program: Program = parser.parse_program().expect("Failed to parse the JS code");

        let mut visitor = ImportExportTransformVisitor::new(None, &handler);
        program.visit_mut_with(&mut visitor);

        let output_code = generate_code(&program, &cm);
        assert!(
            output_code.contains("addExport"),
            "Output code should contain addExport call, got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("foo: foo"),
            "Output code should contain 'foo', got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("baz: bar"),
            "Output code should contain 'baz', got:\n{}",
            output_code
        );
        let diags = diagnostics.lock().unwrap();
        assert!(
            diags.is_empty(),
            "Should not emit errors, got: {:?}",
            *diags
        );
    }

    #[test]
    fn test_export_default_ts_interface() {
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
            export default interface Foo {}
        "#;

        let fm = cm.new_source_file(
            Arc::new(FileName::Custom("export_default_ts_interface.ts".into())),
            js_code.into(),
        );
        let lexer = Lexer::new(
            Syntax::Typescript(Default::default()),
            EsVersion::Es2020,
            StringInput::from(&*fm),
            None,
        );
        let mut parser = Parser::new_from(lexer);
        let mut program: Program = parser.parse_program().expect("Failed to parse the TS code");

        let mut visitor = ImportExportTransformVisitor::new(None, &handler);
        program.visit_mut_with(&mut visitor);

        let output_code = generate_code(&program, &cm);
        // When exporting a TS interface, setExport is called with null as a fallback.
        assert!(
            output_code.contains("setExport"),
            "Output code should contain setExport call, got:\n{}",
            output_code
        );
        assert!(
            output_code.contains("null"),
            "Output code should contain 'null' fallback, got:\n{}",
            output_code
        );
        let diags = diagnostics.lock().unwrap();
        let errors: Vec<_> = diags
            .iter()
            .filter(|msg| msg.contains("Unsupported default export declaration"))
            .collect();
        assert!(
            !errors.is_empty(),
            "Expected error for TS interface default export, got diagnostics: {:?}",
            *diags
        );
    }
}
