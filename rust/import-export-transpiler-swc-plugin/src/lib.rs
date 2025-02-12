use swc_core::common::{errors::HANDLER, BytePos};
use swc_core::common::{Span, SyntaxContext, DUMMY_SP};
use swc_core::ecma::visit::{noop_visit_mut_type, VisitMutWith};
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
    // TODO: How to report the errors?
    // @see https://rustdoc.swc.rs/swc_common/errors/struct.Handler.html

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
}

impl VisitMut for TransformVisitor {
    // Implement necessary visit_mut_* methods for actual custom transform.
    // A comprehensive list of possible visitor methods can be found here:
    // https://rustdoc.swc.rs/swc_ecma_visit/trait.VisitMut.html

    // We don't need to do anything besides imports here
    noop_visit_mut_type!();

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
