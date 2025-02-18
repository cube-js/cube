mod common;

// Recommended strategy to test plugin's transform is verify
// the Visitor's behavior, instead of trying to run `process_transform` with mocks
// unless explicitly required to do so.

use std::sync::{Arc, Mutex};

use common::TestEmitter;
use cubetranspilers::check_dup_prop_transpiler::*;
use swc_core::ecma::ast::{EsVersion, Program};
use swc_core::{
    common::{
        errors::{Handler, HandlerFlags},
        sync::Lrc,
        FileName, SourceMap,
    },
    ecma::visit::VisitMutWith,
};
use swc_ecma_parser::{lexer::Lexer, Parser, StringInput, Syntax};

#[test]
fn test_errors_for_duplicates_first_level() {
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

    let mut visitor = CheckDupPropTransformVisitor::new(None, &handler);
    program.visit_mut_with(&mut visitor);

    let diags = diagnostics.lock().unwrap();
    let msgs: Vec<_> = diags
        .iter()
        .filter(|msg| msg.contains("Duplicate property"))
        .collect();
    assert!(msgs.len() == 1, "Should emit errors",);
}

#[test]
fn test_errors_for_duplicates_deep_level() {
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

    let mut visitor = CheckDupPropTransformVisitor::new(None, &handler);
    program.visit_mut_with(&mut visitor);

    let diags = diagnostics.lock().unwrap();
    let msgs: Vec<_> = diags
        .iter()
        .filter(|msg| msg.contains("Duplicate property"))
        .collect();
    assert!(msgs.len() == 3, "Should emit errors",);
}

#[test]
fn test_no_errors() {
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

    let mut visitor = CheckDupPropTransformVisitor::new(None, &handler);
    program.visit_mut_with(&mut visitor);

    let diags = diagnostics.lock().unwrap();
    assert!(diags.is_empty(), "Should not emit errors",);
}
