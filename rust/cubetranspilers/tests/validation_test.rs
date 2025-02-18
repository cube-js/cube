mod common;

// Recommended strategy to test plugin's transform is verify
// the Visitor's behavior, instead of trying to run `process_transform` with mocks
// unless explicitly required to do so.

use std::sync::{Arc, Mutex};

use common::TestEmitter;
use cubetranspilers::validation_transpiler::*;
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
fn test_warning_for_user_context() {
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

    let mut visitor = ValidationTransformVisitor::new(None, &handler);
    program.visit_mut_with(&mut visitor);

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

    let mut visitor = ValidationTransformVisitor::new(None, &handler);
    program.visit_mut_with(&mut visitor);

    let diags = diagnostics.lock().unwrap();
    assert!(diags.is_empty(), "Should not emit warning",);
}
