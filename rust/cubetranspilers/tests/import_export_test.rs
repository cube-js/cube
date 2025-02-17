mod common;

// Recommended strategy to test plugin's transform is verify
// the Visitor's behavior, instead of trying to run `process_transform` with mocks
// unless explicitly required to do so.

use std::sync::{Arc, Mutex};

use common::{generate_code, TestEmitter};
use cubetranspilers::import_export_transpiler::*;
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
