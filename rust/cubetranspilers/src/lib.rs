use crate::check_dup_prop_transpiler::CheckDupPropTransformVisitor;
use crate::cube_prop_ctx_transpiler::CubePropTransformVisitor;
use crate::import_export_transpiler::ImportExportTransformVisitor;
use crate::validation_transpiler::ValidationTransformVisitor;
use anyhow::{anyhow, Result};
use error_reporter::ErrorReporter;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use swc_core::common::errors::{Handler, HandlerFlags};
use swc_core::common::input::StringInput;
use swc_core::common::sync::{Lrc, OnceCell};
use swc_core::common::{FileName, SourceMap};
use swc_core::ecma::ast::{EsVersion, Program};
use swc_core::ecma::visit::VisitMutWith;
use swc_core::plugin::proxies::PluginSourceMapProxy;
use swc_ecma_codegen::Config;
use swc_ecma_codegen::{text_writer::JsWriter, Emitter as CodeEmitter};
use swc_ecma_parser::lexer::Lexer;
use swc_ecma_parser::{Parser, Syntax};

pub mod check_dup_prop_transpiler;
pub mod cube_prop_ctx_transpiler;
pub mod error_reporter;
pub mod import_export_transpiler;
pub mod validation_transpiler;

#[derive(Deserialize, Clone, Debug)]
pub enum Transpilers {
    CubeCheckDuplicatePropTranspiler,
    CubePropContextTranspiler,
    ImportExportTranspiler,
    ValidationTranspiler,
}

#[derive(Deserialize, Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct TransformConfig {
    pub file_name: String,
    pub transpilers: Vec<Transpilers>,
    pub cube_names: HashSet<String>,
    pub cube_symbols: HashMap<String, HashMap<String, bool>>,
    pub context_symbols: HashMap<String, String>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransformResult {
    pub code: String,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

pub fn run_transpilers(
    sources: String,
    transform_config: TransformConfig,
) -> Result<TransformResult> {
    let sm: Lrc<SourceMap> = Default::default();
    let sf = sm.new_source_file(
        Arc::new(FileName::Custom(transform_config.file_name)),
        sources,
    );

    let lexer = Lexer::new(
        Syntax::Es(Default::default()),
        EsVersion::Es2015,
        StringInput::from(&*sf),
        None,
    );
    let mut parser = Parser::new_from(lexer);

    let mut program = match parser.parse_program() {
        Ok(v) => v,
        Err(_err) => return Err(anyhow!("Failed to parse the JS code")),
    };

    let sm_cell = OnceCell::new();
    sm_cell
        .set(sf.clone())
        .map_err(|_err| anyhow!("Failed to init OnceCell with source file"))?;

    let plugin_source_map = PluginSourceMapProxy {
        source_file: sm_cell,
    };

    let errors = Arc::new(Mutex::new(Vec::new()));
    let warnings = Arc::new(Mutex::new(Vec::new()));

    let reporter = Box::new(ErrorReporter::new(errors.clone(), warnings.clone()));
    let handler = Handler::with_emitter_and_flags(
        reporter,
        HandlerFlags {
            can_emit_warnings: true,
            ..Default::default()
        },
    );

    transform_config
        .transpilers
        .into_iter()
        .for_each(|transpiler| match transpiler {
            Transpilers::CubeCheckDuplicatePropTranspiler => {
                let mut visitor =
                    CheckDupPropTransformVisitor::new(Some(plugin_source_map.clone()), &handler);
                program.visit_mut_with(&mut visitor);
            }
            Transpilers::CubePropContextTranspiler => {
                let mut visitor = CubePropTransformVisitor::new(
                    transform_config.cube_names.clone(),
                    transform_config.cube_symbols.clone(),
                    transform_config.context_symbols.clone(),
                    Some(plugin_source_map.clone()),
                    &handler,
                );
                program.visit_mut_with(&mut visitor);
            }
            Transpilers::ImportExportTranspiler => {
                let mut visitor =
                    ImportExportTransformVisitor::new(Some(plugin_source_map.clone()), &handler);
                program.visit_mut_with(&mut visitor);
            }
            Transpilers::ValidationTranspiler => {
                let mut visitor =
                    ValidationTransformVisitor::new(Some(plugin_source_map.clone()), &handler);
                program.visit_mut_with(&mut visitor);
            }
        });

    let output_code = generate_code(&program, &sm)?;
    let errors = errors.lock().unwrap().clone();
    let warnings = warnings.lock().unwrap().clone();

    Ok(TransformResult {
        code: output_code,
        errors,
        warnings,
    })
}

pub fn generate_code(program: &Program, sm: &Lrc<SourceMap>) -> Result<String> {
    let mut buf = vec![];
    {
        let mut emitter = CodeEmitter {
            cfg: Config::default().with_target(EsVersion::Es2015),
            comments: None,
            wr: JsWriter::new(sm.clone(), "\n", &mut buf, None),
            cm: sm.clone(),
        };
        emitter
            .emit_program(program)
            .map_err(|err| anyhow!("Failed to generate code: {}", err))?;
    }

    let code = String::from_utf8(buf).map_err(|err| anyhow!("Invalid UTF8: {}", err))?;
    Ok(code)
}
