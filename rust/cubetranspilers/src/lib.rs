use crate::check_dup_prop_transpiler::CheckDupPropTransformVisitor;
use crate::cube_prop_ctx_transpiler::CubePropTransformVisitor;
use crate::import_export_transpiler::ImportExportTransformVisitor;
use crate::validation_transpiler::ValidationTransformVisitor;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
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
pub mod import_export_transpiler;
pub mod validation_transpiler;

#[derive(Deserialize, Clone, Debug)]
pub enum Transpilers {
    CubeCheckDuplicatePropTranspiler,
    CubePropContextTranspiler,
    ImportExportTranspiler,
    ValidationTranspiler,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TransformConfig {
    pub file_name: String,
    pub transpilers: Vec<Transpilers>,
    pub cube_names: HashSet<String>,
    pub cube_symbols: HashMap<String, HashMap<String, bool>>,
    pub context_symbols: HashMap<String, String>,
}

impl Default for TransformConfig {
    fn default() -> Self {
        Self {
            file_name: String::new(),
            transpilers: Vec::new(),
            cube_names: HashSet::new(),
            cube_symbols: HashMap::new(),
            context_symbols: HashMap::new(),
        }
    }
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
    sm_cell.set(sf.clone()).map_err(|_err| anyhow!("Failed to init OnceCell with source file"))?;

    let plugin_source_map = PluginSourceMapProxy {
        source_file: sm_cell,
    };

    transform_config
        .transpilers
        .into_iter()
        .for_each(|transpiler| match transpiler {
            Transpilers::CubeCheckDuplicatePropTranspiler => {
                let mut visitor = CheckDupPropTransformVisitor {
                    source_map: Some(plugin_source_map.clone()),
                };
                program.visit_mut_with(&mut visitor);
            }
            Transpilers::CubePropContextTranspiler => {
                let mut visitor = CubePropTransformVisitor {
                    cube_names: transform_config.cube_names.clone(),
                    cube_symbols: transform_config.cube_symbols.clone(),
                    context_symbols: transform_config.context_symbols.clone(),
                    source_map: Some(plugin_source_map.clone()),
                };
                program.visit_mut_with(&mut visitor);
            }
            Transpilers::ImportExportTranspiler => {
                let mut visitor = ImportExportTransformVisitor {
                    source_map: Some(plugin_source_map.clone()),
                };
                program.visit_mut_with(&mut visitor);
            }
            Transpilers::ValidationTranspiler => {
                let mut visitor = ValidationTransformVisitor {
                    source_map: Some(plugin_source_map.clone()),
                };
                program.visit_mut_with(&mut visitor);
            }
        });

    let output_code = generate_code(&program, &sm)?;

    Ok(TransformResult {
        code: output_code,
        errors: vec![],
        warnings: vec![],
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
