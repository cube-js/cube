use std::sync::{Arc, Arc as Lrc, Mutex};
use swc_core::common::errors::{DiagnosticBuilder, Emitter};
use swc_core::common::SourceMap;
use swc_core::ecma::ast::Program;
use swc_ecma_codegen::text_writer::JsWriter;
use swc_ecma_codegen::{Config, Emitter as CodeEmitter};

pub struct TestEmitter {
    pub diagnostics: Arc<Mutex<Vec<String>>>,
}

impl Emitter for TestEmitter {
    fn emit(&mut self, diagnostic: &DiagnosticBuilder) {
        let mut diags = self.diagnostics.lock().unwrap();
        diags.push(diagnostic.message());
    }
}

#[allow(dead_code)]
pub fn generate_code(program: &Program, cm: &Lrc<SourceMap>) -> String {
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
