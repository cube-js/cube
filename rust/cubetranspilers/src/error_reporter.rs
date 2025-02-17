use std::sync::{Arc, Mutex};

use swc_core::common::errors::{DiagnosticBuilder, Emitter};

pub struct ErrorReporter {
    pub errors: Arc<Mutex<Vec<String>>>,
    pub warnings: Arc<Mutex<Vec<String>>>,
}

impl ErrorReporter {
    pub fn new(errors: Arc<Mutex<Vec<String>>>, warnings: Arc<Mutex<Vec<String>>>) -> Self {
        ErrorReporter { errors, warnings }
    }
}

impl Default for ErrorReporter {
    fn default() -> Self {
        ErrorReporter {
            errors: Arc::new(Mutex::new(Vec::new())),
            warnings: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Emitter for ErrorReporter {
    fn emit(&mut self, diagnostic: &DiagnosticBuilder) {
        match diagnostic.diagnostic.level {
            swc_core::common::errors::Level::Bug
            | swc_core::common::errors::Level::Fatal
            | swc_core::common::errors::Level::PhaseFatal
            | swc_core::common::errors::Level::Error => {
                let mut errors = self.errors.lock().unwrap();
                errors.push(diagnostic.message());
            }
            swc_core::common::errors::Level::Warning
            | swc_core::common::errors::Level::Note
            | swc_core::common::errors::Level::Help
            | swc_core::common::errors::Level::Cancelled
            | swc_core::common::errors::Level::FailureNote => {
                let mut warnings = self.warnings.lock().unwrap();
                warnings.push(diagnostic.message());
            }
        }
    }
}
