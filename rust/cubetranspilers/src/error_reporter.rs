use std::cell::RefCell;

use swc_core::common::errors::{DiagnosticBuilder, Emitter};

pub struct ErrorReporter {
    pub errors: RefCell<Vec<String>>,
    pub warnings: RefCell<Vec<String>>,
}

impl ErrorReporter {
    pub fn new(errors: RefCell<Vec<String>>, warnings: RefCell<Vec<String>>) -> Self {
        ErrorReporter { errors, warnings }
    }
}

impl Default for ErrorReporter {
    fn default() -> Self {
        ErrorReporter {
            errors: RefCell::new(Vec::new()),
            warnings: RefCell::new(Vec::new()),
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
                self.errors.borrow_mut().push(diagnostic.message());
            }
            swc_core::common::errors::Level::Warning
            | swc_core::common::errors::Level::Note
            | swc_core::common::errors::Level::Help
            | swc_core::common::errors::Level::Cancelled
            | swc_core::common::errors::Level::FailureNote => {
                self.warnings.borrow_mut().push(diagnostic.message());
            }
        }
    }
}
