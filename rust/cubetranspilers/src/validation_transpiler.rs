use swc_core::common::errors::Handler;
use swc_core::common::BytePos;
use swc_core::common::Span;
use swc_core::ecma::visit::noop_visit_mut_type;
use swc_core::plugin::proxies::PluginSourceMapProxy;
use swc_core::{
    atoms::JsWord,
    ecma::{ast::*, visit::VisitMut},
};

pub struct ValidationTransformVisitor<'a> {
    pub(crate) source_map: Option<PluginSourceMapProxy>,
    handler: &'a Handler,
}

impl<'a> ValidationTransformVisitor<'a> {
    pub fn new(source_map: Option<PluginSourceMapProxy>, handler: &'a Handler) -> Self {
        ValidationTransformVisitor {
            source_map,
            handler,
        }
    }

    fn emit_warn(&self, span: Span, message: &str) {
        self.handler
            .struct_span_warn(span, &self.format_msg(span, message))
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

impl VisitMut for ValidationTransformVisitor<'_> {
    // Implement necessary visit_mut_* methods for actual custom transform.
    // A comprehensive list of possible visitor methods can be found here:
    // https://rustdoc.swc.rs/swc_ecma_visit/trait.VisitMut.html

    // We don't need to do anything besides checking identifiers here
    noop_visit_mut_type!();

    fn visit_mut_ident(&mut self, ident: &mut Ident) {
        let uc_id: JsWord = "USER_CONTEXT".into();
        if ident.sym == uc_id {
            self.emit_warn(
                ident.span,
                "Support for USER_CONTEXT was removed, please migrate to SECURITY_CONTEXT",
            );
            // TODO: How to report the errors?
            // @see https://rustdoc.swc.rs/swc_common/errors/struct.Handler.html
        }
    }
}
