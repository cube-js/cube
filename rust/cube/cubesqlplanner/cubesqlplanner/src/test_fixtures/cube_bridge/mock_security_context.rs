use crate::cube_bridge::security_context::SecurityContext;
use std::any::Any;
use std::rc::Rc;

/// Mock implementation of SecurityContext for testing
pub struct MockSecurityContext;

impl SecurityContext for MockSecurityContext {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
