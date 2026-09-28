use crate::cube_bridge::sql_utils::SqlUtils;
use std::any::Any;
use std::rc::Rc;

/// Mock implementation of SqlUtils for testing
pub struct MockSqlUtils;

impl SqlUtils for MockSqlUtils {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}
