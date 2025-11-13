use crate::planner::sql_evaluator::SqlCall;
use std::rc::Rc;

/// Represents a geo dimension with latitude and longitude
#[derive(Clone)]
pub struct GeoDimension {
    latitude: Rc<SqlCall>,
    longitude: Rc<SqlCall>,
}

impl GeoDimension {
    pub fn new(latitude: Rc<SqlCall>, longitude: Rc<SqlCall>) -> Self {
        Self {
            latitude,
            longitude,
        }
    }

    pub fn latitude(&self) -> &Rc<SqlCall> {
        &self.latitude
    }

    pub fn longitude(&self) -> &Rc<SqlCall> {
        &self.longitude
    }
}