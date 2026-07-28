use super::super::deps::symbol_deps;
use crate::planner::SqlCall;
use std::rc::Rc;

/// `type: geo` dimension from the data model: a geographic dimension
/// defined by `latitude` and `longitude` SQL expressions.
#[derive(Clone)]
pub struct GeoDimension {
    latitude: Rc<SqlCall>,
    longitude: Rc<SqlCall>,
}

symbol_deps! {
    GeoDimension {
        latitude: dep,
        longitude: dep,
    }
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

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(std::iter::once(&self.latitude).chain(std::iter::once(&self.longitude)))
    }

    pub fn is_owned_by_cube(&self) -> bool {
        self.latitude.is_owned_by_cube() || self.longitude.is_owned_by_cube()
    }
}
