use sqlparser::ast;

use super::protocol::{ParameterDescription, RowDescription};
use crate::sql::statement::{BindValue, StatementParamsBinder};

#[derive(Debug)]
pub struct PreparedStatement {
    pub query: ast::Statement,
    pub parameters: ParameterDescription,
    pub description: RowDescription,
}

impl PreparedStatement {
    pub fn bind(&self, values: Vec<BindValue>) -> ast::Statement {
        let binder = StatementParamsBinder::new(values);
        let mut statement = self.query.clone();
        binder.bind(&mut statement);

        statement
    }
}
