use crate::compile::QueryPlan;
use crate::sql::protocol::Format;
use sqlparser::ast;

use super::protocol::{ParameterDescription, RowDescription};
use crate::sql::statement::{BindValue, StatementParamsBinder};

#[derive(Debug)]
pub struct PreparedStatement {
    pub query: ast::Statement,
    pub parameters: ParameterDescription,
    // Fields which will be returned to the client, It can be None if server doesnt return any field
    // for example BEGIN
    pub description: Option<RowDescription>,
}

impl PreparedStatement {
    pub fn bind(&self, values: Vec<BindValue>) -> ast::Statement {
        let binder = StatementParamsBinder::new(values);
        let mut statement = self.query.clone();
        binder.bind(&mut statement);

        statement
    }
}

pub struct Portal {
    pub plan: QueryPlan,
    // Format which is used to return data
    pub format: Format,
    // Fields which will be returned to the client, It can be None if server doesnt return any field
    // for example BEGIN
    pub description: Option<RowDescription>,
}
