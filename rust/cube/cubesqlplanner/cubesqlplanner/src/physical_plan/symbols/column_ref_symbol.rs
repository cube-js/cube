use super::{MemberSqlContext, ToSql};
use crate::physical_plan::QualifiedColumnName;
use crate::planner::symbols::{ColumnRefSymbol, ReferenceValue};
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

impl From<QualifiedColumnName> for ReferenceValue {
    fn from(value: QualifiedColumnName) -> Self {
        Self::Column {
            source: value.source().clone(),
            name: value.name().clone(),
        }
    }
}

/// A symbol that reads `origin` from `column` instead of computing it.
pub fn column_reference(
    origin: &Rc<MemberSymbol>,
    column: QualifiedColumnName,
) -> Rc<MemberSymbol> {
    MemberSymbol::new_column_ref(ColumnRefSymbol::new(origin.clone(), column.into()))
}

/// A symbol that renders `origin` as a value the query pinned for it.
pub fn literal_reference(origin: &Rc<MemberSymbol>, value: String) -> Rc<MemberSymbol> {
    MemberSymbol::new_column_ref(ColumnRefSymbol::new(
        origin.clone(),
        ReferenceValue::Literal(value),
    ))
}

impl ToSql for ColumnRefSymbol {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError> {
        match self.value() {
            ReferenceValue::Column { source, name } => ctx.templates.column_reference(source, name),
            ReferenceValue::Literal(value) => ctx.templates.quote_string(value),
        }
    }
}
