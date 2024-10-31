use super::{QueryPlan, Select, SingleAliasedSource, Subquery};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseCube, BaseJoinCondition, VisitorContext};
use cubenativeutils::CubeError;

use std::rc::Rc;

pub enum JoinSource {
    Subquery(Subquery),
    Cube(Rc<BaseCube>),
    TableReference(String, String),
}

impl JoinSource {
    pub fn new_from_query_plan(plan: Rc<QueryPlan>, alias: String) -> Self {
        Self::Subquery(Subquery::new(plan, alias))
    }

    pub fn new_from_select(plan: Rc<Select>, alias: String) -> Self {
        Self::Subquery(Subquery::new(Rc::new(QueryPlan::Select(plan)), alias))
    }

    pub fn new_from_cube(cube: Rc<BaseCube>) -> Self {
        Self::Cube(cube)
    }

    pub fn new_from_reference(reference: String, alias: String) -> Self {
        Self::TableReference(reference, alias)
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let sql = match &self {
            JoinSource::Cube(cube) => {
                let cubesql = cube.to_sql(context.clone())?;
                format!(" {} ", cubesql)
            }
            JoinSource::Subquery(s) => s.to_sql(templates)?,
            JoinSource::TableReference(r, alias) => format!(" {} as {} ", r, alias),
        };
        Ok(sql)
    }
}

pub struct JoinItem {
    pub from: SingleAliasedSource,
    pub on: Rc<dyn BaseJoinCondition>,
    pub is_inner: bool,
}

pub struct Join {
    pub root: SingleAliasedSource,
    pub joins: Vec<JoinItem>,
}

impl JoinItem {
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let operator = if self.is_inner { "INNER" } else { "LEFT" };
        let on_sql = self.on.to_sql(context.clone())?;
        Ok(format!(
            "{} JOIN {} ON {}",
            operator,
            self.from.to_sql(templates, context)?,
            on_sql
        ))
    }
}

impl Join {
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let joins_sql = self
            .joins
            .iter()
            .map(|j| j.to_sql(templates, context.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let res = format!(
            "{}\n{}",
            self.root.to_sql(templates, context.clone())?,
            joins_sql.join("\n")
        );
        Ok(res)
    }
}
