use super::{Join, QueryPlan, Schema, SchemaCube, Select};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseCube, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum SingleSource {
    Subquery(Rc<QueryPlan>),
    Cube(Rc<BaseCube>),
    TableReference(String, Rc<Schema>),
}

impl SingleSource {
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let sql = match &self {
            SingleSource::Cube(cube) => {
                let cubesql = cube.to_sql(context.clone())?;
                format!(" {} ", cubesql)
            }
            SingleSource::Subquery(s) => format!("({})", s.to_sql(templates)?),
            SingleSource::TableReference(r, _) => format!(" {} ", r),
        };
        Ok(sql)
    }
}

#[derive(Clone)]
pub struct SingleAliasedSource {
    pub source: SingleSource,
    pub alias: String,
}

impl SingleAliasedSource {
    pub fn new_from_cube(cube: Rc<BaseCube>, alias: Option<String>) -> Self {
        let alias = alias.unwrap_or_else(|| cube.default_alias());
        Self {
            source: SingleSource::Cube(cube),
            alias,
        }
    }

    pub fn new_from_table_reference(
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
    ) -> Self {
        let alias = alias.unwrap_or_else(|| PlanSqlTemplates::alias_name(&reference));
        Self {
            source: SingleSource::TableReference(reference, schema),
            alias,
        }
    }

    pub fn new_from_subquery(plan: Rc<QueryPlan>, alias: String) -> Self {
        Self {
            source: SingleSource::Subquery(plan),
            alias,
        }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let sql = self.source.to_sql(templates, context)?;

        Ok(format!(
            "{sql} AS {}",
            templates.quote_identifier(&self.alias)?
        ))
    }

    pub fn make_schema(&self) -> Schema {
        match &self.source {
            SingleSource::Subquery(query) => query.make_schema(Some(self.alias.clone())),
            SingleSource::Cube(cube) => {
                let mut schema = Schema::empty();
                schema.add_cube(SchemaCube::new(cube.name().clone(), self.alias.clone()));
                schema
            }
            SingleSource::TableReference(_, schema) => schema.move_to_source(&self.alias),
        }
    }
}

#[derive(Clone)]
pub enum FromSource {
    Empty,
    Single(SingleAliasedSource),
    Join(Rc<Join>),
}

impl FromSource {
    pub fn get_schema(&self) -> Rc<Schema> {
        let schema = match self {
            FromSource::Empty => Schema::empty(),
            FromSource::Single(source) => source.make_schema(),
            FromSource::Join(join) => join.make_schema(),
        };
        Rc::new(schema)
    }
}

#[derive(Clone)]
pub struct From {
    pub source: FromSource,
    pub schema: Rc<Schema>,
}

impl From {
    pub fn new(source: FromSource) -> Self {
        let schema = source.get_schema();
        Self { source, schema }
    }

    pub fn new_from_cube(cube: Rc<BaseCube>, alias: Option<String>) -> Self {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_cube(
            cube, alias,
        )))
    }

    pub fn new_from_table_reference(
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
    ) -> Self {
        Self::new(FromSource::Single(
            SingleAliasedSource::new_from_table_reference(reference, schema, alias),
        ))
    }

    pub fn new_from_join(join: Rc<Join>) -> Self {
        Self::new(FromSource::Join(join))
    }

    pub fn new_from_subquery(plan: Rc<QueryPlan>, alias: String) -> Self {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_subquery(
            plan, alias,
        )))
    }

    pub fn new_from_subselect(plan: Rc<Select>, alias: String) -> Self {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_subquery(
            Rc::new(QueryPlan::Select(plan)),
            alias,
        )))
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let sql = match &self.source {
            FromSource::Empty => format!(""),
            FromSource::Single(source) => source.to_sql(templates, context.clone())?,
            FromSource::Join(j) => {
                format!("{}", j.to_sql(templates, context.clone())?)
            }
        };
        Ok(sql)
    }
}
