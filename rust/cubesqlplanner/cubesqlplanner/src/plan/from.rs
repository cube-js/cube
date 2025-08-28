use super::{Join, QueryPlan, Schema, Select};
use crate::plan::Union;
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
                let cubesql = cube.to_sql(context.clone(), templates)?;
                format!(" {} ", cubesql)
            }
            SingleSource::Subquery(s) => format!("({})", s.to_sql(templates)?),
            SingleSource::TableReference(r, _) => format!(" {} ", r),
        };
        Ok(sql)
    }

    pub fn schema(&self) -> Rc<Schema> {
        match self {
            SingleSource::Subquery(subquery) => subquery.schema(),
            SingleSource::Cube(_) => Rc::new(Schema::empty()),
            SingleSource::TableReference(_, schema) => schema.clone(),
        }
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

    pub fn new_from_source(source: SingleSource, alias: String) -> Self {
        Self { source, alias }
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
        /* if let SingleSource::Subquery(plan) = &self.source {
            if let QueryPlan::Union(_) = plan.as_ref() {
                //FIXME CubeStore (at least old cubestore) don't support alias for union
                if templates.is_external() {
                    return Ok(sql);
                }
            }
        } */

        templates.query_aliased(&sql, &self.alias)
    }
}

#[derive(Clone)]
pub enum FromSource {
    Empty,
    Single(SingleAliasedSource),
    Join(Rc<Join>),
}

#[derive(Clone)]
pub struct From {
    pub source: FromSource,
}

impl From {
    pub fn new(source: FromSource) -> Rc<Self> {
        Rc::new(Self { source })
    }

    pub fn new_from_cube(cube: Rc<BaseCube>, alias: Option<String>) -> Rc<Self> {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_cube(
            cube, alias,
        )))
    }

    pub fn new_from_table_reference(
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
    ) -> Rc<Self> {
        Self::new(FromSource::Single(
            SingleAliasedSource::new_from_table_reference(reference, schema, alias),
        ))
    }

    pub fn new_from_join(join: Rc<Join>) -> Rc<Self> {
        Self::new(FromSource::Join(join))
    }

    pub fn new_from_subquery(plan: Rc<QueryPlan>, alias: String) -> Rc<Self> {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_subquery(
            plan, alias,
        )))
    }

    pub fn new_from_union(union: Rc<Union>, alias: String) -> Rc<Self> {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_subquery(
            Rc::new(QueryPlan::Union(union)),
            alias,
        )))
    }

    pub fn new_from_subselect(plan: Rc<Select>, alias: String) -> Rc<Self> {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_subquery(
            Rc::new(QueryPlan::Select(plan)),
            alias,
        )))
    }

    /* pub fn all_sources(&self) -> Vec<String> {
        match &self.source {
            FromSource::Empty => vec![],
            FromSource::Single(s) => vec![s.alias.clone()],
            FromSource::Join(j) => {
                let mut sources = vec![j.root.alias.clone()];
                for itm in j.joins.iter() {
                    sources.push(itm.from.alias.clone());
                }
                sources
            }
        }
    } */

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
