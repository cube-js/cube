use super::{time_seria, Schema, SingleAliasedSource};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseJoinCondition, BaseMember, VisitorContext};
use cubenativeutils::CubeError;

use std::rc::Rc;

pub struct RollingWindowJoinCondition {
    tailing_interval: Option<String>,
    leading_interval: Option<String>,
    offset: String,
    is_from_start_to_end: bool,
    time_dimension: Vec<Rc<BaseMember>>,
}

impl RollingWindowJoinCondition {
    pub fn new(
        trailing_interval: Option<String>,
        leading_interval: Option<String>,
        offset: String,
        is_from_start_to_end: bool,
        dimensions: Vec<Rc<BaseMember>>,
    ) -> Self {
        Self {
            tailing_interval,
            leading_interval,
            offset,
            is_from_start_to_end,
            time_dimension,
        }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let result = if self.dimensions.is_empty() {
            format!("1 = 1")
        } else {
            let conditions = vec![];
            self.dimensions
                .iter()
                .map(|dim| -> Result<String, CubeError> {
                    if let Some(trailing_interval) = self.trailing_interval {
                        if tailing_interval == "unbounded" {
                            let seria_column = "date_from",
                        }
                    }


                })
                .collect::<Result<Vec<_>, _>>()?
                .join(" AND ")
        };
        Ok(result)
    }

    fn resolve_member_alias(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        source: &String,
        dimension: &Rc<dyn BaseMember>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let schema = schema.extract_source_schema(source);
        let source = Some(source.clone());
        if let Some(column) = schema.find_column_for_member(&dimension.full_name(), &source) {
            templates.column_reference(&source, &column.alias.clone())
        } else {
            dimension.to_sql(context.clone(), schema.clone())
        }
    }
}

pub struct DimensionJoinCondition {
    left_source: String,
    right_source: String,
    dimensions: Vec<Rc<dyn BaseMember>>,
    null_check: bool,
}

impl DimensionJoinCondition {
    pub fn new(
        left_source: String,
        right_source: String,
        dimensions: Vec<Rc<dyn BaseMember>>,
        null_check: bool,
    ) -> Self {
        Self {
            left_source,
            right_source,
            dimensions,
            null_check,
        }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let result = if self.dimensions.is_empty() {
            format!("1 = 1")
        } else {
            self.dimensions
                .iter()
                .map(|dim| -> Result<String, CubeError> {
                    self.dimension_condition(templates, context.clone(), dim, schema.clone())
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(" AND ")
        };
        Ok(result)
    }

    fn dimension_condition(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        dimension: &Rc<dyn BaseMember>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let left_column = self.resolve_member_alias(
            templates,
            context.clone(),
            &self.left_source,
            dimension,
            schema.clone(),
        )?;
        let right_column = self.resolve_member_alias(
            templates,
            context.clone(),
            &self.right_source,
            dimension,
            schema.clone(),
        )?;
        templates.join_by_dimension_conditions(&left_column, &right_column, self.null_check)
    }

    fn resolve_member_alias(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        source: &String,
        dimension: &Rc<dyn BaseMember>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let schema = schema.extract_source_schema(source);
        let source = Some(source.clone());
        if let Some(column) = schema.find_column_for_member(&dimension.full_name(), &source) {
            templates.column_reference(&source, &column.alias.clone())
        } else {
            dimension.to_sql(context.clone(), schema.clone())
        }
    }
}

pub enum JoinCondition {
    DimensionJoinCondition(DimensionJoinCondition),
    BaseJoinCondition(Rc<dyn BaseJoinCondition>),
}

impl JoinCondition {
    pub fn new_dimension_join(
        left_source: String,
        right_source: String,
        dimensions: Vec<Rc<dyn BaseMember>>,
        null_check: bool,
    ) -> Self {
        Self::DimensionJoinCondition(DimensionJoinCondition::new(
            left_source,
            right_source,
            dimensions,
            null_check,
        ))
    }

    pub fn new_base_join(base: Rc<dyn BaseJoinCondition>) -> Self {
        Self::BaseJoinCondition(base)
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        match &self {
            JoinCondition::DimensionJoinCondition(cond) => cond.to_sql(templates, context, schema),
            JoinCondition::BaseJoinCondition(cond) => cond.to_sql(context, schema),
        }
    }
}

pub struct JoinItem {
    pub from: SingleAliasedSource,
    pub on: JoinCondition,
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
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let on_sql = self.on.to_sql(templates, context.clone(), schema)?;
        let result = templates.join(
            &self.from.to_sql(templates, context)?,
            &on_sql,
            self.is_inner,
        )?;
        Ok(result)
    }
}

impl Join {
    pub fn make_schema(&self) -> Schema {
        let mut schema = self.root.make_schema();
        for itm in self.joins.iter() {
            schema.merge(itm.from.make_schema());
        }
        schema
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let schema = Rc::new(self.make_schema());
        let joins_sql = self
            .joins
            .iter()
            .map(|j| j.to_sql(templates, context.clone(), schema.clone()))
            .collect::<Result<Vec<_>, _>>()?;
        let res = format!(
            "{}\n{}",
            self.root.to_sql(templates, context.clone())?,
            joins_sql.join("\n")
        );
        Ok(res)
    }
}
