use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::BaseDimension;
use super::{BaseMember, VisitorContext};
use crate::plan::Schema;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseTimeDimension {
    dimension: Rc<BaseDimension>,
    query_tools: Rc<QueryTools>,
    granularity: Option<String>,
    date_range: Option<Vec<String>>,
    alias_suffix: String,
}

impl BaseMember for BaseTimeDimension {
    fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        source_schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let field_sql = if let Some(granularity) = &self.granularity {
            let converted_tz = self
                .query_tools
                .base_tools()
                .convert_tz(self.dimension.to_sql(context, source_schema)?)?;
            self.query_tools
                .base_tools()
                .time_grouped_column(granularity.clone(), converted_tz)?
        } else {
            unimplemented!("Time dimensions without granularity not supported yet")
        };
        Ok(field_sql)
    }

    fn alias_name(&self) -> String {
        self.query_tools
            .escape_column_name(&self.unescaped_alias_name())
    }

    fn member_evaluator(&self) -> Rc<EvaluationNode> {
        self.dimension.member_evaluator()
    }

    fn as_base_member(self: Rc<Self>) -> Rc<dyn BaseMember> {
        self.clone()
    }

    fn cube_name(&self) -> &String {
        &self.dimension.cube_name()
    }

    fn name(&self) -> &String {
        &self.dimension.name()
    }

    fn alias_suffix(&self) -> Option<String> {
        Some(self.alias_suffix.clone())
    }
}

impl BaseTimeDimension {
    pub fn try_new_required(
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
        granularity: Option<String>,
        date_range: Option<Vec<String>>,
    ) -> Result<Rc<Self>, CubeError> {
        let alias_suffix = if let Some(granularity) = &granularity {
            granularity.clone()
        } else {
            "day".to_string()
        };
        Ok(Rc::new(Self {
            dimension: BaseDimension::try_new_required(member_evaluator, query_tools.clone())?,
            query_tools,
            granularity,
            date_range,
            alias_suffix,
        }))
    }

    pub fn change_granularity(&self, new_granularity: Option<String>) -> Rc<Self> {
        Rc::new(Self {
            dimension: self.dimension.clone(),
            query_tools: self.query_tools.clone(),
            granularity: new_granularity,
            date_range: self.date_range.clone(),
            alias_suffix: self.alias_suffix.clone(),
        })
    }

    pub fn get_granularity(&self) -> Option<String> {
        self.granularity.clone()
    }

    pub fn has_granularity(&self) -> bool {
        self.granularity.is_some()
    }

    pub fn get_date_range(&self) -> Option<Vec<String>> {
        self.date_range.clone()
    }

    pub fn base_dimension(&self) -> Rc<BaseDimension> {
        self.dimension.clone()
    }

    pub fn member_evaluator(&self) -> Rc<EvaluationNode> {
        self.dimension.member_evaluator()
    }

    pub fn unescaped_alias_name(&self) -> String {
        let granularity = if let Some(granularity) = &self.granularity {
            granularity
        } else {
            "day"
        };

        self.query_tools
            .alias_name(&format!("{}_{}", self.dimension.dimension(), granularity))
    }
}
