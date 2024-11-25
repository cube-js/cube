use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::BaseDimension;
use super::{BaseMember, VisitorContext};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseTimeDimension {
    dimension: Rc<BaseDimension>,
    query_tools: Rc<QueryTools>,
    granularity: Option<String>,
    date_range: Option<Vec<String>>,
}

impl BaseMember for BaseTimeDimension {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let alias_name = self.alias_name();

        let field_sql = if let Some(granularity) = &self.granularity {
            let converted_tz = self
                .query_tools
                .base_tools()
                .convert_tz(self.dimension.dimension_sql(context)?)?;
            self.query_tools
                .base_tools()
                .time_grouped_column(granularity.clone(), converted_tz)?
        } else {
            unimplemented!("Time dimensions without granularity not supported yet")
        };
        Ok(format!("{} {}", field_sql, alias_name))
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
}

impl BaseTimeDimension {
    pub fn try_new(
        dimension: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
        granularity: Option<String>,
        date_range: Option<Vec<String>>,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            dimension: BaseDimension::try_new(dimension, query_tools.clone(), member_evaluator)?,
            query_tools,
            granularity,
            date_range,
        }))
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
