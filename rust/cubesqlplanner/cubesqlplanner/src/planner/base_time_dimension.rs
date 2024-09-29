use super::query_tools::QueryTools;
use super::sql_evaluator::{DimensionEvaluator, EvaluationNode};
use super::BaseDimension;
use super::{evaluate_with_context, BaseMember, Context, IndexedMember};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseTimeDimension {
    dimension: Rc<BaseDimension>,
    query_tools: Rc<QueryTools>,
    granularity: Option<String>,
    date_range: Vec<String>,
}

impl BaseMember for BaseTimeDimension {
    fn to_sql(&self, context: Rc<Context>) -> Result<String, CubeError> {
        let alias_name = self.alias_name()?;

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

    fn alias_name(&self) -> Result<String, CubeError> {
        Ok(self
            .query_tools
            .escape_column_name(&self.unescaped_alias_name()?))
    }
}

impl IndexedMember for BaseTimeDimension {
    fn index(&self) -> usize {
        self.dimension.index()
    }
}

impl BaseTimeDimension {
    pub fn try_new(
        dimension: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
        granularity: Option<String>,
        date_range: Vec<String>,
        index: usize,
    ) -> Result<Rc<Self>, CubeError> {
        Ok(Rc::new(Self {
            dimension: BaseDimension::try_new(
                dimension,
                query_tools.clone(),
                member_evaluator,
                index,
            )?,
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

    pub fn get_date_range(&self) -> Vec<String> {
        self.date_range.clone()
    }

    pub fn base_dimension(&self) -> Rc<BaseDimension> {
        self.dimension.clone()
    }

    pub fn member_evaluator(&self) -> Rc<EvaluationNode> {
        self.dimension.member_evaluator()
    }

    pub fn index(&self) -> usize {
        self.dimension.index()
    }

    pub fn unescaped_alias_name(&self) -> Result<String, CubeError> {
        let granularity = if let Some(granularity) = &self.granularity {
            granularity
        } else {
            "day"
        };

        Ok(self
            .query_tools
            .alias_name(&format!("{}_{}", self.dimension.dimension(), granularity)))
    }
}
