use super::query_tools::QueryTools;
use super::BaseDimension;
use super::BaseField;
use crate::planner::utils::escape_column_name;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseTimeDimension {
    dimension: Rc<BaseDimension>,
    query_tools: Rc<QueryTools>,
    granularity: Option<String>,
    date_range: Vec<String>,
}

impl BaseField for BaseTimeDimension {
    fn to_sql(&self) -> Result<String, CubeError> {
        self.sql()
    }

    fn index(&self) -> usize {
        self.dimension.index()
    }
}

impl BaseTimeDimension {
    pub fn new(
        dimension: String,
        query_tools: Rc<QueryTools>,
        granularity: Option<String>,
        date_range: Vec<String>,
        index: usize,
    ) -> Rc<Self> {
        Rc::new(Self {
            dimension: BaseDimension::new(dimension, query_tools.clone(), index),
            query_tools,
            granularity,
            date_range,
        })
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

    pub fn index(&self) -> usize {
        self.dimension.index()
    }

    //FIXME May be should be part of BaseField Trait
    pub fn alias_name(&self) -> Result<String, CubeError> {
        let granularity = if let Some(granularity) = &self.granularity {
            granularity
        } else {
            "day"
        };

        self.query_tools
            .alias_name(&format!("{}_{}", self.dimension.dimension(), granularity))
    }

    fn sql(&self) -> Result<String, CubeError> {
        let alias_name = escape_column_name(&self.alias_name()?);

        let field_sql = if let Some(granularity) = &self.granularity {
            self.query_tools
                .base_tools()
                .time_grouped_column(granularity.clone(), self.convert_tz()?)?
        } else {
            unimplemented!("Time dimensions without granularity not supported yet")
        };
        Ok(format!("{} {}", field_sql, alias_name))
    }

    fn convert_tz(&self) -> Result<String, CubeError> {
        self.query_tools
            .base_tools()
            .convert_tz(self.dimension.dimension_sql()?)
    }
}
