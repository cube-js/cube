use super::{CommonUtils, QueryPlanner};
use crate::logical_plan::{pretty_print_rc, DimensionSubQuery};
use crate::plan::{FilterItem, QualifiedColumnName};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::collect_sub_query_dimensions;
use crate::planner::sql_evaluator::{
    MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol,
};
use crate::planner::QueryProperties;
use cubenativeutils::CubeError;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

pub struct DimensionSubqueryPlanner {
    utils: CommonUtils,
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    sub_query_dims: HashMap<String, Vec<Rc<MemberSymbol>>>,
    dimensions_refs: RefCell<HashMap<String, QualifiedColumnName>>,
}

impl DimensionSubqueryPlanner {
    pub fn empty(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            sub_query_dims: HashMap::new(),
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
            query_properties,
            dimensions_refs: RefCell::new(HashMap::new()),
        }
    }
    pub fn try_new(
        dimensions: &Vec<Rc<MemberSymbol>>,
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
    ) -> Result<Self, CubeError> {
        let mut sub_query_dims: HashMap<String, Vec<Rc<MemberSymbol>>> = HashMap::new();
        for subquery_dimension in dimensions.iter() {
            let cube_name = subquery_dimension.cube_name().clone();
            sub_query_dims
                .entry(cube_name.clone())
                .or_default()
                .push(subquery_dimension.clone());
        }

        Ok(Self {
            sub_query_dims,
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
            query_properties,
            dimensions_refs: RefCell::new(HashMap::new()),
        })
    }

    pub fn plan_queries(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
    ) -> Result<Vec<Rc<DimensionSubQuery>>, CubeError> {
        let mut result = Vec::new();
        for subquery_dimension in dimensions.iter() {
            result.push(self.plan_query(subquery_dimension.clone())?)
        }
        Ok(result)
    }

    fn plan_query(
        &self,
        subquery_dimension: Rc<MemberSymbol>,
    ) -> Result<Rc<DimensionSubQuery>, CubeError> {
        let dim_name = subquery_dimension.name();
        let cube_name = subquery_dimension.cube_name().clone();
        let dimension_symbol = subquery_dimension.as_dimension()?;

        let primary_keys_dimensions = self.utils.primary_keys_dimensions(&cube_name)?;

        let expression = if let Some(sql_call) = dimension_symbol.member_sql() {
            sql_call.clone()
        } else {
            return Err(CubeError::user(format!(
                "Subquery dimension {} must have `sql` field",
                subquery_dimension.full_name()
            )));
        };

        let member_expression_symbol = MemberExpressionSymbol::try_new(
            cube_name.clone(),
            dim_name.clone(),
            MemberExpressionExpression::SqlCall(expression),
            None,
            self.query_tools.base_tools().clone(),
        )?;
        let measure = MemberSymbol::new_member_expression(member_expression_symbol);

        let (dimensions_filters, time_dimensions_filters) = if dimension_symbol
            .propagate_filters_to_sub_query()
        {
            let dimensions_filters = self
                .extract_filters_without_subqueries(self.query_properties.dimensions_filters())?;
            let time_dimensions_filters = self.extract_filters_without_subqueries(
                self.query_properties.time_dimensions_filters(),
            )?;
            (dimensions_filters, time_dimensions_filters)
        } else {
            (vec![], vec![])
        };

        let sub_query_properties = QueryProperties::try_new_from_precompiled(
            self.query_tools.clone(),
            vec![measure.clone()], //measures,
            primary_keys_dimensions.clone(),
            vec![],
            time_dimensions_filters,
            dimensions_filters,
            vec![],
            vec![],
            vec![],
            None,
            None,
            true,
            false,
            false,
            false,
            Rc::new(vec![]),
        )?;
        let query_planner = QueryPlanner::new(sub_query_properties, self.query_tools.clone());
        let sub_query = query_planner.plan()?;
        let result = Rc::new(DimensionSubQuery {
            query: sub_query,
            primary_keys_dimensions,
            subquery_dimension,
            measure_for_subquery_dimension: measure,
        });
        pretty_print_rc(&result);
        Ok(result)
    }

    fn extract_filters_without_subqueries(
        &self,
        filters: &Vec<FilterItem>,
    ) -> Result<Vec<FilterItem>, CubeError> {
        let mut result = vec![];
        for item in filters.iter() {
            if self.is_filter_without_subqueries(item)? {
                result.push(item.clone());
            }
        }

        Ok(result)
    }

    fn is_filter_without_subqueries(&self, filter_item: &FilterItem) -> Result<bool, CubeError> {
        match filter_item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    if !self.is_filter_without_subqueries(item)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            FilterItem::Item(filter_item) => {
                Ok(collect_sub_query_dimensions(&filter_item.member_evaluator())?.is_empty())
            }
            FilterItem::Segment(filter_item) => {
                Ok(collect_sub_query_dimensions(&filter_item.member_evaluator())?.is_empty())
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sub_query_dims.is_empty()
    }

    pub fn dimensions_refs(&self) -> Ref<HashMap<String, QualifiedColumnName>> {
        self.dimensions_refs.borrow()
    }
}
