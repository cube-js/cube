use super::{CommonUtils, QueryPlanner};
use crate::logical_plan::{
    LogicalMultiStageMember, MultiStageDimensionJoin, MultiStageDimensionRef, MultiStageMemberBody,
    MultiStageSubqueryRef,
};
use crate::planner::collectors::collect_sub_query_dimensions;
use crate::planner::filter::FilterItem;
use crate::planner::planners::multi_stage::{CteRole, CteState};
use crate::planner::query_tools::QueryTools;
use crate::planner::QueryProperties;
use crate::planner::{MeasureSymbol, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Plans `MultiStageDimensionRef` CTEs for `sub_query: true` dimensions.
/// Each subquery dimension becomes its own `LogicalPlan` over the owning
/// cube's primary keys plus the dimension's measure expression; the
/// reference carries an `OnPrimaryKeys` join descriptor so consumers can
/// stitch the CTE back into the host query.
pub struct DimensionSubqueryPlanner {
    utils: CommonUtils,
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    sub_query_dims: HashMap<String, Vec<Rc<MemberSymbol>>>,
}

impl DimensionSubqueryPlanner {
    /// Planner with no sub-query dimensions — used when the host
    /// query has none.
    pub fn empty(query_tools: Rc<QueryTools>, query_properties: Rc<QueryProperties>) -> Self {
        Self {
            sub_query_dims: HashMap::new(),
            utils: CommonUtils::new(query_tools.clone()),
            query_tools,
            query_properties,
        }
    }
    /// Builds a planner over the given sub-query dimensions, indexed
    /// by owning cube.
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
        })
    }

    /// Build a `MultiStageDimensionRef` per subquery dim and publish the
    /// body of each one as a `LogicalMultiStageMember` on `cte_state`.
    /// The caller stores returned refs on `Query.multi_stage_dimensions`
    /// of the Query that consumes them; the QueryProcessor reads them
    /// from there to wire CTE joins and render references.
    pub fn plan_queries(
        &self,
        dimensions: &Vec<Rc<MemberSymbol>>,
        cte_state: &mut CteState,
    ) -> Result<Vec<Rc<MultiStageDimensionRef>>, CubeError> {
        let mut result = Vec::new();
        for subquery_dimension in dimensions.iter() {
            result.push(self.plan_query(subquery_dimension.clone(), cte_state)?);
        }
        Ok(result)
    }

    fn plan_query(
        &self,
        subquery_dimension: Rc<MemberSymbol>,
        cte_state: &mut CteState,
    ) -> Result<Rc<MultiStageDimensionRef>, CubeError> {
        let cube_name = subquery_dimension.cube_name().clone();
        let dimension_symbol = subquery_dimension.as_dimension()?;

        let primary_keys_dimensions = self.utils.primary_keys_dimensions(&cube_name)?;

        let dimension = MemberSymbol::new_measure(MeasureSymbol::new_synthetic_from_dimension(
            &dimension_symbol,
        )?);

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

        let sub_query_properties = QueryProperties::builder()
            .query_tools(self.query_tools.clone())
            .measures(vec![dimension.clone()])
            .dimensions(primary_keys_dimensions.clone())
            .time_dimensions_filters(time_dimensions_filters)
            .dimensions_filters(dimensions_filters)
            .ignore_cumulative(true)
            .disable_external_pre_aggregations(
                self.query_properties.disable_external_pre_aggregations(),
            )
            .build()?;
        let query_planner =
            QueryPlanner::new(sub_query_properties.clone(), self.query_tools.clone());
        let body = query_planner.plan_into(cte_state)?;

        let cte_name = cte_state.next_cte_name(CteRole::MultiStageDimension);
        let schema = body.schema().clone();
        // DSQ uses MultiStageDimensionRef on the consumer side, not
        // MultiStageSubqueryRef — but the CteState dedup cache holds
        // MultiStageSubqueryRef. Stash a parallel SubqueryRef so the
        // entry can serve any future caller looking up by
        // (role, members, state).
        let cte_ref = Rc::new(
            MultiStageSubqueryRef::builder()
                .name(cte_name.clone())
                .symbols(vec![dimension.clone()])
                .schema(schema.clone())
                .build(),
        );
        cte_state.add_member(
            CteRole::MultiStageDimension,
            vec![dimension.clone()],
            sub_query_properties,
            Rc::new(LogicalMultiStageMember {
                name: cte_name.clone(),
                body: MultiStageMemberBody::Query(body),
            }),
            cte_ref,
        );

        Ok(Rc::new(MultiStageDimensionRef {
            name: cte_name,
            schema,
            join: MultiStageDimensionJoin::OnPrimaryKeys {
                cube_name,
                pk_dimensions: primary_keys_dimensions,
            },
            dimension,
        }))
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
}
