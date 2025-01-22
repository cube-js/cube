use super::{CommonUtils, QueryPlanner};
use crate::plan::{Expr, FilterItem, JoinBuilder, JoinCondition, QualifiedColumnName};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::collect_sub_query_dimensions;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, BaseMember};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

pub struct DimensionSubqueryPlanner {
    utils: CommonUtils,
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    sub_query_dims: HashMap<String, Vec<Rc<BaseDimension>>>,
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
        dimensions: &Vec<Rc<BaseDimension>>,
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
    ) -> Result<Self, CubeError> {
        let mut sub_query_dims: HashMap<String, Vec<Rc<BaseDimension>>> = HashMap::new();
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

    pub fn add_joins_for_cube(
        &self,
        builder: &mut JoinBuilder,
        cube_name: &String,
    ) -> Result<(), CubeError> {
        if let Some(sub_query_dims) = self.sub_query_dims.get(cube_name) {
            let primary_keys_dimensions = self.utils.primary_keys_dimensions(cube_name)?;
            for dim in sub_query_dims.iter() {
                self.add_join_impl(builder, cube_name, &primary_keys_dimensions, dim.clone())?
            }
        }
        Ok(())
    }

    pub fn add_join(
        &self,
        builder: &mut JoinBuilder,
        subquery_dimension: Rc<BaseDimension>,
    ) -> Result<(), CubeError> {
        let cube_name = subquery_dimension.cube_name();
        let primary_keys_dimensions = self.utils.primary_keys_dimensions(cube_name)?;
        self.add_join_impl(
            builder,
            cube_name,
            &primary_keys_dimensions,
            subquery_dimension.clone(),
        )
    }

    fn add_join_impl(
        &self,
        builder: &mut JoinBuilder,
        cube_name: &String,
        primary_keys_dimensions: &Vec<Rc<BaseDimension>>,
        subquery_dimension: Rc<BaseDimension>,
    ) -> Result<(), CubeError> {
        let dim_name = subquery_dimension.name();
        let dim_full_name = subquery_dimension.full_name();
        let expression = subquery_dimension.sql_call()?;
        let measure = BaseMeasure::try_new_from_expression(
            expression,
            cube_name.clone(),
            dim_name.clone(),
            None,
            self.query_tools.clone(),
        )?;

        let (dimensions_filters, time_dimensions_filters) = if subquery_dimension
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
            vec![measure], //measures,
            primary_keys_dimensions.clone(),
            vec![],
            time_dimensions_filters,
            dimensions_filters,
            vec![],
            vec![],
            None,
            None,
            true,
            false,
        )?;
        let query_planner = QueryPlanner::new(sub_query_properties, self.query_tools.clone());
        let sub_query = query_planner.plan()?;
        let sub_query_alias = format!("{cube_name}_{dim_name}_subquery");

        let conditions = primary_keys_dimensions
            .iter()
            .map(|dim| {
                let dim = dim.clone().as_base_member();
                let alias_in_sub_query = sub_query.schema().resolve_member_alias(&dim);
                let sub_query_ref = Expr::Reference(QualifiedColumnName::new(
                    Some(sub_query_alias.clone()),
                    alias_in_sub_query.clone(),
                ));

                vec![(sub_query_ref, Expr::new_member(dim))]
            })
            .collect_vec();

        if let Some(dim_ref) = sub_query.schema().resolve_member_reference(&dim_full_name) {
            let qualified_column_name =
                QualifiedColumnName::new(Some(sub_query_alias.clone()), dim_ref);
            self.dimensions_refs
                .borrow_mut()
                .insert(dim_full_name.clone(), qualified_column_name);
        } else {
            return Err(CubeError::internal(format!(
                "Can't find source for subquery dimension {}",
                dim_name
            )));
        }

        builder.left_join_subselect(
            sub_query,
            sub_query_alias,
            JoinCondition::new_dimension_join(conditions, false),
        );
        Ok(())
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
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sub_query_dims.is_empty()
    }

    pub fn dimensions_refs(&self) -> Ref<HashMap<String, QualifiedColumnName>> {
        self.dimensions_refs.borrow()
    }
}
