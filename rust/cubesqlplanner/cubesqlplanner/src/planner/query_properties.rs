use super::filter::compiler::FilterCompiler;
use super::filter::BaseSegment;
use super::query_tools::QueryTools;

use super::sql_evaluator::MemberSymbol;
use super::{BaseDimension, BaseMeasure, BaseMember, BaseMemberHelper, BaseTimeDimension};
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::options_member::OptionsMember;
use crate::plan::{Expr, Filter, FilterItem, MemberExpression};
use crate::planner::sql_evaluator::collectors::{
    collect_multiplied_measures, has_cumulative_members, has_multi_stage_members,
};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct OrderByItem {
    member_evaluator: Rc<MemberSymbol>,
    desc: bool,
}

impl OrderByItem {
    pub fn new(member_evaluator: Rc<MemberSymbol>, desc: bool) -> Self {
        Self {
            member_evaluator,
            desc,
        }
    }

    pub fn name(&self) -> String {
        self.member_evaluator.full_name()
    }

    pub fn member_symbol(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    pub fn desc(&self) -> bool {
        self.desc
    }
}

#[derive(Default, Clone, Debug)]
pub struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<BaseMeasure>>,
    pub regular_measures: Vec<Rc<BaseMeasure>>,
    pub multi_stage_measures: Vec<Rc<BaseMeasure>>,
    pub rendered_as_multiplied_measures: HashSet<String>,
}

impl FullKeyAggregateMeasures {
    pub fn has_multiplied_measures(&self) -> bool {
        !self.multiplied_measures.is_empty()
    }

    pub fn has_multi_stage_measures(&self) -> bool {
        !self.multi_stage_measures.is_empty()
    }
}

#[derive(Clone)]
pub struct QueryProperties {
    measures: Vec<Rc<BaseMeasure>>,
    dimensions: Vec<Rc<BaseDimension>>,
    dimensions_filters: Vec<FilterItem>,
    time_dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    segments: Vec<FilterItem>,
    time_dimensions: Vec<Rc<BaseTimeDimension>>,
    order_by: Vec<OrderByItem>,
    row_limit: Option<usize>,
    offset: Option<usize>,
    query_tools: Rc<QueryTools>,
    ignore_cumulative: bool,
    ungrouped: bool,
    multi_fact_join_groups: Vec<(Rc<dyn JoinDefinition>, Vec<Rc<BaseMeasure>>)>,
}

impl QueryProperties {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Rc<Self>, CubeError> {
        let evaluator_compiler_cell = query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let dimensions = if let Some(dimensions) = &options.dimensions()? {
            dimensions
                .iter()
                .map(|d| match d {
                    OptionsMember::MemberName(member_name) => {
                        let evaluator =
                            evaluator_compiler.add_dimension_evaluator(member_name.clone())?;
                        BaseDimension::try_new_required(evaluator, query_tools.clone())
                    }
                    OptionsMember::MemberExpression(member_expression) => {
                        let cube_name =
                            if let Some(name) = &member_expression.static_data().cube_name {
                                name.clone()
                            } else {
                                "".to_string()
                            };
                        let name =
                            if let Some(name) = &member_expression.static_data().expression_name {
                                name.clone()
                            } else {
                                "".to_string()
                            };
                        let expression_evaluator = evaluator_compiler
                            .compile_sql_call(&cube_name, member_expression.expression()?)?;
                        BaseDimension::try_new_from_expression(
                            expression_evaluator,
                            cube_name,
                            name,
                            member_expression.static_data().definition.clone(),
                            query_tools.clone(),
                        )
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let time_dimensions = if let Some(time_dimensions) = &options.static_data().time_dimensions
        {
            time_dimensions
                .iter()
                .map(|d| {
                    let evaluator =
                        evaluator_compiler.add_dimension_evaluator(d.dimension.clone())?;
                    BaseTimeDimension::try_new_required(
                        query_tools.clone(),
                        evaluator,
                        d.granularity.clone(),
                        d.date_range.clone(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let measures = if let Some(measures) = &options.measures()? {
            measures
                .iter()
                .map(|d| match d {
                    OptionsMember::MemberName(member_name) => {
                        let evaluator =
                            evaluator_compiler.add_measure_evaluator(member_name.clone())?;
                        BaseMeasure::try_new_required(evaluator, query_tools.clone())
                    }
                    OptionsMember::MemberExpression(member_expression) => {
                        let cube_name =
                            if let Some(name) = &member_expression.static_data().cube_name {
                                name.clone()
                            } else {
                                "".to_string()
                            };
                        let name =
                            if let Some(name) = &member_expression.static_data().expression_name {
                                name.clone()
                            } else {
                                "".to_string()
                            };
                        let expression_evaluator = evaluator_compiler
                            .compile_sql_call(&cube_name, member_expression.expression()?)?;
                        BaseMeasure::try_new_from_expression(
                            expression_evaluator,
                            cube_name,
                            name,
                            member_expression.static_data().definition.clone(),
                            query_tools.clone(),
                        )
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
            /* measures
            .iter()
            .map(|m| {
                let evaluator = evaluator_compiler.add_measure_evaluator(m.clone())?;
                BaseMeasure::try_new_required(evaluator, query_tools.clone())
            })
            .collect::<Result<Vec<_>, _>>()? */
        } else {
            Vec::new()
        };

        let segments = if let Some(segments) = &options.segments()? {
            segments
                .iter()
                .map(|d| -> Result<_, CubeError> {
                    let segment = match d {
                        OptionsMember::MemberName(member_name) => {
                            let mut iter = query_tools
                                .cube_evaluator()
                                .parse_path("segments".to_string(), member_name.clone())?
                                .into_iter();
                            let cube_name = iter.next().unwrap();
                            let name = iter.next().unwrap();
                            let definition = query_tools
                                .cube_evaluator()
                                .segment_by_path(member_name.clone())?;
                            let expression_evaluator = evaluator_compiler
                                .compile_sql_call(&cube_name, definition.sql()?)?;
                            BaseSegment::try_new(
                                expression_evaluator,
                                cube_name,
                                name,
                                Some(member_name.clone()),
                                query_tools.clone(),
                            )
                        }
                        OptionsMember::MemberExpression(member_expression) => {
                            let cube_name =
                                if let Some(name) = &member_expression.static_data().cube_name {
                                    name.clone()
                                } else {
                                    "".to_string()
                                };
                            let name = if let Some(name) =
                                &member_expression.static_data().expression_name
                            {
                                name.clone()
                            } else {
                                "".to_string()
                            };
                            let expression_evaluator = evaluator_compiler
                                .compile_sql_call(&cube_name, member_expression.expression()?)?;
                            BaseSegment::try_new(
                                expression_evaluator,
                                cube_name,
                                name,
                                None,
                                query_tools.clone(),
                            )
                        }
                    }?;
                    Ok(FilterItem::Segment(segment))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let mut filter_compiler = FilterCompiler::new(&mut evaluator_compiler, query_tools.clone());
        if let Some(filters) = &options.static_data().filters {
            for filter in filters {
                filter_compiler.add_item(filter)?;
            }
        }
        for time_dimension in &time_dimensions {
            filter_compiler.add_time_dimension_item(time_dimension)?;
        }
        let (dimensions_filters, time_dimensions_filters, measures_filters) =
            filter_compiler.extract_result();

        //FIXME may be this filter should be applied on other place
        let time_dimensions = time_dimensions
            .into_iter()
            .filter(|dim| dim.has_granularity())
            .collect_vec();

        let order_by = if let Some(order) = &options.static_data().order {
            order
                .iter()
                .map(|o| -> Result<_, CubeError> {
                    let member_evaluator =
                        evaluator_compiler.add_auto_resolved_member_evaluator(o.id.clone())?;
                    Ok(OrderByItem::new(member_evaluator, o.is_desc()))
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Self::default_order(&dimensions, &time_dimensions, &measures)
        };

        let row_limit = if let Some(row_limit) = &options.static_data().row_limit {
            row_limit.parse::<usize>().ok()
        } else {
            None
        };
        let offset = if let Some(offset) = &options.static_data().offset {
            offset.parse::<usize>().ok()
        } else {
            None
        };
        let ungrouped = options.static_data().ungrouped.unwrap_or(false);

        let multi_fact_join_groups = Self::compute_join_multi_fact_groups(
            query_tools.clone(),
            &measures,
            &dimensions,
            &time_dimensions,
            &time_dimensions_filters,
            &dimensions_filters,
            &measures_filters,
            &segments,
        )?;

        Ok(Rc::new(Self {
            measures,
            dimensions,
            segments,
            time_dimensions,
            time_dimensions_filters,
            dimensions_filters,
            measures_filters,
            order_by,
            row_limit,
            offset,
            query_tools,
            ignore_cumulative: false,
            ungrouped,
            multi_fact_join_groups,
        }))
    }

    pub fn try_new_from_precompiled(
        query_tools: Rc<QueryTools>,
        measures: Vec<Rc<BaseMeasure>>,
        dimensions: Vec<Rc<BaseDimension>>,
        time_dimensions: Vec<Rc<BaseTimeDimension>>,
        time_dimensions_filters: Vec<FilterItem>,
        dimensions_filters: Vec<FilterItem>,
        measures_filters: Vec<FilterItem>,
        segments: Vec<FilterItem>,
        order_by: Vec<OrderByItem>,
        row_limit: Option<usize>,
        offset: Option<usize>,
        ignore_cumulative: bool,
        ungrouped: bool,
    ) -> Result<Rc<Self>, CubeError> {
        let order_by = if order_by.is_empty() {
            Self::default_order(&dimensions, &time_dimensions, &measures)
        } else {
            order_by
        };

        let multi_fact_join_groups = Self::compute_join_multi_fact_groups(
            query_tools.clone(),
            &measures,
            &dimensions,
            &time_dimensions,
            &time_dimensions_filters,
            &dimensions_filters,
            &measures_filters,
            &segments,
        )?;

        Ok(Rc::new(Self {
            measures,
            dimensions,
            time_dimensions,
            time_dimensions_filters,
            dimensions_filters,
            segments,
            measures_filters,
            order_by,
            row_limit,
            offset,
            query_tools,
            ignore_cumulative,
            ungrouped,
            multi_fact_join_groups,
        }))
    }

    pub fn compute_join_multi_fact_groups_with_measures(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Vec<(Rc<dyn JoinDefinition>, Vec<Rc<BaseMeasure>>)>, CubeError> {
        Self::compute_join_multi_fact_groups(
            self.query_tools.clone(),
            measures,
            &self.dimensions,
            &self.time_dimensions,
            &self.time_dimensions_filters,
            &self.dimensions_filters,
            &self.measures_filters,
            &self.segments,
        )
    }

    pub fn compute_join_multi_fact_groups(
        query_tools: Rc<QueryTools>,
        measures: &Vec<Rc<BaseMeasure>>,
        dimensions: &Vec<Rc<BaseDimension>>,
        time_dimensions: &Vec<Rc<BaseTimeDimension>>,
        time_dimensions_filters: &Vec<FilterItem>,
        dimensions_filters: &Vec<FilterItem>,
        measures_filters: &Vec<FilterItem>,
        segments: &Vec<FilterItem>,
    ) -> Result<Vec<(Rc<dyn JoinDefinition>, Vec<Rc<BaseMeasure>>)>, CubeError> {
        let dimensions_join_hints = query_tools
            .cached_data_mut()
            .join_hints_for_base_member_vec(&dimensions)?;
        let time_dimensions_join_hints = query_tools
            .cached_data_mut()
            .join_hints_for_base_member_vec(&time_dimensions)?;
        let time_dimensions_filters_join_hints = query_tools
            .cached_data_mut()
            .join_hints_for_filter_item_vec(&time_dimensions_filters)?;
        let dimensions_filters_join_hints = query_tools
            .cached_data_mut()
            .join_hints_for_filter_item_vec(&dimensions_filters)?;
        let segments_join_hints = query_tools
            .cached_data_mut()
            .join_hints_for_filter_item_vec(&segments)?;
        let measures_filters_join_hints = query_tools
            .cached_data_mut()
            .join_hints_for_filter_item_vec(&measures_filters)?;

        let mut dimension_and_filter_join_hints_concat = Vec::new();

        dimension_and_filter_join_hints_concat.extend(dimensions_join_hints.into_iter());
        dimension_and_filter_join_hints_concat.extend(time_dimensions_join_hints.into_iter());
        dimension_and_filter_join_hints_concat
            .extend(time_dimensions_filters_join_hints.into_iter());
        dimension_and_filter_join_hints_concat.extend(dimensions_filters_join_hints.into_iter());
        dimension_and_filter_join_hints_concat.extend(segments_join_hints.into_iter());
        // TODO This is not quite correct. Decide on how to handle it. Keeping it here just to blow up on unsupported case
        dimension_and_filter_join_hints_concat.extend(measures_filters_join_hints.into_iter());

        let measures_to_join = if measures.is_empty() {
            let join = query_tools
                .cached_data_mut()
                .join_by_hints(dimension_and_filter_join_hints_concat.clone(), |hints| {
                    query_tools.join_graph().build_join(hints)
                })?;
            vec![(Vec::new(), join)]
        } else {
            measures
                .iter()
                .map(|m| -> Result<_, CubeError> {
                    let measure_join_hints = query_tools
                        .cached_data_mut()
                        .join_hints_for_member(m.member_evaluator())?;
                    let join = query_tools.cached_data_mut().join_by_hints(
                        vec![measure_join_hints]
                            .into_iter()
                            .chain(dimension_and_filter_join_hints_concat.clone().into_iter())
                            .collect::<Vec<_>>(),
                        |hints| query_tools.join_graph().build_join(hints),
                    )?;
                    Ok((vec![m.clone()], join))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(measures_to_join
            .into_iter()
            .into_group_map_by(|(_, (key, _))| key.clone())
            .into_values()
            .map(|measures_and_join| {
                (
                    measures_and_join.iter().next().unwrap().1 .1.clone(),
                    measures_and_join
                        .into_iter()
                        .flat_map(|m| m.0)
                        .collect::<Vec<_>>(),
                )
            })
            .collect())
    }

    pub fn is_multi_fact_join(&self) -> bool {
        self.multi_fact_join_groups.len() > 1
    }

    pub fn simple_query_join(&self) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        if self.multi_fact_join_groups.len() != 1 {
            return Err(CubeError::internal(format!(
                "Expected just one multi-fact join group for simple query but got multiple: {}",
                self.multi_fact_join_groups
                    .iter()
                    .map(|(_, measures)| format!(
                        "({})",
                        measures.iter().map(|m| m.full_name()).join(", ")
                    ))
                    .join(", ")
            )));
        }
        Ok(self.multi_fact_join_groups.iter().next().unwrap().0.clone())
    }

    pub fn measures(&self) -> &Vec<Rc<BaseMeasure>> {
        &self.measures
    }

    pub fn dimensions(&self) -> &Vec<Rc<BaseDimension>> {
        &self.dimensions
    }

    pub fn dimension_symbols(&self) -> Vec<Rc<MemberSymbol>> {
        self.dimensions
            .iter()
            .map(|d| d.member_evaluator().clone())
            .collect()
    }

    pub fn time_dimension_symbols(&self) -> Vec<Rc<MemberSymbol>> {
        self.time_dimensions
            .iter()
            .map(|d| d.member_evaluator().clone())
            .collect()
    }

    pub fn measure_symbols(&self) -> Vec<Rc<MemberSymbol>> {
        self.measures
            .iter()
            .map(|d| d.member_evaluator().clone())
            .collect()
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<BaseTimeDimension>> {
        &self.time_dimensions
    }

    pub fn time_dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.time_dimensions_filters
    }

    pub fn dimensions_filters(&self) -> &Vec<FilterItem> {
        &self.dimensions_filters
    }

    pub fn measures_filters(&self) -> &Vec<FilterItem> {
        &self.measures_filters
    }

    pub fn row_limit(&self) -> Option<usize> {
        self.row_limit
    }

    pub fn offset(&self) -> Option<usize> {
        self.offset
    }

    pub fn order_by(&self) -> &Vec<OrderByItem> {
        &self.order_by
    }

    pub fn set_order_by_to_default(&mut self) {
        self.order_by =
            Self::default_order(&self.dimensions, &self.time_dimensions, &self.measures);
    }

    pub fn ungrouped(&self) -> bool {
        self.ungrouped
    }

    pub fn all_filters(&self) -> Option<Filter> {
        let items = self
            .time_dimensions_filters
            .iter()
            .chain(self.dimensions_filters.iter())
            .chain(self.segments.iter())
            .cloned()
            .collect_vec();
        if items.is_empty() {
            None
        } else {
            Some(Filter { items })
        }
    }

    pub fn segments(&self) -> &Vec<FilterItem> {
        &self.segments
    }

    pub fn all_dimensions_and_measures(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Vec<Rc<dyn BaseMember>>, CubeError> {
        let result = BaseMemberHelper::iter_as_base_member(&self.dimensions)
            .chain(BaseMemberHelper::iter_as_base_member(&self.time_dimensions))
            .chain(BaseMemberHelper::iter_as_base_member(&measures))
            .collect_vec();
        Ok(result)
    }

    pub fn dimensions_for_select(&self) -> Vec<Rc<dyn BaseMember>> {
        let time_dimensions = self
            .time_dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let dimensions = self
            .dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        dimensions.chain(time_dimensions).collect()
    }

    pub fn dimensions_for_select_append(
        &self,
        append: &Vec<Rc<dyn BaseMember>>,
    ) -> Vec<Rc<dyn BaseMember>> {
        let time_dimensions = BaseMemberHelper::iter_as_base_member(&self.time_dimensions);
        let append_dims = append.iter().cloned();
        let dimensions = BaseMemberHelper::iter_as_base_member(&self.dimensions);
        dimensions
            .chain(time_dimensions)
            .chain(append_dims)
            .collect()
    }

    pub fn all_members(&self, exclude_time_dimensions: bool) -> Vec<Rc<dyn BaseMember>> {
        let dimensions = self
            .dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let measures = self
            .measures
            .iter()
            .map(|m| -> Rc<dyn BaseMember> { m.clone() });
        if exclude_time_dimensions {
            dimensions.chain(measures).collect_vec()
        } else {
            let time_dimensions = self
                .time_dimensions
                .iter()
                .map(|d| -> Rc<dyn BaseMember> { d.base_dimension().clone() });
            dimensions
                .chain(time_dimensions)
                .chain(measures)
                .collect_vec()
        }
    }
    pub fn all_member_symbols(&self, exclude_time_dimensions: bool) -> Vec<Rc<MemberSymbol>> {
        self.get_member_symbols(!exclude_time_dimensions, true, true, true, &vec![])
    }

    pub fn get_member_symbols(
        &self,
        include_time_dimensions: bool,
        include_dimensions: bool,
        include_measures: bool,
        include_filters: bool,
        additional_symbols: &Vec<Rc<MemberSymbol>>,
    ) -> Vec<Rc<MemberSymbol>> {
        let mut members = additional_symbols.clone();
        if include_time_dimensions {
            members.append(&mut self.time_dimension_symbols());
        }
        if include_dimensions {
            members.append(&mut self.dimension_symbols());
        }
        if include_measures {
            members.append(&mut self.measure_symbols());
        }
        if include_filters {
            self.fill_all_filter_symbols(&mut members);
        }
        members
            .into_iter()
            .unique_by(|m| m.full_name())
            .collect_vec()
    }

    pub fn fill_all_filter_symbols(&self, members: &mut Vec<Rc<MemberSymbol>>) {
        if let Some(all_filters) = self.all_filters() {
            for filter_item in all_filters.items.iter() {
                filter_item.find_all_member_evaluators(members);
            }
        }
    }

    pub fn group_by(&self) -> Vec<Expr> {
        if self.ungrouped {
            vec![]
        } else {
            self.dimensions
                .iter()
                .map(|f| Expr::Member(MemberExpression::new(f.clone())))
                .chain(
                    self.time_dimensions
                        .iter()
                        .map(|f| Expr::Member(MemberExpression::new(f.clone()))),
                )
                .collect()
        }
    }

    pub fn default_order(
        dimensions: &Vec<Rc<BaseDimension>>,
        time_dimensions: &Vec<Rc<BaseTimeDimension>>,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Vec<OrderByItem> {
        let mut result = Vec::new();
        if let Some(granularity_dim) = time_dimensions.iter().find(|d| d.has_granularity()) {
            result.push(OrderByItem::new(
                granularity_dim.member_evaluator().clone(),
                false,
            ));
        } else if !measures.is_empty() && !dimensions.is_empty() {
            result.push(OrderByItem::new(
                measures[0].member_evaluator().clone(),
                true,
            ));
        } else if !dimensions.is_empty() {
            result.push(OrderByItem::new(
                dimensions[0].member_evaluator().clone(),
                false,
            ));
        }
        result
    }

    pub fn all_filtered_members(&self) -> HashSet<String> {
        let mut result = HashSet::new();
        for item in self.dimensions_filters().iter() {
            self.fill_members_from_filter_item(item, &mut result);
        }
        for item in self.time_dimensions_filters().iter() {
            self.fill_members_from_filter_item(item, &mut result);
        }
        for item in self.measures_filters().iter() {
            self.fill_members_from_filter_item(item, &mut result);
        }
        result
    }

    fn fill_members_from_filter_item(&self, item: &FilterItem, members: &mut HashSet<String>) {
        match item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    self.fill_members_from_filter_item(item, members)
                }
            }
            FilterItem::Item(item) => {
                members.insert(item.member_name());
            }
            FilterItem::Segment(_) => {}
        }
    }

    pub fn is_simple_query(&self) -> Result<bool, CubeError> {
        let full_aggregate_measure = self.full_key_aggregate_measures()?;
        if full_aggregate_measure.multiplied_measures.is_empty()
            && full_aggregate_measure.multi_stage_measures.is_empty()
            && !self.is_multi_fact_join()
        {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn should_use_time_series(&self) -> Result<bool, CubeError> {
        for member in self.all_members(false) {
            if has_cumulative_members(&member.member_evaluator())? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn full_key_aggregate_measures(&self) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        let measures = self.all_used_measures()?;
        for m in measures.iter() {
            if has_multi_stage_members(m.member_evaluator(), self.ignore_cumulative)? {
                result.multi_stage_measures.push(m.clone())
            } else {
                let join = self
                    .compute_join_multi_fact_groups_with_measures(&vec![m.clone()])?
                    .iter()
                    .next()
                    .expect("No join groups returned for single measure multi-fact join group")
                    .0
                    .clone();
                for item in collect_multiplied_measures(
                    self.query_tools.clone(),
                    m.member_evaluator(),
                    join,
                )? {
                    if item.multiplied {
                        result
                            .rendered_as_multiplied_measures
                            .insert(item.measure.full_name());
                    }
                    if item.multiplied && !item.measure.can_used_as_addictive_in_multplied()? {
                        result.multiplied_measures.push(item.measure.clone());
                    } else {
                        result.regular_measures.push(item.measure.clone());
                    }
                }
            }
        }

        Ok(result)
    }

    fn all_used_measures(&self) -> Result<Vec<Rc<BaseMeasure>>, CubeError> {
        let mut measures = self.measures.clone();
        for item in self.measures_filters.iter() {
            self.fill_missed_measures_from_filter(item, &mut measures)?;
        }
        Ok(measures)
    }

    fn fill_missed_measures_from_filter(
        &self,
        item: &FilterItem,
        measures: &mut Vec<Rc<BaseMeasure>>,
    ) -> Result<(), CubeError> {
        match item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    self.fill_missed_measures_from_filter(item, measures)?
                }
            }
            FilterItem::Item(item) => {
                let item_member_name = item.member_name();
                if measures
                    .iter()
                    .find(|m| m.full_name() == item_member_name)
                    .is_none()
                {
                    measures.push(BaseMeasure::try_new_required(
                        item.member_evaluator().clone(),
                        self.query_tools.clone(),
                    )?);
                }
            }
            FilterItem::Segment(_) => {}
        }
        Ok(())
    }
}
