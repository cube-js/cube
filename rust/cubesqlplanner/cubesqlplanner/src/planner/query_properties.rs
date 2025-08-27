use super::filter::compiler::FilterCompiler;
use super::filter::BaseSegment;
use super::query_tools::QueryTools;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::member_expression::MemberExpressionExpressionDef;
use crate::planner::sql_evaluator::{
    MemberExpressionExpression, MemberExpressionSymbol, TimeDimensionSymbol,
};
use crate::planner::GranularityHelper;

use super::sql_evaluator::MemberSymbol;
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::options_member::OptionsMember;
use crate::plan::{Filter, FilterItem};
use crate::planner::sql_evaluator::collectors::{
    collect_multiplied_measures, has_multi_stage_members,
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

#[derive(Debug, Clone)]
pub struct MultipliedMeasure {
    measure: Rc<MemberSymbol>,
    cube_name: String, //May differ from cube_name of the measure for a member_expression that refers to a dimension.
}

impl MultipliedMeasure {
    pub fn new(measure: Rc<MemberSymbol>, cube_name: String) -> Rc<Self> {
        Rc::new(Self { measure, cube_name })
    }

    pub fn measure(&self) -> &Rc<MemberSymbol> {
        &self.measure
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
}

#[derive(Default, Clone, Debug)]
pub struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<MultipliedMeasure>>,
    pub regular_measures: Vec<Rc<MemberSymbol>>,
    pub multi_stage_measures: Vec<Rc<MemberSymbol>>,
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
    measures: Vec<Rc<MemberSymbol>>,
    dimensions: Vec<Rc<MemberSymbol>>,
    dimensions_filters: Vec<FilterItem>,
    time_dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    segments: Vec<FilterItem>,
    time_dimensions: Vec<Rc<MemberSymbol>>,
    order_by: Vec<OrderByItem>,
    row_limit: Option<usize>,
    offset: Option<usize>,
    query_tools: Rc<QueryTools>,
    ignore_cumulative: bool,
    ungrouped: bool,
    multi_fact_join_groups: Vec<(Rc<dyn JoinDefinition>, Vec<Rc<MemberSymbol>>)>,
    pre_aggregation_query: bool,
    total_query: bool,
    query_join_hints: Rc<Vec<JoinHintItem>>,
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
                        evaluator_compiler.add_dimension_evaluator(member_name.clone())
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
                        let expression_call = match member_expression.expression()? {
                            MemberExpressionExpressionDef::Sql(sql) => {
                                evaluator_compiler.compile_sql_call(&cube_name, sql)?
                            }
                            MemberExpressionExpressionDef::Struct(_) => {
                                return Err(CubeError::user(format!(
                                    "Expression struct not supported for dimension"
                                )));
                            }
                        };
                        let member_expression_symbol = MemberExpressionSymbol::try_new(
                            cube_name.clone(),
                            name.clone(),
                            MemberExpressionExpression::SqlCall(expression_call),
                            member_expression.static_data().definition.clone(),
                            query_tools.base_tools().clone(),
                        )?;
                        Ok(MemberSymbol::new_member_expression(
                            member_expression_symbol,
                        ))
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
                .map(|d| -> Result<Rc<MemberSymbol>, CubeError> {
                    let base_symbol =
                        evaluator_compiler.add_dimension_evaluator(d.dimension.clone())?;
                    let granularity_obj = GranularityHelper::make_granularity_obj(
                        query_tools.cube_evaluator().clone(),
                        &mut evaluator_compiler,
                        query_tools.timezone().clone(),
                        &base_symbol.cube_name(),
                        &base_symbol.name(),
                        d.granularity.clone(),
                    )?;
                    let date_range_tuple = if let Some(date_range) = &d.date_range {
                        assert_eq!(date_range.len(), 2);
                        Some((date_range[0].clone(), date_range[1].clone()))
                    } else {
                        None
                    };
                    let symbol = MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
                        base_symbol,
                        d.granularity.clone(),
                        granularity_obj,
                        date_range_tuple,
                    ));
                    Ok(symbol)
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
                        evaluator_compiler.add_measure_evaluator(member_name.clone())
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
                            } else if let Some(name) = &member_expression.static_data().name {
                                format!("{}.{}", cube_name, name)
                            } else {
                                "".to_string()
                            };
                        let expression = match member_expression.expression()? {
                            MemberExpressionExpressionDef::Sql(sql) => {
                                MemberExpressionExpression::SqlCall(
                                    evaluator_compiler.compile_sql_call(&cube_name, sql)?,
                                )
                            }
                            MemberExpressionExpressionDef::Struct(expr) => {
                                if expr.static_data().expression_type != "PatchMeasure" {
                                    return Err(CubeError::user(format!("Only `PatchMeasure` type of memeber expression is supported")));
                                }

                                if let Some(source_measure) = &expr.static_data().source_measure {

                                    let new_measure_type = expr.static_data().replace_aggregation_type.clone();
                                    let mut filters_to_add = vec![];
                                    if let Some(add_filters) = expr.add_filters()? {
                                        for filter in add_filters.iter() {
                                            let node = evaluator_compiler.compile_sql_call(&cube_name, filter.sql()?)?;
                                            filters_to_add.push(node);
                                        }
                                    }
                                    let source_measure_compiled = evaluator_compiler.add_measure_evaluator(source_measure.clone())?;
                                    let symbol = if let Ok(source_measure) = source_measure_compiled.as_measure() {

                                        let patched_measure = source_measure.new_patched(new_measure_type, filters_to_add)?;
                                        MemberSymbol::new_measure(patched_measure)
                                    } else {
                                        source_measure_compiled
                                    };
                                    MemberExpressionExpression::PatchedSymbol(symbol)

                                } else {
                                    return Err(CubeError::user(format!("Source measure is required for `PatchMeasure` type of memeber expression")));
                                }

                            }
                        };
                        let member_expression_symbol = MemberExpressionSymbol::try_new(
                            cube_name.clone(),
                            name.clone(),
                            expression,
                            member_expression.static_data().definition.clone(),
                            query_tools.base_tools().clone(),
                        )?;
                        Ok(MemberSymbol::new_member_expression(member_expression_symbol))
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
                            let expression_evaluator = match member_expression.expression()? {
                                MemberExpressionExpressionDef::Sql(sql) => {
                                    evaluator_compiler.compile_sql_call(&cube_name, sql)?
                                }
                                MemberExpressionExpressionDef::Struct(_) => {
                                    return Err(CubeError::user(format!(
                                        "Expression struct not supported for dimension"
                                    )));
                                }
                            };
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
            .filter(|dim| {
                if let Ok(td) = dim.as_time_dimension() {
                    td.has_granularity()
                } else {
                    true
                }
            })
            .collect_vec();

        let order_by = if let Some(order) = &options.static_data().order {
            order
                .iter()
                .map(|o| -> Result<_, CubeError> {
                    let evaluator = if let Some(found) =
                        dimensions.iter().find(|d| d.name() == o.id)
                    {
                        found.clone()
                    } else if let Some(found) = time_dimensions.iter().find(|d| d.name() == o.id) {
                        found.clone()
                    } else if let Some(found) = measures.iter().find(|d| d.name() == o.id) {
                        found.clone()
                    } else {
                        evaluator_compiler.add_auto_resolved_member_evaluator(o.id.clone())?
                    };
                    Ok(OrderByItem::new(evaluator, o.is_desc()))
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

        let query_join_hints = Rc::new(options.join_hints()?.unwrap_or_default());

        let multi_fact_join_groups = Self::compute_join_multi_fact_groups(
            query_join_hints.clone(),
            query_tools.clone(),
            &measures,
            &dimensions,
            &time_dimensions,
            &time_dimensions_filters,
            &dimensions_filters,
            &measures_filters,
            &segments,
        )?;

        let pre_aggregation_query = options.static_data().pre_aggregation_query.unwrap_or(false);
        let total_query = options.static_data().total_query.unwrap_or(false);

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
            pre_aggregation_query,
            total_query,
            query_join_hints,
        }))
    }

    pub fn try_new_from_precompiled(
        query_tools: Rc<QueryTools>,
        measures: Vec<Rc<MemberSymbol>>,
        dimensions: Vec<Rc<MemberSymbol>>,
        time_dimensions: Vec<Rc<MemberSymbol>>,
        time_dimensions_filters: Vec<FilterItem>,
        dimensions_filters: Vec<FilterItem>,
        measures_filters: Vec<FilterItem>,
        segments: Vec<FilterItem>,
        order_by: Vec<OrderByItem>,
        row_limit: Option<usize>,
        offset: Option<usize>,
        ignore_cumulative: bool,
        ungrouped: bool,
        pre_aggregation_query: bool,
        total_query: bool,
        query_join_hints: Rc<Vec<JoinHintItem>>,
    ) -> Result<Rc<Self>, CubeError> {
        let order_by = if order_by.is_empty() {
            Self::default_order(&dimensions, &time_dimensions, &measures)
        } else {
            order_by
        };

        let multi_fact_join_groups = Self::compute_join_multi_fact_groups(
            query_join_hints.clone(),
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
            pre_aggregation_query,
            total_query,
            query_join_hints,
        }))
    }

    pub fn compute_join_multi_fact_groups_with_measures(
        &self,
        measures: &Vec<Rc<MemberSymbol>>,
    ) -> Result<Vec<(Rc<dyn JoinDefinition>, Vec<Rc<MemberSymbol>>)>, CubeError> {
        Self::compute_join_multi_fact_groups(
            self.query_join_hints.clone(),
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

    pub fn is_total_query(&self) -> bool {
        self.total_query
    }

    pub fn compute_join_multi_fact_groups(
        query_join_hints: Rc<Vec<JoinHintItem>>,
        query_tools: Rc<QueryTools>,
        measures: &Vec<Rc<MemberSymbol>>,
        dimensions: &Vec<Rc<MemberSymbol>>,
        time_dimensions: &Vec<Rc<MemberSymbol>>,
        time_dimensions_filters: &Vec<FilterItem>,
        dimensions_filters: &Vec<FilterItem>,
        measures_filters: &Vec<FilterItem>,
        segments: &Vec<FilterItem>,
    ) -> Result<Vec<(Rc<dyn JoinDefinition>, Vec<Rc<MemberSymbol>>)>, CubeError> {
        let dimensions_join_hints = query_tools
            .cached_data_mut()
            .join_hints_for_member_symbol_vec(&dimensions)?;
        let time_dimensions_join_hints = query_tools
            .cached_data_mut()
            .join_hints_for_member_symbol_vec(&time_dimensions)?;
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

        let mut dimension_and_filter_join_hints_concat = vec![query_join_hints];

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
                    let measure_join_hints =
                        query_tools.cached_data_mut().join_hints_for_member(m)?;
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
                    measures_and_join.first().unwrap().1 .1.clone(),
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
        Ok(self.multi_fact_join_groups.first().unwrap().0.clone())
    }

    pub fn measures(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.measures
    }

    pub fn dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
        &self.dimensions
    }

    pub fn time_dimensions(&self) -> &Vec<Rc<MemberSymbol>> {
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

    pub fn query_join_hints(&self) -> &Rc<Vec<JoinHintItem>> {
        &self.query_join_hints
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

    pub fn is_pre_aggregation_query(&self) -> bool {
        self.pre_aggregation_query
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

    pub fn all_members(&self, exclude_time_dimensions: bool) -> Vec<Rc<MemberSymbol>> {
        let dimensions = self.dimensions.iter().cloned();
        let measures = self.measures.iter().cloned();
        if exclude_time_dimensions {
            dimensions.chain(measures).collect_vec()
        } else {
            let time_dimensions = self.time_dimensions.iter().map(|d| {
                if let Ok(td) = d.as_time_dimension() {
                    td.base_symbol().clone()
                } else {
                    d.clone()
                }
            });
            dimensions
                .chain(time_dimensions)
                .chain(measures)
                .collect_vec()
        }
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
            members.extend(self.time_dimensions.iter().cloned());
        }
        if include_dimensions {
            members.extend(self.dimensions.iter().cloned());
        }
        if include_measures {
            members.extend(self.measures.iter().cloned());
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

    /* pub fn group_by(&self) -> Vec<Expr> {
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
    } */

    pub fn default_order(
        dimensions: &Vec<Rc<MemberSymbol>>,
        time_dimensions: &Vec<Rc<MemberSymbol>>,
        measures: &Vec<Rc<MemberSymbol>>,
    ) -> Vec<OrderByItem> {
        let mut result = Vec::new();
        if let Some(granularity_dim) = time_dimensions.iter().find(|d| {
            if let Ok(td) = d.as_time_dimension() {
                td.has_granularity()
            } else {
                false
            }
        }) {
            result.push(OrderByItem::new(granularity_dim.clone(), false));
        } else if !measures.is_empty() && !dimensions.is_empty() {
            result.push(OrderByItem::new(measures[0].clone(), true));
        } else if !dimensions.is_empty() {
            result.push(OrderByItem::new(dimensions[0].clone(), false));
        }
        result
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

    pub fn full_key_aggregate_measures(&self) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        let measures = self.all_used_measures()?;
        for m in measures.iter() {
            if has_multi_stage_members(m, self.ignore_cumulative || self.pre_aggregation_query)? {
                result.multi_stage_measures.push(m.clone())
            } else {
                let join = self
                    .compute_join_multi_fact_groups_with_measures(&vec![m.clone()])?
                    .first()
                    .expect("No join groups returned for single measure multi-fact join group")
                    .0
                    .clone();
                for item in collect_multiplied_measures(m, join)? {
                    if item.multiplied {
                        result
                            .rendered_as_multiplied_measures
                            .insert(item.measure.full_name());
                    }
                    let is_multiplied_measure = if item.multiplied {
                        if let Ok(measure) = item.measure.as_measure() {
                            if measure.can_used_as_addictive_in_multplied() {
                                false
                            } else {
                                true
                            }
                        } else {
                            true
                        }
                    } else {
                        false
                    };
                    if is_multiplied_measure {
                        result
                            .multiplied_measures
                            .push(MultipliedMeasure::new(item.measure.clone(), item.cube_name));
                    } else {
                        result.regular_measures.push(item.measure.clone());
                    }
                }
            }
        }

        Ok(result)
    }

    fn all_used_measures(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut measures = self.measures.clone();
        for item in self.measures_filters.iter() {
            self.fill_missed_measures_from_filter(item, &mut measures)?;
        }
        Ok(measures)
    }

    fn fill_missed_measures_from_filter(
        &self,
        item: &FilterItem,
        measures: &mut Vec<Rc<MemberSymbol>>,
    ) -> Result<(), CubeError> {
        match item {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    self.fill_missed_measures_from_filter(item, measures)?
                }
            }
            FilterItem::Item(item) => {
                let item_member_name = item.member_name();
                if !measures.iter().any(|m| m.full_name() == item_member_name) {
                    measures.push(item.member_evaluator().clone());
                }
            }
            FilterItem::Segment(_) => {}
        }
        Ok(())
    }
}
