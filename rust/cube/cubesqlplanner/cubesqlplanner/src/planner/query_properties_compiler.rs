//! Translates [`BaseQueryOptions`] into a finalized [`QueryProperties`] —
//! resolves member/segment/filter/order references against the cube
//! evaluator and folds them into the typed builder.

use std::rc::Rc;

use cubenativeutils::CubeError;
use itertools::Itertools;

use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::member_expression::{
    MemberExpressionDefinition, MemberExpressionExpressionDef,
};
use crate::cube_bridge::options_member::OptionsMember;

use super::filter::compiler::FilterCompiler;
use super::filter::{BaseSegment, FilterItem};
use super::join_hints::JoinHints;
use super::query_properties::{OrderByItem, QueryProperties};
use super::query_tools::QueryTools;
use super::{
    Compiler, GranularityHelper, MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol,
    TimeDimensionSymbol,
};

/// One-shot translator from [`BaseQueryOptions`] into a finalized
/// [`QueryProperties`].
pub struct QueryPropertiesCompiler {
    query_tools: Rc<QueryTools>,
}

impl QueryPropertiesCompiler {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools }
    }

    pub fn build(
        self,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Rc<QueryProperties>, CubeError> {
        let options = options.as_ref();
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let dimensions = self.compile_dimensions(&mut evaluator_compiler, options)?;
        let time_dimensions_raw = self.compile_time_dimensions(&mut evaluator_compiler, options)?;
        let measures = self.compile_measures(&mut evaluator_compiler, options)?;
        let segments = self.compile_segments(&mut evaluator_compiler, options)?;

        let (dimensions_filters, time_dimensions_filters, measures_filters) =
            self.compile_filters(&mut evaluator_compiler, options, &time_dimensions_raw)?;

        // FIXME may be this filter should be applied on other place
        let time_dimensions = Self::filter_time_dimensions_with_granularity(time_dimensions_raw);

        let order_by = self.compile_order_by(
            &mut evaluator_compiler,
            options,
            &dimensions,
            &time_dimensions,
            &measures,
        )?;

        let row_limit = options
            .static_data()
            .row_limit
            .as_ref()
            .and_then(|v| v.parse::<usize>().ok());
        let offset = options
            .static_data()
            .offset
            .as_ref()
            .and_then(|v| v.parse::<usize>().ok());
        let ungrouped = options.static_data().ungrouped.unwrap_or(false);
        let pre_aggregation_query = options.static_data().pre_aggregation_query.unwrap_or(false);
        let total_query = options.static_data().total_query.unwrap_or(false);
        let disable_external_pre_aggregations =
            options.static_data().disable_external_pre_aggregations;
        let pre_aggregation_id = options.static_data().pre_aggregation_id.clone();

        let query_join_hints = Rc::new(JoinHints::from_items(
            options.join_hints()?.unwrap_or_default(),
        ));

        QueryProperties::builder()
            .query_tools(self.query_tools)
            .measures(measures)
            .dimensions(dimensions)
            .time_dimensions(time_dimensions)
            .time_dimensions_filters(time_dimensions_filters)
            .dimensions_filters(dimensions_filters)
            .measures_filters(measures_filters)
            .segments(segments)
            .order_by(order_by)
            .row_limit(row_limit)
            .offset(offset)
            .ungrouped(ungrouped)
            .pre_aggregation_query(pre_aggregation_query)
            .total_query(total_query)
            .query_join_hints(query_join_hints)
            .disable_external_pre_aggregations(disable_external_pre_aggregations)
            .pre_aggregation_id(pre_aggregation_id)
            .build()
    }

    fn compile_dimensions(
        &self,
        evaluator_compiler: &mut Compiler,
        options: &dyn BaseQueryOptions,
    ) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let Some(dimensions) = options.dimensions()? else {
            return Ok(Vec::new());
        };
        dimensions
            .iter()
            .map(|d| match d {
                OptionsMember::MemberName(member_name) => {
                    evaluator_compiler.add_dimension_evaluator(member_name.clone())
                }
                OptionsMember::MemberExpression(member_expression) => {
                    Self::compile_member_expression_dimension(evaluator_compiler, member_expression)
                }
            })
            .collect()
    }

    // Struct expressions are rejected; only SQL calls are accepted here.
    fn compile_member_expression_dimension(
        evaluator_compiler: &mut Compiler,
        member_expression: &Rc<dyn MemberExpressionDefinition>,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let cube_name = member_expression
            .static_data()
            .cube_name
            .clone()
            .unwrap_or_default();
        let name = member_expression
            .static_data()
            .expression_name
            .clone()
            .unwrap_or_default();
        let expression_call = match member_expression.expression()? {
            MemberExpressionExpressionDef::Sql(sql) => {
                evaluator_compiler.compile_sql_call(&cube_name, sql)?
            }
            MemberExpressionExpressionDef::Struct(_) => {
                return Err(CubeError::user(
                    "Expression struct not supported for dimension".to_string(),
                ));
            }
        };
        let cube_symbol = evaluator_compiler.add_cube_table_evaluator(cube_name.clone(), vec![])?;
        let member_expression_symbol = MemberExpressionSymbol::try_new(
            cube_symbol,
            name,
            MemberExpressionExpression::SqlCall(expression_call),
            member_expression.static_data().definition.clone(),
            None,
            vec![cube_name],
        )?;
        Ok(MemberSymbol::new_member_expression(
            member_expression_symbol,
        ))
    }

    fn compile_time_dimensions(
        &self,
        evaluator_compiler: &mut Compiler,
        options: &dyn BaseQueryOptions,
    ) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let Some(time_dimensions) = &options.static_data().time_dimensions else {
            return Ok(Vec::new());
        };
        time_dimensions
            .iter()
            .map(|d| -> Result<Rc<MemberSymbol>, CubeError> {
                let base_symbol =
                    evaluator_compiler.add_dimension_evaluator(d.dimension.clone())?;
                let granularity_obj = GranularityHelper::make_granularity_obj(
                    self.query_tools.cube_evaluator().clone(),
                    evaluator_compiler,
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
                Ok(MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
                    base_symbol,
                    d.granularity.clone(),
                    granularity_obj,
                    date_range_tuple,
                )))
            })
            .collect()
    }

    fn compile_measures(
        &self,
        evaluator_compiler: &mut Compiler,
        options: &dyn BaseQueryOptions,
    ) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let Some(measures) = options.measures()? else {
            return Ok(Vec::new());
        };
        measures
            .iter()
            .map(|d| match d {
                OptionsMember::MemberName(member_name) => {
                    evaluator_compiler.add_measure_evaluator(member_name.clone())
                }
                OptionsMember::MemberExpression(member_expression) => {
                    Self::compile_member_expression_measure(evaluator_compiler, member_expression)
                }
            })
            .collect()
    }

    // Accepts either a SQL call or a `PatchMeasure` struct; other struct
    // expression types are rejected.
    fn compile_member_expression_measure(
        evaluator_compiler: &mut Compiler,
        member_expression: &Rc<dyn MemberExpressionDefinition>,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let static_data = member_expression.static_data();
        let cube_name = static_data.cube_name.clone().unwrap_or_default();
        let name = if let Some(name) = &static_data.expression_name {
            name.clone()
        } else if let Some(name) = &static_data.name {
            format!("{}.{}", cube_name, name)
        } else {
            String::new()
        };
        let expression = match member_expression.expression()? {
            MemberExpressionExpressionDef::Sql(sql) => MemberExpressionExpression::SqlCall(
                evaluator_compiler.compile_sql_call(&cube_name, sql)?,
            ),
            MemberExpressionExpressionDef::Struct(expr) => {
                if expr.static_data().expression_type != "PatchMeasure" {
                    return Err(CubeError::user(
                        "Only `PatchMeasure` type of member expression is supported".to_string(),
                    ));
                }

                let Some(source_measure) = &expr.static_data().source_measure else {
                    return Err(CubeError::user(
                        "Source measure is required for `PatchMeasure` type of member expression"
                            .to_string(),
                    ));
                };

                let new_measure_type = expr.static_data().replace_aggregation_type.clone();
                let mut filters_to_add = vec![];
                if let Some(add_filters) = expr.add_filters()? {
                    for filter in add_filters.iter() {
                        let node =
                            evaluator_compiler.compile_sql_call(&cube_name, filter.sql()?)?;
                        filters_to_add.push(node);
                    }
                }
                let source_measure_compiled =
                    evaluator_compiler.add_measure_evaluator(source_measure.clone())?;
                let symbol = if let Ok(source_measure) = source_measure_compiled.as_measure() {
                    let patched_measure =
                        source_measure.new_patched(new_measure_type, filters_to_add)?;
                    MemberSymbol::new_measure(patched_measure)
                } else {
                    source_measure_compiled
                };
                MemberExpressionExpression::PatchedSymbol(symbol)
            }
        };
        let cube_symbol = evaluator_compiler.add_cube_table_evaluator(cube_name.clone(), vec![])?;
        let member_expression_symbol = MemberExpressionSymbol::try_new(
            cube_symbol,
            name,
            expression,
            static_data.definition.clone(),
            None,
            vec![cube_name],
        )?;
        Ok(MemberSymbol::new_member_expression(
            member_expression_symbol,
        ))
    }

    fn compile_segments(
        &self,
        evaluator_compiler: &mut Compiler,
        options: &dyn BaseQueryOptions,
    ) -> Result<Vec<FilterItem>, CubeError> {
        let Some(segments) = options.segments()? else {
            return Ok(Vec::new());
        };
        segments
            .iter()
            .map(|d| -> Result<_, CubeError> {
                let segment = match d {
                    OptionsMember::MemberName(member_name) => {
                        self.compile_named_segment(evaluator_compiler, member_name)?
                    }
                    OptionsMember::MemberExpression(member_expression) => {
                        Self::compile_member_expression_segment(
                            evaluator_compiler,
                            member_expression,
                        )?
                    }
                };
                Ok(FilterItem::Segment(segment))
            })
            .collect()
    }

    fn compile_named_segment(
        &self,
        evaluator_compiler: &mut Compiler,
        member_name: &str,
    ) -> Result<Rc<BaseSegment>, CubeError> {
        let mut iter = self
            .query_tools
            .cube_evaluator()
            .parse_path("segments".to_string(), member_name.to_string())?
            .into_iter();
        let cube_name = iter.next().unwrap();
        let name = iter.next().unwrap();
        let definition = self
            .query_tools
            .cube_evaluator()
            .segment_by_path(member_name.to_string())?;
        let expression_evaluator =
            evaluator_compiler.compile_sql_call(&cube_name, definition.sql()?)?;
        let cube_symbol = evaluator_compiler.add_cube_table_evaluator(cube_name, vec![])?;
        BaseSegment::try_new(
            expression_evaluator,
            cube_symbol,
            name,
            Some(member_name.to_string()),
        )
    }

    fn compile_member_expression_segment(
        evaluator_compiler: &mut Compiler,
        member_expression: &Rc<dyn MemberExpressionDefinition>,
    ) -> Result<Rc<BaseSegment>, CubeError> {
        let cube_name = member_expression
            .static_data()
            .cube_name
            .clone()
            .unwrap_or_default();
        let name = member_expression
            .static_data()
            .expression_name
            .clone()
            .unwrap_or_default();
        let expression_evaluator = match member_expression.expression()? {
            MemberExpressionExpressionDef::Sql(sql) => {
                evaluator_compiler.compile_sql_call(&cube_name, sql)?
            }
            MemberExpressionExpressionDef::Struct(_) => {
                return Err(CubeError::user(
                    "Expression struct not supported for segment".to_string(),
                ));
            }
        };
        let cube_symbol = evaluator_compiler.add_cube_table_evaluator(cube_name, vec![])?;
        BaseSegment::try_new(expression_evaluator, cube_symbol, name, None)
    }

    // Returns `(dimension_filters, time_dimension_filters, measure_filters)`.
    // Includes both the explicit `options.filters` entries and the implicit
    // `dateRange` filter carried by each time dimension.
    fn compile_filters(
        &self,
        evaluator_compiler: &mut Compiler,
        options: &dyn BaseQueryOptions,
        time_dimensions: &[Rc<MemberSymbol>],
    ) -> Result<(Vec<FilterItem>, Vec<FilterItem>, Vec<FilterItem>), CubeError> {
        let mut filter_compiler = FilterCompiler::new(evaluator_compiler, self.query_tools.clone());
        if let Some(filters) = &options.static_data().filters {
            for filter in filters {
                filter_compiler.add_item(filter)?;
            }
        }
        for time_dimension in time_dimensions {
            filter_compiler.add_time_dimension_item(time_dimension)?;
        }
        Ok(filter_compiler.extract_result())
    }

    // Drop time-dimension symbols that have no granularity. Non-time-
    // dimension symbols pass through unchanged.
    fn filter_time_dimensions_with_granularity(
        time_dimensions: Vec<Rc<MemberSymbol>>,
    ) -> Vec<Rc<MemberSymbol>> {
        time_dimensions
            .into_iter()
            .filter(|dim| {
                if let Ok(td) = dim.as_time_dimension() {
                    td.has_granularity()
                } else {
                    true
                }
            })
            .collect_vec()
    }

    // Returns `None` when `options.order` is absent, `Some(translated)`
    // otherwise — including `Some(empty)` if the input was an empty array.
    fn compile_order_by(
        &self,
        evaluator_compiler: &mut Compiler,
        options: &dyn BaseQueryOptions,
        dimensions: &[Rc<MemberSymbol>],
        time_dimensions: &[Rc<MemberSymbol>],
        measures: &[Rc<MemberSymbol>],
    ) -> Result<Option<Vec<OrderByItem>>, CubeError> {
        let Some(order) = &options.static_data().order else {
            return Ok(None);
        };
        let translated = order
            .iter()
            .map(|o| -> Result<_, CubeError> {
                let evaluator = if let Some(found) = dimensions.iter().find(|d| d.name() == o.id) {
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
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(translated))
    }
}
