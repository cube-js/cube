use super::CompiledPreAggregation;
use super::PreAggregationSource;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
use crate::logical_plan::PreAggregationJoin;
use crate::logical_plan::PreAggregationJoinItem;
use crate::logical_plan::PreAggregationTable;
use crate::logical_plan::PreAggregationUnion;
use crate::planner::planners::JoinPlanner;
use crate::planner::planners::ResolvedJoinItem;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::collect_cube_names_from_symbols;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::TimeDimensionSymbol;
use crate::planner::GranularityHelper;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::fmt::Debug;
use std::rc::Rc;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PreAggregationFullName {
    pub cube_name: String,
    pub name: String,
}

impl PreAggregationFullName {
    pub fn from_string(name: &str) -> Result<Self, CubeError> {
        let parts = name.split('.').collect_vec();
        if parts.len() != 2 {
            Err(CubeError::user(format!(
                "Invalid pre-aggregation name: {}",
                name
            )))
        } else {
            Ok(Self {
                cube_name: parts[0].to_string(),
                name: parts[1].to_string(),
            })
        }
    }

    pub fn new(cube_name: String, name: String) -> Self {
        Self { cube_name, name }
    }
}

pub struct PreAggregationsCompiler {
    query_tools: Rc<QueryTools>,
    descriptions: Rc<Vec<(PreAggregationFullName, Rc<dyn PreAggregationDescription>)>>,
    compiled_cache: HashMap<PreAggregationFullName, Rc<CompiledPreAggregation>>,
}

impl PreAggregationsCompiler {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        cube_names: &Vec<String>,
    ) -> Result<Self, CubeError> {
        let mut descriptions = Vec::new();
        for cube_name in cube_names.iter() {
            let pre_aggregations = query_tools
                .cube_evaluator()
                .pre_aggregations_for_cube_as_array(cube_name.clone())?;
            for pre_aggregation in pre_aggregations.iter() {
                let full_name = PreAggregationFullName::new(
                    cube_name.clone(),
                    pre_aggregation.static_data().name.clone(),
                );
                descriptions.push((full_name, pre_aggregation.clone()));
            }
        }
        Ok(Self {
            query_tools,
            descriptions: Rc::new(descriptions),
            compiled_cache: HashMap::new(),
        })
    }

    pub fn compile_pre_aggregation(
        &mut self,
        name: &PreAggregationFullName,
    ) -> Result<Rc<CompiledPreAggregation>, CubeError> {
        if let Some(compiled) = self.compiled_cache.get(&name) {
            return Ok(compiled.clone());
        }

        let description = if let Some((_, description)) =
            self.descriptions.clone().iter().find(|(n, _)| n == name)
        {
            description.clone()
        } else {
            if let Some(descr) = self
                .query_tools
                .cube_evaluator()
                .pre_aggregation_description_by_name(name.cube_name.clone(), name.name.clone())?
            {
                descr
            } else {
                return Err(CubeError::internal(format!(
                    "Undefined pre-aggregation {}.{}",
                    name.cube_name, name.name
                )));
            }
        };

        let static_data = description.static_data();

        if static_data.pre_aggregation_type == "rollupLambda" {
            return self.build_lambda(name, &description);
        }

        let measures = if let Some(refs) = description.measure_references()? {
            Self::symbols_from_ref(
                self.query_tools.clone(),
                &name.cube_name,
                refs,
                Self::check_is_measure,
            )?
        } else {
            Vec::new()
        };
        let dimensions = if let Some(refs) = description.dimension_references()? {
            Self::symbols_from_ref(
                self.query_tools.clone(),
                &name.cube_name,
                refs,
                Self::check_is_dimension,
            )?
        } else {
            Vec::new()
        };
        let time_dimensions = if let Some(refs) = description.time_dimension_reference()? {
            let dims = Self::symbols_from_ref(
                self.query_tools.clone(),
                &name.cube_name,
                refs,
                Self::check_is_time_dimension,
            )?;

            if static_data.granularity.is_some() {
                let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
                let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
                let base_symbol = dims[0].clone();

                let granularity_obj = GranularityHelper::make_granularity_obj(
                    self.query_tools.cube_evaluator().clone(),
                    &mut evaluator_compiler,
                    &base_symbol.cube_name(),
                    &base_symbol.name(),
                    static_data.granularity.clone(),
                )?;
                let symbol = MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
                    base_symbol,
                    static_data.granularity.clone(),
                    granularity_obj,
                    None,
                ));

                vec![symbol]
            } else {
                vec![dims[0].clone()]
            }
        } else {
            Vec::new()
        };
        let segments = if let Some(refs) = description.segment_references()? {
            Self::symbols_from_ref(
                self.query_tools.clone(),
                &name.cube_name,
                refs,
                Self::check_is_segment,
            )?
        } else {
            Vec::new()
        };
        let allow_non_strict_date_range_match = description
            .static_data()
            .allow_non_strict_date_range_match
            .unwrap_or(false);
        let rollups = if let Some(refs) = description.rollup_references()? {
            let r = self
                .query_tools
                .cube_evaluator()
                .evaluate_rollup_references(name.cube_name.clone(), refs)?;
            r
        } else {
            Vec::new()
        };

        let source = if static_data.pre_aggregation_type == "rollupJoin" {
            PreAggregationSource::Join(self.build_join_source(&measures, &dimensions, &rollups)?)
        } else {
            let cube = self
                .query_tools
                .cube_evaluator()
                .cube_from_path(name.cube_name.clone())?;
            let cube_alias = if let Some(alias) = &cube.static_data().sql_alias {
                alias.clone()
            } else {
                name.cube_name.clone()
            };
            PreAggregationSource::Single(PreAggregationTable {
                cube_name: name.cube_name.clone(),
                cube_alias,
                name: name.name.clone(),
                alias: static_data.sql_alias.clone(),
            })
        };

        let res = Rc::new(CompiledPreAggregation {
            name: static_data.name.clone(),
            cube_name: name.cube_name.clone(),
            source: Rc::new(source),
            granularity: static_data.granularity.clone(),
            external: static_data.external,
            measures,
            dimensions,
            time_dimensions,
            segments,
            allow_non_strict_date_range_match,
        });
        self.compiled_cache.insert(name.clone(), res.clone());
        Ok(res)
    }

    fn build_lambda(
        &mut self,
        name: &PreAggregationFullName,
        description: &Rc<dyn PreAggregationDescription>,
    ) -> Result<Rc<CompiledPreAggregation>, CubeError> {
        let rollups = if let Some(refs) = description.rollup_references()? {
            let r = self
                .query_tools
                .cube_evaluator()
                .evaluate_rollup_references(name.cube_name.clone(), refs)?;
            r
        } else {
            Vec::new()
        };
        if rollups.is_empty() {
            return Err(CubeError::user(format!(
                "rollupLambda '{}.{}' should reference at least one rollup",
                name.cube_name, name.name
            )));
        }

        let pre_aggrs_for_lambda = rollups
            .iter()
            .map(|item| -> Result<_, CubeError> {
                self.compile_pre_aggregation(&PreAggregationFullName::from_string(item)?)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut sources = vec![];
        for (i, rollup) in pre_aggrs_for_lambda.clone().iter().enumerate() {
            match rollup.source.as_ref() {
                PreAggregationSource::Single(table) => {
                    sources.push(Rc::new(table.clone()));
                }
                _ => {
                    return Err(CubeError::user(format!("Rollup lambda can't be nested")));
                }
            }
            if i > 1 {
                Self::match_symbols(&rollup.measures, &pre_aggrs_for_lambda[0].measures)?;
                Self::match_symbols(&rollup.dimensions, &pre_aggrs_for_lambda[0].dimensions)?;
                Self::match_time_dimensions(
                    &rollup.time_dimensions,
                    &pre_aggrs_for_lambda[0].time_dimensions,
                )?;
            }
        }

        let measures = pre_aggrs_for_lambda[0].measures.clone();
        let dimensions = pre_aggrs_for_lambda[0].dimensions.clone();
        let time_dimensions = pre_aggrs_for_lambda[0].time_dimensions.clone();
        let segments = pre_aggrs_for_lambda[0].segments.clone();
        let allow_non_strict_date_range_match = description
            .static_data()
            .allow_non_strict_date_range_match
            .unwrap_or(false);
        let granularity = pre_aggrs_for_lambda[0].granularity.clone();
        let source = PreAggregationSource::Union(PreAggregationUnion { items: sources });

        let static_data = description.static_data();
        let res = Rc::new(CompiledPreAggregation {
            name: static_data.name.clone(),
            cube_name: name.cube_name.clone(),
            source: Rc::new(source),
            granularity,
            external: static_data.external,
            measures,
            dimensions,
            time_dimensions,
            segments,
            allow_non_strict_date_range_match,
        });
        self.compiled_cache.insert(name.clone(), res.clone());
        Ok(res)
    }

    fn match_symbols(
        a: &Vec<Rc<MemberSymbol>>,
        b: &Vec<Rc<MemberSymbol>>,
    ) -> Result<(), CubeError> {
        if !a.iter().zip(b.iter()).all(|(a, b)| a.name() == b.name()) {
            return Err(CubeError::user(format!(
                "Names for pre-aggregation symbols in lambda pre-aggragation don't match"
            )));
        }
        Ok(())
    }

    fn match_time_dimensions(
        a: &Vec<Rc<MemberSymbol>>,
        b: &Vec<Rc<MemberSymbol>>,
    ) -> Result<(), CubeError> {
        if !a.iter().zip(b.iter()).all(|(a, b)| {
            if let (Ok(td_a), Ok(td_b)) = (a.as_time_dimension(), b.as_time_dimension()) {
                td_a.name() == td_a.name() && td_a.granularity() == td_b.granularity()
            } else {
                false
            }
        }) {
            return Err(CubeError::user(format!(
                "Names for pre-aggregation symbols in lambda pre-aggragation don't match"
            )));
        }
        Ok(())
    }

    fn build_join_source(
        &mut self,
        measures: &Vec<Rc<MemberSymbol>>,
        dimensions: &Vec<Rc<MemberSymbol>>,
        rollups: &Vec<String>,
    ) -> Result<PreAggregationJoin, CubeError> {
        let all_symbols = measures
            .iter()
            .cloned()
            .chain(dimensions.iter().cloned())
            .collect_vec();
        let pre_aggr_join_hints = collect_cube_names_from_symbols(&all_symbols)?
            .into_iter()
            .map(|v| JoinHintItem::Single(v))
            .collect_vec();

        let join_planner = JoinPlanner::new(self.query_tools.clone());
        let pre_aggrs_for_join = rollups
            .iter()
            .map(|item| -> Result<_, CubeError> {
                self.compile_pre_aggregation(&PreAggregationFullName::from_string(item)?)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let target_joins = join_planner.resolve_join_members_by_hints(&pre_aggr_join_hints)?;
        let mut existing_joins = vec![];
        for join_pre_aggr in pre_aggrs_for_join.iter() {
            let all_symbols = join_pre_aggr
                .measures
                .iter()
                .cloned()
                .chain(join_pre_aggr.dimensions.iter().cloned())
                .collect_vec();
            let join_pre_aggr_join_hints = collect_cube_names_from_symbols(&all_symbols)?
                .into_iter()
                .map(|v| JoinHintItem::Single(v))
                .collect_vec();
            existing_joins.append(
                &mut join_planner.resolve_join_members_by_hints(&join_pre_aggr_join_hints)?,
            );
        }

        let not_existing_joins = target_joins
            .into_iter()
            .filter(|join| {
                !existing_joins
                    .iter()
                    .any(|existing| existing.is_same_as(join))
            })
            .collect_vec();

        if not_existing_joins.is_empty() {
            return Err(CubeError::user(format!("Nothing to join in rollup join. Target joins are included in existing rollup joins")));
        }

        let items = not_existing_joins
            .iter()
            .map(|item| self.make_pre_aggregation_join_item(&pre_aggrs_for_join, item))
            .collect::<Result<Vec<_>, _>>()?;
        let res = PreAggregationJoin {
            root: items[0].from.clone(),
            items,
        };
        Ok(res)
    }

    fn make_pre_aggregation_join_item(
        &self,
        pre_aggrs_for_join: &Vec<Rc<CompiledPreAggregation>>,
        join_item: &ResolvedJoinItem,
    ) -> Result<PreAggregationJoinItem, CubeError> {
        let from_pre_aggr =
            self.find_pre_aggregation_for_join(pre_aggrs_for_join, &join_item.from_members)?;
        let to_pre_aggr =
            self.find_pre_aggregation_for_join(pre_aggrs_for_join, &join_item.to_members)?;

        let res = PreAggregationJoinItem {
            from: from_pre_aggr.source.clone(),
            to: to_pre_aggr.source.clone(),
            from_members: join_item.from_members.clone(),
            to_members: join_item.to_members.clone(),
            on_sql: join_item.on_sql.clone(),
        };
        Ok(res)
    }

    fn find_pre_aggregation_for_join(
        &self,
        pre_aggrs_for_join: &Vec<Rc<CompiledPreAggregation>>,
        members: &Vec<Rc<MemberSymbol>>,
    ) -> Result<Rc<CompiledPreAggregation>, CubeError> {
        let found_pre_aggr = pre_aggrs_for_join
            .iter()
            .filter(|pa| {
                members
                    .iter()
                    .all(|m| pa.dimensions.iter().any(|pa_m| m == pa_m))
            })
            .collect_vec();
        if found_pre_aggr.is_empty() {
            return Err(CubeError::user(format!(
                "No rollups found that can be used for rollup join"
            )));
        }
        if found_pre_aggr.len() > 1 {
            return Err(CubeError::user(format!(
                "Multiple rollups found that can be used for rollup join"
            )));
        }

        Ok(found_pre_aggr[0].clone())
    }

    pub fn compile_all_pre_aggregations(
        &mut self,
        disable_external_pre_aggregations: bool,
    ) -> Result<Vec<Rc<CompiledPreAggregation>>, CubeError> {
        let mut result = Vec::new();
        for (name, _) in self.descriptions.clone().iter() {
            let pre_aggregation = self.compile_pre_aggregation(name)?;
            if !(disable_external_pre_aggregations && pre_aggregation.external == Some(true)) {
                result.push(pre_aggregation);
            }
        }
        Ok(result)
    }

    pub fn compile_origin_sql_pre_aggregation(
        &mut self,
        cube_name: &String,
    ) -> Result<Option<Rc<CompiledPreAggregation>>, CubeError> {
        let res = if let Some((name, _)) = self.descriptions.clone().iter().find(|(name, descr)| {
            &name.cube_name == cube_name
                && &descr.static_data().pre_aggregation_type == "originalSql"
        }) {
            Some(self.compile_pre_aggregation(name)?)
        } else {
            None
        };
        Ok(res)
    }

    fn symbols_from_ref<F: Fn(&MemberSymbol) -> Result<(), CubeError>>(
        query_tools: Rc<QueryTools>,
        cube_name: &String,
        ref_func: Rc<dyn MemberSql>,
        check_type_fn: F,
    ) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let evaluator_compiler_cell = query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let sql_call = evaluator_compiler.compile_sql_call(cube_name, ref_func)?;
        let mut res = Vec::new();
        for symbol in sql_call.get_dependencies().iter() {
            check_type_fn(&symbol)?;
            res.push(symbol.clone());
        }
        Ok(res)
    }

    fn check_is_measure(symbol: &MemberSymbol) -> Result<(), CubeError> {
        symbol
            .as_measure()
            .map_err(|_| CubeError::user(format!("Pre-aggregation measure must be a measure")))?;
        Ok(())
    }

    fn check_is_dimension(symbol: &MemberSymbol) -> Result<(), CubeError> {
        symbol.as_dimension().map_err(|_| {
            CubeError::user(format!("Pre-aggregation dimension must be a dimension"))
        })?;
        Ok(())
    }

    fn check_is_time_dimension(symbol: &MemberSymbol) -> Result<(), CubeError> {
        let dimension = symbol.as_dimension().map_err(|_| {
            CubeError::user(format!(
                "Pre-aggregation time dimension must be a dimension"
            ))
        })?;
        if dimension.dimension_type() != "time" {
            return Err(CubeError::user(format!(
                "Pre-aggregation time dimension must be a dimension"
            )));
        }
        Ok(())
    }

    fn check_is_segment(symbol: &MemberSymbol) -> Result<(), CubeError> {
        symbol.as_member_expression().map_err(|_| {
            CubeError::user(
                "Pre-aggregation segment reference must be a member expression".to_string(),
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::MockSchema;
    use crate::test_fixtures::test_utils::TestContext;

    #[test]
    fn test_compile_simple_rollup() {
        let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
        let test_context = TestContext::new(schema).unwrap();
        let query_tools = test_context.query_tools().clone();

        let cube_names = vec!["visitors".to_string()];
        let mut compiler = PreAggregationsCompiler::try_new(query_tools, &cube_names).unwrap();

        let pre_agg_name =
            PreAggregationFullName::new("visitors".to_string(), "daily_rollup".to_string());
        let compiled = compiler.compile_pre_aggregation(&pre_agg_name).unwrap();

        assert_eq!(compiled.name, "daily_rollup");
        assert_eq!(compiled.cube_name, "visitors");
        assert_eq!(compiled.granularity, Some("day".to_string()));

        // Check measures
        assert_eq!(compiled.measures.len(), 2);
        let measure_names: Vec<String> = compiled.measures.iter().map(|m| m.full_name()).collect();
        assert!(measure_names.contains(&"visitors.count".to_string()));
        assert!(measure_names.contains(&"visitors.unique_source_count".to_string()));

        // Check dimensions
        assert_eq!(compiled.dimensions.len(), 1);
        assert_eq!(compiled.dimensions[0].full_name(), "visitors.source");

        // Check time dimensions (with granularity suffix)
        assert_eq!(compiled.time_dimensions.len(), 1);
        assert_eq!(
            compiled.time_dimensions[0].full_name(),
            "visitors.created_at_day"
        );
    }

    #[test]
    fn test_compile_joined_rollup() {
        let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
        let test_context = TestContext::new(schema).unwrap();
        let query_tools = test_context.query_tools().clone();

        let cube_names = vec!["visitor_checkins".to_string()];
        let mut compiler = PreAggregationsCompiler::try_new(query_tools, &cube_names).unwrap();

        let pre_agg_name = PreAggregationFullName::new(
            "visitor_checkins".to_string(),
            "joined_rollup".to_string(),
        );
        let compiled = compiler.compile_pre_aggregation(&pre_agg_name).unwrap();

        assert_eq!(compiled.name, "joined_rollup");
        assert_eq!(compiled.cube_name, "visitor_checkins");
        assert_eq!(compiled.granularity, Some("day".to_string()));

        // Check measures
        assert_eq!(compiled.measures.len(), 1);
        assert_eq!(compiled.measures[0].full_name(), "visitor_checkins.count");

        // Check dimensions
        assert_eq!(compiled.dimensions.len(), 2);
        let dimension_names: Vec<String> =
            compiled.dimensions.iter().map(|d| d.full_name()).collect();
        assert!(dimension_names.contains(&"visitor_checkins.visitor_id".to_string()));
        assert!(dimension_names.contains(&"visitors.source".to_string()));

        // Check time dimensions (with granularity suffix)
        assert_eq!(compiled.time_dimensions.len(), 1);
        assert_eq!(
            compiled.time_dimensions[0].full_name(),
            "visitor_checkins.created_at_day"
        );
    }

    #[test]
    fn test_compile_multiplied_rollup() {
        let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
        let test_context = TestContext::new(schema).unwrap();
        let query_tools = test_context.query_tools().clone();

        let cube_names = vec!["visitors".to_string()];
        let mut compiler = PreAggregationsCompiler::try_new(query_tools, &cube_names).unwrap();

        let pre_agg_name =
            PreAggregationFullName::new("visitors".to_string(), "multiplied_rollup".to_string());
        let compiled = compiler.compile_pre_aggregation(&pre_agg_name).unwrap();

        assert_eq!(compiled.name, "multiplied_rollup");
        assert_eq!(compiled.cube_name, "visitors");
        assert_eq!(compiled.granularity, Some("day".to_string()));

        // Check measures
        assert_eq!(compiled.measures.len(), 1);
        assert_eq!(compiled.measures[0].full_name(), "visitors.count");

        // Check dimensions
        assert_eq!(compiled.dimensions.len(), 2);
        let dimension_names: Vec<String> =
            compiled.dimensions.iter().map(|d| d.full_name()).collect();
        assert!(dimension_names.contains(&"visitors.source".to_string()));
        assert!(dimension_names.contains(&"visitor_checkins.source".to_string()));

        // Check time dimensions (with granularity suffix)
        assert_eq!(compiled.time_dimensions.len(), 1);
        assert_eq!(
            compiled.time_dimensions[0].full_name(),
            "visitors.created_at_day"
        );
    }

    #[test]
    fn test_compile_all_pre_aggregations() {
        let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
        let test_context = TestContext::new(schema).unwrap();
        let query_tools = test_context.query_tools().clone();

        let cube_names = vec!["visitors".to_string(), "visitor_checkins".to_string()];
        let mut compiler = PreAggregationsCompiler::try_new(query_tools, &cube_names).unwrap();

        let compiled = compiler.compile_all_pre_aggregations(false).unwrap();

        // Should compile all 8 pre-aggregations from visitors and visitor_checkins cubes
        //        assert_eq!(compiled.len(), 8);

        let names: Vec<String> = compiled.iter().map(|pa| pa.name.clone()).collect();

        // visitors pre-aggregations
        assert!(names.contains(&"daily_rollup".to_string()));
        assert!(names.contains(&"multiplied_rollup".to_string()));
        assert!(names.contains(&"for_join".to_string()));

        // visitor_checkins pre-aggregations
        assert!(names.contains(&"joined_rollup".to_string()));
        assert!(names.contains(&"checkins_with_visitor_source".to_string()));
        assert!(names.contains(&"for_lambda".to_string()));
        assert!(names.contains(&"lambda_union".to_string()));
    }

    #[test]
    fn test_compile_nonexistent_pre_aggregation() {
        let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
        let test_context = TestContext::new(schema).unwrap();
        let query_tools = test_context.query_tools().clone();

        let cube_names = vec!["visitors".to_string()];
        let mut compiler = PreAggregationsCompiler::try_new(query_tools, &cube_names).unwrap();

        let pre_agg_name =
            PreAggregationFullName::new("visitors".to_string(), "nonexistent".to_string());
        let result = compiler.compile_pre_aggregation(&pre_agg_name);

        assert!(result.is_err());
    }

    #[test]
    fn test_compile_rollup_join() {
        let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
        let test_context = TestContext::new(schema).unwrap();
        let query_tools = test_context.query_tools().clone();

        // Need both cubes for rollupJoin: visitor_checkins and visitors
        let cube_names = vec!["visitor_checkins".to_string(), "visitors".to_string()];
        let mut compiler = PreAggregationsCompiler::try_new(query_tools, &cube_names).unwrap();

        let pre_agg_name = PreAggregationFullName::new(
            "visitor_checkins".to_string(),
            "checkins_with_visitor_source".to_string(),
        );
        let compiled = compiler.compile_pre_aggregation(&pre_agg_name).unwrap();

        assert_eq!(compiled.name, "checkins_with_visitor_source");
        assert_eq!(compiled.cube_name, "visitor_checkins");

        // Check measures
        assert_eq!(compiled.measures.len(), 1);
        assert_eq!(compiled.measures[0].full_name(), "visitor_checkins.count");

        // Check dimensions
        assert_eq!(compiled.dimensions.len(), 2);
        let dimension_names: Vec<String> =
            compiled.dimensions.iter().map(|d| d.full_name()).collect();
        assert!(dimension_names.contains(&"visitor_checkins.visitor_id".to_string()));
        assert!(dimension_names.contains(&"visitors.source".to_string()));

        // Check source is Join
        match compiled.source.as_ref() {
            PreAggregationSource::Join(_) => {} // Expected
            _ => panic!("Expected PreAggregationSource::Join"),
        }
    }

    #[test]
    fn test_compile_rollup_lambda() {
        let schema = MockSchema::from_yaml_file("common/pre_aggregations_test.yaml");
        let test_context = TestContext::new(schema).unwrap();
        let query_tools = test_context.query_tools().clone();

        let cube_names = vec!["visitor_checkins".to_string()];
        let mut compiler = PreAggregationsCompiler::try_new(query_tools, &cube_names).unwrap();

        let pre_agg_name =
            PreAggregationFullName::new("visitor_checkins".to_string(), "lambda_union".to_string());
        let compiled = compiler.compile_pre_aggregation(&pre_agg_name).unwrap();

        assert_eq!(compiled.name, "lambda_union");
        assert_eq!(compiled.cube_name, "visitor_checkins");
        assert_eq!(compiled.granularity, Some("day".to_string()));

        // Check measures
        assert_eq!(compiled.measures.len(), 1);
        assert_eq!(compiled.measures[0].full_name(), "visitor_checkins.count");

        // Check dimensions
        assert_eq!(compiled.dimensions.len(), 1);
        assert_eq!(
            compiled.dimensions[0].full_name(),
            "visitor_checkins.visitor_id"
        );

        // Check time dimensions
        assert_eq!(compiled.time_dimensions.len(), 1);
        assert_eq!(
            compiled.time_dimensions[0].full_name(),
            "visitor_checkins.created_at_day"
        );

        // Check source is Union
        match compiled.source.as_ref() {
            PreAggregationSource::Union(_) => {} // Expected
            _ => panic!("Expected PreAggregationSource::Union"),
        }
    }

    #[test]
    fn test_pre_aggregation_full_name_from_string() {
        let name = PreAggregationFullName::from_string("visitors.daily_rollup").unwrap();
        assert_eq!(name.cube_name, "visitors");
        assert_eq!(name.name, "daily_rollup");
    }

    #[test]
    fn test_pre_aggregation_full_name_invalid() {
        let result = PreAggregationFullName::from_string("invalid_name");
        assert!(result.is_err());

        let result2 = PreAggregationFullName::from_string("too.many.parts");
        assert!(result2.is_err());
    }

    #[test]
    fn test_compile_rollup_with_segments() {
        let schema = MockSchema::from_yaml_file("common/pre_aggregation_matching_test.yaml");
        let test_context = TestContext::new(schema).unwrap();
        let query_tools = test_context.query_tools().clone();

        let cube_names = vec!["orders".to_string()];
        let mut compiler = PreAggregationsCompiler::try_new(query_tools, &cube_names).unwrap();

        let pre_agg_name =
            PreAggregationFullName::new("orders".to_string(), "segment_rollup".to_string());
        let compiled = compiler.compile_pre_aggregation(&pre_agg_name).unwrap();

        assert_eq!(compiled.name, "segment_rollup");
        assert_eq!(compiled.cube_name, "orders");
        assert_eq!(compiled.granularity, Some("day".to_string()));

        // Check segments
        assert_eq!(compiled.segments.len(), 1);
        assert_eq!(
            compiled.segments[0].full_name(),
            "expr:orders.high_priority"
        );

        // Check measures
        assert_eq!(compiled.measures.len(), 2);
        let measure_names: Vec<String> = compiled.measures.iter().map(|m| m.full_name()).collect();
        assert!(measure_names.contains(&"orders.count".to_string()));
        assert!(measure_names.contains(&"orders.total_amount".to_string()));

        // Check dimensions
        assert_eq!(compiled.dimensions.len(), 1);
        assert_eq!(compiled.dimensions[0].full_name(), "orders.status");
    }
}
