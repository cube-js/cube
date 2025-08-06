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
            vec![(dims[0].clone(), static_data.granularity.clone())]
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
        a: &Vec<(Rc<MemberSymbol>, Option<String>)>,
        b: &Vec<(Rc<MemberSymbol>, Option<String>)>,
    ) -> Result<(), CubeError> {
        if !a
            .iter()
            .zip(b.iter())
            .all(|(a, b)| a.0.name() == b.0.name() && a.1 == b.1)
        {
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
    ) -> Result<Vec<Rc<CompiledPreAggregation>>, CubeError> {
        let mut result = Vec::new();
        for (name, _) in self.descriptions.clone().iter() {
            result.push(self.compile_pre_aggregation(&name)?);
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
}
