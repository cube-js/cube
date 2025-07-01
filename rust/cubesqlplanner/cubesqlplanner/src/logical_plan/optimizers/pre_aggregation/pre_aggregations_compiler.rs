use super::CompiledPreAggregation;
use super::PreAggregationSource;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::collect_cube_names_from_symbols;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::planners::JoinPlanner;
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

        if let Some((_, description)) = self.descriptions.clone().iter().find(|(n, _)| n == name) {
            let static_data = description.static_data();
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
            //FIXME sqlAlias!!!
            let table_name = self
                .query_tools
                .base_tools()
                .pre_aggregation_table_name(name.cube_name.clone(), name.name.clone())?;
            let rollups = if let Some(refs) = description.rollup_references()? {
                let r = self
                    .query_tools
                    .cube_evaluator()
                    .evaluate_rollup_references(name.cube_name.clone(), refs)?;
                r
            } else {
                Vec::new()
            };

            if static_data.pre_aggregation_type == "rollupJoin" {
                self.build_join_source(&measures, &dimensions, &rollups)?;
            }

            let res = Rc::new(CompiledPreAggregation {
                name: static_data.name.clone(),
                cube_name: name.cube_name.clone(),
                source: PreAggregationSource::Table(table_name),
                granularity: static_data.granularity.clone(),
                external: static_data.external,
                measures,
                dimensions,
                time_dimensions,
                allow_non_strict_date_range_match,
            });
            self.compiled_cache.insert(name.clone(), res.clone());
            Ok(res)
        } else {
            Err(CubeError::internal(format!(
                "Undefined pre-aggregation {}.{}",
                name.cube_name, name.name
            )))
        }
    }

    fn build_join_source(
        &mut self,
        measures: &Vec<Rc<MemberSymbol>>,
        dimensions: &Vec<Rc<MemberSymbol>>,
        rollups: &Vec<String>,
    ) -> Result<(), CubeError> {
        println!("!!!!! build join source");
        let all_symbols = measures
            .iter()
            .cloned()
            .chain(dimensions.iter().cloned())
            .collect_vec();
        let pre_aggr_cube_names = collect_cube_names_from_symbols(&all_symbols)?;
        println!("!!!! pre aggr cube names {:?}", pre_aggr_cube_names);

        todo!()
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
