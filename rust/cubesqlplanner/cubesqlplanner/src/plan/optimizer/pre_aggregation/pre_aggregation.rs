use super::select_meta::SelectMeta;
use super::SelectMetaCollector;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
use crate::plan::optimizer::collectors::AllSymbolsCollector;
use crate::plan::Select;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::collect_cube_names_from_vec;
use crate::planner::sql_evaluator::sql_nodes::time_dimension;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::GranularityHelper;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone, Debug)]
enum DimensionMatchType {
    Strict,
    NotStrict,
    None,
}

impl DimensionMatchType {
    pub fn combine(&self, other: &DimensionMatchType) -> DimensionMatchType {
        if matches!(self, DimensionMatchType::None) || matches!(other, DimensionMatchType::None) {
            return DimensionMatchType::None;
        }
        if matches!(self, DimensionMatchType::NotStrict)
            || matches!(other, DimensionMatchType::NotStrict)
        {
            return DimensionMatchType::NotStrict;
        }
        return DimensionMatchType::Strict;
    }
}

pub struct PreAggregationOptimizer {
    query_tools: Rc<QueryTools>,
}

impl PreAggregationOptimizer {
    pub fn new(query_tools: Rc<QueryTools>) -> Self {
        Self { query_tools }
    }

    pub fn try_optimize(&self, query_plan: Rc<Select>) -> Result<Option<Rc<Select>>, CubeError> {
        let mut meta_collector = SelectMetaCollector::new();
        meta_collector.collect(query_plan.clone())?;
        let meta = meta_collector.extract_result()?;

        let mut compiled_pre_aggregations = Vec::new();
        for cube_name in meta.cube_names().iter() {
            let pre_aggregations = self
                .query_tools
                .cube_evaluator()
                .pre_aggregations_for_cube_as_array(cube_name.clone())?;
            for pre_aggregation in pre_aggregations.iter() {
                let compiled = CompiledPreAggregation::try_new(
                    self.query_tools.clone(),
                    cube_name,
                    pre_aggregation.clone(),
                )?;
                compiled_pre_aggregations.push(compiled);
            }
        }

        for pre_aggregation in compiled_pre_aggregations.iter() {
            self.can_use_pre_aggregation(&meta, pre_aggregation)?;
        }

        Ok(Some(query_plan.clone()))
    }

    fn can_use_pre_aggregation(&self, select_meta: &SelectMeta, pre_aggregation: &CompiledPreAggregation) -> Result<bool, CubeError> {
        let dimensions_match = self.is_dimensions_match(select_meta, pre_aggregation)?;
        let time_dimension_match = &self.is_time_dimensions_match(select_meta, pre_aggregation)?;
        println!("pre aggr: {}, dimensions_match: {:?}, time_dimension_match: {:?}", pre_aggregation.name, dimensions_match, time_dimension_match);
        let pre_aggr_match = dimensions_match.combine(&time_dimension_match);
        
        Ok(false)
    }

    fn is_dimensions_match(
        &self,
        select_meta: &SelectMeta,
        pre_aggregation: &CompiledPreAggregation,
    ) -> Result<DimensionMatchType, CubeError> {
        if select_meta.dimensions().iter().all(|d| {
            println!("!!! dim name: {}", d.full_name());
            pre_aggregation
                .dimensions
                .iter()
                .find(|pre_agg_dim| pre_agg_dim.full_name() == d.full_name())
                .is_some()
        }) {
            if select_meta.dimensions().len() == pre_aggregation.dimensions.len() {
                Ok(DimensionMatchType::Strict)
            } else {
                Ok(DimensionMatchType::NotStrict)
            }
        } else {
            Ok(DimensionMatchType::None)
        }
    }
    fn is_time_dimensions_match(
        &self,
        select_meta: &SelectMeta,
        pre_aggregation: &CompiledPreAggregation,
    ) -> Result<DimensionMatchType, CubeError> {
        let mut result = DimensionMatchType::Strict;
        for time_dimension in select_meta.time_dimensions() {
            if let Some((pre_aggregation_time_dimension, pre_aggr_granularity)) = pre_aggregation
                .time_dimensions
                .iter()
                .find(|(pre_agg_dim, _)| pre_agg_dim.full_name() == time_dimension.base_symbol().full_name())
            {
                if pre_aggr_granularity == time_dimension.granularity() {
                    result = result.combine(&DimensionMatchType::Strict);
                } else {
                    let min_granularity = GranularityHelper::min_granularity(
                        &time_dimension.granularity(),
                        &pre_aggr_granularity,
                    )?;
                    if &min_granularity == pre_aggr_granularity {
                        result = result.combine(&DimensionMatchType::NotStrict);
                    } else {
                        return Ok(DimensionMatchType::None);
                    }
                }
            } else {
                return Ok(DimensionMatchType::None);
            }
        }
        Ok(result)
    }
}

struct CompiledPreAggregation {
    pub name: String,
    pub granularity: Option<String>,
    pub external: Option<bool>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub time_dimensions: Vec<(Rc<MemberSymbol>, Option<String>)>,
}

impl CompiledPreAggregation {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        cube_name: &String,
        description: Rc<dyn PreAggregationDescription>,
    ) -> Result<Rc<Self>, CubeError> {
        let static_data = description.static_data();
        let measures = if let Some(refs) = description.measure_references()? {
            Self::symbols_from_ref(query_tools.clone(), cube_name, refs, Self::check_if_measure)?
        } else {
            Vec::new()
        };
        let dimensions = if let Some(refs) = description.dimension_references()? {
            Self::symbols_from_ref(
                query_tools.clone(),
                cube_name,
                refs,
                Self::check_if_dimension,
            )?
        } else {
            Vec::new()
        };
        let time_dimensions = if let Some(refs) = description.time_dimension_reference()? {
            let dims = Self::symbols_from_ref(
                query_tools.clone(),
                cube_name,
                refs,
                Self::check_if_time_dimension,
            )?;
            if dims.len() != 1 {
                return Err(CubeError::user(format!(
                    "Pre aggregation should contains only one time dimension"
                )));
            }
            vec![(dims[0].clone(), static_data.granularity.clone())] //TODO remove unwrap
        } else {
            Vec::new()
        };
        let res = Rc::new(Self {
            name: static_data.name.clone(),
            granularity: static_data.granularity.clone(),
            external: static_data.external,
            measures,
            dimensions,
            time_dimensions,
        });
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

    fn check_if_measure(symbol: &MemberSymbol) -> Result<(), CubeError> {
        if !matches!(symbol, &MemberSymbol::Measure(_)) {
            return Err(CubeError::user(format!(
                "Pre-aggregation measure must be a measure"
            )));
        }
        Ok(())
    }

    fn check_if_dimension(symbol: &MemberSymbol) -> Result<(), CubeError> {
        if !matches!(symbol, &MemberSymbol::Dimension(_)) {
            return Err(CubeError::user(format!(
                "Pre-aggregation dimension must be a dimension"
            )));
        }
        Ok(())
    }

    fn check_if_time_dimension(symbol: &MemberSymbol) -> Result<(), CubeError> {
        match symbol {
            MemberSymbol::Dimension(dimension_symbol) => {
                if dimension_symbol.dimension_type() != "time" {
                    return Err(CubeError::user(format!(
                        "Pre-aggregation time dimension must be a dimension of type `time`"
                    )));
                }
            }
            _ => {
                return Err(CubeError::user(format!(
                    "Pre-aggregation time dimension must be a dimension"
                )));
            }
        }
        Ok(())
    }
}
