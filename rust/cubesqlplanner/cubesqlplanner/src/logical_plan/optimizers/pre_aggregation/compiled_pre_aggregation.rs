use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::fmt::Debug;
use std::rc::Rc;
#[derive(Clone)]
pub struct CompiledPreAggregation {
    pub cube_name: String,
    pub name: String,
    pub granularity: Option<String>,
    pub external: Option<bool>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub time_dimensions: Vec<(Rc<MemberSymbol>, Option<String>)>,
    pub allow_non_strict_date_range_match: bool,
}

impl Debug for CompiledPreAggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledPreAggregation")
            .field("cube_name", &self.cube_name)
            .field("name", &self.name)
            .field("granularity", &self.granularity)
            .field("external", &self.external)
            .field("measures", &self.measures)
            .field("dimensions", &self.dimensions)
            .field("time_dimensions", &self.time_dimensions)
            .field(
                "allow_non_strict_date_range_match",
                &self.allow_non_strict_date_range_match,
            )
            .finish()
    }
}

impl CompiledPreAggregation {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        cube_name: &String,
        description: Rc<dyn PreAggregationDescription>,
    ) -> Result<Rc<Self>, CubeError> {
        let static_data = description.static_data();
        let measures = if let Some(refs) = description.measure_references()? {
            Self::symbols_from_ref(query_tools.clone(), cube_name, refs, Self::check_is_measure)?
        } else {
            Vec::new()
        };
        let dimensions = if let Some(refs) = description.dimension_references()? {
            Self::symbols_from_ref(
                query_tools.clone(),
                cube_name,
                refs,
                Self::check_is_dimension,
            )?
        } else {
            Vec::new()
        };
        let time_dimensions = if let Some(refs) = description.time_dimension_reference()? {
            let dims = Self::symbols_from_ref(
                query_tools.clone(),
                cube_name,
                refs,
                Self::check_is_time_dimension,
            )?;
            /*             if dims.len() != 1 {
                return Err(CubeError::user(format!(
                    "Pre aggregation should contains only one time dimension"
                )));
            } */
            vec![(dims[0].clone(), static_data.granularity.clone())] //TODO remove unwrap
        } else {
            Vec::new()
        };
        let allow_non_strict_date_range_match = description
            .static_data()
            .allow_non_strict_date_range_match
            .unwrap_or(false);
        let res = Rc::new(Self {
            name: static_data.name.clone(),
            cube_name: cube_name.clone(),
            granularity: static_data.granularity.clone(),
            external: static_data.external,
            measures,
            dimensions,
            time_dimensions,
            allow_non_strict_date_range_match,
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
