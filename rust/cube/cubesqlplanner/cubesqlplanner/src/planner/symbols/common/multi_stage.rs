use super::super::measure_symbol::MeasureTimeShifts;
use super::super::MemberSymbol;
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::planner::filter::compiler::FilterCompiler;
use crate::planner::filter::FilterItem;
use crate::planner::Compiler;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MultiStageFilterMode {
    Relative,
    Fixed,
}

impl MultiStageFilterMode {
    fn from_str(s: &str) -> Result<Self, CubeError> {
        match s {
            "relative" => Ok(Self::Relative),
            "fixed" => Ok(Self::Fixed),
            other => Err(CubeError::user(format!(
                "Unknown multi-stage filter mode '{}', expected 'relative' or 'fixed'",
                other
            ))),
        }
    }
}

/// Compiled multi-stage `filter:` directive.
///
/// `mode` defaults to `Relative` when omitted in the user-facing schema —
/// normalized at construction time so the planner sees a single concrete
/// value. `include_*` entries are full `FilterItem` predicates split by
/// member type at construction time (using `FilterCompiler`). The split lets
/// the planner just append each bucket to the matching `QueryProperties`
/// filter list without re-classifying. They are AND-combined with whatever
/// survives `exclude` / `keep_only` against the chosen base state.
#[derive(Clone)]
pub struct MultiStageFilter {
    pub mode: MultiStageFilterMode,
    pub exclude: Option<Vec<Rc<MemberSymbol>>>,
    pub keep_only: Option<Vec<Rc<MemberSymbol>>>,
    pub include_dimension: Vec<FilterItem>,
    pub include_time_dimension: Vec<FilterItem>,
    pub include_measure: Vec<FilterItem>,
}

#[derive(Clone)]
pub struct MultiStageProperties {
    pub add_group_by: Option<Vec<Rc<MemberSymbol>>>,
    pub filter: Option<MultiStageFilter>,
    pub reduce_by: Option<Vec<Rc<MemberSymbol>>>,
    pub group_by: Option<Vec<Rc<MemberSymbol>>>,
    pub time_shift: Option<MeasureTimeShifts>,
}

impl MultiStageProperties {
    pub fn from_measure_definition(
        cube_name: &String,
        definition: &Rc<dyn MeasureDefinition>,
        time_shift: Option<MeasureTimeShifts>,
        compiler: &mut Compiler,
    ) -> Result<Option<Self>, CubeError> {
        if !definition.static_data().multi_stage.unwrap_or(false) {
            return Ok(None);
        }

        let static_data = definition.static_data();
        let reduce_by = resolve_reference_paths(&static_data.reduce_by_references, compiler)?;
        let add_group_by = resolve_reference_paths(&static_data.add_group_by_references, compiler)?;
        let group_by = resolve_reference_paths(&static_data.group_by_references, compiler)?;
        let filter = build_filter(cube_name, definition.filter()?, compiler)?;

        Ok(Some(Self {
            add_group_by,
            filter,
            reduce_by,
            group_by,
            time_shift,
        }))
    }

    pub fn from_dimension_definition(
        cube_name: &String,
        definition: &Rc<dyn DimensionDefinition>,
        compiler: &mut Compiler,
    ) -> Result<Option<Self>, CubeError> {
        if !definition.static_data().multi_stage.unwrap_or(false) {
            return Ok(None);
        }

        let add_group_by =
            resolve_reference_paths(&definition.static_data().add_group_by_references, compiler)?;
        let filter = build_filter(cube_name, definition.filter()?, compiler)?;

        Ok(Some(Self {
            add_group_by,
            filter,
            reduce_by: None,
            group_by: None,
            time_shift: None,
        }))
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let map_refs = |refs: &Option<Vec<Rc<MemberSymbol>>>| -> Result<_, CubeError> {
            match refs {
                Some(items) => Ok(Some(items.iter().map(f).collect::<Result<Vec<_>, _>>()?)),
                None => Ok(None),
            }
        };

        let filter = match &self.filter {
            Some(f_old) => Some(MultiStageFilter {
                mode: f_old.mode.clone(),
                exclude: map_refs(&f_old.exclude)?,
                keep_only: map_refs(&f_old.keep_only)?,
                // include_* items are FilterItems that already hold their own
                // resolved member references; transformations of dependency
                // chains apply at the symbol level, so we keep them as-is.
                include_dimension: f_old.include_dimension.clone(),
                include_time_dimension: f_old.include_time_dimension.clone(),
                include_measure: f_old.include_measure.clone(),
            }),
            None => None,
        };

        Ok(Self {
            add_group_by: map_refs(&self.add_group_by)?,
            filter,
            reduce_by: map_refs(&self.reduce_by)?,
            group_by: map_refs(&self.group_by)?,
            time_shift: self.time_shift.clone(),
        })
    }
}

fn resolve_reference_paths(
    refs: &Option<Vec<String>>,
    compiler: &mut Compiler,
) -> Result<Option<Vec<Rc<MemberSymbol>>>, CubeError> {
    match refs {
        Some(paths) => {
            let symbols = paths
                .iter()
                .map(|p| compiler.add_dimension_evaluator(p.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Some(symbols))
        }
        None => Ok(None),
    }
}

fn build_filter(
    _cube_name: &String,
    filter: Option<Rc<dyn crate::cube_bridge::multi_stage_filter::MultiStageFilterReferences>>,
    compiler: &mut Compiler,
) -> Result<Option<MultiStageFilter>, CubeError> {
    let filter = match filter {
        Some(f) => f,
        None => return Ok(None),
    };

    let static_data = filter.static_data();
    if static_data.exclude.is_some() && static_data.keep_only.is_some() {
        return Err(CubeError::user(
            "Multi-stage filter cannot specify both `exclude` and `keep_only` — they are mutually exclusive ways of restricting the inherited context.".to_string(),
        ));
    }
    let mode = match &static_data.mode {
        Some(s) => MultiStageFilterMode::from_str(s)?,
        None => MultiStageFilterMode::Relative,
    };
    let exclude = resolve_reference_paths(&static_data.exclude, compiler)?;
    let keep_only = resolve_reference_paths(&static_data.keep_only, compiler)?;

    let mut include_dimension = Vec::new();
    let mut include_time_dimension = Vec::new();
    let mut include_measure = Vec::new();
    if let Some(items) = &static_data.include {
        if !items.is_empty() {
            let query_tools = compiler.query_tools()?;
            let mut filter_compiler = FilterCompiler::new(compiler, query_tools);
            for item in items {
                filter_compiler.add_item(item)?;
            }
            let (dim, time_dim, meas) = filter_compiler.extract_result();
            include_dimension = dim;
            include_time_dimension = time_dim;
            include_measure = meas;
        }
    }

    Ok(Some(MultiStageFilter {
        mode,
        exclude,
        keep_only,
        include_dimension,
        include_time_dimension,
        include_measure,
    }))
}
