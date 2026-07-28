use super::super::measure_symbol::MeasureTimeShifts;
use super::super::MemberSymbol;
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::measure_definition::{MeasureDefinition, MeasureDefinitionStatic};
use crate::cube_bridge::multi_stage_grain::MultiStageGrainReferences;
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
    // Currently always empty: `FilterCompiler::add_item` only buckets
    // Dimension / Measure, so time-dim include filters land in
    // `include_dimension`. Field kept for structural symmetry with
    // `QueryProperties` (dim / time-dim / measure); will be populated once
    // `FilterCompiler` classifies time-dimension filters separately.
    pub include_time_dimension: Vec<FilterItem>,
    pub include_measure: Vec<FilterItem>,
}

/// Set operation on the inherited grain context of a multi-stage member.
///
/// The three lists mutate the parent grain — `exclude` removes,
/// `keep_only` intersects, `include` adds.
#[derive(Clone, Default)]
pub struct MultiStageGrain {
    pub exclude: Option<Vec<Rc<MemberSymbol>>>,
    pub keep_only: Option<Vec<Rc<MemberSymbol>>>,
    pub include: Option<Vec<Rc<MemberSymbol>>>,
}

#[derive(Clone)]
pub struct MultiStageProperties {
    pub grain: MultiStageGrain,
    pub filter: Option<MultiStageFilter>,
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

        let grain = match definition.grain()? {
            Some(g) => build_grain_from_directive(g, compiler)?,
            None => build_grain_from_legacy(&definition.static_data(), compiler)?,
        };

        let filter = build_filter(cube_name, definition.filter()?, compiler)?;

        Ok(Some(Self {
            grain,
            filter,
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

        let include =
            resolve_reference_paths(&definition.static_data().add_group_by_references, compiler)?;
        let filter = build_filter(cube_name, definition.filter()?, compiler)?;

        Ok(Some(Self {
            grain: MultiStageGrain {
                include,
                ..Default::default()
            },
            filter,
            time_shift: None,
        }))
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

fn build_grain_from_directive(
    grain: Rc<dyn MultiStageGrainReferences>,
    compiler: &mut Compiler,
) -> Result<MultiStageGrain, CubeError> {
    let static_data = grain.static_data();
    if static_data.exclude.is_some() && static_data.keep_only.is_some() {
        return Err(CubeError::user(
            "Multi-stage grain cannot specify both `exclude` and `keep_only` — they are mutually exclusive ways of restricting the inherited context.".to_string(),
        ));
    }
    Ok(MultiStageGrain {
        exclude: resolve_reference_paths(&static_data.exclude, compiler)?,
        keep_only: resolve_reference_paths(&static_data.keep_only, compiler)?,
        include: resolve_reference_paths(&static_data.include, compiler)?,
    })
}

fn build_grain_from_legacy(
    static_data: &MeasureDefinitionStatic,
    compiler: &mut Compiler,
) -> Result<MultiStageGrain, CubeError> {
    Ok(MultiStageGrain {
        exclude: resolve_reference_paths(&static_data.reduce_by_references, compiler)?,
        keep_only: resolve_reference_paths(&static_data.group_by_references, compiler)?,
        include: resolve_reference_paths(&static_data.add_group_by_references, compiler)?,
    })
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
