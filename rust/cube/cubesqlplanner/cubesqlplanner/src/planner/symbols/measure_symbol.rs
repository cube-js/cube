use super::common::{Case, CompiledMemberPath, MultiStageProperties};
use super::measure_kinds::{CalculatedMeasure, CalculatedMeasureType, MeasureKind};
use super::SymbolPath;
use super::{MemberSymbol, SymbolFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::{MeasureDefinition, RollingWindow};
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::collectors::find_owned_by_cube_child;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::SqlInterval;
use crate::planner::{Compiler, CubeRef, SqlCall};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cmp::{Eq, PartialEq};
use std::collections::HashMap;
use std::rc::Rc;

/// Per-measure `order_by` entry from the data-model definition: a
#[derive(Clone)]
pub struct MeasureOrderBy {
    sql_call: Rc<SqlCall>,
    direction: String,
}

impl MeasureOrderBy {
    pub fn new(sql_call: Rc<SqlCall>, direction: String) -> Self {
        Self {
            sql_call,
            direction,
        }
    }

    pub fn sql_call(&self) -> &Rc<SqlCall> {
        &self.sql_call
    }

    pub fn set_sql_call(&mut self, sql_call: Rc<SqlCall>) {
        self.sql_call = sql_call;
    }

    pub fn direction(&self) -> &String {
        &self.direction
    }
}

/// Time-shift entry attached to a specific time dimension. Shifts
/// that dimension's date range by either a fixed interval or a named
/// slot.
#[derive(Clone, Debug)]
pub struct DimensionTimeShift {
    pub interval: Option<SqlInterval>,
    pub name: Option<String>,
    pub dimension: Rc<MemberSymbol>,
}

impl PartialEq for DimensionTimeShift {
    fn eq(&self, other: &Self) -> bool {
        self.interval == other.interval
            && self.dimension.full_name() == other.dimension.full_name()
            && self.name == other.name
    }
}

impl Eq for DimensionTimeShift {}

/// Form of a measure's `time_shift` declaration.
///
/// - `Dimensions` — one or more shifts, each bound to a specific time
///   dimension.
/// - `Common` — a single interval applied to every time dimension in
///   the query.
/// - `Named` — a single named slot applied to every time dimension.
#[derive(Clone, Debug)]
pub enum MeasureTimeShifts {
    Dimensions(Vec<DimensionTimeShift>),
    Common(SqlInterval),
    Named(String),
}

/// `MemberSymbol::Measure` body: Tesseract representation of a
/// `measure` declared in the data model — an aggregation, count or
/// calculated value the query exposes.
#[derive(Clone)]
pub struct MeasureSymbol {
    compiled_path: CompiledMemberPath,
    kind: MeasureKind,
    rolling_window: Option<RollingWindow>,
    multi_stage: Option<MultiStageProperties>,
    is_reference: bool,
    is_view: bool,
    case: Option<Case>,
    measure_filters: Vec<Rc<SqlCall>>,
    measure_drill_filters: Vec<Rc<SqlCall>>,
    measure_order_by: Vec<MeasureOrderBy>,
    is_splitted_source: bool,
    mask_sql: Option<Rc<SqlCall>>,
}

impl MeasureSymbol {
    pub fn new(
        compiled_path: CompiledMemberPath,
        is_reference: bool,
        is_view: bool,
        case: Option<Case>,
        kind: MeasureKind,
        rolling_window: Option<RollingWindow>,
        multi_stage: Option<MultiStageProperties>,
        measure_filters: Vec<Rc<SqlCall>>,
        measure_drill_filters: Vec<Rc<SqlCall>>,
        measure_order_by: Vec<MeasureOrderBy>,
        mask_sql: Option<Rc<SqlCall>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            compiled_path,
            is_reference,
            is_view,
            case,
            kind,
            rolling_window,
            measure_filters,
            measure_drill_filters,
            measure_order_by,
            multi_stage,
            is_splitted_source: false,
            mask_sql,
        })
    }

    /// Returns a non-rolling copy of the symbol. A rolling-window
    /// measure carries both the windowing context and the SQL of the
    /// inner value it operates on; unrolling drops the window and
    /// yields that inner value. Multi-stage rolling measures collapse
    /// to a `Calculated` kind so they can be rendered without window-
    /// function machinery.
    pub fn new_unrolling(&self) -> Rc<Self> {
        if self.is_rolling_window() {
            let kind = if self.is_multi_stage() {
                if let Some(sql) = self.kind.member_sql() {
                    MeasureKind::Calculated(CalculatedMeasure::new(
                        CalculatedMeasureType::Number,
                        sql.clone(),
                    ))
                } else {
                    MeasureKind::Calculated(CalculatedMeasure::new_without_sql(
                        CalculatedMeasureType::Number,
                    ))
                }
            } else {
                self.kind.clone()
            };
            Rc::new(Self {
                compiled_path: self.compiled_path.clone(),
                kind,
                rolling_window: None,
                multi_stage: None,
                is_reference: false,
                is_view: self.is_view,
                case: self.case.clone(),
                measure_filters: self.measure_filters.clone(),
                measure_drill_filters: self.measure_drill_filters.clone(),
                measure_order_by: self.measure_order_by.clone(),
                is_splitted_source: self.is_splitted_source,
                mask_sql: self.mask_sql.clone(),
            })
        } else {
            Rc::new(self.clone())
        }
    }

    /// Returns a copy of the symbol with the measure type optionally
    /// replaced (subject to per-kind compatibility checks) and
    /// additional measure filters merged in.
    pub fn new_patched(
        &self,
        new_measure_type: Option<String>,
        add_filters: Vec<Rc<SqlCall>>,
    ) -> Result<Rc<Self>, CubeError> {
        let result_kind = if let Some(new_measure_type) = new_measure_type {
            if !self.kind.can_replace_type_with(&new_measure_type) {
                return Err(CubeError::user(format!(
                    "Unsupported measure type replacement for {}: {} => {}",
                    self.compiled_path.name(),
                    self.kind.measure_type_str(),
                    new_measure_type
                )));
            }
            self.kind.with_new_type(&new_measure_type)?
        } else {
            self.kind.clone()
        };

        let mut measure_filters = self.measure_filters.clone();
        if !add_filters.is_empty() {
            if !result_kind.supports_additional_filters() {
                return Err(CubeError::user(format!(
                    "Unsupported additional filters for measure {} type {}",
                    self.compiled_path.name(),
                    result_kind.measure_type_str()
                )));
            }
            measure_filters.extend(add_filters);
        }
        Ok(Rc::new(Self {
            compiled_path: self.compiled_path.clone(),
            kind: result_kind,
            rolling_window: self.rolling_window.clone(),
            multi_stage: self.multi_stage.clone(),
            is_reference: self.is_reference,
            is_view: self.is_view,
            case: self.case.clone(),
            measure_filters,
            measure_drill_filters: self.measure_drill_filters.clone(),
            measure_order_by: self.measure_order_by.clone(),
            is_splitted_source: self.is_splitted_source,
            mask_sql: self.mask_sql.clone(),
        }))
    }

    pub(super) fn replace_case(&self, new_case: Case) -> Rc<MeasureSymbol> {
        let mut new = self.clone();
        new.case = Some(new_case);
        Rc::new(new)
    }

    pub fn compiled_path(&self) -> &CompiledMemberPath {
        &self.compiled_path
    }

    /// Trims the join-chain prefix from `compiled_path` in place so
    /// the path points only at the owning cube.
    pub fn strip_join_prefix(&mut self) {
        self.compiled_path = self.compiled_path.strip_join_prefix();
    }

    /// Full unique identifier of the symbol: cube path, member name
    /// and any suffix that distinguishes one symbol from another.
    pub fn full_name(&self) -> String {
        self.compiled_path.full_name().clone()
    }

    /// Default alias of the measure, derived from the compiled member
    /// path.
    pub fn alias(&self) -> String {
        self.compiled_path.alias().clone()
    }

    pub fn is_splitted_source(&self) -> bool {
        self.is_splitted_source
    }

    pub fn time_shift(&self) -> Option<&MeasureTimeShifts> {
        self.multi_stage
            .as_ref()
            .and_then(|m| m.time_shift.as_ref())
    }

    pub fn is_calculated(&self) -> bool {
        self.kind.is_calculated()
    }

    pub fn case(&self) -> Option<&Case> {
        self.case.as_ref()
    }

    /// Optional SQL expression that wraps the measure's rendered
    /// output to mask its value (data hiding / column-level masking).
    pub fn mask_sql(&self) -> &Option<Rc<SqlCall>> {
        &self.mask_sql
    }

    /// True when the measure's aggregation distributes over row union
    /// (sum-like). Multi-stage measures are never additive — their
    /// value depends on the windowed stage, not on a plain sum.
    pub fn is_additive(&self) -> bool {
        if self.is_multi_stage() {
            false
        } else {
            self.kind.is_additive()
        }
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let mut result = self.clone();
        result.kind = result.kind.apply_to_deps(f)?;

        for sql in result.measure_filters.iter_mut() {
            *sql = sql.apply_recursive(f)?
        }

        for sql in result.measure_drill_filters.iter_mut() {
            *sql = sql.apply_recursive(f)?
        }

        for order in result.measure_order_by.iter_mut() {
            order.set_sql_call(order.sql_call().apply_recursive(f)?);
        }

        if let Some(case) = &self.case {
            result.case = Some(case.apply_to_deps(f)?)
        }

        if let Some(mask) = &self.mask_sql {
            result.mask_sql = Some(mask.apply_recursive(f)?);
        }

        if let Some(ms) = &self.multi_stage {
            result.multi_stage = Some(ms.apply_to_deps(f)?);
        }

        Ok(MemberSymbol::new_measure(Rc::new(result)))
    }

    /// SQL calls inside the measure's kind and `case` body.
    /// `mask_sql` is intentionally excluded: it is compiled against
    /// the cube that owns the measure, which differs from the symbol's
    /// own `cube_name` when the measure is exposed through a view.
    /// `measure_filters` and `measure_order_by` are also skipped here
    /// — the legacy BaseQuery validator does not check them, and we
    /// preserve that behaviour for compatibility.
    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        let result = self
            .kind
            .iter_sql_calls()
            .chain(self.case.iter().flat_map(|case| case.iter_sql_calls()));
        Box::new(result)
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = self.kind.get_dependencies();
        for filter in self.measure_filters.iter() {
            filter.extract_symbol_deps(&mut deps);
        }
        for filter in self.measure_drill_filters.iter() {
            filter.extract_symbol_deps(&mut deps);
        }
        for order in self.measure_order_by.iter() {
            order.sql_call().extract_symbol_deps(&mut deps);
        }
        if let Some(case) = &self.case {
            case.extract_symbol_deps(&mut deps);
        }
        if let Some(mask) = &self.mask_sql {
            mask.extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = self.kind.get_cube_refs();
        for filter in self.measure_filters.iter() {
            filter.extract_cube_refs(&mut refs);
        }
        for filter in self.measure_drill_filters.iter() {
            filter.extract_cube_refs(&mut refs);
        }
        for order in self.measure_order_by.iter() {
            order.sql_call().extract_cube_refs(&mut refs);
        }
        if let Some(case) = &self.case {
            case.extract_cube_refs(&mut refs);
        }
        if let Some(mask) = &self.mask_sql {
            mask.extract_cube_refs(&mut refs);
        }
        refs
    }

    /// Render form of this measure when it sits under a row-multiplying
    /// join: a `count` switches to a distinct `MultipliedCount`, every
    /// other kind is returned unchanged.
    pub fn into_multiplied(&self) -> Rc<MemberSymbol> {
        self.with_kind(self.kind.into_multiplied())
    }

    /// `Some(render form)` when this measure, under a row-multiplying
    /// join, can still be computed directly in the main query (it stays
    /// additive there): a key-based count rolls up as a distinct
    /// `MultipliedCount`, distinct aggregations are already immune.
    /// `None` when it must be isolated in a multiplied subquery instead.
    pub fn convert_multiplied_to_regular(&self) -> Option<Rc<MemberSymbol>> {
        self.kind
            .regular_in_multiplied()
            .map(|kind| self.with_kind(kind))
    }

    fn with_kind(&self, kind: MeasureKind) -> Rc<MemberSymbol> {
        let mut new = self.clone();
        new.kind = kind;
        MemberSymbol::new_measure(Rc::new(new))
    }

    /// True when the cube on the symbol's path is required in the
    /// join to read the measure from the database. Multi-stage
    /// measures are never owned by a cube; otherwise ownership is the
    /// union of the kind, the measure filters and the `case` body.
    pub fn owned_by_cube(&self) -> bool {
        if self.is_multi_stage() {
            return false;
        }
        let mut owned = self.kind.is_owned_by_cube();
        for sql in &self.measure_filters {
            owned |= sql.is_owned_by_cube();
        }
        for sql in &self.measure_drill_filters {
            owned |= sql.is_owned_by_cube();
        }
        if let Some(case) = &self.case {
            owned |= case.is_owned_by_cube();
        }
        owned
    }

    pub fn is_reference(&self) -> bool {
        self.is_reference
    }

    pub fn is_view(&self) -> bool {
        self.is_view
    }

    /// The member this measure references, or `None` if it is not a
    /// reference.
    pub fn reference_member(&self) -> Option<Rc<MemberSymbol>> {
        if !self.is_reference() {
            return None;
        }
        let deps = self.get_dependencies();
        if deps.is_empty() {
            return None;
        }
        deps.first().cloned()
    }

    pub fn measure_type(&self) -> &str {
        self.kind.measure_type_str()
    }

    pub fn kind(&self) -> &MeasureKind {
        &self.kind
    }

    pub fn rolling_window(&self) -> &Option<RollingWindow> {
        &self.rolling_window
    }

    pub fn is_rolling_window(&self) -> bool {
        self.rolling_window().is_some()
    }

    /// True for rolling-window measures.
    pub fn is_cumulative(&self) -> bool {
        self.is_rolling_window()
    }

    pub fn measure_filters(&self) -> &Vec<Rc<SqlCall>> {
        &self.measure_filters
    }

    pub fn measure_drill_filters(&self) -> &Vec<Rc<SqlCall>> {
        &self.measure_drill_filters
    }

    pub fn measure_order_by(&self) -> &Vec<MeasureOrderBy> {
        &self.measure_order_by
    }

    pub fn multi_stage(&self) -> Option<&MultiStageProperties> {
        self.multi_stage.as_ref()
    }

    pub fn is_multi_stage(&self) -> bool {
        self.multi_stage.is_some()
    }

    pub fn cube_name(&self) -> String {
        self.compiled_path.cube_name().clone()
    }

    pub fn join_map(&self) -> &Option<Vec<Vec<String>>> {
        self.compiled_path.join_map()
    }

    pub fn name(&self) -> String {
        self.compiled_path.name().clone()
    }

    pub fn path(&self) -> &Vec<String> {
        self.compiled_path.path()
    }
}

/// Builds a `MeasureSymbol` from a measure definition pulled out of
/// the cube schema.
pub struct MeasureSymbolFactory {
    path: SymbolPath,
    sql: Option<Rc<dyn MemberSql>>,
    mask_sql: Option<Rc<dyn MemberSql>>,
    definition: Rc<dyn MeasureDefinition>,
    cube_evaluator: Rc<dyn CubeEvaluator>,
}

impl MeasureSymbolFactory {
    pub fn try_new(
        path: SymbolPath,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let definition = cube_evaluator.measure_by_path(path.full_name().clone())?;
        let sql = definition.sql()?;
        let mask_sql = definition.mask_sql()?;
        Ok(Self {
            path,
            sql,
            mask_sql,
            definition,
            cube_evaluator,
        })
    }
}

impl SymbolFactory for MeasureSymbolFactory {
    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError> {
        let Self {
            path,
            sql,
            mask_sql,
            definition,
            cube_evaluator,
        } = self;

        let pk_sqls = if sql.is_none() {
            cube_evaluator
                .static_data()
                .primary_keys
                .get(path.cube_name())
                .cloned()
                .unwrap_or_else(|| vec![])
                .into_iter()
                .map(|primary_key| -> Result<_, CubeError> {
                    let key_dimension_name = format!("{}.{}", path.cube_name(), primary_key);
                    let key_dimension =
                        cube_evaluator.dimension_by_path(key_dimension_name.clone())?;
                    let key_dimension_sql = if let Some(key_dimension_sql) = key_dimension.sql()? {
                        Ok(key_dimension_sql)
                    } else {
                        Err(CubeError::internal(format!(
                            "Key dimension {} hasn't sql evaluator",
                            key_dimension_name
                        )))
                    }?;
                    compiler.compile_sql_call(path.cube_name(), key_dimension_sql)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };

        let mut measure_filters = vec![];
        if let Some(filters) = definition.filters()? {
            for filter in filters.iter() {
                let node = compiler.compile_sql_call(path.cube_name(), filter.sql()?)?;
                measure_filters.push(node);
            }
        }

        let mut measure_drill_filters = vec![];
        if let Some(filters) = definition.drill_filters()? {
            for filter in filters.iter() {
                let node = compiler.compile_sql_call(path.cube_name(), filter.sql()?)?;
                measure_drill_filters.push(node);
            }
        }

        let mut measure_order_by = vec![];
        if let Some(group_by) = definition.order_by()? {
            for item in group_by.iter() {
                let node = compiler.compile_sql_call(path.cube_name(), item.sql()?)?;
                measure_order_by.push(MeasureOrderBy::new(node, item.dir()?));
            }
        }
        let sql = if let Some(sql) = sql {
            Some(compiler.compile_sql_call(path.cube_name(), sql)?)
        } else {
            None
        };

        let is_sql_is_direct_ref = sql.as_ref().is_some_and(|s| s.is_direct_reference());

        // mask.sql references are written in the context of the cube that
        // owns the measure. When a measure is exposed through a view, the
        // measure's sql is a direct reference to the underlying cube member;
        // compile mask.sql against that referenced member's cube so CUBE /
        // cross-cube references inside the mask resolve the same way as on
        // the owning cube — and as they do on the legacy BaseQuery path,
        // which routes mask compilation through aliasMember for the same
        // reason.
        let mask_sql_cube_name = sql
            .as_ref()
            .and_then(|s| s.resolve_direct_reference())
            .map(|dep| dep.cube_name())
            .unwrap_or_else(|| path.cube_name().clone());
        let mask_sql = if let Some(mask_sql) = mask_sql {
            Some(compiler.compile_sql_call(&mask_sql_cube_name, mask_sql)?)
        } else {
            None
        };

        let time_shifts = if let Some(time_shift_references) =
            &definition.static_data().time_shift_references
        {
            let mut shifts: HashMap<String, DimensionTimeShift> = HashMap::new();
            let mut common_shift = None;
            let mut named_shift = None;
            for shift_ref in time_shift_references.iter() {
                let interval = match &shift_ref.interval {
                    Some(raw) => {
                        let mut iv = raw.parse::<SqlInterval>()?;
                        if shift_ref.shift_type.as_deref().unwrap_or("prior") == "next" {
                            iv = -iv;
                        }

                        Some(iv)
                    }
                    None => None,
                };
                let name = shift_ref.name.clone();
                if let Some(time_dimension) = &shift_ref.time_dimension {
                    let dimension = compiler.add_dimension_evaluator(time_dimension.clone())?;
                    let dimension = find_owned_by_cube_child(&dimension)?;
                    let dimension_name = dimension.full_name();
                    if let Some(exists) = shifts.get(&dimension_name) {
                        if exists.interval != interval || exists.name != name {
                            return Err(CubeError::user(format!(
                                "Different time shifts for one dimension {} not allowed",
                                dimension_name
                            )));
                        }
                    } else {
                        shifts.insert(
                            dimension_name.clone(),
                            DimensionTimeShift {
                                interval: interval.clone(),
                                name: name.clone(),
                                dimension: dimension.clone(),
                            },
                        );
                    };
                } else if let Some(name) = &shift_ref.name {
                    if named_shift.is_none() {
                        named_shift = Some(name.clone());
                    } else {
                        if named_shift != Some(name.clone()) {
                            return Err(CubeError::user(format!(
                                "Measure can contain only one named time_shift (without time_dimension).",
                            )));
                        }
                    }
                } else {
                    if common_shift.is_none() {
                        common_shift = interval;
                    } else {
                        if common_shift != interval {
                            return Err(CubeError::user(format!(
                                    "Measure can contain only one common time_shift (without time_dimension).",
                                )));
                        }
                    }
                }
            }

            if (common_shift.is_some() || named_shift.is_some()) && !shifts.is_empty() {
                return Err(CubeError::user(format!(
                        "Measure cannot mix common time_shifts (without time_dimension) with dimension-specific ones.",
                    )));
            } else if common_shift.is_some() && named_shift.is_some() {
                return Err(CubeError::user(format!(
                    "Measure cannot mix common unnamed and named time_shifts.",
                )));
            } else if let Some(cs) = common_shift {
                Some(MeasureTimeShifts::Common(cs))
            } else if let Some(ns) = named_shift {
                Some(MeasureTimeShifts::Named(ns))
            } else {
                Some(MeasureTimeShifts::Dimensions(
                    shifts.into_values().collect_vec(),
                ))
            }
        } else {
            None
        };

        let case = if let Some(native_case) = definition.case()? {
            Some(Case::try_new(path.cube_name(), native_case, compiler)?)
        } else {
            None
        };

        let multi_stage = MultiStageProperties::from_measure_definition(
            path.cube_name(),
            &definition,
            time_shifts,
            compiler,
        )?;

        let measure_type_str = &definition.static_data().measure_type;
        let rolling_window = definition.static_data().rolling_window.clone();
        let is_multi_stage = multi_stage.is_some();

        let kind = MeasureKind::from_type_str(measure_type_str, sql, pk_sqls)?;
        let is_calculated = kind.is_calculated() && !is_multi_stage;

        let owned_by_cube = if is_multi_stage {
            false
        } else {
            let mut owned = kind.is_owned_by_cube();
            for sql in &measure_filters {
                owned |= sql.is_owned_by_cube();
            }
            for sql in &measure_drill_filters {
                owned |= sql.is_owned_by_cube();
            }
            if let Some(case) = &case {
                owned |= case.is_owned_by_cube();
            }
            owned
        };

        let cube = cube_evaluator.cube_from_path(path.cube_name().clone())?;
        let alias = compiler
            .alias_for_member(path.full_name())
            .unwrap_or_else(|| {
                PlanSqlTemplates::member_alias_name(
                    cube.static_data().resolved_alias(),
                    path.symbol_name(),
                    &None,
                )
            });

        let is_view = cube.static_data().is_view.unwrap_or(false);

        let is_reference = (is_view && is_sql_is_direct_ref)
            || (!owned_by_cube
                && is_sql_is_direct_ref
                && is_calculated
                && !is_multi_stage
                && case.is_none()
                && measure_filters.is_empty()
                && measure_drill_filters.is_empty()
                && measure_order_by.is_empty());

        let cube_symbol = compiler.add_cube_table_evaluator(path.cube_name().clone(), vec![])?;

        let compiled_path = CompiledMemberPath::new(
            cube_symbol,
            path.full_name().clone(),
            path.symbol_name().clone(),
            alias,
            path.path().clone(),
        );

        Ok(MemberSymbol::new_measure(MeasureSymbol::new(
            compiled_path,
            is_reference,
            is_view,
            case,
            kind,
            rolling_window,
            multi_stage,
            measure_filters,
            measure_drill_filters,
            measure_order_by,
            mask_sql,
        )))
    }
}

impl crate::utils::debug::DebugSql for MeasureSymbol {
    fn debug_sql(&self, expand_deps: bool) -> String {
        // Handle case expressions
        if let Some(case) = &self.case {
            return case.debug_sql(expand_deps);
        }

        // Get base SQL
        let base_sql = if let Some(sql) = self.kind.member_sql() {
            sql.debug_sql(expand_deps)
        } else {
            "".to_string()
        };

        // Handle filtered measures
        if !self.measure_filters.is_empty() {
            let filters = self
                .measure_filters
                .iter()
                .map(|f| f.debug_sql(expand_deps))
                .collect::<Vec<_>>()
                .join(" AND ");
            return format!("{} FILTER (WHERE {})", base_sql, filters);
        }

        base_sql
    }
}
