use super::common::Case;
use super::common::CompiledMemberPath;
use super::common::MultiStageProperties;
use super::dimension_kinds::{
    CaseDimension, DimensionKind, GeoDimension, RegularDimension, SwitchDimension,
};
use super::SymbolPath;
use super::{DimensionType, MemberSymbol, SymbolFactory};
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::GranularityHelper;
use crate::planner::SqlInterval;
use crate::planner::TimeDimensionSymbol;
use crate::planner::{Compiler, CubeRef, SqlCall};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Time-shift entry on a dimension of a calendar cube: shifts the
/// date range by either a fixed interval, a named slot (e.g.
/// `prev_year`), or a custom SQL expression.
#[derive(Clone)]
pub struct CalendarDimensionTimeShift {
    pub interval: Option<SqlInterval>,
    pub name: Option<String>,
    pub sql: Option<Rc<SqlCall>>,
}

/// `MemberSymbol::Dimension` body: Tesseract representation of a
/// `dimension` declared in the data model — a value the query can
/// group, filter or order by, but never aggregate.
#[derive(Clone)]
pub struct DimensionSymbol {
    compiled_path: CompiledMemberPath,
    kind: DimensionKind,
    is_reference: bool, // Symbol is a direct reference to another symbol without any calculations
    is_view: bool,
    multi_stage: Option<MultiStageProperties>,
    time_shift: Vec<CalendarDimensionTimeShift>,
    time_shift_pk_full_name: Option<String>,
    is_self_time_shift_pk: bool, // If the dimension itself is a primary key and has time shifts, we can not reevaluate itself again while processing time shifts to avoid infinite recursion. So we raise this flag instead.
    is_sub_query: bool,
    propagate_filters_to_sub_query: bool,
    mask_sql: Option<Rc<SqlCall>>,
}

impl DimensionSymbol {
    pub fn new(
        compiled_path: CompiledMemberPath,
        kind: DimensionKind,
        is_reference: bool,
        is_view: bool,
        multi_stage: Option<MultiStageProperties>,
        time_shift: Vec<CalendarDimensionTimeShift>,
        time_shift_pk_full_name: Option<String>,
        is_self_time_shift_pk: bool,
        is_sub_query: bool,
        propagate_filters_to_sub_query: bool,
        mask_sql: Option<Rc<SqlCall>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            compiled_path,
            kind,
            is_reference,
            is_view,
            multi_stage,
            time_shift,
            time_shift_pk_full_name,
            is_self_time_shift_pk,
            is_sub_query,
            propagate_filters_to_sub_query,
            mask_sql,
        })
    }

    pub fn is_calc_group(&self) -> bool {
        self.kind.is_calc_group()
    }

    /// String values declared on a `Switch` dimension; empty for any
    /// other kind.
    pub fn values(&self) -> &[String] {
        match &self.kind {
            DimensionKind::Switch(s) => s.values(),
            _ => &[],
        }
    }

    pub(super) fn replace_case(&self, new_case: Case) -> Rc<DimensionSymbol> {
        let mut new = self.clone();
        if new_case.is_single_value() {
            //FIXME - Hack: we don't treat a single-element case as a multi-stage dimension
            new.multi_stage = None;
        }
        if let DimensionKind::Case(ref c) = new.kind {
            new.kind = DimensionKind::Case(c.replace_case(new_case));
        }
        Rc::new(new)
    }

    /// Case-expression body for `DimensionKind::Case`; `None` otherwise.
    pub fn case(&self) -> Option<&Case> {
        match &self.kind {
            DimensionKind::Case(c) => Some(c.case()),
            _ => None,
        }
    }

    /// `None` if the dimension has no primary SQL expression of its own.
    pub fn member_sql(&self) -> Option<&Rc<SqlCall>> {
        match &self.kind {
            DimensionKind::Regular(r) => Some(r.member_sql()),
            DimensionKind::Switch(s) => s.member_sql(),
            DimensionKind::Case(c) => c.member_sql(),
            DimensionKind::Geo(_) => None,
        }
    }

    pub fn time_shift(&self) -> &Vec<CalendarDimensionTimeShift> {
        &self.time_shift
    }

    pub fn time_shift_pk_full_name(&self) -> Option<String> {
        self.time_shift_pk_full_name.clone()
    }

    pub fn compiled_path(&self) -> &CompiledMemberPath {
        &self.compiled_path
    }

    /// Trims the join-chain prefix from `compiled_path` in place so the
    /// path points only at the owning cube.
    pub fn strip_join_prefix(&mut self) {
        self.compiled_path = self.compiled_path.strip_join_prefix();
    }

    /// Full unique identifier of the symbol: cube path, member name and
    /// any suffix that distinguishes one symbol from another.
    pub fn full_name(&self) -> String {
        self.compiled_path.full_name().clone()
    }

    /// Default alias of the dimension, derived from the compiled member
    /// path.
    pub fn alias(&self) -> String {
        self.compiled_path.alias().clone()
    }

    /// True when the cube on the symbol's path actually owns this
    /// dimension — the cube is required in the join to read the
    /// dimension from the database. False for view-exposed dimensions,
    /// multi-stage dimensions, switches, and members defined as pure
    /// compositions of other members (no `{CUBE}` references).
    pub fn owned_by_cube(&self) -> bool {
        !self.is_multi_stage() && !self.kind.is_switch() && self.kind.is_owned_by_cube()
    }

    pub fn multi_stage(&self) -> Option<&MultiStageProperties> {
        self.multi_stage.as_ref()
    }

    pub fn is_multi_stage(&self) -> bool {
        self.multi_stage.is_some()
    }

    /// Direct mapping from the `sub_query` field of the dimension
    /// definition in the data model.
    pub fn is_sub_query(&self) -> bool {
        self.is_sub_query
    }

    /// Optional SQL expression that wraps the dimension's rendered
    /// output to mask its value (data hiding / column-level masking).
    pub fn mask_sql(&self) -> &Option<Rc<SqlCall>> {
        &self.mask_sql
    }

    pub fn add_group_by(&self) -> Option<&Vec<Rc<MemberSymbol>>> {
        self.multi_stage
            .as_ref()
            .and_then(|m| m.grain.include.as_ref())
    }

    pub fn dimension_type(&self) -> &str {
        self.kind.dimension_type_str()
    }

    pub fn kind(&self) -> &DimensionKind {
        &self.kind
    }

    pub fn is_time(&self) -> bool {
        self.kind.is_time()
    }

    pub fn is_geo(&self) -> bool {
        self.kind.is_geo()
    }

    pub fn is_switch(&self) -> bool {
        self.kind.is_switch()
    }

    pub fn is_case(&self) -> bool {
        self.kind.is_case()
    }

    /// Direct mapping from the `propagate_filters_to_sub_query` field
    /// of the dimension definition in the data model.
    pub fn propagate_filters_to_sub_query(&self) -> bool {
        self.propagate_filters_to_sub_query
    }

    pub fn is_reference(&self) -> bool {
        self.is_reference
    }

    pub fn is_view(&self) -> bool {
        self.is_view
    }

    /// The member this dimension references, or `None` if it is not a
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

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let mut result = self.clone();
        result.kind = self.kind.apply_to_deps(f)?;
        if let Some(mask) = &self.mask_sql {
            result.mask_sql = Some(mask.apply_recursive(f)?);
        }
        if let Some(ms) = &self.multi_stage {
            result.multi_stage = Some(ms.apply_to_deps(f)?);
        }
        Ok(MemberSymbol::new_dimension(Rc::new(result)))
    }

    /// SQL calls inside the kind body. `mask_sql` is intentionally
    /// excluded: it is compiled against the cube that owns the
    /// dimension, which differs from the symbol's own `cube_name` when
    /// the dimension is exposed through a view. Including it in
    /// cube-ref validation would produce false foreign-cube errors.
    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        self.kind.iter_sql_calls()
    }

    /// All member dependencies of the dimension.
    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = self.kind.get_dependencies();
        if let Some(mask) = &self.mask_sql {
            mask.extract_symbol_deps(&mut deps);
        }
        deps
    }

    /// All cube references of the dimension.
    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = self.kind.get_cube_refs();
        if let Some(mask) = &self.mask_sql {
            mask.extract_cube_refs(&mut refs);
        }
        refs
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

    /// Finds the calendar time-shift defined for the exact `interval`
    /// and returns it together with the primary-key full name. `None`
    /// when either the matching shift or the primary key is missing.
    pub fn calendar_time_shift_for_interval(
        &self,
        interval: &SqlInterval,
    ) -> Option<(String, CalendarDimensionTimeShift)> {
        if let Some(ts) = self.time_shift.iter().find(|shift| {
            if let Some(s_i) = &shift.interval {
                s_i == interval
            } else {
                false
            }
        }) {
            if let Some(pk) = &self.time_shift_pk_full_name() {
                return Some((pk.clone(), ts.clone()));
            }
        }
        None
    }

    /// Finds the named calendar time-shift and returns it together
    /// with the primary-key full name. Falls back to this dimension's
    /// own full name when the dimension is itself the calendar primary
    /// key.
    pub fn calendar_time_shift_for_named_interval(
        &self,
        interval_name: &String,
    ) -> Option<(String, CalendarDimensionTimeShift)> {
        if let Some(ts) = self.time_shift.iter().find(|shift| {
            if let Some(s_n) = &shift.name {
                s_n == interval_name
            } else {
                false
            }
        }) {
            if let Some(pk) = &self.time_shift_pk_full_name {
                return Some((pk.clone(), ts.clone()));
            } else if self.is_self_time_shift_pk {
                return Some((self.full_name(), ts.clone()));
            }
        }
        None
    }
}

/// Builds a `DimensionSymbol` from a dimension definition pulled out
/// of the cube schema. When the requested path includes a granularity,
/// the result is wrapped in a `TimeDimensionSymbol` instead.
pub struct DimensionSymbolFactory {
    path: SymbolPath,
    sql: Option<Rc<dyn MemberSql>>,
    mask_sql: Option<Rc<dyn MemberSql>>,
    definition: Rc<dyn DimensionDefinition>,
    cube_evaluator: Rc<dyn CubeEvaluator>,
}

impl DimensionSymbolFactory {
    pub fn try_new(
        path: SymbolPath,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let definition = cube_evaluator.dimension_by_path(path.full_name().clone())?;
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

impl SymbolFactory for DimensionSymbolFactory {
    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError> {
        let Self {
            path,
            sql,
            mask_sql,
            definition,
            cube_evaluator,
        } = self;

        let dimension_type = definition.static_data().dimension_type.clone();

        let sql = if let Some(sql) = sql {
            Some(compiler.compile_sql_call(path.cube_name(), sql)?)
        } else {
            None
        };

        let is_sql_direct_ref = sql.as_ref().is_some_and(|s| s.is_direct_reference());

        // mask.sql references are written in the context of the cube that
        // owns the dimension. When a dimension is exposed through a view,
        // the dimension's sql is a direct reference to the underlying cube
        // member; compile mask.sql against that referenced member's cube so
        // CUBE / cross-cube references inside the mask resolve the same way
        // as on the owning cube — and as they do on the legacy BaseQuery
        // path, which routes mask compilation through aliasMember for the
        // same reason.
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

        let case = if let Some(native_case) = definition.case()? {
            Some(Case::try_new(path.cube_name(), native_case, compiler)?)
        } else {
            None
        };

        let time_shift = if let Some(time_shift) = definition.time_shift()? {
            time_shift
                .iter()
                .map(|item| -> Result<_, CubeError> {
                    let interval = match &item.static_data().interval {
                        Some(raw) => {
                            let mut iv = raw.parse::<SqlInterval>()?;
                            if item.static_data().timeshift_type.as_deref() == Some("next") {
                                iv = -iv;
                            }

                            Some(iv)
                        }
                        None => None,
                    };
                    let name = item.static_data().name.clone();
                    let sql = if let Some(sql) = item.sql()? {
                        Some(compiler.compile_sql_call(path.cube_name(), sql)?)
                    } else {
                        None
                    };
                    Ok(CalendarDimensionTimeShift {
                        interval,
                        name,
                        sql,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
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
        let is_calendar = cube.static_data().is_calendar.unwrap_or(false);
        let mut is_self_time_shift_pk = false;

        // If the cube is a calendar, we need to find the primary key member
        // so that we can use it for time shifts processing.
        let time_shift_pk = if is_calendar {
            let pk_members = cube_evaluator
                .static_data()
                .primary_keys
                .get(path.cube_name())
                .cloned()
                .unwrap_or_else(|| vec![]);

            if pk_members.iter().any(|pk| &**pk == path.symbol_name()) {
                is_self_time_shift_pk = true;
            }

            if pk_members.len() > 1 {
                return Err(CubeError::user(format!(
                    "Cube '{}' has multiple primary keys, but only one is allowed for calendar cubes",
                    path.cube_name()
                )));
            }

            pk_members
                .first()
                .map(|pk| format!("{}.{}", path.cube_name(), pk))
        } else {
            None
        };

        let multi_stage = MultiStageProperties::from_dimension_definition(
            path.cube_name(),
            &definition,
            compiler,
        )?;

        let is_sub_query = definition.static_data().sub_query.unwrap_or(false);
        let is_multi_stage = multi_stage.is_some();

        let kind = if let Some(case_val) = case {
            let dim_type = DimensionType::from_str(&dimension_type)?;
            DimensionKind::Case(CaseDimension::new(dim_type, case_val, sql))
        } else if dimension_type == "geo" {
            if let (Some(lat_item), Some(lon_item)) =
                (definition.latitude()?, definition.longitude()?)
            {
                let latitude = compiler.compile_sql_call(path.cube_name(), lat_item.sql()?)?;
                let longitude = compiler.compile_sql_call(path.cube_name(), lon_item.sql()?)?;
                DimensionKind::Geo(GeoDimension::new(latitude, longitude))
            } else {
                return Err(CubeError::user(format!(
                    "Geo dimension '{}' must have latitude and longitude",
                    path.full_name()
                )));
            }
        } else if dimension_type == "switch" {
            let values = definition.static_data().values.clone().unwrap_or_default();
            DimensionKind::Switch(SwitchDimension::new(values, sql))
        } else {
            let dim_type = DimensionType::from_str(&dimension_type)?;
            match sql {
                Some(sql) => DimensionKind::Regular(RegularDimension::new(dim_type, sql)),
                None => {
                    return Err(CubeError::internal(format!(
                        "Dimension '{}' must have sql",
                        path.full_name()
                    )));
                }
            }
        };

        let owned_by_cube = if is_multi_stage || kind.is_switch() {
            false
        } else {
            kind.is_owned_by_cube()
        };
        let is_reference = (is_view && is_sql_direct_ref)
            || (!owned_by_cube
                && !is_sub_query
                && is_sql_direct_ref
                && !kind.is_case()
                && !kind.is_geo()
                && !is_multi_stage);

        let propagate_filters_to_sub_query = definition
            .static_data()
            .propagate_filters_to_sub_query
            .unwrap_or(false);

        let cube_symbol = compiler.add_cube_table_evaluator(path.cube_name().clone(), vec![])?;

        let compiled_path = CompiledMemberPath::new(
            cube_symbol,
            path.full_name().clone(),
            path.symbol_name().clone(),
            alias,
            path.path().clone(),
        );

        let symbol = MemberSymbol::new_dimension(DimensionSymbol::new(
            compiled_path,
            kind,
            is_reference,
            is_view,
            multi_stage,
            time_shift,
            time_shift_pk,
            is_self_time_shift_pk,
            is_sub_query,
            propagate_filters_to_sub_query,
            mask_sql,
        ));

        if let Some(granularity) = path.granularity() {
            if let Some(granularity_obj) = GranularityHelper::make_granularity_obj(
                cube_evaluator.clone(),
                compiler,
                path.cube_name(),
                path.symbol_name(),
                Some(granularity.clone()),
            )? {
                let time_dim_symbol = MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
                    symbol,
                    Some(granularity.clone()),
                    Some(granularity_obj),
                    None,
                ));
                return Ok(time_dim_symbol);
            } else {
                return Err(CubeError::user(format!(
                    "Undefined granularity {} for time dimension {}",
                    granularity,
                    symbol.full_name()
                )));
            }
        }

        Ok(symbol)
    }
}

impl crate::utils::debug::DebugSql for DimensionSymbol {
    fn debug_sql(&self, expand_deps: bool) -> String {
        match &self.kind {
            DimensionKind::Case(c) => c.case().debug_sql(expand_deps),
            DimensionKind::Geo(g) => {
                let lat = g.latitude().debug_sql(expand_deps);
                let lon = g.longitude().debug_sql(expand_deps);
                format!("GEO({}, {})", lat, lon)
            }
            DimensionKind::Switch(s) if s.is_calc_group() => {
                format!("SWITCH({})", self.full_name())
            }
            _ => {
                if let Some(sql) = self.member_sql() {
                    sql.debug_sql(expand_deps)
                } else {
                    "".to_string()
                }
            }
        }
    }
}
