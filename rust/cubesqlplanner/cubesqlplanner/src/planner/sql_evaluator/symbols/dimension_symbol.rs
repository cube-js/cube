use super::common::Case;
use super::dimension_kinds::{
    CaseDimension, DimensionKind, GeoDimension, RegularDimension, SwitchDimension,
};
use super::SymbolPath;
use super::{DimensionType, MemberSymbol, SymbolFactory};
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, Compiler, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_evaluator::{CubeTableSymbol, TimeDimensionSymbol};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::GranularityHelper;
use crate::planner::SqlInterval;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct CalendarDimensionTimeShift {
    pub interval: Option<SqlInterval>,
    pub name: Option<String>,
    pub sql: Option<Rc<SqlCall>>,
}

#[derive(Clone)]
pub struct DimensionSymbol {
    cube: Rc<CubeTableSymbol>,
    name: String,
    kind: DimensionKind,
    alias: String,
    is_reference: bool, // Symbol is a direct reference to another symbol without any calculations
    is_view: bool,
    add_group_by: Option<Vec<Rc<MemberSymbol>>>,
    time_shift: Vec<CalendarDimensionTimeShift>,
    time_shift_pk_full_name: Option<String>,
    is_self_time_shift_pk: bool, // If the dimension itself is a primary key and has time shifts, we can not reevaluate itself again while processing time shifts to avoid infinite recursion. So we raise this flag instead.
    is_multi_stage: bool,
    is_sub_query: bool,
    propagate_filters_to_sub_query: bool,
}

impl DimensionSymbol {
    pub fn new(
        cube: Rc<CubeTableSymbol>,
        name: String,
        kind: DimensionKind,
        alias: String,
        is_reference: bool,
        is_view: bool,
        add_group_by: Option<Vec<Rc<MemberSymbol>>>,
        time_shift: Vec<CalendarDimensionTimeShift>,
        time_shift_pk_full_name: Option<String>,
        is_self_time_shift_pk: bool,
        is_multi_stage: bool,
        is_sub_query: bool,
        propagate_filters_to_sub_query: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            cube,
            name,
            kind,
            alias,
            is_reference,
            is_view,
            add_group_by,
            time_shift,
            time_shift_pk_full_name,
            is_self_time_shift_pk,
            is_multi_stage,
            is_sub_query,
            propagate_filters_to_sub_query,
        })
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        self.kind.evaluate_sql(
            &self.name,
            &self.full_name(),
            visitor,
            node_processor,
            query_tools,
            templates,
        )
    }

    pub fn is_calc_group(&self) -> bool {
        self.kind.is_calc_group()
    }

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
            new.is_multi_stage = false;
        }
        if let DimensionKind::Case(ref c) = new.kind {
            new.kind = DimensionKind::Case(c.replace_case(new_case));
        }
        Rc::new(new)
    }

    pub fn latitude(&self) -> Option<Rc<SqlCall>> {
        match &self.kind {
            DimensionKind::Geo(g) => Some(g.latitude().clone()),
            _ => None,
        }
    }

    pub fn longitude(&self) -> Option<Rc<SqlCall>> {
        match &self.kind {
            DimensionKind::Geo(g) => Some(g.longitude().clone()),
            _ => None,
        }
    }

    pub fn case(&self) -> Option<&Case> {
        match &self.kind {
            DimensionKind::Case(c) => Some(c.case()),
            _ => None,
        }
    }

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

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.cube.cube_name(), self.name)
    }

    pub fn alias(&self) -> String {
        self.alias.clone()
    }

    pub fn owned_by_cube(&self) -> bool {
        !self.is_multi_stage && !self.kind.is_switch() && self.kind.is_owned_by_cube()
    }

    pub fn is_multi_stage(&self) -> bool {
        self.is_multi_stage
    }

    pub fn is_sub_query(&self) -> bool {
        self.is_sub_query
    }

    pub fn add_group_by(&self) -> &Option<Vec<Rc<MemberSymbol>>> {
        &self.add_group_by
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

    pub fn propagate_filters_to_sub_query(&self) -> bool {
        self.propagate_filters_to_sub_query
    }

    pub fn is_reference(&self) -> bool {
        self.is_reference
    }

    pub fn is_view(&self) -> bool {
        self.is_view
    }

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
        Ok(MemberSymbol::new_dimension(Rc::new(result)))
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        self.kind.iter_sql_calls()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        self.kind.get_dependencies()
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        self.kind.get_dependencies_with_path()
    }

    pub fn cube_name(&self) -> &String {
        self.cube.cube_name()
    }

    pub fn join_map(&self) -> &Option<Vec<Vec<String>>> {
        self.cube.join_map()
    }

    pub fn name(&self) -> &String {
        &self.name
    }

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

pub struct DimensionSymbolFactory {
    path: SymbolPath,
    sql: Option<Rc<dyn MemberSql>>,
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
        Ok(Self {
            path,
            sql,
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
            definition,
            cube_evaluator,
        } = self;

        let dimension_type = definition.static_data().dimension_type.clone();

        let sql = if let Some(sql) = sql {
            Some(compiler.compile_sql_call(path.cube_name(), sql)?)
        } else {
            None
        };

        let is_sql_direct_ref = if let Some(sql) = &sql {
            sql.is_direct_reference()
        } else {
            false
        };

        let (latitude, longitude) = if dimension_type == "geo" {
            if let (Some(latitude_item), Some(longitude_item)) =
                (definition.latitude()?, definition.longitude()?)
            {
                let latitude = compiler.compile_sql_call(path.cube_name(), latitude_item.sql()?)?;
                let longitude =
                    compiler.compile_sql_call(path.cube_name(), longitude_item.sql()?)?;
                (Some(latitude), Some(longitude))
            } else {
                return Err(CubeError::user(format!(
                    "Geo dimension '{}'must have latitude and longitude",
                    path.full_name()
                )));
            }
        } else {
            (None, None)
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
        let alias = PlanSqlTemplates::member_alias_name(
            cube.static_data().resolved_alias(),
            path.symbol_name(),
            &None,
        );
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

        let values = if dimension_type == "switch" {
            definition.static_data().values.clone().unwrap_or_default()
        } else {
            vec![]
        };

        let add_group_by =
            if let Some(add_group_by) = &definition.static_data().add_group_by_references {
                let symbols = add_group_by
                    .iter()
                    .map(|add_group_by| compiler.add_dimension_evaluator(add_group_by.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                Some(symbols)
            } else {
                None
            };

        let is_sub_query = definition.static_data().sub_query.unwrap_or(false);
        let is_multi_stage = definition.static_data().multi_stage.unwrap_or(false);

        // Build the appropriate DimensionKind first
        let kind = if let Some(case_val) = case {
            let dim_type = DimensionType::from_str(&dimension_type)?;
            DimensionKind::Case(CaseDimension::new(dim_type, case_val, sql))
        } else if dimension_type == "geo" {
            DimensionKind::Geo(GeoDimension::new(
                latitude.expect("geo latitude validated above"),
                longitude.expect("geo longitude validated above"),
            ))
        } else if dimension_type == "switch" {
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

        let cube_symbol = compiler
            .add_cube_table_evaluator(path.cube_name().clone())?
            .as_cube_table()?;

        let symbol = MemberSymbol::new_dimension(DimensionSymbol::new(
            cube_symbol,
            path.symbol_name().clone(),
            kind,
            alias,
            is_reference,
            is_view,
            add_group_by,
            time_shift,
            time_shift_pk,
            is_self_time_shift_pk,
            is_multi_stage,
            is_sub_query,
            propagate_filters_to_sub_query,
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
