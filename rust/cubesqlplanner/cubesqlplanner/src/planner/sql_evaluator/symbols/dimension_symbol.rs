use super::common::Case;
use super::{MemberSymbol, SymbolFactory};
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
    dimension_type: String,
    alias: String,
    member_sql: Option<Rc<SqlCall>>,
    latitude: Option<Rc<SqlCall>>,
    longitude: Option<Rc<SqlCall>>,
    values: Vec<String>,
    case: Option<Case>,
    definition: Rc<dyn DimensionDefinition>,
    is_reference: bool, // Symbol is a direct reference to another symbol without any calculations
    is_view: bool,
    time_shift: Vec<CalendarDimensionTimeShift>,
    time_shift_pk_full_name: Option<String>,
    is_self_time_shift_pk: bool, // If the dimension itself is a primary key and has time shifts, we can not reevaluate itself again while processing time shifts to avoid infinite recursion. So we raise this flag instead.
    owned_by_cube: bool,
    is_multi_stage: bool,
    is_sub_query: bool,
    propagate_filters_to_sub_query: bool,
}

impl DimensionSymbol {
    pub fn new(
        cube: Rc<CubeTableSymbol>,
        name: String,
        dimension_type: String,
        alias: String,
        member_sql: Option<Rc<SqlCall>>,
        is_reference: bool,
        is_view: bool,
        latitude: Option<Rc<SqlCall>>,
        longitude: Option<Rc<SqlCall>>,
        values: Vec<String>,
        case: Option<Case>,
        definition: Rc<dyn DimensionDefinition>,
        time_shift: Vec<CalendarDimensionTimeShift>,
        time_shift_pk_full_name: Option<String>,
        is_self_time_shift_pk: bool,
        owned_by_cube: bool,
        is_multi_stage: bool,
        is_sub_query: bool,
        propagate_filters_to_sub_query: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            cube,
            name,
            dimension_type,
            alias,
            member_sql,
            is_reference,
            latitude,
            longitude,
            values,
            definition,
            case,
            is_view,
            time_shift,
            time_shift_pk_full_name,
            is_self_time_shift_pk,
            owned_by_cube,
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
        if self.member_sql.is_none() && self.dimension_type == "switch" {
            Ok(templates.quote_identifier(&self.name)?) //We don't return cube_name -
                                                        //it should be added in
                                                        //autoprefix processing
        } else if let Some(member_sql) = &self.member_sql {
            let sql = member_sql.eval(visitor, node_processor, query_tools, templates)?;
            Ok(sql)
        } else {
            Err(CubeError::internal(format!(
                "Dimension {} hasn't sql evaluator",
                self.full_name()
            )))
        }
    }

    pub fn is_calc_group(&self) -> bool {
        self.member_sql.is_none() && self.dimension_type == "switch"
    }

    pub fn values(&self) -> &Vec<String> {
        &self.values
    }

    pub(super) fn replace_case(&self, new_case: Case) -> Rc<DimensionSymbol> {
        let mut new = self.clone();
        new.case = Some(new_case);
        Rc::new(new)
    }

    pub fn latitude(&self) -> Option<Rc<SqlCall>> {
        self.latitude.clone()
    }

    pub fn longitude(&self) -> Option<Rc<SqlCall>> {
        self.longitude.clone()
    }

    pub fn case(&self) -> Option<&Case> {
        self.case.as_ref()
    }

    pub fn member_sql(&self) -> &Option<Rc<SqlCall>> {
        &self.member_sql
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
        self.owned_by_cube
    }

    pub fn is_multi_stage(&self) -> bool {
        self.is_multi_stage
    }

    pub fn is_sub_query(&self) -> bool {
        self.is_sub_query
    }

    pub fn dimension_type(&self) -> &String {
        &self.dimension_type
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
        if let Some(member_sql) = &self.member_sql {
            result.member_sql = Some(member_sql.apply_recursive(f)?);
        }
        if let Some(latitude) = &self.latitude {
            result.latitude = Some(latitude.apply_recursive(f)?);
        }
        if let Some(longitude) = &self.longitude {
            result.longitude = Some(longitude.apply_recursive(f)?);
        }

        if let Some(case) = &self.case {
            result.case = Some(case.apply_to_deps(f)?)
        }

        Ok(MemberSymbol::new_dimension(Rc::new(result)))
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps(&mut deps);
        }
        if let Some(member_sql) = &self.latitude {
            member_sql.extract_symbol_deps(&mut deps);
        }
        if let Some(member_sql) = &self.longitude {
            member_sql.extract_symbol_deps(&mut deps);
        }
        if let Some(case) = &self.case {
            case.extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        if let Some(member_sql) = &self.latitude {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        if let Some(member_sql) = &self.longitude {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        if let Some(case) = &self.case {
            case.extract_symbol_deps_with_path(&mut deps);
        }
        deps
    }

    pub fn cube_name(&self) -> &String {
        &self.cube.cube_name()
    }

    pub fn definition(&self) -> &Rc<dyn DimensionDefinition> {
        &self.definition
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
    cube_name: String,
    name: String,
    granularity: Option<String>,
    sql: Option<Rc<dyn MemberSql>>,
    definition: Rc<dyn DimensionDefinition>,
    cube_evaluator: Rc<dyn CubeEvaluator>,
}

impl DimensionSymbolFactory {
    pub fn try_new(
        full_name: &String,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let mut iter = cube_evaluator
            .parse_path("dimensions".to_string(), full_name.clone())?
            .into_iter();
        let cube_name = iter.next().unwrap();
        let name = iter.next().unwrap();
        let granularity = iter.next();
        let definition = cube_evaluator.dimension_by_path(full_name.clone())?;
        Ok(Self {
            cube_name,
            name,
            granularity,
            sql: definition.sql()?,
            definition,
            cube_evaluator,
        })
    }
}

impl SymbolFactory for DimensionSymbolFactory {
    fn symbol_name() -> String {
        "dimension".to_string()
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        if let Some(member_sql) = &self.sql {
            Ok(member_sql.args_names().clone())
        } else {
            Ok(vec![])
        }
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        self.sql.clone()
    }

    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError> {
        let Self {
            cube_name,
            name,
            granularity,
            sql,
            definition,
            cube_evaluator,
        } = self;

        let dimension_type = definition.static_data().dimension_type.clone();

        let sql = if let Some(sql) = sql {
            Some(compiler.compile_sql_call(&cube_name, sql)?)
        } else {
            None
        };

        let is_sql_direct_ref = if let Some(sql) = &sql {
            sql.is_direct_reference(compiler.base_tools())?
        } else {
            false
        };

        let (latitude, longitude) = if dimension_type == "geo" {
            if let (Some(latitude_item), Some(longitude_item)) =
                (definition.latitude()?, definition.longitude()?)
            {
                let latitude = compiler.compile_sql_call(&cube_name, latitude_item.sql()?)?;
                let longitude = compiler.compile_sql_call(&cube_name, longitude_item.sql()?)?;
                (Some(latitude), Some(longitude))
            } else {
                return Err(CubeError::user(format!(
                    "Geo dimension '{}.{}'must have latitude and longitude",
                    cube_name, name
                )));
            }
        } else {
            (None, None)
        };

        let case = if let Some(native_case) = definition.case()? {
            Some(Case::try_new(&cube_name, native_case, compiler)?)
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
                        Some(compiler.compile_sql_call(&cube_name, sql)?)
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

        let cube = cube_evaluator.cube_from_path(cube_name.clone())?;
        let alias =
            PlanSqlTemplates::memeber_alias_name(cube.static_data().resolved_alias(), &name, &None);
        let is_view = cube.static_data().is_view.unwrap_or(false);
        let is_calendar = cube.static_data().is_calendar.unwrap_or(false);
        let mut is_self_time_shift_pk = false;

        // If the cube is a calendar, we need to find the primary key member
        // so that we can use it for time shifts processing.
        let time_shift_pk = if is_calendar {
            let pk_members = cube_evaluator
                .static_data()
                .primary_keys
                .get(&cube_name)
                .cloned()
                .unwrap_or_else(|| vec![]);

            if pk_members.iter().any(|pk| **pk == name) {
                is_self_time_shift_pk = true;
            }

            if pk_members.len() > 1 {
                return Err(CubeError::user(format!(
                    "Cube '{}' has multiple primary keys, but only one is allowed for calendar cubes",
                    cube_name
                )));
            }

            pk_members.first().map(|pk| format!("{}.{}", cube_name, pk))
        } else {
            None
        };

        let values = if definition.static_data().dimension_type == "switch" {
            definition.static_data().values.clone().unwrap_or_default()
        } else {
            vec![]
        };

        let is_multi_stage = definition.static_data().multi_stage.unwrap_or(false);

        //TODO move owned logic to rust
        let owned_by_cube = definition.static_data().owned_by_cube.unwrap_or(true);
        let owned_by_cube =
            owned_by_cube && !is_multi_stage && definition.static_data().dimension_type != "switch";
        let is_sub_query = definition.static_data().sub_query.unwrap_or(false);
        let is_reference = (is_view && is_sql_direct_ref)
            || (!owned_by_cube
                && !is_sub_query
                && is_sql_direct_ref
                && case.is_none()
                && latitude.is_none()
                && longitude.is_none()
                && !is_multi_stage);

        let propagate_filters_to_sub_query = definition
            .static_data()
            .propagate_filters_to_sub_query
            .unwrap_or(false);

        let cube_symbol = compiler
            .add_cube_table_evaluator(cube_name.clone())?
            .as_cube_table()?;

        let symbol = MemberSymbol::new_dimension(DimensionSymbol::new(
            cube_symbol,
            name.clone(),
            dimension_type,
            alias,
            sql,
            is_reference,
            is_view,
            latitude,
            longitude,
            values,
            case,
            definition,
            time_shift,
            time_shift_pk,
            is_self_time_shift_pk,
            owned_by_cube,
            is_multi_stage,
            is_sub_query,
            propagate_filters_to_sub_query,
        ));

        if let Some(granularity) = &granularity {
            if let Some(granularity_obj) = GranularityHelper::make_granularity_obj(
                cube_evaluator.clone(),
                compiler,
                &cube_name,
                &name,
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
