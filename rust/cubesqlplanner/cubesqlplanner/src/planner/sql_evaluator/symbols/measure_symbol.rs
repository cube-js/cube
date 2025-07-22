use super::{MemberSymbol, SymbolFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::{MeasureDefinition, RollingWindow};
use crate::cube_bridge::member_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::find_owned_by_cube_child;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, Compiler, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::SqlInterval;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::cmp::{Eq, PartialEq};
use std::collections::HashMap;
use std::rc::Rc;

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

    pub fn direction(&self) -> &String {
        &self.direction
    }
}

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

#[derive(Clone, Debug)]
pub enum MeasureTimeShifts {
    Dimensions(Vec<DimensionTimeShift>),
    Common(SqlInterval),
    Named(String),
}

#[derive(Clone)]
pub struct MeasureSymbol {
    cube_name: String,
    name: String,
    alias: String,
    owned_by_cube: bool,
    measure_type: String,
    rolling_window: Option<RollingWindow>,
    is_multi_stage: bool,
    is_reference: bool,
    is_view: bool,
    measure_filters: Vec<Rc<SqlCall>>,
    measure_drill_filters: Vec<Rc<SqlCall>>,
    time_shift: Option<MeasureTimeShifts>,
    measure_order_by: Vec<MeasureOrderBy>,
    reduce_by: Option<Vec<Rc<MemberSymbol>>>,
    add_group_by: Option<Vec<Rc<MemberSymbol>>>,
    group_by: Option<Vec<Rc<MemberSymbol>>>,
    member_sql: Option<Rc<SqlCall>>,
    pk_sqls: Vec<Rc<SqlCall>>,
    is_splitted_source: bool,
}

impl MeasureSymbol {
    pub fn new(
        cube_name: String,
        name: String,
        alias: String,
        member_sql: Option<Rc<SqlCall>>,
        is_reference: bool,
        is_view: bool,
        pk_sqls: Vec<Rc<SqlCall>>,
        definition: Rc<dyn MeasureDefinition>,
        measure_filters: Vec<Rc<SqlCall>>,
        measure_drill_filters: Vec<Rc<SqlCall>>,
        time_shift: Option<MeasureTimeShifts>,
        measure_order_by: Vec<MeasureOrderBy>,
        reduce_by: Option<Vec<Rc<MemberSymbol>>>,
        add_group_by: Option<Vec<Rc<MemberSymbol>>>,
        group_by: Option<Vec<Rc<MemberSymbol>>>,
    ) -> Rc<Self> {
        let owned_by_cube = definition.static_data().owned_by_cube.unwrap_or(true);
        let measure_type = definition.static_data().measure_type.clone();
        let rolling_window = definition.static_data().rolling_window.clone();
        let is_multi_stage = definition.static_data().multi_stage.unwrap_or(false);
        Rc::new(Self {
            cube_name,
            name,
            alias,
            member_sql,
            is_reference,
            is_view,
            pk_sqls,
            owned_by_cube,
            measure_type,
            rolling_window,
            measure_filters,
            measure_drill_filters,
            measure_order_by,
            is_multi_stage,
            time_shift,
            is_splitted_source: false,
            reduce_by,
            add_group_by,
            group_by,
        })
    }

    pub fn new_unrolling(&self) -> Rc<Self> {
        if self.is_rolling_window() {
            let measure_type = if self.is_multi_stage {
                format!("number")
            } else {
                self.measure_type.clone()
            };
            Rc::new(Self {
                cube_name: self.cube_name.clone(),
                name: self.name.clone(),
                alias: self.alias.clone(),
                owned_by_cube: self.owned_by_cube,
                measure_type,
                rolling_window: None,
                is_multi_stage: false,
                is_reference: false,
                is_view: self.is_view,
                measure_filters: self.measure_filters.clone(),
                measure_drill_filters: self.measure_drill_filters.clone(),
                time_shift: self.time_shift.clone(),
                measure_order_by: self.measure_order_by.clone(),
                reduce_by: self.reduce_by.clone(),
                add_group_by: self.add_group_by.clone(),
                group_by: self.group_by.clone(),
                member_sql: self.member_sql.clone(),
                pk_sqls: self.pk_sqls.clone(),
                is_splitted_source: self.is_splitted_source,
            })
        } else {
            Rc::new(self.clone())
        }
    }

    pub fn new_patched(
        &self,
        new_measure_type: Option<String>,
        add_filters: Vec<Rc<SqlCall>>,
    ) -> Result<Rc<Self>, CubeError> {
        let result_measure_type = if let Some(new_measure_type) = new_measure_type {
            match self.measure_type.as_str() {
                "sum" | "avg" | "min" | "max" => match new_measure_type.as_str() {
                    "sum" | "avg" | "min" | "max" | "count_distinct" | "count_distinct_approx" => {}
                    _ => {
                        return Err(CubeError::user(format!(
                            "Unsupported measure type replacement for {}: {} => {}",
                            self.name, self.measure_type, new_measure_type
                        )))
                    }
                },
                "count_distinct" | "count_distinct_approx" => match new_measure_type.as_str() {
                    "count_distinct" | "count_distinct_approx" => {}
                    _ => {
                        return Err(CubeError::user(format!(
                            "Unsupported measure type replacement for {}: {} => {}",
                            self.name, self.measure_type, new_measure_type
                        )))
                    }
                },

                _ => {
                    return Err(CubeError::user(format!(
                        "Unsupported measure type replacement for {}: {} => {}",
                        self.name, self.measure_type, new_measure_type
                    )))
                }
            }
            new_measure_type
        } else {
            self.measure_type.clone()
        };

        let mut measure_filters = self.measure_filters.clone();
        if !add_filters.is_empty() {
            match result_measure_type.as_str() {
                "sum"
                | "avg"
                | "min"
                | "max"
                | "count"
                | "count_distinct"
                | "count_distinct_approx" => {}
                _ => {
                    return Err(CubeError::user(format!(
                        "Unsupported additional filters for measure {} type {}",
                        self.name, result_measure_type
                    )))
                }
            }
            measure_filters.extend(add_filters.into_iter());
        }
        Ok(Rc::new(Self {
            cube_name: self.cube_name.clone(),
            name: self.name.clone(),
            alias: self.alias.clone(),
            owned_by_cube: self.owned_by_cube,
            measure_type: result_measure_type,
            rolling_window: self.rolling_window.clone(),
            is_multi_stage: self.is_multi_stage,
            is_reference: self.is_reference,
            is_view: self.is_view,
            measure_filters,
            measure_drill_filters: self.measure_drill_filters.clone(),
            time_shift: self.time_shift.clone(),
            measure_order_by: self.measure_order_by.clone(),
            reduce_by: self.reduce_by.clone(),
            add_group_by: self.add_group_by.clone(),
            group_by: self.group_by.clone(),
            member_sql: self.member_sql.clone(),
            pk_sqls: self.pk_sqls.clone(),
            is_splitted_source: self.is_splitted_source,
        }))
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.cube_name, self.name)
    }

    pub fn alias(&self) -> String {
        self.alias.clone()
    }

    pub fn is_splitted_source(&self) -> bool {
        self.is_splitted_source
    }

    pub fn pk_sqls(&self) -> &Vec<Rc<SqlCall>> {
        &self.pk_sqls
    }

    pub fn time_shift(&self) -> &Option<MeasureTimeShifts> {
        &self.time_shift
    }

    pub fn is_calculated(&self) -> bool {
        Self::is_calculated_type(&self.measure_type)
    }

    pub fn is_calculated_type(measure_type: &str) -> bool {
        match measure_type {
            "number" | "string" | "time" | "boolean" => true,
            _ => false,
        }
    }

    pub fn is_addictive(&self) -> bool {
        if self.is_multi_stage() {
            false
        } else {
            match self.measure_type().as_str() {
                "sum" | "count" | "countDistinctApprox" | "min" | "max" => true,
                _ => false,
            }
        }
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(member_sql) = &self.member_sql {
            let sql = member_sql.eval(visitor, node_processor, query_tools, templates)?;
            Ok(sql)
        } else {
            Err(CubeError::internal(format!(
                "Measure {} hasn't sql evaluator",
                self.full_name()
            )))
        }
    }

    pub fn has_sql(&self) -> bool {
        self.member_sql.is_some()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps(&mut deps);
        }
        for pk in self.pk_sqls.iter() {
            pk.extract_symbol_deps(&mut deps);
        }
        for filter in self.measure_filters.iter() {
            filter.extract_symbol_deps(&mut deps);
        }
        for filter in self.measure_drill_filters.iter() {
            filter.extract_symbol_deps(&mut deps);
        }
        for order in self.measure_order_by.iter() {
            order.sql_call().extract_symbol_deps(&mut deps);
        }
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_symbol_deps_with_path(&mut deps);
        }
        for pk in self.pk_sqls.iter() {
            pk.extract_symbol_deps_with_path(&mut deps);
        }
        for filter in self.measure_filters.iter() {
            filter.extract_symbol_deps_with_path(&mut deps);
        }
        for filter in self.measure_drill_filters.iter() {
            filter.extract_symbol_deps_with_path(&mut deps);
        }
        for order in self.measure_order_by.iter() {
            order.sql_call().extract_symbol_deps_with_path(&mut deps);
        }
        deps
    }

    pub fn get_dependent_cubes(&self) -> Vec<String> {
        let mut cubes = vec![];
        if let Some(member_sql) = &self.member_sql {
            member_sql.extract_cube_deps(&mut cubes);
        }
        for pk in self.pk_sqls.iter() {
            pk.extract_cube_deps(&mut cubes);
        }
        for filter in self.measure_filters.iter() {
            filter.extract_cube_deps(&mut cubes);
        }
        for filter in self.measure_drill_filters.iter() {
            filter.extract_cube_deps(&mut cubes);
        }
        for order in self.measure_order_by.iter() {
            order.sql_call().extract_cube_deps(&mut cubes);
        }
        cubes
    }

    pub fn can_used_as_addictive_in_multplied(&self) -> bool {
        if &self.measure_type == "countDistinct" || &self.measure_type == "countDistinctApprox" {
            true
        } else if &self.measure_type == "count" && self.member_sql.is_none() {
            true
        } else {
            false
        }
    }

    pub fn owned_by_cube(&self) -> bool {
        self.owned_by_cube
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

    pub fn measure_type(&self) -> &String {
        &self.measure_type
    }

    pub fn rolling_window(&self) -> &Option<RollingWindow> {
        &self.rolling_window
    }

    pub fn is_rolling_window(&self) -> bool {
        self.rolling_window().is_some()
    }

    pub fn is_running_total(&self) -> bool {
        self.measure_type() == "runningTotal"
    }

    pub fn is_cumulative(&self) -> bool {
        self.is_rolling_window() || self.is_running_total()
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

    pub fn reduce_by(&self) -> &Option<Vec<Rc<MemberSymbol>>> {
        &self.reduce_by
    }

    pub fn add_group_by(&self) -> &Option<Vec<Rc<MemberSymbol>>> {
        &self.add_group_by
    }

    pub fn group_by(&self) -> &Option<Vec<Rc<MemberSymbol>>> {
        &self.group_by
    }

    pub fn is_multi_stage(&self) -> bool {
        self.is_multi_stage
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }
    pub fn name(&self) -> &String {
        &self.name
    }
}

pub struct MeasureSymbolFactory {
    cube_name: String,
    name: String,
    sql: Option<Rc<dyn MemberSql>>,
    definition: Rc<dyn MeasureDefinition>,
    cube_evaluator: Rc<dyn CubeEvaluator>,
}

impl MeasureSymbolFactory {
    pub fn try_new(
        full_name: &String,
        cube_evaluator: Rc<dyn CubeEvaluator>,
    ) -> Result<Self, CubeError> {
        let mut iter = cube_evaluator
            .parse_path("measures".to_string(), full_name.clone())?
            .into_iter();
        let cube_name = iter.next().unwrap();
        let name = iter.next().unwrap();
        let definition = cube_evaluator.measure_by_path(full_name.clone())?;
        let sql = definition.sql()?;
        Ok(Self {
            cube_name,
            name,
            sql,
            definition,
            cube_evaluator,
        })
    }
}

impl SymbolFactory for MeasureSymbolFactory {
    fn symbol_name() -> String {
        "measure".to_string()
    }
    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn member_sql(&self) -> Option<Rc<dyn MemberSql>> {
        self.sql.clone()
    }

    fn deps_names(&self) -> Result<Vec<String>, CubeError> {
        if let Some(member_sql) = &self.sql {
            Ok(member_sql.args_names().clone())
        } else {
            Ok(vec![])
        }
    }

    fn build(self, compiler: &mut Compiler) -> Result<Rc<MemberSymbol>, CubeError> {
        let Self {
            cube_name,
            name,
            sql,
            definition,
            cube_evaluator,
        } = self;
        let pk_sqls = if sql.is_none() {
            cube_evaluator
                .static_data()
                .primary_keys
                .get(&cube_name)
                .cloned()
                .unwrap_or_else(|| vec![])
                .into_iter()
                .map(|primary_key| -> Result<_, CubeError> {
                    let key_dimension_name = format!("{}.{}", cube_name, primary_key);
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
                    compiler.compile_sql_call(&cube_name, key_dimension_sql)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![]
        };

        let mut measure_filters = vec![];
        if let Some(filters) = definition.filters()? {
            for filter in filters.iter() {
                let node = compiler.compile_sql_call(&cube_name, filter.sql()?)?;
                measure_filters.push(node);
            }
        }

        let mut measure_drill_filters = vec![];
        if let Some(filters) = definition.drill_filters()? {
            for filter in filters.iter() {
                let node = compiler.compile_sql_call(&cube_name, filter.sql()?)?;
                measure_drill_filters.push(node);
            }
        }

        let mut measure_order_by = vec![];
        if let Some(group_by) = definition.order_by()? {
            for item in group_by.iter() {
                let node = compiler.compile_sql_call(&cube_name, item.sql()?)?;
                measure_order_by.push(MeasureOrderBy::new(node, item.dir()?));
            }
        }
        let sql = if let Some(sql) = sql {
            Some(compiler.compile_sql_call(&cube_name, sql)?)
        } else {
            None
        };

        let is_sql_is_direct_ref = if let Some(sql) = &sql {
            sql.is_direct_reference(compiler.base_tools())?
        } else {
            false
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
                            dimension_name,
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

        let reduce_by = if let Some(reduce_by) = &definition.static_data().reduce_by_references {
            let symbols = reduce_by
                .iter()
                .map(|reduce_by| compiler.add_dimension_evaluator(reduce_by.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            Some(symbols)
        } else {
            None
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

        let group_by = if let Some(group_by) = &definition.static_data().group_by_references {
            let symbols = group_by
                .iter()
                .map(|group_by| compiler.add_dimension_evaluator(group_by.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            Some(symbols)
        } else {
            None
        };

        let is_calculated =
            MeasureSymbol::is_calculated_type(&definition.static_data().measure_type)
                && !definition.static_data().multi_stage.unwrap_or(false);
        let owned_by_cube = definition.static_data().owned_by_cube.unwrap_or(true);
        let is_multi_stage = definition.static_data().multi_stage.unwrap_or(false);
        let cube = cube_evaluator.cube_from_path(cube_name.clone())?;
        let alias =
            PlanSqlTemplates::memeber_alias_name(cube.static_data().resolved_alias(), &name, &None);

        let is_view = cube.static_data().is_view.unwrap_or(false);

        let is_reference = is_view
            || (!owned_by_cube
                && is_sql_is_direct_ref
                && is_calculated
                && !is_multi_stage
                && measure_filters.is_empty()
                && measure_drill_filters.is_empty()
                && time_shifts.is_none()
                && measure_order_by.is_empty()
                && reduce_by.is_none()
                && add_group_by.is_none()
                && group_by.is_none());

        Ok(MemberSymbol::new_measure(MeasureSymbol::new(
            cube_name,
            name,
            alias,
            sql,
            is_reference,
            is_view,
            pk_sqls,
            definition,
            measure_filters,
            measure_drill_filters,
            time_shifts,
            measure_order_by,
            reduce_by,
            add_group_by,
            group_by,
        )))
    }
}
