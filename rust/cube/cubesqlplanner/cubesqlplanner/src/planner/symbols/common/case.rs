use super::super::deps::{symbol_deps, DepVisitor, DepVisitorMut, SymbolDeps};
use crate::planner::filter::FilterItem;
use crate::{
    cube_bridge::{
        case_switch_definition::CaseSwitchDefinition as NativeCaseSwitchDefinition,
        case_variant::CaseVariant, string_or_sql::StringOrSql,
    },
    planner::{symbols::transforms::find_value_restriction, Compiler, MemberSymbol, SqlCall},
};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::ops::ControlFlow;
use std::rc::Rc;

#[derive(Clone)]
pub enum CaseLabel {
    String(String),
    Sql(Rc<SqlCall>),
}

impl SymbolDeps for CaseLabel {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        match self {
            Self::String(_) => ControlFlow::Continue(()),
            Self::Sql(sql) => sql.visit_deps(visitor),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Self::String(_) => Ok(()),
            Self::Sql(sql) => sql.visit_deps_mut(visitor),
        }
    }
}

#[derive(Clone)]
pub struct CaseWhenItem {
    pub sql: Rc<SqlCall>,
    pub label: CaseLabel,
}

symbol_deps! {
    CaseWhenItem {
        sql: dep,
        label: dep,
    }
}

#[derive(Clone)]
pub struct CaseDefinition {
    pub items: Vec<CaseWhenItem>,
    pub else_label: CaseLabel,
}

symbol_deps! {
    CaseDefinition {
        items: dep,
        else_label: dep,
    }
}

impl CaseDefinition {
    fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        Box::new(self.items.iter().map(|item| &item.sql))
    }

    fn is_owned_by_cube(&self) -> bool {
        let mut owned = false;
        for itm in self.items.iter() {
            owned |= itm.sql.is_owned_by_cube();
        }
        owned
    }
}

#[derive(Clone)]
pub struct CaseSwitchWhenItem {
    pub value: String,
    pub sql: Rc<SqlCall>,
}

symbol_deps! {
    CaseSwitchWhenItem {
        value: skip,
        sql: dep,
    }
}

#[derive(Clone)]
pub enum CaseSwitchItem {
    Sql(Rc<SqlCall>),
    Member(Rc<MemberSymbol>),
}

impl SymbolDeps for CaseSwitchItem {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        match self {
            Self::Sql(sql_call) => sql_call.visit_deps(visitor),
            Self::Member(member) => visitor.symbol(member),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Self::Sql(sql_call) => sql_call.visit_deps_mut(visitor),
            Self::Member(member) => visitor.symbol(member),
        }
    }
}

impl CaseSwitchItem {
    fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match self {
            CaseSwitchItem::Sql(sql_call) => Box::new(std::iter::once(sql_call)),
            CaseSwitchItem::Member(_) => Box::new(std::iter::empty()),
        }
    }
}

#[derive(Clone)]
pub struct CaseSwitchDefinition {
    pub switch: CaseSwitchItem,
    pub items: Vec<CaseSwitchWhenItem>,
    pub else_sql: Option<Rc<SqlCall>>,
}

symbol_deps! {
    CaseSwitchDefinition {
        switch: dep,
        items: dep,
        else_sql: dep,
    }
}

impl CaseSwitchDefinition {
    pub fn try_new(
        cube_name: &String,
        definition: Rc<dyn NativeCaseSwitchDefinition>,
        compiler: &mut Compiler,
    ) -> Result<Self, CubeError> {
        let switch_sql = compiler.compile_sql_call(&cube_name, definition.switch()?)?;
        let switch = if let Some(member) = switch_sql.resolve_direct_reference() {
            CaseSwitchItem::Member(member)
        } else {
            CaseSwitchItem::Sql(switch_sql)
        };

        let items = definition
            .when()?
            .iter()
            .map(|item| -> Result<_, CubeError> {
                let sql = compiler.compile_sql_call(&cube_name, item.sql()?)?;
                let value = item.static_data().value.clone();
                Ok(CaseSwitchWhenItem { sql, value })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let else_sql = compiler.compile_sql_call(&cube_name, definition.else_sql()?.sql()?)?;
        let mut res = CaseSwitchDefinition {
            switch,
            items,
            else_sql: Some(else_sql),
        };
        res.remove_unreachable_branches();
        Ok(res)
    }

    pub fn is_single_value(&self) -> bool {
        let mut values_len = self.items.len();
        if self.else_sql.is_some() {
            values_len += 1;
        }
        values_len == 1
    }

    fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        let result = self
            .switch
            .iter_sql_calls()
            .chain(self.items.iter().map(|item| &item.sql));
        if let Some(else_sql) = &self.else_sql {
            Box::new(result.chain(std::iter::once(else_sql)))
        } else {
            Box::new(result)
        }
    }
    fn is_owned_by_cube(&self) -> bool {
        let mut owned = false;
        if let CaseSwitchItem::Sql(sql) = &self.switch {
            owned |= sql.is_owned_by_cube();
        }
        for itm in self.items.iter() {
            owned |= itm.sql.is_owned_by_cube();
        }
        if let Some(sql) = &self.else_sql {
            owned |= sql.is_owned_by_cube();
        }
        owned
    }

    fn get_switch_values(&self) -> Option<Vec<String>> {
        if let CaseSwitchItem::Member(member) = &self.switch {
            if let Ok(switch_dim) = member.as_dimension() {
                if switch_dim.is_switch() {
                    return Some(switch_dim.values().to_vec());
                }
            }
        }
        None
    }

    pub fn get_else_values(&self) -> Option<Vec<String>> {
        if let Some(mut switch_values) = self.get_switch_values() {
            switch_values.retain(|v| !self.items.iter().any(|itm| v == &itm.value));
            Some(switch_values)
        } else {
            None
        }
    }

    fn remove_unreachable_branches(&mut self) {
        if let Some(switch_values) = self.get_switch_values() {
            self.items.retain(|itm| switch_values.contains(&itm.value));
        }
        if let Some(else_values) = self.get_else_values() {
            if else_values.is_empty() {
                self.else_sql = None;
            }
        }
    }

    fn apply_static_filter(&self, filters: &Vec<FilterItem>) -> Option<CaseSwitchDefinition> {
        if let CaseSwitchItem::Member(switch_member) = &self.switch {
            if let Some(values) = find_value_restriction(filters, switch_member) {
                let values = if let Some(values_from_switch) = self.get_switch_values() {
                    values_from_switch
                        .into_iter()
                        .filter(|v| values.contains(v))
                        .collect_vec()
                } else {
                    values
                };
                if !values.is_empty() {
                    let items = self
                        .items
                        .iter()
                        .filter(|itm| values.contains(&itm.value))
                        .cloned()
                        .collect_vec();
                    let all_values_in_case = self.items.iter().map(|itm| &itm.value).collect_vec();
                    let else_sql = if values.iter().all(|v| all_values_in_case.contains(&v)) {
                        None
                    } else {
                        self.else_sql.clone()
                    };
                    return Some(Self {
                        switch: self.switch.clone(),
                        items,
                        else_sql,
                    });
                }
            }
        }
        None
    }
}

/// Body of a case-defined member, mapped from the `case` field of
/// the data-model definition.
///
/// - `Case` — classic `CASE WHEN condition THEN label ELSE label END`.
/// - `CaseSwitch` — switch-style `CASE switch WHEN value THEN sql
///   ELSE sql END`. `switch` may resolve to a direct reference to
///   another dimension (typically a `type: switch` dimension whose
///   `values` drive the branches).
#[derive(Clone)]
pub enum Case {
    Case(CaseDefinition),
    CaseSwitch(CaseSwitchDefinition),
}

impl Case {
    pub fn try_new(
        cube_name: &String,
        definition: CaseVariant,
        compiler: &mut Compiler,
    ) -> Result<Self, CubeError> {
        let res = match definition {
            CaseVariant::Case(case_definition) => {
                let items = case_definition
                    .when()?
                    .iter()
                    .map(|item| -> Result<_, CubeError> {
                        let sql = compiler.compile_sql_call(&cube_name, item.sql()?)?;
                        let label = match item.label()? {
                            StringOrSql::String(s) => CaseLabel::String(s.clone()),
                            StringOrSql::MemberSql(sql_struct) => {
                                let sql =
                                    compiler.compile_sql_call(&cube_name, sql_struct.sql()?)?;
                                CaseLabel::Sql(sql)
                            }
                        };
                        Ok(CaseWhenItem { sql, label })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let else_label = match case_definition.else_label()?.label()? {
                    StringOrSql::String(s) => CaseLabel::String(s.clone()),
                    StringOrSql::MemberSql(sql_struct) => {
                        let sql = compiler.compile_sql_call(&cube_name, sql_struct.sql()?)?;
                        CaseLabel::Sql(sql)
                    }
                };
                Case::Case(CaseDefinition { items, else_label })
            }
            CaseVariant::CaseSwitch(case_definition) => Case::CaseSwitch(
                CaseSwitchDefinition::try_new(cube_name, case_definition.clone(), compiler)?,
            ),
        };
        Ok(res)
    }

    pub fn case_switch_dimension(&self) -> Option<Rc<MemberSymbol>> {
        if let Case::CaseSwitch(case) = &self {
            if let CaseSwitchItem::Member(member) = &case.switch {
                return Some(member.clone());
            }
        }
        None
    }

    pub fn apply_static_filter(&self, filters: &Vec<FilterItem>) -> Option<Self> {
        match self {
            Case::Case(_) => None,
            Case::CaseSwitch(case) => case
                .apply_static_filter(filters)
                .map(|r| Case::CaseSwitch(r)),
        }
    }
    pub fn is_single_value(&self) -> bool {
        match self {
            Case::Case(_) => false,
            Case::CaseSwitch(case) => case.is_single_value(),
        }
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match self {
            Case::Case(case) => Box::new(case.iter_sql_calls()),
            Case::CaseSwitch(case) => Box::new(case.iter_sql_calls()),
        }
    }
    pub fn is_owned_by_cube(&self) -> bool {
        match self {
            Case::Case(case) => case.is_owned_by_cube(),
            Case::CaseSwitch(case) => case.is_owned_by_cube(),
        }
    }
}

impl SymbolDeps for Case {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        match self {
            Case::Case(def) => def.visit_deps(visitor),
            Case::CaseSwitch(def) => def.visit_deps(visitor),
        }
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        match self {
            Case::Case(def) => def.visit_deps_mut(visitor),
            Case::CaseSwitch(def) => def.visit_deps_mut(visitor),
        }
    }
}

impl crate::utils::debug::DebugSql for Case {
    fn debug_sql(&self, expand_deps: bool) -> String {
        match self {
            Case::Case(case_def) => {
                let mut result = "CASE\n".to_string();

                for when in &case_def.items {
                    let condition = when.sql.debug_sql(expand_deps);
                    let then = match &when.label {
                        CaseLabel::String(s) => format!("'{}'", s),
                        CaseLabel::Sql(sql) => sql.debug_sql(expand_deps),
                    };
                    result.push_str(&format!("  WHEN {} THEN {}\n", condition, then));
                }

                let else_sql = match &case_def.else_label {
                    CaseLabel::String(s) => format!("'{}'", s),
                    CaseLabel::Sql(sql) => sql.debug_sql(expand_deps),
                };
                result.push_str(&format!("  ELSE {}\n", else_sql));

                result.push_str("END");
                result
            }
            Case::CaseSwitch(case_switch) => {
                let switch_sql = match &case_switch.switch {
                    CaseSwitchItem::Sql(sql) => sql.debug_sql(expand_deps),
                    CaseSwitchItem::Member(member) => {
                        if expand_deps {
                            member.debug_sql(true)
                        } else {
                            format!("{{{}}}", member.full_name())
                        }
                    }
                };

                let mut result = format!("CASE {}\n", switch_sql);

                for when in &case_switch.items {
                    let then = when.sql.debug_sql(expand_deps);
                    result.push_str(&format!("  WHEN '{}' THEN {}\n", when.value, then));
                }

                if let Some(else_sql) = &case_switch.else_sql {
                    let else_sql_str = else_sql.debug_sql(expand_deps);
                    result.push_str(&format!("  ELSE {}\n", else_sql_str));
                }

                result.push_str("END");
                result
            }
        }
    }
}
