use crate::plan::FilterItem;
use crate::{
    cube_bridge::{case_variant::CaseVariant, string_or_sql::StringOrSql},
    planner::sql_evaluator::{find_value_restriction, Compiler, MemberSymbol, SqlCall},
};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Clone)]
pub enum CaseLabel {
    String(String),
    Sql(Rc<SqlCall>),
}

#[derive(Clone)]
pub struct CaseWhenItem {
    pub sql: Rc<SqlCall>,
    pub label: CaseLabel,
}

#[derive(Clone)]
pub struct CaseDefinition {
    pub items: Vec<CaseWhenItem>,
    pub else_label: CaseLabel,
}

impl CaseDefinition {
    fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        for itm in self.items.iter() {
            itm.sql.extract_symbol_deps(result);
            if let CaseLabel::Sql(sql) = &itm.label {
                sql.extract_symbol_deps(result);
            }
        }
        if let CaseLabel::Sql(sql) = &self.else_label {
            sql.extract_symbol_deps(result);
        }
    }
    fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        for itm in self.items.iter() {
            itm.sql.extract_symbol_deps_with_path(result);
            if let CaseLabel::Sql(sql) = &itm.label {
                sql.extract_symbol_deps_with_path(result);
            }
        }
        if let CaseLabel::Sql(sql) = &self.else_label {
            sql.extract_symbol_deps_with_path(result);
        }
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let items = self
            .items
            .iter()
            .map(|itm| -> Result<_, CubeError> {
                let label = match &itm.label {
                    CaseLabel::String(_) => itm.label.clone(),
                    CaseLabel::Sql(sql_call) => CaseLabel::Sql(sql_call.apply_recursive(f)?),
                };
                Ok(CaseWhenItem {
                    sql: itm.sql.apply_recursive(f)?,
                    label,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let else_label = match &self.else_label {
            CaseLabel::String(_) => self.else_label.clone(),
            CaseLabel::Sql(sql_call) => CaseLabel::Sql(sql_call.apply_recursive(f)?),
        };
        let res = CaseDefinition { items, else_label };
        Ok(res)
    }
}

#[derive(Clone)]
pub struct CaseSwitchWhenItem {
    pub value: String,
    pub sql: Rc<SqlCall>,
}

#[derive(Clone)]
pub enum CaseSwitchItem {
    Sql(Rc<SqlCall>),
    Member(Rc<MemberSymbol>),
}

impl CaseSwitchItem {
    fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        match self {
            CaseSwitchItem::Sql(sql_call) => sql_call.extract_symbol_deps(result),
            CaseSwitchItem::Member(member_symbol) => result.push(member_symbol.clone()),
        }
    }

    fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        match self {
            CaseSwitchItem::Sql(sql_call) => sql_call.extract_symbol_deps_with_path(result),
            CaseSwitchItem::Member(member_symbol) => result.push((member_symbol.clone(), vec![])),
        }
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let res = match self {
            CaseSwitchItem::Sql(sql_call) => CaseSwitchItem::Sql(sql_call.apply_recursive(f)?),
            CaseSwitchItem::Member(member) => CaseSwitchItem::Member(member.apply_recursive(f)?),
        };
        Ok(res)
    }
}

#[derive(Clone)]
pub struct CaseSwitchDefinition {
    pub switch: CaseSwitchItem,
    pub items: Vec<CaseSwitchWhenItem>,
    pub else_sql: Option<Rc<SqlCall>>,
}

impl CaseSwitchDefinition {
    fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        self.switch.extract_symbol_deps(result);
        for itm in self.items.iter() {
            itm.sql.extract_symbol_deps(result);
        }
        if let Some(else_sql) = &self.else_sql {
            else_sql.extract_symbol_deps(result);
        }
    }
    fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        self.switch.extract_symbol_deps_with_path(result);
        for itm in self.items.iter() {
            itm.sql.extract_symbol_deps_with_path(result);
        }
        if let Some(else_sql) = &self.else_sql {
            else_sql.extract_symbol_deps_with_path(result);
        }
    }

    fn get_switch_values(&self) -> Option<Vec<String>> {
        if let CaseSwitchItem::Member(member) = &self.switch {
            if let Ok(switch_dim) = member.as_dimension() {
                if switch_dim.dimension_type() == "switch" {
                    return Some(switch_dim.values().clone());
                }
            }
        }
        None
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
    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let switch = self.switch.apply_to_deps(f)?;
        let items = self
            .items
            .iter()
            .map(|itm| -> Result<_, CubeError> {
                Ok(CaseSwitchWhenItem {
                    sql: itm.sql.apply_recursive(f)?,
                    value: itm.value.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let else_sql = if let Some(else_sql) = &self.else_sql {
            Some(else_sql.apply_recursive(f)?)
        } else {
            None
        };
        let res = CaseSwitchDefinition {
            switch,
            items,
            else_sql,
        };
        Ok(res)
    }
}

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
            CaseVariant::CaseSwitch(case_definition) => {
                let items = case_definition
                    .when()?
                    .iter()
                    .map(|item| -> Result<_, CubeError> {
                        let sql = compiler.compile_sql_call(&cube_name, item.sql()?)?;
                        let value = item.static_data().value.clone();
                        Ok(CaseSwitchWhenItem { sql, value })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let else_sql =
                    compiler.compile_sql_call(&cube_name, case_definition.else_sql()?.sql()?)?;
                let switch_sql =
                    compiler.compile_sql_call(&cube_name, case_definition.switch()?)?;
                let switch = if let Some(member) =
                    switch_sql.resolve_direct_reference(compiler.base_tools())?
                {
                    CaseSwitchItem::Member(member)
                } else {
                    CaseSwitchItem::Sql(switch_sql)
                };
                Case::CaseSwitch(CaseSwitchDefinition {
                    switch,
                    items,
                    else_sql: Some(else_sql),
                })
            }
        };
        Ok(res)
    }

    pub fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        match self {
            Case::Case(def) => def.extract_symbol_deps(result),
            Case::CaseSwitch(def) => def.extract_symbol_deps(result),
        }
    }
    pub fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        match self {
            Case::Case(def) => def.extract_symbol_deps_with_path(result),
            Case::CaseSwitch(def) => def.extract_symbol_deps_with_path(result),
        }
    }

    pub fn apply_static_filter(&self, filters: &Vec<FilterItem>) -> Option<Self> {
        match self {
            Case::Case(case) => None,
            Case::CaseSwitch(case) => case
                .apply_static_filter(filters)
                .map(|r| Case::CaseSwitch(r)),
        }
    }
    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let res = match self {
            Case::Case(case) => Case::Case(case.apply_to_deps(f)?),
            Case::CaseSwitch(case) => Case::CaseSwitch(case.apply_to_deps(f)?),
        };
        Ok(res)
    }
}
