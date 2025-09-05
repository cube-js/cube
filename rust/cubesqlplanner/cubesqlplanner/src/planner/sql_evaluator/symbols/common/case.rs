use crate::plan::FilterItem;
use crate::{
    cube_bridge::{case_variant::CaseVariant, string_or_sql::StringOrSql},
    planner::sql_evaluator::{find_single_value_restriction, Compiler, MemberSymbol, SqlCall},
};
use cubenativeutils::CubeError;
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

    fn apply_static_filter(&self, _filters: &Vec<FilterItem>) -> Option<Rc<SqlCall>> {
        None
    }
}

#[derive(Clone)]
pub struct CaseSwitchWhenItem {
    pub value: String,
    pub sql: Rc<SqlCall>,
}

#[derive(Clone)]
pub struct CaseSwitchItem {
    pub sql: Rc<SqlCall>,
    pub symbol_reference: Option<Rc<MemberSymbol>>,
}

#[derive(Clone)]
pub struct CaseSwitchDefinition {
    pub switch: CaseSwitchItem,
    pub items: Vec<CaseSwitchWhenItem>,
    pub else_sql: Rc<SqlCall>,
}

impl CaseSwitchDefinition {
    fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        self.switch.sql.extract_symbol_deps(result);
        for itm in self.items.iter() {
            itm.sql.extract_symbol_deps(result);
        }
        self.else_sql.extract_symbol_deps(result);
    }
    fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        self.switch.sql.extract_symbol_deps_with_path(result);
        for itm in self.items.iter() {
            itm.sql.extract_symbol_deps_with_path(result);
        }
        self.else_sql.extract_symbol_deps_with_path(result);
    }

    fn apply_static_filter(&self, filters: &Vec<FilterItem>) -> Option<Rc<SqlCall>> {
        if let Some(switch_ref) = &self.switch.symbol_reference {
            if let Some(single_value) = find_single_value_restriction(filters, switch_ref) {
                if let Some(result) = self.items.iter().find(|itm| itm.value == single_value) {
                    Some(result.sql.clone())
                } else {
                    Some(self.else_sql.clone())
                }
            } else {
                None
            }
        } else {
            None
        }
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
                let switch_reference =
                    switch_sql.resolve_direct_reference(compiler.base_tools())?;
                let switch = CaseSwitchItem {
                    sql: switch_sql,
                    symbol_reference: switch_reference,
                };
                Case::CaseSwitch(CaseSwitchDefinition {
                    switch,
                    items,
                    else_sql,
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

    pub fn apply_static_filter(&self, filters: &Vec<FilterItem>) -> Option<Rc<SqlCall>> {
        match self {
            Case::Case(case) => case.apply_static_filter(filters),
            Case::CaseSwitch(case) => case.apply_static_filter(filters),
        }
    }
}
