use crate::{
    cube_bridge::{case_variant::CaseVariant, string_or_sql::StringOrSql},
    planner::sql_evaluator::{Compiler, MemberSymbol, SqlCall},
};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub enum CaseLabel {
    String(String),
    Sql(Rc<SqlCall>),
}

pub struct CaseWhenItem {
    pub sql: Rc<SqlCall>,
    pub label: CaseLabel,
}

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
}

pub struct CaseSwitchWhenItem {
    pub value: String,
    pub sql: Rc<SqlCall>,
}

pub enum CaseSwitchItem {
    Symbol(Rc<MemberSymbol>),
    Sql(Rc<SqlCall>),
}

pub struct CaseSwitchDefinition {
    pub switch: CaseSwitchItem,
    pub items: Vec<CaseSwitchWhenItem>,
    pub else_sql: Rc<SqlCall>,
}

impl CaseSwitchDefinition {
    fn extract_symbol_deps(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        match &self.switch {
            CaseSwitchItem::Symbol(member_symbol) => result.push(member_symbol.clone()),
            CaseSwitchItem::Sql(sql) => sql.extract_symbol_deps(result),
        }
        for itm in self.items.iter() {
            itm.sql.extract_symbol_deps(result);
        }
        self.else_sql.extract_symbol_deps(result);
    }
    fn extract_symbol_deps_with_path(&self, result: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        match &self.switch {
            CaseSwitchItem::Symbol(member_symbol) => result.push((member_symbol.clone(), vec![])),
            CaseSwitchItem::Sql(sql) => sql.extract_symbol_deps_with_path(result),
        }
        for itm in self.items.iter() {
            itm.sql.extract_symbol_deps_with_path(result);
        }
        self.else_sql.extract_symbol_deps_with_path(result);
    }
}

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
                let switch = if let Some(symbol) =
                    switch_sql.resolve_direct_reference(compiler.base_tools())?
                {
                    CaseSwitchItem::Symbol(symbol)
                } else {
                    CaseSwitchItem::Sql(switch_sql)
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
}
