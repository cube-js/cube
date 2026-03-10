use super::super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{sql_nodes::SqlNode, CubeRef, SqlCall, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum CountSql {
    Auto(Vec<Rc<SqlCall>>),
    Explicit(Rc<SqlCall>),
}

#[derive(Clone)]
pub struct CountMeasure {
    sql: CountSql,
}

impl CountMeasure {
    pub fn new(sql: CountSql) -> Self {
        Self { sql }
    }

    pub fn sql(&self) -> &CountSql {
        &self.sql
    }

    pub fn evaluate_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match &self.sql {
            CountSql::Explicit(sql) => sql.eval(visitor, node_processor, query_tools, templates),
            CountSql::Auto(pk_sqls) => {
                if pk_sqls.len() > 1 {
                    let pk_strings = pk_sqls
                        .iter()
                        .map(|pk| -> Result<_, CubeError> {
                            let res = pk.eval(
                                visitor,
                                node_processor.clone(),
                                query_tools.clone(),
                                templates,
                            )?;
                            templates.cast_to_string(&res)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    templates.concat_strings(&pk_strings)
                } else if pk_sqls.len() == 1 {
                    let pk_sql = pk_sqls.first().unwrap();
                    pk_sql.eval(visitor, node_processor, query_tools, templates)
                } else {
                    Ok("*".to_string())
                }
            }
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        let mut deps = vec![];
        match &self.sql {
            CountSql::Explicit(sql) => sql.extract_symbol_deps(&mut deps),
            CountSql::Auto(pk_sqls) => {
                for pk in pk_sqls {
                    pk.extract_symbol_deps(&mut deps);
                }
            }
        }
        deps
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        let mut deps = vec![];
        match &self.sql {
            CountSql::Explicit(sql) => sql.extract_symbol_deps_with_path(&mut deps),
            CountSql::Auto(pk_sqls) => {
                for pk in pk_sqls {
                    pk.extract_symbol_deps_with_path(&mut deps);
                }
            }
        }
        deps
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let sql = match &self.sql {
            CountSql::Explicit(sql) => CountSql::Explicit(sql.apply_recursive(f)?),
            CountSql::Auto(pk_sqls) => {
                let new_pks = pk_sqls
                    .iter()
                    .map(|pk| pk.apply_recursive(f))
                    .collect::<Result<Vec<_>, _>>()?;
                CountSql::Auto(new_pks)
            }
        };
        Ok(Self { sql })
    }

    pub fn iter_sql_calls(&self) -> Box<dyn Iterator<Item = &Rc<SqlCall>> + '_> {
        match &self.sql {
            CountSql::Explicit(sql) => Box::new(std::iter::once(sql)),
            CountSql::Auto(pk_sqls) => Box::new(pk_sqls.iter()),
        }
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        let mut refs = vec![];
        match &self.sql {
            CountSql::Explicit(sql) => sql.extract_cube_refs(&mut refs),
            CountSql::Auto(pk_sqls) => {
                for pk in pk_sqls {
                    pk.extract_cube_refs(&mut refs);
                }
            }
        }
        refs
    }

    pub fn is_owned_by_cube(&self) -> bool {
        matches!(self.sql, CountSql::Auto(_))
    }
}
