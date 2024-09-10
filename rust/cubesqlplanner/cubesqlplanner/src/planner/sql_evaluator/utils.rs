use super::dependecy::Dependency;
use super::{MemberEvaluator, MemberEvaluatorFactory};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg, MemberSqlStruct};
use crate::planner::query_tools::QueryTools;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub fn evaluate_sql(
    tools: Rc<QueryTools>,
    sql: Rc<dyn MemberSql>,
    deps: &Vec<Dependency>,
) -> Result<String, CubeError> {
    let args = deps
        .iter()
        .map(|dep| -> Result<MemberSqlArg, CubeError> {
            match dep {
                Dependency::SingleDependency(dep) => {
                    Ok(MemberSqlArg::String(dep.evaluate(tools.clone())?))
                }
                Dependency::StructDependency(dep) => {
                    let mut res = MemberSqlStruct::default();
                    if let Some(sql_fn) = &dep.sql_fn {
                        res.sql_fn = Some(sql_fn.evaluate(tools.clone())?);
                    }
                    if let Some(to_string_fn) = &dep.to_string_fn {
                        res.to_string_fn = Some(to_string_fn.evaluate(tools.clone())?);
                    }
                    for (k, v) in dep.properties.iter() {
                        match v {
                            Dependency::SingleDependency(dep) => {
                                res.properties
                                    .insert(k.clone(), dep.evaluate(tools.clone())?);
                            }
                            Dependency::StructDependency(_) => unimplemented!(),
                        }
                    }
                    Ok(MemberSqlArg::Struct(res))
                }
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    sql.call(args)
}
