use super::{
    dependecy::{ContextSymbolDep, Dependency},
    EvaluationNode,
};
use crate::cube_bridge::memeber_sql::{MemberSqlArg, MemberSqlStruct};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait TraversalVisitor {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<bool, CubeError>;

    fn apply(&mut self, node: &Rc<EvaluationNode>) -> Result<(), CubeError> {
        if self.on_node_traverse(node)? {
            self.travese_deps(node)?;
        }
        Ok(())
    }

    fn travese_deps(&mut self, node: &Rc<EvaluationNode>) -> Result<(), CubeError> {
        for dep in node.deps() {
            self.traverse_single_dep(dep, node)?;
        }
        Ok(())
    }

    fn traverse_single_dep(
        &mut self,
        dep: &Dependency,
        node: &Rc<EvaluationNode>,
    ) -> Result<(), CubeError> {
        match dep {
            Dependency::SingleDependency(dep) => self.apply(dep),
            Dependency::StructDependency(dep) => {
                if dep.sql_fn.is_some() {
                    self.apply(node)?;
                }
                if let Some(to_string_fn) = &dep.to_string_fn {
                    self.apply(to_string_fn)?;
                }
                for (_, v) in dep.properties.iter() {
                    match v {
                        Dependency::SingleDependency(dep) => {
                            self.apply(dep)?;
                        }
                        Dependency::StructDependency(_) => unimplemented!(),
                        Dependency::ContextDependency(_) => {}
                    }
                }
                Ok(())
            }
            Dependency::ContextDependency(_) => Ok(()),
        }
    }
}

pub trait EvaluatorVisitor {
    fn on_node_enter(&mut self, _node: &Rc<EvaluationNode>) -> Result<(), CubeError> {
        Ok(())
    }
    fn evaluate_deps(&mut self, node: &Rc<EvaluationNode>) -> Result<Vec<MemberSqlArg>, CubeError> {
        node.deps()
            .iter()
            .map(|d| self.evaluate_single_dep(&d, &node))
            .collect()
    }

    fn evaluate_single_dep(
        &mut self,
        dep: &Dependency,
        node: &Rc<EvaluationNode>,
    ) -> Result<MemberSqlArg, CubeError> {
        default_single_dep_evaluator(self, dep, node)
    }

    fn apply(&mut self, node: &Rc<EvaluationNode>) -> Result<String, CubeError>;

    fn apply_context_symbol(
        &mut self,
        contex_symbol: &ContextSymbolDep,
    ) -> Result<MemberSqlArg, CubeError>;
}

pub fn default_single_dep_evaluator<V: EvaluatorVisitor + ?Sized>(
    visitor: &mut V,
    dep: &Dependency,
    _node: &Rc<EvaluationNode>,
) -> Result<MemberSqlArg, CubeError> {
    match dep {
        Dependency::SingleDependency(dep) => Ok(MemberSqlArg::String(visitor.apply(dep)?)),
        Dependency::StructDependency(dep) => {
            let mut res = MemberSqlStruct::default();
            if let Some(sql_fn) = &dep.sql_fn {
                res.sql_fn = Some(visitor.apply(sql_fn)?);
            }
            if let Some(to_string_fn) = &dep.to_string_fn {
                res.to_string_fn = Some(visitor.apply(to_string_fn)?);
            }
            for (k, v) in dep.properties.iter() {
                match v {
                    Dependency::SingleDependency(dep) => {
                        res.properties.insert(k.clone(), visitor.apply(dep)?);
                    }
                    Dependency::StructDependency(_) => unimplemented!(),
                    Dependency::ContextDependency(_) => unimplemented!(),
                }
            }
            Ok(MemberSqlArg::Struct(res))
        }
        Dependency::ContextDependency(contex_symbol) => visitor.apply_context_symbol(contex_symbol),
    }
}
