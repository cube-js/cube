use super::{dependecy::Dependency, EvaluationNode};
use crate::cube_bridge::memeber_sql::{MemberSql, MemberSqlArg, MemberSqlStruct};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait TraversalVisitor {
    fn on_node_traverse(&mut self, node: &Rc<EvaluationNode>) -> Result<(), CubeError>;

    fn apply(&mut self, node: &Rc<EvaluationNode>) -> Result<(), CubeError> {
        self.on_node_traverse(node)?;
        self.travese_deps(node)?;
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
                let mut res = MemberSqlStruct::default();
                if let Some(sql_fn) = &dep.sql_fn {
                    self.apply(node)?;
                }
                if let Some(to_string_fn) = &dep.to_string_fn {
                    self.apply(to_string_fn)?;
                }
                for (k, v) in dep.properties.iter() {
                    match v {
                        Dependency::SingleDependency(dep) => {
                            self.apply(dep)?;
                        }
                        Dependency::StructDependency(_) => unimplemented!(),
                    }
                }
                Ok(())
            }
        }
    }
}

pub trait EvaluatorVisitor {
    fn on_node_enter(&mut self, node: &Rc<EvaluationNode>) -> Result<(), CubeError> {
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

    fn apply(&mut self, node: &Rc<EvaluationNode>) -> Result<String, CubeError> {
        self.on_node_enter(node)?;
        let deps = self.evaluate_deps(node)?;
        let result = self.evaluate_sql(node, deps)?;
        self.post_process(node, result)
    }

    fn evaluate_sql(
        &mut self,
        node: &Rc<EvaluationNode>,
        args: Vec<MemberSqlArg>,
    ) -> Result<String, CubeError>;

    fn post_process(
        &mut self,
        node: &Rc<EvaluationNode>,
        result: String,
    ) -> Result<String, CubeError> {
        Ok(result)
    }
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
                }
            }
            Ok(MemberSqlArg::Struct(res))
        }
    }
}
