use super::{dependecy::Dependency, EvaluationNode};
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
