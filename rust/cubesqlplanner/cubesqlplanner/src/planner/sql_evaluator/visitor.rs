use super::{
    dependecy::{Dependency, StructDependency},
    EvaluationNode,
};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub trait TraversalVisitor {
    type State;
    fn on_node_traverse(
        &mut self,
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<Option<Self::State>, CubeError>;

    fn apply(&mut self, node: &Rc<EvaluationNode>, state: &Self::State) -> Result<(), CubeError> {
        if let Some(state) = self.on_node_traverse(node, state)? {
            self.travese_deps(node, &state)?;
        }
        Ok(())
    }

    fn travese_deps(
        &mut self,
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<(), CubeError> {
        for dep in node.deps() {
            self.traverse_single_dep(dep, node, state)?;
        }
        Ok(())
    }

    fn traverse_single_dep(
        &mut self,
        dep: &Dependency,
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<(), CubeError> {
        match dep {
            Dependency::SingleDependency(dep) => self.apply(dep, state),
            Dependency::StructDependency(dep) => self.traverse_struct_dep(dep, node, state),
            Dependency::ContextDependency(_) => Ok(()),
        }
    }

    fn traverse_struct_dep(
        &mut self,
        dep: &StructDependency,
        node: &Rc<EvaluationNode>,
        state: &Self::State,
    ) -> Result<(), CubeError> {
        if dep.sql_fn.is_some() {
            self.apply(node, state)?;
        }
        if let Some(to_string_fn) = &dep.to_string_fn {
            self.apply(to_string_fn, state)?;
        }
        for (_, v) in dep.properties.iter() {
            match v {
                Dependency::SingleDependency(dep) => {
                    self.apply(dep, state)?;
                }
                Dependency::StructDependency(dep) => self.traverse_struct_dep(dep, node, state)?,
                Dependency::ContextDependency(_) => {}
            }
        }
        Ok(())
    }
}
