use crate::planner::sql_evaluator::MemberSymbol;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct FindOwnedByCubeChildCollector {}

impl FindOwnedByCubeChildCollector {
    pub fn new() -> Self {
        Self {}
    }

    pub fn find(&self, node: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
        self.find_impl(node, &node.full_name())
    }

    fn find_impl(
        &self,
        node: &Rc<MemberSymbol>,
        origin_node_name: &String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        match node.as_ref() {
            MemberSymbol::Dimension(dimension_symbol) => {
                if dimension_symbol.owned_by_cube() {
                    Ok(node.clone())
                } else {
                    self.process_deps(&node, origin_node_name)
                }
            }
            MemberSymbol::TimeDimension(time_dimension_symbol) => {
                self.find_impl(time_dimension_symbol.base_symbol(), origin_node_name)
            }
            MemberSymbol::Measure(measure_symbol) => {
                if measure_symbol.owned_by_cube() {
                    Ok(node.clone())
                } else {
                    self.process_deps(&node, origin_node_name)
                }
            }
            _ => Err(CubeError::internal(format!(
                "FindOwnedByCubeChild cannot be processed on node {}",
                node.full_name()
            ))),
        }
    }

    fn process_deps(
        &self,
        node: &Rc<MemberSymbol>,
        origin_node_name: &String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let deps = node.get_dependencies();
        if deps.len() == 1 {
            self.find_impl(&deps[0], origin_node_name)
        } else {
            Err(CubeError::internal(format!(
                "Cannot find owned by cube child for {}",
                origin_node_name
            )))
        }
    }
}

pub fn find_owned_by_cube_child(node: &Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError> {
    let visitor = FindOwnedByCubeChildCollector::new();
    visitor.find(node)
}
