use crate::cube_bridge::join_hints::JoinHintItem;
use crate::planner::join_hints::JoinHints;
use crate::planner::{CubeId, CubeRef, MemberSymbol, TraversalVisitor};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct JoinHintsCollector {
    hints: Vec<JoinHintItem>,
}

impl JoinHintsCollector {
    pub fn new() -> Self {
        Self { hints: Vec::new() }
    }

    pub fn extract_result(self) -> Vec<JoinHintItem> {
        self.hints.into_iter().unique().collect()
    }
}

impl TraversalVisitor for JoinHintsCollector {
    type State = ();
    fn on_node_traverse(
        &mut self,
        node: &Rc<MemberSymbol>,
        _: &Self::State,
    ) -> Result<Option<Self::State>, CubeError> {
        if let MemberSymbol::Ref(ref_symbol) = node.as_ref() {
            if !node.is_multi_stage() {
                return Ok(Some(()));
            }
            if let Some(target) = ref_symbol.target_member() {
                self.on_node_traverse(target, &())?;
            }
            return Ok(None);
        }
        if node.is_multi_stage() {
            if let Ok(dim) = node.as_dimension() {
                if let Some(include) = dim.multi_stage().and_then(|m| m.grain.include.as_ref()) {
                    for item in include.iter() {
                        self.apply(item, &())?;
                    }
                }
                for dep in dim.get_dependencies().into_iter() {
                    if let Ok(dim) = dep.as_dimension() {
                        if dim.is_multi_stage() {
                            self.on_node_traverse(&dep, &())?;
                        }
                    }
                }
            }
            return Ok(None);
        }

        match node.as_ref() {
            MemberSymbol::Dimension(e) => {
                if !e.is_view() {
                    self.hints.push(join_hint(node.path(), &e.cube_name()));
                }
                if e.is_sub_query() {
                    return Ok(None);
                }
            }
            MemberSymbol::TimeDimension(e) => return self.on_node_traverse(e.base_symbol(), &()),
            MemberSymbol::Measure(e) => {
                if !e.is_view() {
                    self.hints.push(join_hint(node.path(), &e.cube_name()));
                }
            }
            MemberSymbol::MemberExpression(_) | MemberSymbol::Ref(_) => {}
        };
        Ok(Some(()))
    }

    fn on_cube_ref(&mut self, cube_ref: &CubeRef, _state: &Self::State) -> Result<(), CubeError> {
        if let CubeRef::Name(symbol) = cube_ref {
            self.hints
                .push(join_hint(symbol.path(), symbol.cube_name()));
        }
        Ok(())
    }
}

// Join hints go to the data-model join graph, so they name target cubes.
fn join_hint(path: &[CubeId], cube: &CubeId) -> JoinHintItem {
    match path {
        [] => JoinHintItem::Single(cube.target().to_string()),
        [single] => JoinHintItem::Single(single.target().to_string()),
        _ => JoinHintItem::Vector(path.iter().map(|c| c.target().to_string()).collect()),
    }
}

pub fn collect_join_hints(node: &Rc<MemberSymbol>) -> Result<JoinHints, CubeError> {
    let mut visitor = JoinHintsCollector::new();
    visitor.apply(node, &())?;
    let mut collected_hints = visitor.extract_result();

    let join_map = node.join_map();

    if let Some(join_map) = join_map {
        for hint in collected_hints.iter_mut() {
            match hint {
                // If hints array has single element, check if it can be enriched with join hints
                JoinHintItem::Single(hints) => {
                    for path in join_map.iter() {
                        if let Some(hint_index) = path.iter().position(|p| p == hints) {
                            *hint = JoinHintItem::Vector(path[0..=hint_index].to_vec());
                            break;
                        }
                    }
                }
                // If hints is an array with multiple elements, it means it already
                // includes full join hint path
                JoinHintItem::Vector(_) => {}
            }
        }
    }

    Ok(JoinHints::from_items(collected_hints))
}

pub fn collect_join_hints_for_measures(
    measures: &Vec<Rc<MemberSymbol>>,
) -> Result<JoinHints, CubeError> {
    let mut visitor = JoinHintsCollector::new();
    for meas in measures.iter() {
        visitor.apply(&meas, &())?;
    }

    let res = visitor.extract_result();
    Ok(JoinHints::from_items(res))
}
