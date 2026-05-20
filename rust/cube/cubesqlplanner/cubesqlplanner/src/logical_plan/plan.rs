use super::*;
use std::rc::Rc;

/// Root container of a planned query: a WITH-clause `ctes` pool plus a
/// `root` Query that consumes them. Not part of `PlanNode` — it sits
/// one level above the tree, marking the boundary where a CTE pool is
/// materialised. Nested plans (DSQ body, multi-stage leaf body) live
/// on `LogicalMultiStageMember::body` via `MultiStageMemberBody::Plan`;
/// tree walkers cross that boundary through the dedicated visitor
/// entry point, not through `PlanNode.inputs`.
#[derive(Clone)]
pub struct LogicalPlan {
    pub ctes: Vec<Rc<LogicalMultiStageMember>>,
    pub root: Rc<Query>,
}

impl LogicalPlan {
    pub fn new(ctes: Vec<Rc<LogicalMultiStageMember>>, root: Rc<Query>) -> Rc<Self> {
        Rc::new(Self { ctes, root })
    }

    /// Wrap a Query as a plan with no CTEs of its own — used for bodies
    /// that don't bring a CTE pool (Stage inode, multiplied-measure
    /// bodies, etc.).
    pub fn just(root: Rc<Query>) -> Rc<Self> {
        Rc::new(Self {
            ctes: Vec::new(),
            root,
        })
    }

    pub fn ctes(&self) -> &Vec<Rc<LogicalMultiStageMember>> {
        &self.ctes
    }

    pub fn root(&self) -> &Rc<Query> {
        &self.root
    }

    pub fn with_root(self: &Rc<Self>, root: Rc<Query>) -> Rc<Self> {
        Rc::new(Self {
            ctes: self.ctes.clone(),
            root,
        })
    }

    pub fn with_ctes(self: &Rc<Self>, ctes: Vec<Rc<LogicalMultiStageMember>>) -> Rc<Self> {
        Rc::new(Self {
            ctes,
            root: self.root.clone(),
        })
    }
}

impl PrettyPrint for LogicalPlan {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println("LogicalPlan:", state);
        let inner = state.new_level();
        let details = inner.new_level();
        if !self.ctes.is_empty() {
            result.println("ctes:", &inner);
            for cte in self.ctes.iter() {
                cte.pretty_print(result, &details);
            }
        }
        result.println("root:", &inner);
        self.root.pretty_print(result, &details);
    }
}
