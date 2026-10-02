use crate::cube_bridge::join_hints::JoinHintItem;
use crate::planner::CubeId;

/// A cube a query needs joined, or a path of cubes to join it through.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum JoinHint {
    Single(CubeId),
    Vector(Vec<CubeId>),
}

impl JoinHint {
    pub fn from_bridge(item: &JoinHintItem) -> Self {
        match item {
            JoinHintItem::Single(name) => Self::Single(CubeId::cube(name.clone())),
            JoinHintItem::Vector(path) => {
                Self::Vector(path.iter().map(|name| CubeId::cube(name.clone())).collect())
            }
        }
    }

    pub fn cubes(&self) -> &[CubeId] {
        match self {
            Self::Single(cube) => std::slice::from_ref(cube),
            Self::Vector(path) => path,
        }
    }
}

/// Ordered list of cube-join hints. `push` / `extend` drop an entry that
/// is redundant against the one before it — an item repeating the previous
/// one verbatim, or a `Single` duplicating either the previous `Single` or
/// the tail of the previous `Vector`.
///
/// That is a local rule, not a normal form. `from_items` stores what it is
/// given as-is, and nothing collapses a `Vector` that is a strict prefix of
/// another (`[V[customers], V[customers, orders]]`) or a repeat that is not
/// adjacent. So two hint lists that resolve to the same join tree can still
/// differ — which matters, since `JoinHints` is a join tree cache key:
/// equal hints hit the same entry, but unequal ones are not proof of
/// different trees.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct JoinHints {
    items: Vec<JoinHint>,
}

impl JoinHints {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn from_items(items: Vec<JoinHint>) -> Self {
        Self { items }
    }

    pub fn from_bridge(items: &[JoinHintItem]) -> Self {
        Self::from_items(items.iter().map(JoinHint::from_bridge).collect())
    }

    pub fn push(&mut self, item: JoinHint) {
        if let Some(last) = self.items.last() {
            if last == &item {
                return;
            }
            if let (JoinHint::Single(name), JoinHint::Vector(v)) = (&item, last) {
                if v.last() == Some(name) {
                    return;
                }
            }
        }
        self.items.push(item);
    }

    pub fn extend(&mut self, other: &JoinHints) {
        for item in &other.items {
            self.push(item.clone());
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn items(&self) -> &[JoinHint] {
        &self.items
    }

    pub fn iter(&self) -> std::slice::Iter<'_, JoinHint> {
        self.items.iter()
    }

    pub fn into_items(self) -> Vec<JoinHint> {
        self.items
    }

    /// Splits the hints into what the data model's join graph resolves and
    /// the joined cube instances it knows nothing about, which the planner
    /// joins on its own. An instance is replaced in the graph's hints by the
    /// cube its chain of joins starts from, so that cube is in the tree the
    /// instance is attached to. Instances come ancestors first, each once.
    pub fn split_joined(&self) -> (Vec<JoinHintItem>, Vec<CubeId>) {
        let mut graph_hints = Vec::new();
        let mut joined: Vec<CubeId> = Vec::new();
        let add_joined = |cube: &CubeId, joined: &mut Vec<CubeId>| {
            for instance in cube.joined_chain() {
                if !joined.contains(&instance) {
                    joined.push(instance);
                }
            }
        };
        for item in &self.items {
            match item {
                JoinHint::Single(cube) => {
                    add_joined(cube, &mut joined);
                    graph_hints.push(JoinHintItem::Single(cube.root().target().to_string()));
                }
                JoinHint::Vector(path) => {
                    let prefix_len = path.iter().take_while(|c| !c.is_joined()).count();
                    if prefix_len == path.len() {
                        graph_hints.push(JoinHintItem::Vector(
                            path.iter().map(|c| c.target().to_string()).collect(),
                        ));
                        continue;
                    }
                    match prefix_len {
                        0 => graph_hints
                            .push(JoinHintItem::Single(path[0].root().target().to_string())),
                        1 => graph_hints.push(JoinHintItem::Single(path[0].target().to_string())),
                        _ => graph_hints.push(JoinHintItem::Vector(
                            path[..prefix_len]
                                .iter()
                                .map(|c| c.target().to_string())
                                .collect(),
                        )),
                    }
                    for cube in &path[prefix_len..] {
                        if cube.is_joined() {
                            add_joined(cube, &mut joined);
                        } else {
                            graph_hints.push(JoinHintItem::Single(cube.target().to_string()));
                        }
                    }
                }
            }
        }
        (graph_hints, joined)
    }
}

impl IntoIterator for JoinHints {
    type Item = JoinHint;
    type IntoIter = std::vec::IntoIter<JoinHint>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a> IntoIterator for &'a JoinHints {
    type Item = &'a JoinHint;
    type IntoIter = std::slice::Iter<'a, JoinHint>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(name: &str) -> JoinHint {
        JoinHint::Single(CubeId::cube(name))
    }

    fn v(names: &[&str]) -> JoinHint {
        JoinHint::Vector(names.iter().map(|n| CubeId::cube(*n)).collect())
    }

    #[test]
    fn test_from_items_preserves_order() {
        let hints = JoinHints::from_items(vec![s("orders"), v(&["users", "orders"]), s("abc")]);

        assert_eq!(hints.len(), 3);
        assert_eq!(hints.items()[0], s("orders"));
        assert_eq!(hints.items()[1], v(&["users", "orders"]));
        assert_eq!(hints.items()[2], s("abc"));
    }

    #[test]
    fn test_push_and_extend() {
        let mut a = JoinHints::new();
        assert!(a.is_empty());

        a.push(s("orders"));
        a.push(s("abc"));
        assert_eq!(a.len(), 2);

        let b = JoinHints::from_items(vec![s("zzz"), v(&["a", "b"])]);
        a.extend(&b);
        assert_eq!(a.len(), 4);
        assert_eq!(a.items()[0], s("orders"));
        assert_eq!(a.items()[1], s("abc"));
        assert_eq!(a.items()[2], s("zzz"));
        assert_eq!(a.items()[3], v(&["a", "b"]));
    }

    #[test]
    fn test_extend_skips_redundant_at_boundary() {
        let mut a = JoinHints::new();
        a.push(s("orders"));

        let b = JoinHints::from_items(vec![s("orders"), s("abc")]);
        a.extend(&b);
        assert_eq!(a.len(), 2);
        assert_eq!(a.items()[0], s("orders"));
        assert_eq!(a.items()[1], s("abc"));

        let mut c = JoinHints::new();
        c.push(v(&["x", "abc"]));
        let d = JoinHints::from_items(vec![s("abc"), s("zzz")]);
        c.extend(&d);
        assert_eq!(
            c.len(),
            2,
            "Single after Vector ending with same name is skipped on extend"
        );
        assert_eq!(c.items()[0], v(&["x", "abc"]));
        assert_eq!(c.items()[1], s("zzz"));
    }

    #[test]
    fn test_push_skips_redundant_single() {
        let mut hints = JoinHints::new();
        hints.push(s("orders"));
        hints.push(s("orders"));
        assert_eq!(hints.len(), 1);

        hints.push(v(&["users", "orders"]));
        hints.push(s("orders"));
        assert_eq!(
            hints.len(),
            2,
            "Single after Vector ending with same name is skipped"
        );

        hints.push(s("abc"));
        assert_eq!(hints.len(), 3, "Different Single is added");
    }

    #[test]
    fn test_push_skips_repeated_vector() {
        let mut hints = JoinHints::new();
        hints.push(v(&["customers", "orders"]));
        hints.push(v(&["customers", "orders"]));
        assert_eq!(
            hints.len(),
            1,
            "Vector repeating the previous one is skipped"
        );

        hints.push(v(&["customers", "returns"]));
        assert_eq!(hints.len(), 2, "Different Vector is added");

        hints.push(v(&["customers", "orders"]));
        assert_eq!(
            hints.len(),
            3,
            "Only adjacent repeats are dropped, not every earlier occurrence"
        );
    }

    #[test]
    fn test_into_items_and_into_iter() {
        let hints = JoinHints::from_items(vec![s("b"), s("a"), v(&["x", "y"])]);
        let cloned = hints.clone();

        let collected: Vec<_> = cloned.into_iter().collect();
        assert_eq!(collected.len(), 3);

        let items = hints.into_items();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], s("b"));
        assert_eq!(items[1], s("a"));
    }

    #[test]
    fn test_split_joined_without_instances_keeps_hints() {
        let hints = JoinHints::from_items(vec![s("orders"), v(&["users", "orders"])]);
        let (graph, joined) = hints.split_joined();
        assert_eq!(
            graph,
            vec![
                JoinHintItem::Single("orders".to_string()),
                JoinHintItem::Vector(vec!["users".to_string(), "orders".to_string()]),
            ]
        );
        assert!(joined.is_empty());
    }

    #[test]
    fn test_split_joined_cuts_instances() {
        let orders = CubeId::cube("orders");
        let customer = CubeId::joined(orders.clone(), "customer", "users");
        let departments = CubeId::joined(customer.clone(), "departments", "departments");
        let manager = CubeId::joined(orders.clone(), "manager", "users");
        let hints = JoinHints::from_items(vec![
            JoinHint::Vector(vec![
                CubeId::cube("view_root"),
                orders.clone(),
                customer.clone(),
                departments.clone(),
            ]),
            JoinHint::Single(manager.clone()),
        ]);
        let (graph, joined) = hints.split_joined();
        assert_eq!(
            graph,
            vec![
                JoinHintItem::Vector(vec!["view_root".to_string(), "orders".to_string()]),
                JoinHintItem::Single("orders".to_string()),
            ]
        );
        assert_eq!(joined, vec![customer, departments, manager]);
    }
}
