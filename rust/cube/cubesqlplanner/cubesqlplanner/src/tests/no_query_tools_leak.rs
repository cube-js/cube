//! Regression guard against Rc-cycle leaks in the planner.
//!
//! The whole per-query object graph hangs off `Rc<QueryTools>`. If anything
//! reachable from `QueryTools` holds a strong `Rc<QueryTools>` back (e.g. a
//! value cached inside `QueryTools` that, in turn, points at it), the refcount
//! never reaches zero and the entire graph — including the compiled minijinja
//! SQL templates — leaks on every query. That is what OOMs the refresh worker.
//!
//! There is no general "is anything still alive?" reflection in Rust, but for
//! an `Rc`-managed graph a `Weak` is exactly that probe: after every legitimate
//! owner is dropped, a still-upgradable `Weak` means a cycle kept the value
//! alive. `assert_released` encodes that check and is reusable for any `Rc<T>`.

use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;
use indoc::indoc;
use std::rc::{Rc, Weak};

/// Assert that `weak` has no strong owners left. Use after dropping everything
/// that should own the value: a surviving strong ref means an Rc cycle leaked it.
#[track_caller]
fn assert_released<T>(weak: &Weak<T>, what: &str) {
    let alive = weak.strong_count();
    assert_eq!(
        alive, 0,
        "{what} leaked: {alive} strong Rc ref(s) still alive after all owners were dropped \
         (Rc cycle — the per-query graph cannot be freed)"
    );
}

/// Building SQL for a query that spans a join populates the join-tree cache,
/// which historically created the cycle
/// `QueryTools -> join_tree_cache -> JoinTree -> BaseCube -> Rc<QueryTools>`.
/// After planning and dropping the context, `QueryTools` must be fully released.
#[test]
fn query_tools_released_after_join_query() {
    let schema = MockSchema::from_yaml_file("common/multi_fact.yaml");
    let ctx = TestContext::new(schema).unwrap();

    // Probe the leaf `QueryTools` (the hub that owns the compiled minijinja
    // templates), NOT the `State` wrapper — `State` is held only by transient
    // planners and is released trivially, so downgrading it would prove nothing.
    let weak = Rc::downgrade(ctx.query_tools().query_tools());

    // orders.count + customers.name spans the orders<->customers join, so the
    // planner builds and caches a JoinTree (whose BaseCube points back).
    let query_yaml = indoc! {"
        measures:
          - orders.count
        dimensions:
          - customers.name
    "};
    let options = ctx.create_query_options_from_yaml(query_yaml);
    let _sql = ctx
        .build_sql_from_options(options)
        .expect("planning should succeed");

    drop(ctx);

    assert_released(&weak, "QueryTools");
}
