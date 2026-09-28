//! Depth guard for chained multi-stage members.
//!
//! Each multi-stage member is planned as its own stage, by descending from a member into its
//! children on the caller's thread, so a long enough chain exhausts that thread's stack and the
//! process aborts -- a crash no error handler sees, on either side of the native boundary. This
//! turns it into a reportable error.
//!
//! Only members planned as a stage are counted: a reference inherits `multi_stage` from what it
//! resolves to but planning collapses it, and a calculation level costs a fraction of a stage.

use crate::planner::symbols::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// Used when the query carries no limit of its own.
///
/// A backstop against the crash, not a policy on how deep a model may go: it is set far above
/// any hand-written model, because where planning actually runs out of stack depends on what
/// each stage plans and on the stack the caller happens to have. For a query served from a
/// rollup the binding limit is `CUBESTORE_MAX_QUERY_PLAN_DEPTH` instead, which measures the
/// plan Cube Store will decode and counts about two of its levels per stage.
pub const DEFAULT_MAX_MULTI_STAGE_DEPTH: usize = 150;

pub fn check_multi_stage_depth(roots: &[Rc<MemberSymbol>], limit: usize) -> Result<(), CubeError> {
    // Roots share a member graph, so they share the memo: measuring each one against its own
    // would re-expand that graph per root.
    let mut measured = Measured::default();
    for root in roots {
        let depth = multi_stage_depth(root, &mut measured);
        if depth > limit {
            return Err(CubeError::user(format!(
                "Member '{}' chains {} multi-stage members deep, against a limit of {}. Each \
                 one is planned as a separate stage, and this many cannot be planned. Collapse \
                 the intermediate stages into fewer members, or raise \
                 CUBEJS_MAX_MULTI_STAGE_DEPTH.",
                root.full_name(),
                depth,
                limit
            )));
        }
    }
    Ok(())
}

/// Stages measured so far, keyed by symbol address.
#[derive(Default)]
struct Measured {
    depth_below: HashMap<*const MemberSymbol, usize>,
    /// The symbols those addresses stand for, kept alive so a freed symbol's address cannot come
    /// back as a different one.
    symbols: Vec<Rc<MemberSymbol>>,
}

/// Multi-stage members on the longest dependency path starting at `root`, `root` included.
///
/// Walks an explicit stack: the graphs this guards against are exactly the ones a recursive walk
/// could not survive.
fn multi_stage_depth(root: &Rc<MemberSymbol>, measured: &mut Measured) -> usize {
    let mut on_path: HashSet<*const MemberSymbol> = HashSet::new();
    let mut pending = vec![(root.clone(), false)];

    while let Some((symbol, dependencies_visited)) = pending.pop() {
        let key = Rc::as_ptr(&symbol);
        if dependencies_visited {
            let below = symbol
                .get_dependencies()
                .iter()
                .filter_map(|dependency| measured.depth_below.get(&Rc::as_ptr(dependency)))
                .copied()
                .max()
                .unwrap_or(0);
            let opens_a_stage = symbol.is_multi_stage() && !symbol.is_reference();
            measured
                .depth_below
                .insert(key, below + usize::from(opens_a_stage));
            on_path.remove(&key);
            measured.symbols.push(symbol);
            continue;
        }
        if measured.depth_below.contains_key(&key) || !on_path.insert(key) {
            // Already measured, or a cycle: a cyclic model does not terminate in planning
            // either, and this walk must not be the thing that hangs on it.
            continue;
        }
        let dependencies = symbol.get_dependencies();
        pending.push((symbol, true));
        pending.extend(dependencies.into_iter().map(|d| (d, false)));
    }

    measured
        .depth_below
        .get(&Rc::as_ptr(root))
        .copied()
        .unwrap_or_default()
}
