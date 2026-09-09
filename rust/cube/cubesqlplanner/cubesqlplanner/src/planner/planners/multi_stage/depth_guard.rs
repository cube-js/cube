//! Depth guard for chained multi-stage members.
//!
//! Every multi-stage member on a dependency path becomes its own CTE, and `MultiStageQueryPlanner`
//! plans them by descending from a member into its children. Planning runs synchronously on the
//! caller's thread, so a long enough chain exhausts that thread's stack and the process aborts —
//! a crash no error handler sees, on either side of the native boundary.
//!
//! The guard turns that into a reportable error. It measures only multi-stage members that are
//! planned as a stage: calculation chains recurse as well, but cost a fraction of a stage per
//! level, and a reference -- a view member proxying one, say -- inherits `multi_stage` from what
//! it resolves to while planning collapses it into that member and opens no stage of its own.

use crate::planner::symbols::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::OnceLock;

/// Multi-stage members a single dependency path may carry.
///
/// Far above any hand-written model — real ones chain a handful of stages — and below the depth
/// at which planning runs out of stack, which a release build reaches around a few hundred
/// minimal stages, sooner for heavy ones and sooner still on a smaller caller stack.
const DEFAULT_MAX_MULTI_STAGE_DEPTH: usize = 32;

fn max_multi_stage_depth() -> usize {
    static MAX_DEPTH: OnceLock<usize> = OnceLock::new();
    *MAX_DEPTH.get_or_init(|| match std::env::var("CUBEJS_MAX_MULTI_STAGE_DEPTH") {
        // A malformed value falls back to the default rather than refusing to plan: this is a
        // safety valve, and a typo in it must not take queries down.
        Ok(value) => match value.parse::<usize>() {
            Ok(0) | Err(_) => DEFAULT_MAX_MULTI_STAGE_DEPTH,
            Ok(depth) => depth,
        },
        Err(_) => DEFAULT_MAX_MULTI_STAGE_DEPTH,
    })
}

pub fn check_multi_stage_depth(roots: &[Rc<MemberSymbol>]) -> Result<(), CubeError> {
    let limit = max_multi_stage_depth();
    for root in roots {
        let depth = multi_stage_depth(root);
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

/// Multi-stage members on the longest dependency path starting at `root`, `root` included.
///
/// Walks an explicit stack and memoizes per symbol identity: the graphs this guards against are
/// exactly the ones a recursive walk could not survive, and a member reachable by many paths
/// must not be re-expanded per path.
fn multi_stage_depth(root: &Rc<MemberSymbol>) -> usize {
    let mut depth_below: HashMap<*const MemberSymbol, usize> = HashMap::new();
    let mut on_path: HashSet<*const MemberSymbol> = HashSet::new();
    // Addresses are the memo keys, so every symbol one stands for has to outlive the walk;
    // otherwise a freed symbol's address could come back as a different one.
    let mut measured: Vec<Rc<MemberSymbol>> = Vec::new();
    let mut pending = vec![(root.clone(), false)];

    while let Some((symbol, dependencies_visited)) = pending.pop() {
        let key = Rc::as_ptr(&symbol);
        if dependencies_visited {
            let below = symbol
                .get_dependencies()
                .iter()
                .filter_map(|dependency| depth_below.get(&Rc::as_ptr(dependency)))
                .copied()
                .max()
                .unwrap_or(0);
            let opens_a_stage = symbol.is_multi_stage() && !symbol.is_reference();
            depth_below.insert(key, below + usize::from(opens_a_stage));
            on_path.remove(&key);
            measured.push(symbol);
            continue;
        }
        if depth_below.contains_key(&key) || !on_path.insert(key) {
            // Already measured, or a cycle: a cyclic model does not terminate in planning
            // either, and this walk must not be the thing that hangs on it.
            continue;
        }
        let dependencies = symbol.get_dependencies();
        pending.push((symbol, true));
        pending.extend(dependencies.into_iter().map(|d| (d, false)));
    }

    depth_below
        .get(&Rc::as_ptr(root))
        .copied()
        .unwrap_or_default()
}
