use crate::planner::join_hints::JoinHints;
use crate::planner::query_tools::JoinKey;
use crate::planner::JoinTree;
use cubenativeutils::CubeError;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

/// Per-query cache of join trees keyed by the `JoinHints` that produced
/// them. The join graph is immutable within a `QueryTools` lifetime, so
/// the same hints always resolve to the same join — caching avoids
/// re-crossing the JS bridge and recompiling ON SQL when the same hints
/// are resolved repeatedly (e.g. once per pre-aggregation candidate).
#[derive(Default)]
pub struct JoinTreeCache {
    by_hints: RefCell<HashMap<JoinHints, (JoinKey, Rc<JoinTree>)>>,
}

impl JoinTreeCache {
    /// Returns the cached `(JoinKey, JoinTree)` for `hints`, building and
    /// storing it via `build` on a miss. `build` is supplied per call so
    /// the cache holds no reference back to `QueryTools`.
    pub fn get_or_build(
        &self,
        hints: &JoinHints,
        build: impl FnOnce() -> Result<(JoinKey, Rc<JoinTree>), CubeError>,
    ) -> Result<(JoinKey, Rc<JoinTree>), CubeError> {
        if let Some(cached) = self.by_hints.borrow().get(hints) {
            return Ok(cached.clone());
        }
        let built = build()?;
        self.by_hints
            .borrow_mut()
            .insert(hints.clone(), built.clone());
        Ok(built)
    }
}
