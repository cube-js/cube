use crate::planner::multi_fact_join_groups::MultiFactJoinGroups;
use crate::planner::{CubeId, MemberId, MemberSymbol, SqlCall};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::fmt::Debug;
use std::rc::Rc;

/// One side of a join key, paired with the column the rollup on that
/// side stores it in. A key kept as the rollup's time dimension lives
/// in a granularity-suffixed column, so the ON clause cannot be
/// rendered from the symbol alone.
#[derive(Clone, Debug)]
pub struct PreAggregationJoinMember {
    pub symbol: Rc<MemberSymbol>,
    pub column: String,
    /// Granularity the column is truncated to, `None` for a key stored
    /// as a plain dimension.
    pub granularity: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PreAggregationJoinItem {
    pub from: Rc<PreAggregationSource>,
    pub to: Rc<PreAggregationSource>,
    pub from_members: Vec<PreAggregationJoinMember>,
    pub to_members: Vec<PreAggregationJoinMember>,
    pub on_sql: Rc<SqlCall>,
}

#[derive(Clone, Debug)]
pub struct PreAggregationJoin {
    pub root: Rc<PreAggregationSource>,
    pub items: Vec<PreAggregationJoinItem>,
}

impl PreAggregationJoin {
    pub fn key_members(&self) -> impl Iterator<Item = &PreAggregationJoinMember> {
        self.items
            .iter()
            .flat_map(|item| item.from_members.iter().chain(item.to_members.iter()))
    }

    /// The join keys no leg stores untruncated.
    pub fn truncated_keys(&self) -> HashSet<MemberId> {
        let (raw, truncated): (Vec<_>, Vec<_>) =
            self.key_members().partition(|m| m.granularity.is_none());
        let raw: HashSet<_> = raw
            .iter()
            .map(|m| m.symbol.peel_refs().id().clone())
            .collect();
        truncated
            .iter()
            .map(|m| m.symbol.peel_refs().id().clone())
            .filter(|id| !raw.contains(id))
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct PreAggregationUnion {
    pub items: Vec<PreAggregationUnionItem>,
}

/// A member rollup of a `rollupLambda` union, with the member symbols *it*
/// stores its columns under; the lambda itself exposes only its first rollup's.
#[derive(Clone, Debug)]
pub struct PreAggregationUnionItem {
    pub table: Rc<PreAggregationTable>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub time_dimensions: Vec<Rc<MemberSymbol>>,
    pub segments: Vec<Rc<MemberSymbol>>,
}

impl PreAggregationUnionItem {
    /// The member among `branch_members` that stores `lambda_member` in this
    /// branch: the same member, or the one with its short name on the branch cube.
    pub fn find_member(
        &self,
        lambda_member: &Rc<MemberSymbol>,
        branch_members: &[Rc<MemberSymbol>],
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(member) = branch_members.iter().find(|m| m.id() == lambda_member.id()) {
            return Ok(member.clone());
        }
        let short_name = lambda_member.name();
        let branch_cube = CubeId::cube(&self.table.cube_name);
        if let Some(member) = branch_members
            .iter()
            .find(|m| m.name() == short_name && m.cube_id() == branch_cube)
        {
            return Ok(member.clone());
        }
        Err(CubeError::internal(format!(
            "Lambda pre-aggregation member '{}' has no match in union branch '{}'",
            lambda_member.full_name(),
            self.table.cube_name
        )))
    }
}

#[derive(Clone, Debug)]
pub struct PreAggregationTable {
    pub cube_name: String,
    pub cube_alias: String,
    pub name: String,
    pub alias: Option<String>,
    pub usage_index: Option<usize>,
}

#[derive(Clone, Debug)]
pub enum PreAggregationSource {
    Single(PreAggregationTable),
    Join(PreAggregationJoin),
    Union(PreAggregationUnion),
}

#[derive(Clone)]
pub struct CompiledPreAggregation {
    pub cube_name: String,
    pub name: String,
    pub source: Rc<PreAggregationSource>,
    pub granularity: Option<String>,
    pub external: Option<bool>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub time_dimensions: Vec<Rc<MemberSymbol>>,
    pub segments: Vec<Rc<MemberSymbol>>,
    pub allow_non_strict_date_range_match: bool,
    pub multi_fact_join_groups: MultiFactJoinGroups,
}

impl Debug for CompiledPreAggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledPreAggregation")
            .field("cube_name", &self.cube_name)
            .field("name", &self.name)
            .field("granularity", &self.granularity)
            .field("external", &self.external)
            .field("measures", &self.measures)
            .field("dimensions", &self.dimensions)
            .field("time_dimensions", &self.time_dimensions)
            .field("segments", &self.segments)
            .field(
                "allow_non_strict_date_range_match",
                &self.allow_non_strict_date_range_match,
            )
            .finish()
    }
}
