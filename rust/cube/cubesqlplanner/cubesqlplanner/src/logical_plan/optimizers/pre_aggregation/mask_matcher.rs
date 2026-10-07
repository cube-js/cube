use super::{CompiledPreAggregation, DimensionMatcher, MatchState};
use crate::planner::query_tools::QueryTools;
use crate::planner::{MemberId, MemberSymbol};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

/// Decides whether the masks of the members a query reads can be rendered
/// over a pre-aggregation. A stored column holds the raw value, so a masked
/// member is masked on top of it, and its mask may read only what the
/// pre-aggregation stores: a mask over a source column, or over a member the
/// pre-aggregation can't provide, has nothing to read there. A stored column
/// computed from a masked member holds a raw-derived value no mask covers.
pub struct MaskMatcher<'a> {
    query_tools: Rc<QueryTools>,
    pre_aggregation: &'a CompiledPreAggregation,
    dimension_matcher: DimensionMatcher<'a>,
    visited: HashSet<MemberId>,
}

impl<'a> MaskMatcher<'a> {
    pub fn new(query_tools: Rc<QueryTools>, pre_aggregation: &'a CompiledPreAggregation) -> Self {
        Self {
            dimension_matcher: DimensionMatcher::new(query_tools.clone(), pre_aggregation),
            query_tools,
            pre_aggregation,
            visited: HashSet::new(),
        }
    }

    pub fn try_match<'b>(
        &mut self,
        members: impl IntoIterator<Item = &'b Rc<MemberSymbol>>,
    ) -> Result<bool, CubeError> {
        for member in members {
            if !self.is_renderable(member)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn is_renderable(&mut self, symbol: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
        if !self.visited.insert(symbol.id().clone()) {
            return Ok(true);
        }
        if let MemberSymbol::TimeDimension(td) = symbol.as_ref() {
            return self.is_renderable(td.base_symbol());
        }

        let full_name = symbol.full_name();
        if self.query_tools.is_member_masked(&full_name) {
            if let Some(mask) = symbol.mask_sql() {
                if !mask.get_cube_refs().is_empty() {
                    return Ok(false);
                }
                for dep in mask.get_dependencies() {
                    if !self.is_stored(&dep)? || !self.is_renderable(&dep)? {
                        return Ok(false);
                    }
                }
            }
            let Some(mask_filter) = self.query_tools.member_mask_filter(&full_name) else {
                return Ok(true);
            };
            // The filter is rendered unmasked, and the member itself is
            // still rendered on the rows it lets through.
            for member in mask_filter.all_member_evaluators() {
                if !self.is_stored(&member)? {
                    return Ok(false);
                }
            }
        }

        // A stored column holds a value computed from the raw values of the
        // members it reads, so none of them may be masked.
        if self.is_stored_column(symbol) {
            return Ok(!self.reads_masked_member(symbol));
        }
        for dep in symbol.get_dependencies() {
            if !self.is_renderable(&dep)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn is_stored_column(&self, symbol: &Rc<MemberSymbol>) -> bool {
        let id = symbol.id();
        match symbol.as_ref() {
            MemberSymbol::Dimension(_) => self
                .pre_aggregation
                .dimensions
                .iter()
                .map(|d| d.peel_refs())
                .chain(
                    self.pre_aggregation
                        .time_dimensions
                        .iter()
                        .filter_map(|td| {
                            td.as_time_dimension()
                                .ok()
                                .map(|td| td.base_symbol().peel_refs())
                        }),
                )
                .any(|d| d.id() == id),
            MemberSymbol::Measure(_) => self
                .pre_aggregation
                .measures
                .iter()
                .any(|m| m.peel_refs().id() == id),
            MemberSymbol::MemberExpression(_) => {
                self.pre_aggregation.segments.iter().any(|s| s.id() == id)
            }
            MemberSymbol::TimeDimension(_) | MemberSymbol::Ref(_) => false,
        }
    }

    fn reads_masked_member(&self, symbol: &Rc<MemberSymbol>) -> bool {
        symbol.get_dependencies().iter().any(|dep| {
            let dep = match dep.as_ref() {
                MemberSymbol::TimeDimension(td) => td.base_symbol().clone(),
                _ => dep.clone(),
            };
            self.query_tools.is_member_masked(&dep.full_name()) || self.reads_masked_member(&dep)
        })
    }

    fn is_stored(&mut self, symbol: &Rc<MemberSymbol>) -> Result<bool, CubeError> {
        Ok(self.dimension_matcher.match_symbol(symbol)? != MatchState::NotMatched)
    }
}
