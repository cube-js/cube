use crate::planner::filter::{BaseSegment, FilterGroup, FilterItem};
use crate::planner::{MemberId, MemberSymbol};
use std::rc::Rc;

/// True if any id in `member_ids` matches the filter member or any member
/// reachable from it via the reference chain. Matching the whole chain, not
/// just the member's own name, lets `exclude`/`keep_only` — which list base
/// members — also apply when the measure is queried through a view, where the
/// carried filter names the view member that references that base.
fn chain_contains(member_ids: &[MemberId], symbol: &Rc<MemberSymbol>) -> bool {
    if member_ids.contains(symbol.id()) {
        return true;
    }
    let mut current = symbol.reference_member();
    while let Some(reference) = current {
        if member_ids.contains(reference.id()) {
            return true;
        }
        current = reference.reference_member();
    }
    false
}

/// Like `chain_contains`, but for a segment: its own id matches the
/// directive's plain member form, and its evaluator chain matches the view→base
/// reference the same way as dimension filters.
fn segment_matches(member_ids: &[MemberId], seg: &Rc<BaseSegment>) -> bool {
    member_ids.contains(seg.id()) || chain_contains(member_ids, &seg.member_evaluator())
}

pub fn exclude_members(member_ids: &[MemberId], filters: &[FilterItem]) -> Vec<FilterItem> {
    let mut result = Vec::new();
    for item in filters.iter() {
        match item {
            FilterItem::Group(group) => {
                let new_items = exclude_members(member_ids, &group.items);
                if !new_items.is_empty() {
                    result.push(FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        new_items,
                    ))));
                }
            }
            FilterItem::Item(itm) => {
                if !chain_contains(member_ids, &itm.member_evaluator()) {
                    result.push(FilterItem::Item(itm.clone()));
                }
            }
            FilterItem::Segment(seg) => {
                if !segment_matches(member_ids, seg) {
                    result.push(FilterItem::Segment(seg.clone()));
                }
            }
        }
    }
    result
}

pub fn keep_only_members(member_ids: &[MemberId], filters: &[FilterItem]) -> Vec<FilterItem> {
    let mut result = Vec::new();
    for item in filters.iter() {
        match item {
            FilterItem::Group(group) => {
                let new_items = keep_only_members(member_ids, &group.items);
                if !new_items.is_empty() {
                    result.push(FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        new_items,
                    ))));
                }
            }
            FilterItem::Item(itm) => {
                if chain_contains(member_ids, &itm.member_evaluator()) {
                    result.push(FilterItem::Item(itm.clone()));
                }
            }
            FilterItem::Segment(seg) => {
                if segment_matches(member_ids, seg) {
                    result.push(FilterItem::Segment(seg.clone()));
                }
            }
        }
    }
    result
}

pub fn has_filter_for_member(member: &MemberId, filters: &[FilterItem]) -> bool {
    for item in filters.iter() {
        match item {
            FilterItem::Group(group) => {
                if has_filter_for_member(member, &group.items) {
                    return true;
                }
            }
            FilterItem::Item(itm) => {
                if &itm.member_id() == member {
                    return true;
                }
            }
            FilterItem::Segment(_) => {}
        }
    }
    false
}

/// Structural equality that also compares the member each leaf filter targets.
/// `FilterItem`'s own `PartialEq` compares a filter's type, operator and values
/// but not its member, so two filters differing only in the dimension they
/// restrict count as equal there. Groups are compared element-wise in order.
/// Segments carry their member in their id and compare as-is.
pub fn eq_with_member(a: &FilterItem, b: &FilterItem) -> bool {
    match (a, b) {
        (FilterItem::Item(a), FilterItem::Item(b)) => a.member_id() == b.member_id() && a == b,
        (FilterItem::Group(a), FilterItem::Group(b)) => {
            a.operator == b.operator
                && a.items.len() == b.items.len()
                && a.items
                    .iter()
                    .zip(b.items.iter())
                    .all(|(a, b)| eq_with_member(a, b))
        }
        _ => a == b,
    }
}

/// True when `items` holds a filter equal to `item` under [`eq_with_member`].
pub fn contains_with_member(items: &[FilterItem], item: &FilterItem) -> bool {
    items
        .iter()
        .any(|candidate| eq_with_member(item, candidate))
}
