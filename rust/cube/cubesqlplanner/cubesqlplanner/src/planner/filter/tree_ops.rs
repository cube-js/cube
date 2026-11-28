use crate::planner::filter::{BaseSegment, FilterGroup, FilterGroupOperator, FilterItem};
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

fn segment_matches(member_ids: &[MemberId], seg: &Rc<BaseSegment>) -> bool {
    chain_contains(member_ids, &seg.member_evaluator())
}

pub fn exclude_members(member_ids: &[MemberId], filters: &[FilterItem]) -> Vec<FilterItem> {
    drop_leaves(filters, &|leaf| leaf_matches(member_ids, leaf))
}

pub fn keep_only_members(member_ids: &[MemberId], filters: &[FilterItem]) -> Vec<FilterItem> {
    drop_leaves(filters, &|leaf| !leaf_matches(member_ids, leaf))
}

fn leaf_matches(member_ids: &[MemberId], leaf: &FilterItem) -> bool {
    match leaf {
        FilterItem::Item(itm) => chain_contains(member_ids, &itm.member_evaluator()),
        FilterItem::Segment(seg) => segment_matches(member_ids, seg),
        FilterItem::Group(_) => false,
    }
}

// A dropped leaf counts as true, so the result is never stricter than the
// input: an AND loses it, and an OR holding it is true as a whole and goes too.
fn drop_leaves(
    filters: &[FilterItem],
    is_dropped: &dyn Fn(&FilterItem) -> bool,
) -> Vec<FilterItem> {
    filters
        .iter()
        .filter_map(|item| drop_leaves_in_item(item, is_dropped))
        .collect()
}

// None when `item` is always true once the dropped leaves are.
fn drop_leaves_in_item(
    item: &FilterItem,
    is_dropped: &dyn Fn(&FilterItem) -> bool,
) -> Option<FilterItem> {
    match item {
        FilterItem::Group(group) => {
            let mut items = Vec::new();
            for child in group.items.iter() {
                match drop_leaves_in_item(child, is_dropped) {
                    Some(child) => items.push(child),
                    None if group.operator == FilterGroupOperator::Or => return None,
                    None => {}
                }
            }
            if items.is_empty() {
                None
            } else {
                Some(FilterItem::Group(Rc::new(FilterGroup::new(
                    group.operator.clone(),
                    items,
                ))))
            }
        }
        leaf => (!is_dropped(leaf)).then(|| leaf.clone()),
    }
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
