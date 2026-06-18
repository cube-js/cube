use super::{BaseFilter, BaseSegment};
use crate::planner::MemberSymbol;
use std::fmt;
use std::rc::Rc;

/// Boolean operator combining the items of a `FilterGroup`.
#[derive(Clone, PartialEq)]
pub enum FilterGroupOperator {
    Or,
    And,
}

/// Boolean combination of nested `FilterItem`s, joined by `operator`.
#[derive(Clone)]
pub struct FilterGroup {
    pub operator: FilterGroupOperator,
    pub items: Vec<FilterItem>,
}

impl PartialEq for FilterGroup {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator && self.items == other.items
    }
}

impl FilterGroup {
    pub fn new(operator: FilterGroupOperator, items: Vec<FilterItem>) -> Self {
        Self { operator, items }
    }
}

/// Node in a filter tree:
///
/// - `Group` — a nested boolean group (AND/OR).
/// - `Item` — a leaf filter on a single member.
/// - `Segment` — a segment-based filter (bool expression named in
///   the data model).
#[derive(Clone, PartialEq)]
pub enum FilterItem {
    Group(Rc<FilterGroup>),
    Item(Rc<BaseFilter>),
    Segment(Rc<BaseSegment>),
}

/// Top-level filter tree of a query — its `items` are implicitly
/// AND-joined.
#[derive(Clone)]
pub struct Filter {
    pub items: Vec<FilterItem>,
}

impl fmt::Display for FilterGroupOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FilterGroupOperator::Or => write!(f, "OR"),
            FilterGroupOperator::And => write!(f, "AND"),
        }
    }
}

impl Filter {
    /// All members referenced anywhere in the filter tree, flattened
    /// recursively through groups.
    pub fn all_member_evaluators(&self) -> Vec<Rc<MemberSymbol>> {
        let mut result = Vec::new();
        for item in self.items.iter() {
            item.find_all_member_evaluators(&mut result);
        }
        result
    }

    /// Collapses the filter into a single `FilterItem`: `None` when
    /// empty, the only item directly when one is present, or an
    /// AND-`Group` wrapping the rest.
    pub fn to_filter_item(&self) -> Option<FilterItem> {
        if self.items.is_empty() {
            None
        } else if self.items.len() == 1 {
            Some(self.items[0].clone())
        } else {
            Some(FilterItem::Group(Rc::new(FilterGroup::new(
                FilterGroupOperator::And,
                self.items.clone(),
            ))))
        }
    }
}

impl FilterItem {
    pub fn all_member_evaluators(&self) -> Vec<Rc<MemberSymbol>> {
        let mut result = Vec::new();
        self.find_all_member_evaluators(&mut result);
        result
    }

    pub fn find_all_member_evaluators(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        match self {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    item.find_all_member_evaluators(result)
                }
            }
            FilterItem::Item(item) => result.push(item.member_evaluator().clone()),
            FilterItem::Segment(item) => result.push(item.member_evaluator().clone()),
        }
    }

    // Extracts all member symbols from this filter tree.
    // Returns None when the tree is invalid (e.g. an empty group, or
    // anything below a Segment item), Some(members) otherwise.
    fn extract_filter_members(&self) -> Option<Vec<Rc<MemberSymbol>>> {
        match self {
            FilterItem::Group(group) => {
                // Empty groups are considered invalid
                if group.items.is_empty() {
                    return None;
                }

                let mut all_members = Vec::new();

                // Recursively extract from all children
                for child in &group.items {
                    match child.extract_filter_members() {
                        None => return None, // If any child is invalid, entire tree is invalid
                        Some(mut members) => all_members.append(&mut members),
                    }
                }

                Some(all_members)
            }
            FilterItem::Item(item) => Some(vec![item.member_evaluator().clone()]),
            FilterItem::Segment(_) => None,
        }
    }

    /// Returns the largest subtree that only references the given
    /// `target_members`, or `None` if no such subtree exists. Only
    /// AND groups are traversed for partial matching — OR groups
    /// either match as a whole or are dropped.
    pub fn find_subtree_for_members(&self, target_members: &[&String]) -> Option<FilterItem> {
        match self {
            FilterItem::Group(group) => {
                // Empty groups return None
                if group.items.is_empty() {
                    return None;
                }

                // Extract all members from this filter subtree
                let filter_members = self.extract_filter_members()?;

                // Check if all members in this filter are in the target set
                let all_members_match = filter_members.iter().all(|member| {
                    target_members.iter().any(|target| {
                        &&member.clone().resolve_reference_chain().full_name() == target
                    })
                });

                if all_members_match {
                    // All members match - return this entire filter subtree
                    return Some(self.clone());
                }

                // Only process AND groups for partial matching
                if group.operator == FilterGroupOperator::And {
                    let matching_children: Vec<FilterItem> = group
                        .items
                        .iter()
                        .filter_map(|child| child.find_subtree_for_members(target_members))
                        .collect();

                    if matching_children.is_empty() {
                        return None;
                    }

                    if matching_children.len() == 1 {
                        // Single match - return it directly without wrapping
                        return Some(matching_children.into_iter().next().unwrap());
                    }

                    // Multiple matches - wrap in new AND group
                    return Some(FilterItem::Group(Rc::new(FilterGroup::new(
                        FilterGroupOperator::And,
                        matching_children,
                    ))));
                }

                // OR groups are not supported
                None
            }
            FilterItem::Item(item) => {
                let member = item.member_evaluator();

                // Check if this item's member is in the target set
                if target_members
                    .iter()
                    .any(|target| &&member.clone().resolve_reference_chain().full_name() == target)
                {
                    Some(self.clone())
                } else {
                    None
                }
            }
            FilterItem::Segment(_) => None,
        }
    }

    /// Collects the set of allowed values for `symbol` from the
    /// filter tree.
    ///
    /// - `None` — no restriction is placed on `symbol`.
    /// - `Some(vec![])` — restrictions exist but contradict each
    ///   other (empty set).
    /// - `Some(values)` — explicit list of values the symbol may
    ///   take. AND groups intersect, OR groups union.
    pub fn find_value_restriction(&self, symbol: &Rc<MemberSymbol>) -> Option<Vec<String>> {
        match self {
            FilterItem::Item(item) => {
                // Check if this filter item applies to the target symbol
                if &item.member_evaluator().resolve_reference_chain() == symbol {
                    item.get_value_restrictions()
                } else {
                    None
                }
            }

            FilterItem::Group(group) => match group.operator {
                FilterGroupOperator::Or => {
                    // OR logic: collect all possible values from all branches
                    let mut all_values = Vec::new();
                    let mut found_any_restriction = false;

                    for child in &group.items {
                        match child.find_value_restriction(symbol) {
                            None => {
                                // This branch has no restrictions - OR makes entire result unrestricted
                                return None;
                            }
                            Some(values) => {
                                found_any_restriction = true;
                                // Add all values from this branch to our collection
                                all_values.extend(values);
                            }
                        }
                    }

                    if found_any_restriction {
                        // Remove duplicates and return combined value set
                        all_values.sort();
                        all_values.dedup();
                        Some(all_values)
                    } else {
                        None
                    }
                }

                FilterGroupOperator::And => {
                    // AND logic: find intersection of all restrictions
                    let mut result_values: Option<Vec<String>> = None;

                    for child in &group.items {
                        if let Some(values) = child.find_value_restriction(symbol) {
                            match &result_values {
                                None => {
                                    // First restriction found - use it as base
                                    result_values = Some(values);
                                }
                                Some(existing) => {
                                    // Find intersection with existing restrictions
                                    let intersection: Vec<String> = existing
                                        .iter()
                                        .filter(|v| values.contains(v))
                                        .cloned()
                                        .collect();

                                    if intersection.is_empty() {
                                        // Contradiction: no values satisfy all conditions
                                        return Some(vec![]);
                                    }

                                    result_values = Some(intersection);
                                }
                            }
                        }
                        // If child has no restrictions, AND logic continues with existing restrictions
                    }

                    result_values
                }
            },
            FilterItem::Segment(_) => {
                // Segments don't provide value restrictions for individual symbols
                None
            }
        }
    }
}
