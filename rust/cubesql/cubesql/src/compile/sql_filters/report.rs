//! Reading the filters a planned query reports, and the identity of a
//! filter as the request states it.

use super::{resolve::MetaMember, FilterKey, MAX_FILTER_NODES};
use crate::{
    compile::{
        date_parser::parse_date_str,
        engine::df::{
            scan::CubeScanNode,
            wrapper::{CubeScanWrappedSqlNode, CubeScanWrapperNode},
        },
    },
    transport::MetaContext,
    CubeError,
};
use chrono::Duration;
use cubeclient::models::V1LoadRequestQueryFilterItem;
use datafusion::logical_plan::{LogicalPlan, PlanVisitor};
use std::collections::{HashMap, HashSet};

/// Reduces a time member's values back to the form they were written in, from
/// the planner's `2020-01-01T00:00:00.000Z`; by member, not operator.
fn report_filter(
    mut filter: V1LoadRequestQueryFilterItem,
    ctx: &MetaContext,
) -> V1LoadRequestQueryFilterItem {
    if let Some(items) = filter.and.take() {
        filter.and = Some(report_group_items(items, ctx));
        return filter;
    }
    if let Some(items) = filter.or.take() {
        filter.or = Some(report_group_items(items, ctx));
        return filter;
    }

    if !filters_a_time_member(&filter, ctx) {
        return filter;
    }

    filter.values = filter.values.map(|values| {
        values
            .iter()
            .map(|value| report_date_value(value))
            .collect()
    });
    filter
}

/// A date at the start or the end of its day is reported as the date alone, as a
/// query writes it; a value carrying a real time is reported as it stands.
pub(super) fn report_date_value(value: &str) -> String {
    let normalized = normalize_filter_value(value);
    if normalized.len() == DATE_LENGTH {
        normalized
    } else {
        value.to_string()
    }
}

/// The upper bound of a date range as the REST API reads it: a bare date
/// stands for the whole of that day, which `/v1/load` renders as
/// `T23:59:59.999`. Written the same way here, so that the two paths filter
/// the same rows; [`report_date_value`] folds it back to the date.
pub(super) fn date_range_upper(value: &str) -> String {
    if is_iso_date_like(value) && value.len() == DATE_LENGTH {
        format!("{}T23:59:59.999", value)
    } else {
        value.to_string()
    }
}

/// The length of an ISO date, `YYYY-MM-DD`.
const DATE_LENGTH: usize = 10;

fn report_group_items(items: Vec<serde_json::Value>, ctx: &MetaContext) -> Vec<serde_json::Value> {
    items
        .into_iter()
        .map(
            |item| match serde_json::from_value::<V1LoadRequestQueryFilterItem>(item.clone()) {
                Ok(item_filter) => {
                    serde_json::to_value(report_filter(item_filter, ctx)).unwrap_or(item)
                }
                Err(_) => item,
            },
        )
        .collect()
}

/// Extracts Cube filters from all CubeScan nodes of a logical plan, including
/// those of CTEs and subqueries. Time dimension date ranges are represented as
/// `inDateRange` filter items, and date values are reported in the form the
/// query wrote them.
pub fn extract_filters_from_plan(
    plan: &LogicalPlan,
    ctx: &MetaContext,
) -> Result<Vec<V1LoadRequestQueryFilterItem>, CubeError> {
    struct CollectFiltersVisitor<'ctx>(Vec<V1LoadRequestQueryFilterItem>, &'ctx MetaContext);

    impl PlanVisitor for CollectFiltersVisitor<'_> {
        type Error = CubeError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
            if let LogicalPlan::Extension(ext) = plan {
                if let Some(scan_node) = ext.node.as_any().downcast_ref::<CubeScanNode>() {
                    if let Some(filters) = &scan_node.request.filters {
                        let ctx = self.1;
                        self.0.extend(
                            filters
                                .iter()
                                .cloned()
                                .map(|filter| report_filter(filter, ctx)),
                        );
                    }
                    for time_dimension in scan_node.request.time_dimensions.iter().flatten() {
                        let Some(date_range) = &time_dimension.date_range else {
                            continue;
                        };
                        let values = match date_range {
                            serde_json::Value::Array(values) => values
                                .iter()
                                .filter_map(|value| value.as_str().map(|s| s.to_string()))
                                .collect(),
                            serde_json::Value::String(value) => vec![value.clone()],
                            _ => continue,
                        };
                        self.0.push(report_filter(
                            V1LoadRequestQueryFilterItem {
                                member: Some(time_dimension.dimension.clone()),
                                operator: Some("inDateRange".to_string()),
                                values: Some(values),
                                ..Default::default()
                            },
                            self.1,
                        ));
                    }
                } else if let Some(wrapper_node) =
                    ext.node.as_any().downcast_ref::<CubeScanWrapperNode>()
                {
                    wrapper_node.wrapped_plan.accept(self)?;
                } else if let Some(wrapper_node) =
                    ext.node.as_any().downcast_ref::<CubeScanWrappedSqlNode>()
                {
                    wrapper_node.wrapped_plan.accept(self)?;
                }
            }
            Ok(true)
        }
    }

    let mut visitor = CollectFiltersVisitor(Vec::new(), ctx);
    // The extracted set is used as the verification oracle, so a partial
    // collection must not be mistaken for a complete one
    plan.accept(&mut visitor)?;

    let mut seen = HashSet::new();
    Ok(visitor
        .0
        .into_iter()
        .filter(|filter| seen.insert(filter_key(filter, ctx)))
        .collect())
}

/// The filters a query reports, which is what decides whether a filter this
/// API cannot find by value in the query is one the query holds all the same.
pub(super) struct ReportedFilters {
    keys: HashSet<FilterKey>,
    /// How many of the reported filters stand on each member, a group
    /// counting once for every member it holds.
    per_member: HashMap<String, usize>,
}

impl ReportedFilters {
    /// For a modification that matches nothing already in the query, and so
    /// has no use for what it reports.
    pub(super) fn none() -> Self {
        Self {
            keys: HashSet::new(),
            per_member: HashMap::new(),
        }
    }

    pub(super) fn of(filters: &[V1LoadRequestQueryFilterItem]) -> Self {
        let mut per_member = HashMap::new();
        for filter in filters {
            for member in filter_members(filter) {
                *per_member.entry(member).or_insert(0) += 1;
            }
        }
        Self {
            keys: filters.iter().map(exact_filter_key).collect(),
            per_member,
        }
    }

    /// Whether the query reports the filter and nothing else on its member, so
    /// that removing every predicate on the column removes this filter alone.
    pub(super) fn is_sole_on_member(&self, filter: &V1LoadRequestQueryFilterItem) -> bool {
        if !self.keys.contains(&exact_filter_key(filter)) {
            return false;
        }
        let members = filter_members(filter);
        let [member] = &members.into_iter().collect::<Vec<_>>()[..] else {
            return false;
        };
        self.per_member.get(member) == Some(&1)
    }
}

/// Whether any member the filter stands on holds a time, and so carries
/// values the planner reports in a form the query need not have written.
pub(super) fn filters_a_time_member(
    filter: &V1LoadRequestQueryFilterItem,
    ctx: &MetaContext,
) -> bool {
    filter_members(filter).iter().any(|member| {
        member
            .split_once('.')
            .and_then(|(cube, name)| MetaMember::get_from_ctx(ctx, cube, name).ok())
            .is_some_and(|member| member.is_time())
    })
}

/// Whether every member the filter stands on holds a time. A literal is
/// reduced as a date only then: in a group mixing a time member with another,
/// the other member's `'2024-01-01'` and `'2024-01-01T00:00:00.000Z'` differ.
pub(super) fn filters_only_time_members(
    filter: &V1LoadRequestQueryFilterItem,
    ctx: &MetaContext,
) -> bool {
    let members = filter_members(filter);
    !members.is_empty()
        && members.iter().all(|member| {
            member
                .split_once('.')
                .and_then(|(cube, name)| MetaMember::get_from_ctx(ctx, cube, name).ok())
                .is_some_and(|member| member.is_time())
        })
}

/// The members a filter stands on, those of a group included.
fn filter_members(filter: &V1LoadRequestQueryFilterItem) -> HashSet<String> {
    let mut members = HashSet::new();
    let mut pending = vec![normalized_filter_json(filter, None)];
    let mut visited = 0;

    while let Some(item) = pending.pop() {
        visited += 1;
        if visited > MAX_FILTER_NODES {
            break;
        }
        if let Some(member) = item.get("member").and_then(|member| member.as_str()) {
            members.insert(member.to_string());
        }
        for group in ["and", "or"] {
            if let Some(items) = item.get(group).and_then(|items| items.as_array()) {
                pending.extend(items.iter().cloned());
            }
        }
    }

    members
}

/// The filters with every repeat of an earlier one dropped, first occurrence
/// kept in place.
pub(super) fn dedupe_filters(
    filters: &[V1LoadRequestQueryFilterItem],
    ctx: &MetaContext,
) -> Vec<V1LoadRequestQueryFilterItem> {
    let mut seen = HashSet::new();
    filters
        .iter()
        .filter(|filter| seen.insert(filter_key(filter, ctx)))
        .cloned()
        .collect()
}

/// A filter exactly as written, for deciding whether it is the very filter a
/// query reports. Unlike [`filter_key`] this reduces nothing: another form of
/// a value is another value, which the column path has to be sure of.
fn exact_filter_key(filter: &V1LoadRequestQueryFilterItem) -> FilterKey {
    serde_json::to_string(filter).unwrap_or_default()
}

/// A canonical representation of a filter (or an and/or filter group)
/// used for perfect-match comparison. Date values are reduced on time members
/// alone, as reporting reduces them.
pub(super) fn filter_key(filter: &V1LoadRequestQueryFilterItem, ctx: &MetaContext) -> FilterKey {
    normalized_filter_json(filter, Some(ctx)).to_string()
}

/// Keys that have to be present in the filters of a plan for the filter to
/// count as applied. A top-level `and` group is flattened by the rewrite
/// engine into sibling filters, so it is verified through its members.
pub(super) fn verification_keys(
    filter: &V1LoadRequestQueryFilterItem,
    ctx: &MetaContext,
) -> Vec<FilterKey> {
    let json = normalized_filter_json(filter, Some(ctx));
    match json.get("and").and_then(|items| items.as_array()) {
        Some(items) => items.iter().map(|item| item.to_string()).collect(),
        None => vec![json.to_string()],
    }
}

/// Whether a bound is applied as an end of a reported range on its member,
/// which is how the engine reports two bounds; an exclusive bound is moved
/// past the instant it excludes, as the engine moves it.
pub(super) fn bound_folded_into_range(
    filter: &V1LoadRequestQueryFilterItem,
    reported: &[V1LoadRequestQueryFilterItem],
) -> bool {
    enum RangeEnd {
        Start(String),
        Stop(String),
    }

    let (Some(member), Some(operator), Some([value])) = (
        filter.member.as_deref(),
        filter.operator.as_deref(),
        filter.values.as_deref(),
    ) else {
        return false;
    };
    let end = match operator {
        "afterOrOnDate" => RangeEnd::Start(normalize_filter_value(value)),
        "beforeOrOnDate" => RangeEnd::Stop(normalize_filter_value(value)),
        "afterDate" => match shifted_by_millisecond(value, 1) {
            Some(start) => RangeEnd::Start(start),
            None => return false,
        },
        "beforeDate" => match shifted_by_millisecond(value, -1) {
            Some(stop) => RangeEnd::Stop(stop),
            None => return false,
        },
        _ => return false,
    };

    reported.iter().any(|range| {
        range.member.as_deref() == Some(member)
            && range.operator.as_deref() == Some("inDateRange")
            && match (&end, range.values.as_deref()) {
                (RangeEnd::Start(start), Some([reported, _])) => {
                    normalize_filter_value(reported) == *start
                }
                (RangeEnd::Stop(stop), Some([_, reported])) => {
                    normalize_filter_value(reported) == *stop
                }
                _ => false,
            }
    })
}

/// The value moved by a millisecond, in the form a reported range end takes.
fn shifted_by_millisecond(value: &str, by: i64) -> Option<String> {
    let shifted = parse_date_str(value)
        .ok()?
        .checked_add_signed(Duration::milliseconds(by))?;
    Some(normalize_filter_value(
        &shifted.format("%Y-%m-%dT%H:%M:%S%.3f").to_string(),
    ))
}

/// LIKE-family operators, which take any number of values.
const LIKE_FAMILY_OPERATORS: [&str; 6] = [
    "contains",
    "notContains",
    "startsWith",
    "notStartsWith",
    "endsWith",
    "notEndsWith",
];

/// The canonical JSON of a filter as the rewrite engine reports one, at every
/// depth: a single-member group is its member, a multi-value LIKE-family
/// filter is a group of single-value ones (OR; AND when negated), same-kind
/// nesting is flattened.
/// Without a context no value is reduced, which is enough for reading members.
fn normalized_filter_json(
    filter: &V1LoadRequestQueryFilterItem,
    ctx: Option<&MetaContext>,
) -> serde_json::Value {
    let group = filter
        .and
        .as_ref()
        .map(|items| ("and", items))
        .or_else(|| filter.or.as_ref().map(|items| ("or", items)));

    if let Some((op, items)) = group {
        let items = items
            .iter()
            .map(|item| {
                match serde_json::from_value::<V1LoadRequestQueryFilterItem>(item.clone()) {
                    Ok(item_filter) => normalized_filter_json(&item_filter, ctx),
                    Err(_) => item.clone(),
                }
            })
            .collect::<Vec<_>>();

        if let [single] = &items[..] {
            return single.clone();
        }

        let mut flattened = Vec::with_capacity(items.len());
        for item in items {
            match item.get(op).and_then(|nested| nested.as_array()) {
                Some(nested) => flattened.extend(nested.iter().cloned()),
                None => flattened.push(item),
            }
        }
        return serde_json::json!({ op: flattened });
    }

    if let Some(expanded) = like_family_expansion(filter, ctx) {
        return expanded;
    }

    leaf_filter_json(filter, ctx)
}

/// Expands a LIKE-family filter of several values into the group of
/// single-value filters it is emitted as. Returns `None` for anything else.
fn like_family_expansion(
    filter: &V1LoadRequestQueryFilterItem,
    ctx: Option<&MetaContext>,
) -> Option<serde_json::Value> {
    let operator = filter.operator.as_deref()?;
    let values = filter.values.as_ref()?;
    if values.len() < 2 || !LIKE_FAMILY_OPERATORS.contains(&operator) {
        return None;
    }

    let items = values
        .iter()
        .map(|value| {
            leaf_filter_json(
                &V1LoadRequestQueryFilterItem {
                    member: filter.member.clone(),
                    operator: Some(operator.to_string()),
                    values: Some(vec![value.clone()]),
                    ..Default::default()
                },
                ctx,
            )
        })
        .collect::<Vec<_>>();

    // The negated operators chain with AND, the plain ones with OR
    let op = if operator.starts_with("not") {
        "and"
    } else {
        "or"
    };
    Some(serde_json::json!({ op: items }))
}

fn leaf_filter_json(
    filter: &V1LoadRequestQueryFilterItem,
    ctx: Option<&MetaContext>,
) -> serde_json::Value {
    let on_time = ctx.is_some_and(|ctx| filters_a_time_member(filter, ctx));
    let values = filter
        .values
        .iter()
        .flatten()
        .map(|value| {
            if on_time {
                normalize_filter_value(value)
            } else {
                value.clone()
            }
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "member": filter.member.as_deref().unwrap_or_default(),
        "operator": filter.operator.as_deref().unwrap_or_default(),
        "values": values,
    })
}

/// Reduces a date at the start or the end of its day, as the planner spells
/// `2024-01-01` and as this API pads a range's upper bound, to the date it
/// stands for. Anything not shaped like a date is left alone.
pub(super) fn normalize_filter_value(value: &str) -> String {
    if !is_iso_date_like(value) {
        return value.to_string();
    }

    let value = strip_any_suffix(value, &["Z", "+00:00", "+0000"]);
    let value = strip_any_suffix(value, &[".000"]);
    let value = strip_any_suffix(
        value,
        &["T00:00:00", " 00:00:00", "T23:59:59.999", " 23:59:59.999"],
    );
    value.to_string()
}

fn strip_any_suffix<'a>(value: &'a str, suffixes: &[&str]) -> &'a str {
    suffixes
        .iter()
        .find_map(|suffix| value.strip_suffix(suffix))
        .unwrap_or(value)
}

/// Whether the value starts with an ISO-8601 date, optionally followed by a
/// time part.
fn is_iso_date_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 10
        && bytes[..10].iter().enumerate().all(|(i, b)| match i {
            4 | 7 => *b == b'-',
            _ => b.is_ascii_digit(),
        })
        && (bytes.len() == 10 || bytes[10] == b'T' || bytes[10] == b' ')
}

/// A short human-readable filter description for error messages.
pub(super) fn filter_description(filter: &V1LoadRequestQueryFilterItem) -> String {
    if let Some(member) = &filter.member {
        format!("on \"{}\"", member)
    } else if filter.and.is_some() {
        "group \"and\"".to_string()
    } else if filter.or.is_some() {
        "group \"or\"".to_string()
    } else {
        "".to_string()
    }
}
