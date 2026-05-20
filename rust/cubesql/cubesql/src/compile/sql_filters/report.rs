//! Reading the filters a planned query reports, and the identity of a
//! filter as the request states it.

use super::{
    resolve::{MetaMember, ValueKind},
    FilterKey, MAX_FILTER_NODES,
};
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
use chrono::{Duration, NaiveDateTime};
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

    // A range of one date is a one-item `IN`, which is how it is written back;
    // a range needs two bounds, and a relative one is not a date
    let one_date = matches!(filter.values.as_deref(), Some([value]) if is_iso_date_like(value));
    if filter.operator.as_deref() == Some("inDateRange") && one_date {
        filter.operator = Some("equals".to_string());
    }

    let operator = filter.operator.clone();
    filter.values = filter.values.map(|values| {
        values
            .iter()
            .enumerate()
            .map(|(i, value)| report_date_value(value, is_range_upper(operator.as_deref(), i)))
            .collect()
    });
    filter
}

/// Whether the value is the upper bound of a date range, where a bare date
/// stands for the end of its day rather than its start.
fn is_range_upper(operator: Option<&str>, index: usize) -> bool {
    index == 1 && matches!(operator, Some("inDateRange" | "notInDateRange"))
}

/// A date at the start of its day, or at its end as a range's upper bound, is
/// reported as the date alone; any other value is reported as it stands.
pub(super) fn report_date_value(value: &str, range_upper: bool) -> String {
    let normalized = if range_upper {
        normalize_range_upper(value)
    } else {
        normalize_filter_value(value)
    };
    if normalized.len() == DATE_LENGTH {
        normalized
    } else {
        value.to_string()
    }
}

/// A range's upper bound as `/v1/load` writes it: a bare date is the whole of
/// its day, `T23:59:59.999`.
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

/// Extracts the Cube filters of every CubeScan of a plan, CTEs and subqueries
/// included; a time dimension's range is an `inDateRange` filter.
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

    pub(super) fn of(filters: &[V1LoadRequestQueryFilterItem], ctx: &MetaContext) -> Self {
        let mut per_member = HashMap::new();
        for filter in filters {
            for member in filter_members(filter) {
                *per_member.entry(member).or_insert(0) += 1;
            }
        }
        Self {
            keys: filters
                .iter()
                .map(|filter| exact_filter_key(filter, ctx))
                .collect(),
            per_member,
        }
    }

    /// Whether the filter is one the query reports.
    pub(super) fn holds(&self, filter: &V1LoadRequestQueryFilterItem, ctx: &MetaContext) -> bool {
        self.keys.contains(&exact_filter_key(filter, ctx))
    }

    /// Whether the query reports the filter and nothing else on its member, so
    /// that removing every predicate on the column removes this filter alone.
    pub(super) fn is_sole_on_member(
        &self,
        filter: &V1LoadRequestQueryFilterItem,
        ctx: &MetaContext,
    ) -> bool {
        if !self.keys.contains(&exact_filter_key(filter, ctx)) {
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

/// Whether the filter stands on one member, a numeric one.
fn filters_a_numeric_member(filter: &V1LoadRequestQueryFilterItem, ctx: &MetaContext) -> bool {
    filter
        .member
        .as_deref()
        .and_then(|member| member.split_once('.'))
        .and_then(|(cube, name)| MetaMember::get_from_ctx(ctx, cube, name).ok())
        .is_some_and(|member| member.value_kind() == ValueKind::Numeric)
}

/// A number in the form the plan reports it, so that `1.0` and `1e5` are the
/// `1` and `100000` it comes back as. An `i64` stays exact, as the plan reads
/// it, so two above 2^53 are not one; a larger integer is a float there too.
fn canonical_number(value: &str) -> String {
    let value = value.trim();
    // `-9223372036854775808` is a minus over a literal past `i64`, a float
    if value
        .strip_prefix('-')
        .unwrap_or(value)
        .parse::<i64>()
        .is_ok()
    {
        if let Ok(integer) = value.parse::<i64>() {
            return integer.to_string();
        }
    }
    match value.parse::<f64>() {
        Ok(number) if number.is_finite() => {
            let number = if number == 0.0 { 0.0 } else { number };
            number.to_string()
        }
        _ => value.to_string(),
    }
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

/// A filter as written, for deciding whether it is the very filter a query
/// reports: another form of a date is another value, but a number is one in
/// any form, as the plan reads it.
fn exact_filter_key(filter: &V1LoadRequestQueryFilterItem, ctx: &MetaContext) -> FilterKey {
    if !filters_a_numeric_member(filter, ctx) {
        return serde_json::to_string(filter).unwrap_or_default();
    }
    let canonical = V1LoadRequestQueryFilterItem {
        values: filter
            .values
            .as_ref()
            .map(|values| values.iter().map(|value| canonical_number(value)).collect()),
        ..filter.clone()
    };
    serde_json::to_string(&canonical).unwrap_or_default()
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
        Start(NaiveDateTime),
        Stop(NaiveDateTime),
    }

    let (Some(member), Some(operator), Some([value])) = (
        filter.member.as_deref(),
        filter.operator.as_deref(),
        filter.values.as_deref(),
    ) else {
        return false;
    };
    let Some(at) = instant(value, false) else {
        return false;
    };
    let millisecond = Duration::milliseconds(1);
    let end = match operator {
        "afterOrOnDate" => RangeEnd::Start(at),
        "beforeOrOnDate" => RangeEnd::Stop(at),
        "afterDate" => RangeEnd::Start(at + millisecond),
        "beforeDate" => RangeEnd::Stop(at - millisecond),
        _ => return false,
    };

    reported.iter().any(|range| {
        range.member.as_deref() == Some(member)
            && range.operator.as_deref() == Some("inDateRange")
            && match (&end, range.values.as_deref()) {
                (RangeEnd::Start(start), Some([reported, _])) => {
                    instant(reported, false) == Some(*start)
                }
                (RangeEnd::Stop(stop), Some([_, reported])) => {
                    instant(reported, true) == Some(*stop)
                }
                _ => false,
            }
    })
}

/// The instant a value stands for: a bare date is the start of its day, or its
/// last millisecond as a range's upper bound.
fn instant(value: &str, range_upper: bool) -> Option<NaiveDateTime> {
    let at = parse_date_str(value).ok()?;
    if range_upper && is_iso_date_like(value) && value.len() == DATE_LENGTH {
        return at.checked_add_signed(Duration::days(1) - Duration::milliseconds(1));
    }
    Some(at)
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

/// Canonical JSON of a filter as the engine reports it: single-member groups
/// unwrapped, multi-value LIKE-family filters expanded, same-kind nesting
/// flattened.
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
    let on_number = ctx.is_some_and(|ctx| filters_a_numeric_member(filter, ctx));
    let operator = filter.operator.as_deref();
    let values = filter
        .values
        .iter()
        .flatten()
        .enumerate()
        .map(|(i, value)| match (on_time, is_range_upper(operator, i)) {
            _ if on_number => canonical_number(value),
            (false, _) => value.clone(),
            (true, false) => normalize_filter_value(value),
            (true, true) => normalize_range_upper(value),
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "member": filter.member.as_deref().unwrap_or_default(),
        "operator": filter.operator.as_deref().unwrap_or_default(),
        "values": values,
    })
}

/// Reduces a date at the start of its day, as the planner spells `2024-01-01`,
/// to the date alone, which is what a bare date means everywhere but a range's
/// upper bound. Anything not shaped like a date is left alone.
pub(super) fn normalize_filter_value(value: &str) -> String {
    if !is_iso_date_like(value) {
        return value.to_string();
    }
    let value = strip_any_suffix(value, &["Z", "+00:00", "+0000"]);
    let value = strip_any_suffix(value, &[".000"]);
    strip_any_suffix(value, &["T00:00:00", " 00:00:00"]).to_string()
}

/// Reduces a range's upper bound at the end of its day, as this API pads it,
/// to the date alone, which is what a bare date means there.
fn normalize_range_upper(value: &str) -> String {
    if !is_iso_date_like(value) {
        return value.to_string();
    }
    let value = strip_any_suffix(value, &["Z", "+00:00", "+0000"]);
    strip_any_suffix(value, &["T23:59:59.999", " 23:59:59.999"]).to_string()
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
