use crate::{
    query_message_parser::QueryResult,
    transport::{
        AnnotatedConfigItem, ConfigItem, MemberOrMemberExpression, MembersMap, NormalizedQuery,
        QueryTimeDimension, QueryType, ResultType, TransformDataRequest,
    },
};
use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use indexmap::{Equivalent, IndexMap};
use itertools::multizip;
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;
use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    fmt::Display,
    hash::{BuildHasher, Hash, Hasher},
    sync::{Arc, LazyLock},
};

pub const COMPARE_DATE_RANGE_FIELD: &str = "compareDateRange";
pub const COMPARE_DATE_RANGE_SEPARATOR: &str = " - ";
pub const BLENDING_QUERY_KEY_PREFIX: &str = "time.";
pub const BLENDING_QUERY_RES_SEPARATOR: &str = ".";
pub const MEMBER_SEPARATOR: &str = ".";

pub static GRANULARITY_LEVELS: LazyLock<HashMap<&'static str, u8>> = LazyLock::new(|| {
    HashMap::from([
        ("second", 1),
        ("minute", 2),
        ("hour", 3),
        ("day", 4),
        ("week", 5),
        ("month", 6),
        ("quarter", 7),
        ("year", 8),
    ])
});
const DEFAULT_LEVEL_FOR_UNKNOWN: u8 = 10;

/// IndexMap key whose hash is computed once at construction. Combined with
/// [`PrehashedBuildHasher`], this makes per-row `insert` skip the SipHash13
/// pass over the string bytes — the hasher just stores and returns the
/// pre-computed `u64`.
pub struct InternedKey {
    hash: u64,
    text: Box<str>,
}

impl InternedKey {
    pub fn new(text: &str) -> Self {
        Self {
            hash: hash_str(text),
            text: text.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}

fn hash_str(s: &str) -> u64 {
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}

impl Hash for InternedKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl PartialEq for InternedKey {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.text == other.text
    }
}
impl Eq for InternedKey {}

impl std::fmt::Debug for InternedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.text, f)
    }
}

impl std::fmt::Display for InternedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

impl Serialize for InternedKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.text)
    }
}

impl<'de> Deserialize<'de> for InternedKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text: String = String::deserialize(deserializer)?;
        Ok(InternedKey::new(&text))
    }
}

/// Lookup key for `IndexMap<Arc<InternedKey>, V, PrehashedBuildHasher>` that
/// avoids allocating an `Arc<InternedKey>` per lookup when the caller only has
/// a borrowed `&str` (e.g. per-cell `field_name` lookups from the SQL scan
/// path in `cubejs-backend-native`). Computes the hash of the borrowed `&str`
/// once at construction.
pub struct InternedKeyLookup<'a> {
    hash: u64,
    text: &'a str,
}

impl<'a> InternedKeyLookup<'a> {
    pub fn new(text: &'a str) -> Self {
        Self {
            hash: hash_str(text),
            text,
        }
    }
}

impl Hash for InternedKeyLookup<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl Equivalent<Arc<InternedKey>> for InternedKeyLookup<'_> {
    fn equivalent(&self, key: &Arc<InternedKey>) -> bool {
        self.hash == key.hash && self.text == key.as_str()
    }
}

/// Pass-through [`BuildHasher`] for IndexMaps keyed by [`InternedKey`] /
/// [`InternedKeyLookup`]: takes the `u64` they emit and returns it unchanged.
#[derive(Default, Clone)]
pub struct PrehashedBuildHasher;

impl BuildHasher for PrehashedBuildHasher {
    type Hasher = PrehashedHasher;

    fn build_hasher(&self) -> PrehashedHasher {
        PrehashedHasher(0)
    }
}

pub struct PrehashedHasher(u64);

impl Hasher for PrehashedHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("PrehashedHasher only accepts pre-computed u64 hashes via write_u64");
    }

    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

pub type VanillaRow = IndexMap<Arc<InternedKey>, DBResponsePrimitive, PrehashedBuildHasher>;

pub fn empty_vanilla_row(capacity: usize) -> VanillaRow {
    IndexMap::with_capacity_and_hasher(capacity, PrehashedBuildHasher)
}

/// Transform specified `value` with specified `type` to the network protocol type.
pub fn transform_value(value: DBResponsePrimitive, type_: &str) -> DBResponsePrimitive {
    match value {
        DBResponsePrimitive::String(ref s) if type_ == "time" => {
            let formatted = DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f %Z").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f %:z").map(|dt| {
                        Utc.from_utc_datetime(&dt)
                            .format("%Y-%m-%dT%H:%M:%S%.3f")
                            .to_string()
                    })
                })
                .unwrap_or_else(|_| s.clone());
            DBResponsePrimitive::String(formatted)
        }
        other => other,
    }
}

/// Parse date range value from time dimension.
pub fn get_date_range_value(
    time_dimensions: Option<&Vec<QueryTimeDimension>>,
) -> Result<DBResponsePrimitive> {
    let time_dimensions = match time_dimensions {
        Some(time_dimensions) => time_dimensions,
        None => bail!("QueryTimeDimension should be specified for the compare date range query."),
    };

    let dim = match time_dimensions.first() {
        Some(dim) => dim,
        None => bail!("No time dimension provided."),
    };

    let date_range: &Vec<String> = match &dim.date_range {
        Some(date_range) => date_range,
        None => bail!("Inconsistent QueryTimeDimension configuration: dateRange required."),
    };

    if date_range.len() == 1 {
        bail!(
            "Inconsistent dateRange configuration for the compare date range query: {}",
            date_range[0]
        );
    }

    Ok(DBResponsePrimitive::String(
        date_range.join(COMPARE_DATE_RANGE_SEPARATOR),
    ))
}

/// Parse blending query key from time dimension for query.
pub fn get_blending_query_key(time_dimensions: Option<&Vec<QueryTimeDimension>>) -> Result<String> {
    let dim = time_dimensions
        .and_then(|dims| dims.first().cloned())
        .context("QueryTimeDimension should be specified for the blending query.")?;

    let granularity = dim
        .granularity.clone()
        .context(format!(
            "Inconsistent QueryTimeDimension configuration for the blending query, granularity required: {:?}",
            dim
        ))?;

    Ok(format!("{}{}", BLENDING_QUERY_KEY_PREFIX, granularity))
}

/// Parse blending query key from time dimension for response.
pub fn get_blending_response_key(
    time_dimensions: Option<&Vec<QueryTimeDimension>>,
) -> Result<String> {
    let dim = time_dimensions
        .and_then(|dims| dims.first().cloned())
        .context("QueryTimeDimension should be specified for the blending query.")?;

    let granularity = dim
        .granularity.clone()
        .context(format!(
            "Inconsistent QueryTimeDimension configuration for the blending query, granularity required: {:?}",
            dim
        ))?;

    let dimension = dim.dimension.clone();

    Ok(format!(
        "{}{}{}",
        dimension, BLENDING_QUERY_RES_SEPARATOR, granularity
    ))
}

fn member_name(m: &MemberOrMemberExpression) -> Option<&str> {
    match m {
        MemberOrMemberExpression::Member(s) => Some(s.as_str()),
        MemberOrMemberExpression::ParsedMemberExpression(e) => Some(e.name.as_str()),
        MemberOrMemberExpression::MemberExpression(e) => Some(e.name.as_str()),
    }
}

fn ensure_member_in_annotation(
    member: &str,
    annotation: &HashMap<String, ConfigItem>,
) -> Result<()> {
    if !annotation.contains_key(member) {
        bail!(
            concat!(
                "You requested hidden member: '{}'. Please make it visible using `public: true`. ",
                "Please note primaryKey fields are `public: false` by default: ",
                "https://cube.dev/docs/schema/reference/joins#setting-a-primary-key."
            ),
            member
        );
    }
    Ok(())
}

/// When the query result is empty (no columns/rows), we still need to verify
/// that all requested members are present in the annotation.
/// This catches RBAC-denied members that would otherwise silently return empty data.
/// Note: segments are excluded because the annotation map only contains
/// measures, dimensions, and time dimensions.
fn validate_query_members_in_annotation(
    query: &NormalizedQuery,
    annotation: &HashMap<String, ConfigItem>,
) -> Result<()> {
    for m in query
        .measures
        .iter()
        .flat_map(|v| v.iter())
        .chain(query.dimensions.iter().flat_map(|v| v.iter()))
        .filter_map(member_name)
    {
        ensure_member_in_annotation(m, annotation)?;
    }

    // Only validate time dimensions that have a granularity set.
    // Time dimensions without granularity are used purely for date-range filtering
    // and don't produce result columns, so they are not present in the annotation map.
    for td in query
        .time_dimensions
        .iter()
        .flat_map(|v| v.iter())
        .filter(|td| td.granularity.is_some())
    {
        ensure_member_in_annotation(td.dimension.as_str(), annotation)?;
    }

    Ok(())
}

/// Parse member names from request/response.
pub fn get_members(
    query_type: &QueryType,
    query: &NormalizedQuery,
    db_data: &QueryResult,
    alias_to_member_name_map: &HashMap<String, String>,
    annotation: &HashMap<String, ConfigItem>,
) -> Result<(MembersMap, Vec<String>)> {
    let mut members_map: MembersMap = IndexMap::new();
    // IndexMap maintains insertion order, ensuring deterministic column ordering.
    // `db_data.columns` preserves the original database result order — for JS
    // input it's `JsRawColumnarData::members` (a `Vec` of column names in order).
    // Not sure if it solves the original comment below.
    // Original Comment:
    // Hashmaps don't guarantee the order of the elements while iterating
    // this fires in get_compact_row because members map doesn't hold the members for
    // date range queries, which are added later and thus columns in final recordset are not
    // in sync with the order of members in members list.
    let mut members_arr: Vec<String> = vec![];

    if db_data.members.is_empty() {
        validate_query_members_in_annotation(query, annotation)?;
        return Ok((members_map, members_arr));
    }

    // FIXME: For now custom granularities are not supported, only common ones.
    // There is no granularity type/class implementation in rust yet.
    let mut minimal_granularities: HashMap<String, (u8, String)> = HashMap::new();

    for column in db_data.members.iter() {
        let member_name = alias_to_member_name_map
            .get(column)
            .context(format!("Member name not found for alias: '{}'", column))?;

        ensure_member_in_annotation(member_name, annotation)?;

        members_map.insert(member_name.clone(), column.clone());
        members_arr.push(member_name.clone());

        let path = member_name.split(MEMBER_SEPARATOR).collect::<Vec<&str>>();
        let calc_member = format!("{}{}{}", path[0], MEMBER_SEPARATOR, path[1]);

        if path.len() == 3
            && query.dimensions.as_ref().is_none_or(|dims| {
                !dims
                    .iter()
                    .any(|dim| *dim == MemberOrMemberExpression::Member(calc_member.clone()))
            })
        {
            let granularity = path[2];
            // For cases when the same dimension with few different granularities is present
            //We should not duplicate the dimension without granularity
            let level = GRANULARITY_LEVELS
                .get(granularity)
                .cloned()
                .unwrap_or(DEFAULT_LEVEL_FOR_UNKNOWN);

            match minimal_granularities.get(&calc_member) {
                Some((existing_level, _)) if *existing_level < level => {}
                _ => {
                    minimal_granularities.insert(calc_member, (level, column.clone()));
                }
            }
        }
    }

    // Handle deprecated time dimensions without granularity
    for (member_name, (_, column)) in minimal_granularities {
        members_map.insert(member_name.clone(), column.clone());
        members_arr.push(member_name.clone());
    }

    match query_type {
        QueryType::CompareDateRangeQuery => {
            members_map.insert(
                COMPARE_DATE_RANGE_FIELD.to_string(),
                QueryType::CompareDateRangeQuery.to_string(),
            );
            members_arr.push(COMPARE_DATE_RANGE_FIELD.to_string());
        }
        QueryType::BlendingQuery => {
            let blending_key = get_blending_query_key(query.time_dimensions.as_ref())
                .context("Failed to generate blending query key")?;
            if let Some(dim) = query
                .time_dimensions
                .as_ref()
                .and_then(|dims| dims.first().cloned())
            {
                let val = members_map.get(&dim.dimension).unwrap();
                members_map.insert(blending_key.clone(), val.clone());
                members_arr.push(blending_key);
            }
        }
        _ => {}
    }

    Ok((members_map, members_arr))
}

/// One output cell in a compact row. Built once per request by
/// [`build_compact_plan`] so the per-row materializer ([`get_compact_row`])
/// only does a single bounds check (`column.get(row_idx)`) and the
/// [`transform_value`] call. The plan borrows the column slice directly,
/// eliminating the per-cell `db_data.data.get(col).and_then(...)` double
/// lookup the row-major loop would otherwise do on every cell.
pub(crate) enum CompactPlanEntry<'a> {
    /// Read `column[row_idx]` and run [`transform_value`]. `column` is a slice
    /// of the corresponding [`ColumnarArray`]; the fat pointer inlines
    /// `(ptr, len)` so the per-cell access avoids the extra Vec metadata
    /// indirection.
    Cell {
        column: &'a [DBResponsePrimitive],
        member_type: &'a str,
    },
    /// Constant value replicated across every row (the
    /// `compareDateRange` synthetic tail for [`QueryType::CompareDateRangeQuery`]).
    Constant(DBResponsePrimitive),
}

pub struct CompactPlan<'a> {
    entries: Vec<CompactPlanEntry<'a>>,
}

pub(crate) fn build_compact_plan<'a>(
    members: &[String],
    members_to_alias_map: &IndexMap<String, String>,
    annotation: &'a HashMap<String, ConfigItem>,
    cube_store_result: &'a QueryResult,
    query_type: &QueryType,
    time_dimensions: Option<&Vec<QueryTimeDimension>>,
) -> Result<CompactPlan<'a>> {
    let mut entries: Vec<CompactPlanEntry<'a>> = Vec::with_capacity(members.len());

    for m in members {
        if let Some(annotation_item) = annotation.get(m) {
            if let Some(alias) = members_to_alias_map.get(m) {
                if let Some(&column_index) = cube_store_result.columns_pos.get(alias) {
                    entries.push(CompactPlanEntry::Cell {
                        column: cube_store_result.data[column_index].as_slice(),
                        member_type: annotation_item.member_type.as_deref().unwrap_or(""),
                    });
                }
            }
        }
    }

    match query_type {
        QueryType::CompareDateRangeQuery => {
            entries.push(CompactPlanEntry::Constant(get_date_range_value(
                time_dimensions,
            )?));
        }
        QueryType::BlendingQuery => {
            let blending_key = get_blending_response_key(time_dimensions)?;
            if let Some(alias) = members_to_alias_map.get(&blending_key) {
                if let Some(&column_index) = cube_store_result.columns_pos.get(alias) {
                    // Preserve the (likely-quirky) lookup at the original
                    // `get_compact_row`: member_type comes from
                    // `annotation[alias]`, not `annotation[member]`.
                    let member_type = annotation
                        .get(alias)
                        .map_or("", |a| a.member_type.as_deref().unwrap_or(""));
                    let column = cube_store_result.data[column_index].as_slice();
                    entries.push(CompactPlanEntry::Cell {
                        column,
                        member_type,
                    });
                }
            }
        }
        _ => {}
    }

    Ok(CompactPlan { entries })
}

/// Convert DB response row to the compact output. The plan carries the
/// per-cell column slice directly, so this loop only does one bounds check
/// (`column.get(row_idx)`) per cell — no `db_data.data.get(col)` indirection.
pub fn get_compact_row(plan: &CompactPlan<'_>, row_idx: usize) -> Vec<DBResponsePrimitive> {
    let mut row: Vec<DBResponsePrimitive> = Vec::with_capacity(plan.entries.len());

    for entry in &plan.entries {
        match entry {
            CompactPlanEntry::Cell {
                column,
                member_type,
            } => {
                row.push(transform_value(column[row_idx].clone(), member_type));
            }
            CompactPlanEntry::Constant(v) => {
                row.push(v.clone());
            }
        }
    }

    row
}

/// Per-column information that is constant across all rows for a given request.
/// Built once and walked per row to avoid redoing hash lookups, annotation checks,
/// and member-name parsing for every cell. Holds the column slice directly so
/// the per-row materializer does one bounds check per cell instead of the
/// `db_data.data.get(col).and_then(...)` double lookup.
pub struct VanillaColumnPlan<'a> {
    /// Slice of the corresponding [`ColumnarArray`]. Fat pointer inlines
    /// `(ptr, len)`, so the per-cell access avoids the extra Vec metadata
    /// indirection.
    column: &'a [DBResponsePrimitive],
    /// Interned IndexMap key for this column with a pre-computed hash.
    /// Cloned via [`Arc::clone`] per row (atomic refcount inc).
    key: Arc<InternedKey>,
    member_type: &'a str,
}

pub(crate) struct VanillaGranularityTrack<'a> {
    /// Slice of `member_name` containing only the `{cube}.{dim}` prefix.
    base_member: &'a str,
    level: u8,
}

/// Resolved at plan time: for each deprecated-style base time dimension (one
/// that appears in the query only via `{cube}.{dim}.{granularity}` aliases),
/// the list of source columns whose value can be reused under the bare
/// `{cube}.{dim}` key. Candidates are kept in column-encounter order. At row
/// time we pick the lowest-level candidate whose value is actually present —
/// so a row missing the finest column still falls back to a coarser one, as
/// the previous per-row HashMap did. Ties resolve to the last column.
pub(crate) struct VanillaGranularityExtra {
    /// Interned IndexMap key for the bare `{cube}.{dim}` base member.
    /// Built once at plan time and cloned via [`Arc::clone`] per row.
    base_key: Arc<InternedKey>,
    candidates: Vec<(u8, Arc<InternedKey>)>,
}

pub struct VanillaPlan<'a> {
    columns: Vec<VanillaColumnPlan<'a>>,
    minimal_granularity_extras: Vec<VanillaGranularityExtra>,
    /// Pre-computed tail entry that depends only on the query, not the row.
    tail: VanillaTail,
}

enum VanillaTail {
    None,
    CompareDateRange {
        key: Arc<InternedKey>,
        value: DBResponsePrimitive,
    },
    Blending {
        blending_key: Arc<InternedKey>,
        /// Used only for lookup against the per-row map — never inserted.
        response_key: InternedKey,
    },
}

pub fn build_vanilla_plan<'a>(
    cube_store_result: &'a QueryResult,
    alias_to_member_name_map: &'a HashMap<String, String>,
    annotation: &'a HashMap<String, ConfigItem>,
    query: &NormalizedQuery,
    query_type: &QueryType,
) -> Result<VanillaPlan<'a>> {
    let mut columns = Vec::with_capacity(cube_store_result.columns_pos.len());
    let mut candidates_for_base: IndexMap<&'a str, Vec<(u8, Arc<InternedKey>)>> = IndexMap::new();

    for (alias, &index) in &cube_store_result.columns_pos {
        let member_name = match alias_to_member_name_map.get(alias) {
            Some(m) => m.as_str(),
            None => bail!("Missing member name for alias: {}", alias),
        };
        ensure_member_in_annotation(member_name, annotation)?;
        let annotation_for_member = annotation.get(member_name).unwrap();
        let member_type = annotation_for_member.member_type.as_deref().unwrap_or("");

        let key = Arc::new(InternedKey::new(member_name));

        if let Some(track) = compute_vanilla_granularity_track(member_name, query) {
            candidates_for_base
                .entry(track.base_member)
                .or_default()
                .push((track.level, Arc::clone(&key)));
        }

        let column = cube_store_result.data[index].as_slice();

        columns.push(VanillaColumnPlan {
            column,
            key,
            member_type,
        });
    }

    let minimal_granularity_extras = candidates_for_base
        .into_iter()
        .map(|(base_member, candidates)| VanillaGranularityExtra {
            base_key: Arc::new(InternedKey::new(base_member)),
            candidates,
        })
        .collect();

    let tail = match query_type {
        QueryType::CompareDateRangeQuery => VanillaTail::CompareDateRange {
            key: Arc::new(InternedKey::new(COMPARE_DATE_RANGE_FIELD)),
            value: get_date_range_value(query.time_dimensions.as_ref())?,
        },
        QueryType::BlendingQuery => VanillaTail::Blending {
            blending_key: Arc::new(InternedKey::new(&get_blending_query_key(
                query.time_dimensions.as_ref(),
            )?)),
            response_key: InternedKey::new(&get_blending_response_key(
                query.time_dimensions.as_ref(),
            )?),
        },
        _ => VanillaTail::None,
    };

    Ok(VanillaPlan {
        columns,
        minimal_granularity_extras,
        tail,
    })
}

// FIXME: For now custom granularities are not supported, only common ones.
// There is no granularity type/class implementation in rust yet.
fn compute_vanilla_granularity_track<'a>(
    member_name: &'a str,
    query: &NormalizedQuery,
) -> Option<VanillaGranularityTrack<'a>> {
    // Require exactly two `.` separators — i.e. the `{cube}.{dim}.{granularity}` form.
    let mut indices = member_name.match_indices(MEMBER_SEPARATOR);
    indices.next()?;

    let second = indices.next()?.0;
    if indices.next().is_some() {
        return None;
    }

    let base_member = &member_name[..second];
    let granularity = &member_name[second + MEMBER_SEPARATOR.len()..];

    // Check that a member without granularity is absent in the query
    let already_requested = query.dimensions.as_ref().is_some_and(|dims| {
        dims.iter()
            .any(|dim| matches!(dim, MemberOrMemberExpression::Member(s) if s == base_member))
    });
    if already_requested {
        return None;
    }

    let level = GRANULARITY_LEVELS
        .get(granularity)
        .cloned()
        .unwrap_or(DEFAULT_LEVEL_FOR_UNKNOWN);
    Some(VanillaGranularityTrack { base_member, level })
}

/// Source for one output column when materializing the [`TransformedData::Columnar`] result.
pub(crate) enum ColumnarColumnSource {
    /// Pull `db_row[index]` from every input row and run [`transform_value`].
    DbColumn { index: usize },
    /// Constant value replicated across every output row (e.g. the synthetic
    /// `compareDateRange` column for [`QueryType::CompareDateRangeQuery`]).
    Constant(DBResponsePrimitive),
    /// Lookup chain failed for this member; fill the output column with `Null`
    /// to keep one column per member.
    NullFilled,
}

pub(crate) struct ColumnarColumnPlan<'a> {
    member_type: &'a str,
    source: ColumnarColumnSource,
}

fn build_columnar_plan<'a>(
    members: &[String],
    members_to_alias_map: &IndexMap<String, String>,
    annotation: &'a HashMap<String, ConfigItem>,
    columns_pos: &IndexMap<String, usize>,
    query_type: &QueryType,
    time_dimensions: Option<&Vec<QueryTimeDimension>>,
) -> Result<Vec<ColumnarColumnPlan<'a>>> {
    let mut plan: Vec<ColumnarColumnPlan<'a>> = Vec::with_capacity(members.len());

    for (i, m) in members.iter().enumerate() {
        let is_last = i + 1 == members.len();

        let resolved =
            annotation
                .get(m)
                .and_then(|annotation_item| match members_to_alias_map.get(m) {
                    Some(alias) => columns_pos
                        .get(alias)
                        .map(|&index| (annotation_item, index)),
                    None => None,
                });

        if let Some((annotation_item, index)) = resolved {
            plan.push(ColumnarColumnPlan {
                member_type: annotation_item.member_type.as_deref().unwrap_or(""),
                source: ColumnarColumnSource::DbColumn { index },
            });
            continue;
        }

        // Synthetic tail column added by `get_members` for these query types.
        if is_last {
            match query_type {
                QueryType::CompareDateRangeQuery => {
                    plan.push(ColumnarColumnPlan {
                        member_type: "",
                        source: ColumnarColumnSource::Constant(get_date_range_value(
                            time_dimensions,
                        )?),
                    });
                    continue;
                }
                QueryType::BlendingQuery => {
                    let response_key = get_blending_response_key(time_dimensions)?;
                    if let Some(alias) = members_to_alias_map.get(&response_key) {
                        if let Some(&index) = columns_pos.get(alias) {
                            // Preserve the (likely-quirky) lookup at
                            // `get_compact_row`: member_type comes from
                            // `annotation[alias]`, not `annotation[member]`.
                            let member_type = annotation
                                .get(alias)
                                .map_or("", |a| a.member_type.as_deref().unwrap_or(""));
                            plan.push(ColumnarColumnPlan {
                                member_type,
                                source: ColumnarColumnSource::DbColumn { index },
                            });
                            continue;
                        }
                    }
                }
                _ => {}
            }
        }

        plan.push(ColumnarColumnPlan {
            member_type: "",
            source: ColumnarColumnSource::NullFilled,
        });
    }

    Ok(plan)
}

fn build_columnar_columns(
    plan: &[ColumnarColumnPlan<'_>],
    db_data: &QueryResult,
) -> Vec<ColumnarArray> {
    let row_count = db_data.row_count;
    let mut columns: Vec<ColumnarArray> = plan
        .iter()
        .map(|_| ColumnarArray::with_capacity(row_count))
        .collect();

    for (col_idx, plan_entry) in plan.iter().enumerate() {
        let out = &mut columns[col_idx];
        match &plan_entry.source {
            ColumnarColumnSource::DbColumn { index } => {
                for cell in db_data.data[*index].iter() {
                    out.push(transform_value(cell.clone(), plan_entry.member_type));
                }
            }
            ColumnarColumnSource::Constant(v) => {
                out.resize(row_count, v.clone());
            }
            ColumnarColumnSource::NullFilled => {
                out.resize(row_count, DBResponsePrimitive::Null);
            }
        }
    }

    columns
}

/// Convert DB response object to the vanilla output format. Keys are
/// pre-hashed [`InternedKey`] values shared via [`Arc::clone`] from the plan,
/// turning per-cell hashing/key allocation into an atomic refcount inc. The
/// plan also carries the column slice directly, so the per-row loop does one
/// bounds check (`column.column.get(row_idx)`) per cell instead of the
/// `db_data.data.get(col).and_then(...)` double lookup.
pub fn get_vanilla_row(plan: &VanillaPlan<'_>, row_idx: usize) -> Result<VanillaRow> {
    // +1 to cover the optional tail entry (compareDateRange / blending key).
    let mut row = IndexMap::with_capacity_and_hasher(
        plan.columns.len() + plan.minimal_granularity_extras.len() + 1,
        PrehashedBuildHasher,
    );

    for column in &plan.columns {
        let transformed_value = transform_value(column.column[row_idx].clone(), column.member_type);
        row.insert(Arc::clone(&column.key), transformed_value);
    }

    // Handle deprecated time dimensions without granularity. The candidate
    // columns were collected at plan build time; pick the lowest-level one
    // whose transformed value is actually present in this row
    if !plan.minimal_granularity_extras.is_empty() {
        for extra in &plan.minimal_granularity_extras {
            let mut best: Option<(u8, &DBResponsePrimitive)> = None;

            for (level, source_key) in &extra.candidates {
                let Some(value) = row.get::<InternedKey>(source_key) else {
                    continue;
                };

                match best {
                    Some((best_level, _)) if best_level < *level => {}
                    _ => best = Some((*level, value)),
                }
            }

            if let Some((_, value)) = best {
                row.insert(Arc::clone(&extra.base_key), value.clone());
            }
        }
    }

    match &plan.tail {
        VanillaTail::None => {}
        VanillaTail::CompareDateRange { key, value } => {
            row.insert(Arc::clone(key), value.clone());
        }
        VanillaTail::Blending {
            blending_key,
            response_key,
        } => {
            if let Some(value) = row.get::<InternedKey>(response_key) {
                row.insert(Arc::clone(blending_key), value.clone());
            }
        }
    }

    Ok(row)
}

/// Helper to get a list if unique granularities from normalized queries
pub fn get_query_granularities(queries: &[&NormalizedQuery]) -> Vec<String> {
    queries
        .iter()
        .filter_map(|query| {
            query
                .time_dimensions
                .as_ref()
                .and_then(|tds| tds.first())
                .and_then(|td| td.granularity.clone())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

/// Get Pivot Query for a list of queries
pub fn get_pivot_query(
    query_type: &QueryType,
    queries: &Vec<&NormalizedQuery>,
) -> Result<NormalizedQuery> {
    let mut pivot_query = queries
        .first()
        .copied()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Queries list cannot be empty"))?;

    match query_type {
        QueryType::BlendingQuery => {
            // Merge and deduplicate measures and dimensions across all queries
            let mut merged_measures = HashSet::new();
            let mut merged_dimensions = HashSet::new();

            for query in queries {
                if let Some(measures) = &query.measures {
                    merged_measures.extend(measures.iter().cloned());
                }
                if let Some(dimensions) = &query.dimensions {
                    merged_dimensions.extend(dimensions.iter().cloned());
                }
            }

            pivot_query.measures = if !merged_measures.is_empty() {
                Some(merged_measures.into_iter().collect())
            } else {
                None
            };
            pivot_query.dimensions = if !merged_dimensions.is_empty() {
                Some(merged_dimensions.into_iter().collect())
            } else {
                None
            };

            // Add time dimensions
            let granularities = get_query_granularities(queries);
            if !granularities.is_empty() {
                pivot_query.time_dimensions = Some(vec![QueryTimeDimension {
                    dimension: "time".to_string(),
                    date_range: None,
                    compare_date_range: None,
                    granularity: granularities.first().cloned(),
                }]);
            }
        }
        QueryType::CompareDateRangeQuery => {
            let mut dimensions = vec![MemberOrMemberExpression::Member(
                "compareDateRange".to_string(),
            )];
            if let Some(dims) = pivot_query.dimensions {
                dimensions.extend(dims.clone());
            }
            pivot_query.dimensions = Option::from(dimensions);
        }
        _ => {}
    }

    pivot_query.query_type = Option::from(query_type.clone());

    Ok(pivot_query)
}

pub fn get_final_cubestore_result_array(
    transform_requests: &[TransformDataRequest],
    cube_store_results: &[Arc<QueryResult>],
    result_data: &mut [RequestResultData],
) -> Result<()> {
    for (transform_data, cube_store_result, result) in multizip((
        transform_requests.iter(),
        cube_store_results.iter(),
        result_data.iter_mut(),
    )) {
        result.prepare_results(transform_data, cube_store_result)?;
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum TransformedData {
    Compact {
        members: Vec<String>,
        dataset: Vec<Vec<DBResponsePrimitive>>,
    },
    Columnar {
        members: Vec<String>,
        columns: Vec<ColumnarArray>,
    },
    Vanilla(Vec<VanillaRow>),
}

impl TransformedData {
    /// Transforms queried data array to the output format.
    pub fn transform(
        request_data: &TransformDataRequest,
        cube_store_result: &QueryResult,
    ) -> Result<Self> {
        let alias_to_member_name_map = &request_data.alias_to_member_name_map;
        let annotation = &request_data.annotation;
        let query = &request_data.query;
        let query_type = &request_data.query_type.clone().unwrap_or_default();
        let res_type = request_data.res_type.clone();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            cube_store_result,
            alias_to_member_name_map,
            annotation,
        )?;

        match res_type {
            Some(ResultType::Compact) => {
                let plan = build_compact_plan(
                    &members,
                    &members_to_alias_map,
                    annotation,
                    cube_store_result,
                    query_type,
                    query.time_dimensions.as_ref(),
                )?;
                let row_count = cube_store_result.row_count;
                let dataset: Vec<_> = (0..row_count)
                    .map(|row_idx| get_compact_row(&plan, row_idx))
                    .collect();
                Ok(TransformedData::Compact { members, dataset })
            }
            Some(ResultType::Columnar) => {
                let plan = build_columnar_plan(
                    &members,
                    &members_to_alias_map,
                    annotation,
                    &cube_store_result.columns_pos,
                    query_type,
                    query.time_dimensions.as_ref(),
                )?;
                let columns = build_columnar_columns(&plan, cube_store_result);
                Ok(TransformedData::Columnar { members, columns })
            }
            _ => {
                let plan = build_vanilla_plan(
                    cube_store_result,
                    alias_to_member_name_map,
                    annotation,
                    query,
                    query_type,
                )?;
                let row_count = cube_store_result.row_count;
                let dataset: Vec<_> = (0..row_count)
                    .map(|row_idx| get_vanilla_row(&plan, row_idx))
                    .collect::<Result<Vec<_>>>()?;
                Ok(TransformedData::Vanilla(dataset))
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestResultDataMulti {
    pub query_type: QueryType,
    pub results: Vec<RequestResultData>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pivot_query: Option<NormalizedQuery>,
    pub slow_query: bool,
}

impl RequestResultDataMulti {
    /// Processes multiple results and populates the final `RequestResultDataMulti` structure
    /// which is sent to the client.
    pub fn prepare_results(
        &mut self,
        request_data: &[TransformDataRequest],
        cube_store_result: &[Arc<QueryResult>],
    ) -> Result<()> {
        for (transform_data, cube_store_result, result) in multizip((
            request_data.iter(),
            cube_store_result.iter(),
            self.results.iter_mut(),
        )) {
            result.prepare_results(transform_data, cube_store_result)?;
        }

        let normalized_queries = self
            .results
            .iter()
            .map(|result| &result.query)
            .collect::<Vec<_>>();

        self.pivot_query = Some(get_pivot_query(&self.query_type, &normalized_queries)?);

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RequestResultData {
    pub query: NormalizedQuery,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_refresh_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_key_values: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub used_pre_aggregations: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transformed_query: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub annotation: HashMap<String, HashMap<String, AnnotatedConfigItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ext_db_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external: Option<bool>,
    pub slow_query: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<TransformedData>,
}

impl RequestResultData {
    /// Populates the `RequestResultData` structure with the transformed Query result.
    pub fn prepare_results(
        &mut self,
        request_data: &TransformDataRequest,
        cube_store_result: &QueryResult,
    ) -> Result<()> {
        let transformed = TransformedData::transform(request_data, cube_store_result)?;
        self.data = Some(transformed);

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestResultArray {
    pub results: Vec<RequestResultData>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum DBResponsePrimitive {
    Null,
    Boolean(bool),
    Number(f64),
    String(String),
    Uncommon(Value),
}

// Hand-written `Deserialize` that avoids serde's untagged-enum buffering.
impl<'de> Deserialize<'de> for DBResponsePrimitive {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DBResponsePrimitiveVisitor;

        impl<'de> Visitor<'de> for DBResponsePrimitiveVisitor {
            type Value = DBResponsePrimitive;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON primitive (null, bool, number, string) or container")
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::Boolean(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::Number(v as f64))
            }

            fn visit_i128<E: de::Error>(self, v: i128) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::Number(v as f64))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::Number(v as f64))
            }

            fn visit_u128<E: de::Error>(self, v: u128) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::Number(v as f64))
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::Number(v))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::String(v.to_owned()))
            }

            fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::String(v.to_owned()))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::String(v))
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::Null)
            }

            fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(DBResponsePrimitive::Null)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                Deserialize::deserialize(deserializer)
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let value = Value::deserialize(de::value::SeqAccessDeserializer::new(seq))?;
                Ok(DBResponsePrimitive::Uncommon(value))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let value = Value::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(DBResponsePrimitive::Uncommon(value))
            }
        }

        deserializer.deserialize_any(DBResponsePrimitiveVisitor)
    }
}

impl Display for DBResponsePrimitive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            DBResponsePrimitive::Null => "null".to_string(),
            DBResponsePrimitive::Boolean(b) => b.to_string(),
            DBResponsePrimitive::Number(n) => n.to_string(),
            DBResponsePrimitive::String(s) => s.clone(),
            DBResponsePrimitive::Uncommon(v) => {
                serde_json::to_string(&v).unwrap_or_else(|_| v.to_string())
            }
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct ColumnarArray(pub Vec<DBResponsePrimitive>);

impl ColumnarArray {
    #[inline]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    #[inline]
    pub fn as_slice(&self) -> &[DBResponsePrimitive] {
        &self.0
    }
}

impl From<Vec<DBResponsePrimitive>> for ColumnarArray {
    #[inline]
    fn from(v: Vec<DBResponsePrimitive>) -> Self {
        Self(v)
    }
}

impl From<ColumnarArray> for Vec<DBResponsePrimitive> {
    #[inline]
    fn from(c: ColumnarArray) -> Self {
        c.0
    }
}

impl std::ops::Deref for ColumnarArray {
    type Target = Vec<DBResponsePrimitive>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ColumnarArray {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::JsRawColumnarData;
    use anyhow::Result;
    use serde_json::from_str;
    use std::{fmt, sync::LazyLock};

    type TestSuiteData = HashMap<String, TestData>;

    #[derive(Clone, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct TestData {
        request: TransformDataRequest,
        query_result: JsRawColumnarData,
        final_result_default: Option<TransformedData>,
        final_result_compact: Option<TransformedData>,
    }

    const TEST_SUITE_JSON: &str = r#"
{
  "regular_discount_by_city": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_discount": "ECommerceRecordsUs2021.avg_discount",
        "e_commerce_records_us2021__city": "ECommerceRecordsUs2021.city"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_discount": {
          "title": "E Commerce Records Us2021 Avg Discount",
          "shortTitle": "Avg Discount",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.city": {
          "title": "E Commerce Records Us2021 City",
          "shortTitle": "City",
          "type": "string"
        }
      },
      "query": {
        "dimensions": [
          "ECommerceRecordsUs2021.city"
        ],
        "measures": [
          "ECommerceRecordsUs2021.avg_discount"
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "filters": [],
        "timeDimensions": []
      },
      "queryType": "regularQuery"
    },
    "queryResult": {
      "members": [
        "e_commerce_records_us2021__city",
        "e_commerce_records_us2021__avg_discount"
      ],
      "columns": [
        [
          "Missouri City",
          "Abilene"
        ],
        [
          "0.80000000000000000000",
          "0.80000000000000000000"
        ]
      ]
    },
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.city": "Missouri City",
        "ECommerceRecordsUs2021.avg_discount": "0.80000000000000000000"
      },
      {
        "ECommerceRecordsUs2021.city": "Abilene",
        "ECommerceRecordsUs2021.avg_discount": "0.80000000000000000000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.city",
        "ECommerceRecordsUs2021.avg_discount"
      ],
      "dataset": [
        [
          "Missouri City",
          "0.80000000000000000000"
        ],
        [
          "Abilene",
          "0.80000000000000000000"
        ]
      ]
    }
  },
  "regular_profit_by_postal_code": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_profit": "ECommerceRecordsUs2021.avg_profit",
        "e_commerce_records_us2021__postal_code": "ECommerceRecordsUs2021.postalCode"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_profit": {
          "title": "E Commerce Records Us2021 Avg Profit",
          "shortTitle": "Avg Profit",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.postalCode": {
          "title": "E Commerce Records Us2021 Postal Code",
          "shortTitle": "Postal Code",
          "type": "string"
        }
      },
      "query": {
        "dimensions": [
          "ECommerceRecordsUs2021.postalCode"
        ],
        "measures": [
          "ECommerceRecordsUs2021.avg_profit"
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "filters": [],
        "timeDimensions": []
      },
      "queryType": "regularQuery"
    },
    "queryResult": {
      "members": [
        "e_commerce_records_us2021__postal_code",
        "e_commerce_records_us2021__avg_profit"
      ],
      "columns": [
        [
          "95823",
          "64055"
        ],
        [
          "646.1258666666666667",
          "487.8315000000000000"
        ]
      ]
    },
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.postalCode": "95823",
        "ECommerceRecordsUs2021.avg_profit": "646.1258666666666667"
      },
      {
        "ECommerceRecordsUs2021.postalCode": "64055",
        "ECommerceRecordsUs2021.avg_profit": "487.8315000000000000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.postalCode",
        "ECommerceRecordsUs2021.avg_profit"
      ],
      "dataset": [
        [
          "95823",
          "646.1258666666666667"
        ],
        [
          "64055",
          "487.8315000000000000"
        ]
      ]
    }
  },
  "compare_date_range_count_by_order_date": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__count": "ECommerceRecordsUs2021.count",
        "e_commerce_records_us2021__order_date_day": "ECommerceRecordsUs2021.orderDate.day"
      },
      "annotation": {
        "ECommerceRecordsUs2021.count": {
          "title": "E Commerce Records Us2021 Count",
          "shortTitle": "Count",
          "type": "number",
          "drillMembers": [
            "ECommerceRecordsUs2021.city",
            "ECommerceRecordsUs2021.country",
            "ECommerceRecordsUs2021.customerId",
            "ECommerceRecordsUs2021.orderId",
            "ECommerceRecordsUs2021.productId",
            "ECommerceRecordsUs2021.productName",
            "ECommerceRecordsUs2021.orderDate"
          ],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": [
              "ECommerceRecordsUs2021.city",
              "ECommerceRecordsUs2021.country",
              "ECommerceRecordsUs2021.customerId",
              "ECommerceRecordsUs2021.orderId",
              "ECommerceRecordsUs2021.productId",
              "ECommerceRecordsUs2021.productName",
              "ECommerceRecordsUs2021.orderDate"
            ]
          }
        },
        "ECommerceRecordsUs2021.orderDate.day": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.count"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "day",
            "dateRange": [
              "2020-01-01T00:00:00.000",
              "2020-01-31T23:59:59.999"
            ]
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "filters": [],
        "dimensions": []
      },
      "queryType": "compareDateRangeQuery"
    },
    "queryResult": {
      "members": [
        "e_commerce_records_us2021__order_date_day",
        "e_commerce_records_us2021__count"
      ],
      "columns": [
        [
          "2020-01-01T00:00:00.000",
          null
        ],
        [
          "10",
          null
        ]
      ]
    },
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.day": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.count": "10",
        "compareDateRange": "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999"
      },
      {
        "ECommerceRecordsUs2021.orderDate.day": null,
        "ECommerceRecordsUs2021.orderDate": null,
        "ECommerceRecordsUs2021.count": null,
        "compareDateRange": null
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.day",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.count",
        "compareDateRange"
      ],
      "dataset": [
        [
          "2020-01-01T00:00:00.000",
          "2020-01-01T00:00:00.000",
          "10",
          "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999"
        ],
        [
          null,
          null,
          null,
          "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999"
        ]
      ]
    }
  },
  "compare_date_range_count_by_order_date2": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__count": "ECommerceRecordsUs2021.count",
        "e_commerce_records_us2021__order_date_day": "ECommerceRecordsUs2021.orderDate.day"
      },
      "annotation": {
        "ECommerceRecordsUs2021.count": {
          "title": "E Commerce Records Us2021 Count",
          "shortTitle": "Count",
          "type": "number",
          "drillMembers": [
            "ECommerceRecordsUs2021.city",
            "ECommerceRecordsUs2021.country",
            "ECommerceRecordsUs2021.customerId",
            "ECommerceRecordsUs2021.orderId",
            "ECommerceRecordsUs2021.productId",
            "ECommerceRecordsUs2021.productName",
            "ECommerceRecordsUs2021.orderDate"
          ],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": [
              "ECommerceRecordsUs2021.city",
              "ECommerceRecordsUs2021.country",
              "ECommerceRecordsUs2021.customerId",
              "ECommerceRecordsUs2021.orderId",
              "ECommerceRecordsUs2021.productId",
              "ECommerceRecordsUs2021.productName",
              "ECommerceRecordsUs2021.orderDate"
            ]
          }
        },
        "ECommerceRecordsUs2021.orderDate.day": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.count"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "day",
            "dateRange": [
              "2020-03-01T00:00:00.000",
              "2020-03-31T23:59:59.999"
            ]
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "filters": [],
        "dimensions": []
      },
      "queryType": "compareDateRangeQuery"
    },
    "queryResult": {
      "members": [
        "e_commerce_records_us2021__order_date_day",
        "e_commerce_records_us2021__count"
      ],
      "columns": [
        [
          "2020-03-02T00:00:00.000",
          "2020-03-03T00:00:00.000"
        ],
        [
          "11",
          "7"
        ]
      ]
    },
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.day": "2020-03-02T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-03-02T00:00:00.000",
        "ECommerceRecordsUs2021.count": "11",
        "compareDateRange": "2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999"
      },
      {
        "ECommerceRecordsUs2021.orderDate.day": "2020-03-03T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-03-03T00:00:00.000",
        "ECommerceRecordsUs2021.count": "7",
        "compareDateRange": "2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.day",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.count",
        "compareDateRange"
      ],
      "dataset": [
        [
          "2020-03-02T00:00:00.000",
          "2020-03-02T00:00:00.000",
          "11",
          "2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999"
        ],
        [
          "2020-03-03T00:00:00.000",
          "2020-03-03T00:00:00.000",
          "7",
          "2020-03-01T00:00:00.000 - 2020-03-31T23:59:59.999"
        ]
      ]
    }
  },
  "blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_discount": "ECommerceRecordsUs2021.avg_discount",
        "e_commerce_records_us2021__order_date_month": "ECommerceRecordsUs2021.orderDate.month"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_discount": {
          "title": "E Commerce Records Us2021 Avg Discount",
          "shortTitle": "Avg Discount",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.orderDate.month": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.avg_discount"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "month",
            "dateRange": [
              "2020-01-01T00:00:00.000",
              "2020-12-30T23:59:59.999"
            ]
          }
        ],
        "filters": [
          {
            "operator": "equals",
            "values": [
              "Standard Class"
            ],
            "member": "ECommerceRecordsUs2021.shipMode"
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "dimensions": []
      },
      "queryType": "blendingQuery"
    },
    "queryResult": {
      "members": [
        "e_commerce_records_us2021__order_date_month",
        "e_commerce_records_us2021__avg_discount"
      ],
      "columns": [
        [
          "2020-01-01T00:00:00.000",
          "2020-02-01T00:00:00.000"
        ],
        [
          "0.15638297872340425532",
          "0.17573529411764705882"
        ]
      ]
    },
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.15638297872340425532",
        "time.month": "2020-01-01T00:00:00.000"
      },
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.17573529411764705882",
        "time.month": "2020-02-01T00:00:00.000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.month",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.avg_discount",
        "time.month"
      ],
      "dataset": [
        [
          "2020-01-01T00:00:00.000",
          "2020-01-01T00:00:00.000",
          "0.15638297872340425532",
          "2020-01-01T00:00:00.000"
        ],
        [
          "2020-02-01T00:00:00.000",
          "2020-02-01T00:00:00.000",
          "0.17573529411764705882",
          "2020-02-01T00:00:00.000"
        ]
      ]
    }
  },
  "blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode2": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_discount": "ECommerceRecordsUs2021.avg_discount",
        "e_commerce_records_us2021__order_date_month": "ECommerceRecordsUs2021.orderDate.month"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_discount": {
          "title": "E Commerce Records Us2021 Avg Discount",
          "shortTitle": "Avg Discount",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.orderDate.month": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.avg_discount"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "month",
            "dateRange": [
              "2020-01-01T00:00:00.000",
              "2020-12-30T23:59:59.999"
            ]
          }
        ],
        "filters": [
          {
            "operator": "equals",
            "values": [
              "First Class"
            ],
            "member": "ECommerceRecordsUs2021.shipMode"
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "dimensions": []
      },
      "queryType": "blendingQuery"
    },
    "queryResult": {
      "members": [
        "e_commerce_records_us2021__order_date_month",
        "e_commerce_records_us2021__avg_discount"
      ],
      "columns": [
        [
          "2020-01-01T00:00:00.000",
          "2020-02-01T00:00:00.000"
        ],
        [
          "0.28571428571428571429",
          "0.21777777777777777778"
        ]
      ]
    },
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.28571428571428571429",
        "time.month": "2020-01-01T00:00:00.000"
      },
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.21777777777777777778",
        "time.month": "2020-02-01T00:00:00.000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.month",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.avg_discount",
        "time.month"
      ],
      "dataset": [
        [
          "2020-01-01T00:00:00.000",
          "2020-01-01T00:00:00.000",
          "0.28571428571428571429",
          "2020-01-01T00:00:00.000"
        ],
        [
          "2020-02-01T00:00:00.000",
          "2020-02-01T00:00:00.000",
          "0.21777777777777777778",
          "2020-02-01T00:00:00.000"
        ]
      ]
    }
  },
  "blending_query_multiple_granularities": {
    "request": {
      "aliasToMemberNameMap": {
        "e_commerce_records_us2021__avg_discount": "ECommerceRecordsUs2021.avg_discount",
        "e_commerce_records_us2021__order_date_month": "ECommerceRecordsUs2021.orderDate.month",
        "e_commerce_records_us2021__order_date_week": "ECommerceRecordsUs2021.orderDate.week"
      },
      "annotation": {
        "ECommerceRecordsUs2021.avg_discount": {
          "title": "E Commerce Records Us2021 Avg Discount",
          "shortTitle": "Avg Discount",
          "type": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          }
        },
        "ECommerceRecordsUs2021.orderDate.month": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate.week": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        },
        "ECommerceRecordsUs2021.orderDate": {
          "title": "E Commerce Records Us2021 Order Date",
          "shortTitle": "Order Date",
          "type": "time"
        }
      },
      "query": {
        "measures": [
          "ECommerceRecordsUs2021.avg_discount"
        ],
        "timeDimensions": [
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "month",
            "dateRange": [
              "2020-01-01T00:00:00.000",
              "2020-12-30T23:59:59.999"
            ]
          },
          {
            "dimension": "ECommerceRecordsUs2021.orderDate",
            "granularity": "week",
            "dateRange": [
              "2020-01-01T00:00:00.000",
              "2020-12-30T23:59:59.999"
            ]
          }
        ],
        "filters": [
          {
            "operator": "equals",
            "values": [
              "First Class"
            ],
            "member": "ECommerceRecordsUs2021.shipMode"
          }
        ],
        "limit": 2,
        "rowLimit": 2,
        "timezone": "UTC",
        "order": [],
        "dimensions": []
      },
      "queryType": "blendingQuery"
    },
    "queryResult": {
      "members": [
        "e_commerce_records_us2021__order_date_month",
        "e_commerce_records_us2021__order_date_week",
        "e_commerce_records_us2021__avg_discount"
      ],
      "columns": [
        [
          "2020-01-01T00:00:00.000",
          "2020-02-01T00:00:00.000"
        ],
        [
          "2019-12-30T00:00:00.000",
          "2020-01-27T00:00:00.000"
        ],
        [
          "0.28571428571428571429",
          "0.21777777777777777778"
        ]
      ]
    },
    "finalResultDefault": [
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-01-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate.week": "2019-12-30T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2019-12-30T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.28571428571428571429",
        "time.month": "2020-01-01T00:00:00.000"
      },
      {
        "ECommerceRecordsUs2021.orderDate.month": "2020-02-01T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate.week": "2020-01-27T00:00:00.000",
        "ECommerceRecordsUs2021.orderDate": "2020-01-27T00:00:00.000",
        "ECommerceRecordsUs2021.avg_discount": "0.21777777777777777778",
        "time.month": "2020-02-01T00:00:00.000"
      }
    ],
    "finalResultCompact": {
      "members": [
        "ECommerceRecordsUs2021.orderDate.month",
        "ECommerceRecordsUs2021.orderDate.week",
        "ECommerceRecordsUs2021.orderDate",
        "ECommerceRecordsUs2021.avg_discount",
        "time.month"
      ],
      "dataset": [
        [
          "2020-01-01T00:00:00.000",
          "2019-12-30T00:00:00.000",
          "2019-12-30T00:00:00.000",
          "0.28571428571428571429",
          "2020-01-01T00:00:00.000"
        ],
        [
          "2020-02-01T00:00:00.000",
          "2020-01-27T00:00:00.000",
          "2020-01-27T00:00:00.000",
          "0.21777777777777777778",
          "2020-02-01T00:00:00.000"
        ]
      ]
    }
  }
}
    "#;

    static TEST_SUITE_DATA: LazyLock<TestSuiteData> =
        LazyLock::new(|| from_str(TEST_SUITE_JSON).unwrap());

    #[derive(Debug)]
    pub struct TestError(String);

    impl Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "Error: {}", self.0)
        }
    }

    impl std::error::Error for TestError {}

    /// Smart comparator of datasets.
    /// Hashmaps don't guarantee the order of the elements while iterating,
    /// so it's not possible to simply compare generated one and the one from the json.
    fn compare_transformed_data(
        left: &TransformedData,
        right: &TransformedData,
    ) -> Result<(), TestError> {
        match (left, right) {
            (
                TransformedData::Compact {
                    members: left_members,
                    dataset: left_dataset,
                },
                TransformedData::Compact {
                    members: right_members,
                    dataset: right_dataset,
                },
            ) => {
                let mut left_sorted_members = left_members.clone();
                let mut right_sorted_members = right_members.clone();
                left_sorted_members.sort();
                right_sorted_members.sort();

                if left_sorted_members != right_sorted_members {
                    return Err(TestError("Members do not match after sorting".to_string()));
                }

                if left_dataset.len() != right_dataset.len() {
                    return Err(TestError("Datasets have different lengths".to_string()));
                }

                let mut member_index_map = HashMap::new();
                for (i, member) in left_members.iter().enumerate() {
                    if let Some(right_index) = right_members.iter().position(|x| x == member) {
                        member_index_map.insert(i, right_index);
                    } else {
                        return Err(TestError("Member not found in right object".to_string()));
                    }
                }

                for (i, left_row) in left_dataset.iter().enumerate() {
                    let right_row = &right_dataset[i];

                    for (j, left_value) in left_row.iter().enumerate() {
                        let mapped_index = *member_index_map.get(&j).unwrap();
                        let right_value = &right_row[mapped_index];
                        if left_value != right_value {
                            return Err(TestError(format!(
                                "Dataset values at row {} and column {} do not match: {} != {}",
                                i, j, left_value, right_value
                            )));
                        }
                    }
                }

                Ok(())
            }
            (
                TransformedData::Columnar {
                    members: left_members,
                    columns: left_columns,
                },
                TransformedData::Columnar {
                    members: right_members,
                    columns: right_columns,
                },
            ) => {
                let mut left_sorted_members = left_members.clone();
                let mut right_sorted_members = right_members.clone();
                left_sorted_members.sort();
                right_sorted_members.sort();

                if left_sorted_members != right_sorted_members {
                    return Err(TestError("Members do not match after sorting".to_string()));
                }

                if left_columns.len() != right_columns.len() {
                    return Err(TestError(
                        "Column counts do not match between Columnar results".to_string(),
                    ));
                }

                let mut member_index_map = HashMap::new();
                for (i, member) in left_members.iter().enumerate() {
                    if let Some(right_index) = right_members.iter().position(|x| x == member) {
                        member_index_map.insert(i, right_index);
                    } else {
                        return Err(TestError("Member not found in right object".to_string()));
                    }
                }

                for (left_idx, left_column) in left_columns.iter().enumerate() {
                    let right_idx = *member_index_map.get(&left_idx).unwrap();
                    let right_column = &right_columns[right_idx];
                    if left_column.len() != right_column.len() {
                        return Err(TestError(format!(
                            "Column {} (member {}) row counts differ: {} != {}",
                            left_idx,
                            left_members[left_idx],
                            left_column.len(),
                            right_column.len()
                        )));
                    }
                    for (row, left_value) in left_column.iter().enumerate() {
                        let right_value = &right_column[row];
                        if left_value != right_value {
                            return Err(TestError(format!(
                                "Columnar value at row {} for member '{}' differs: {} != {}",
                                row, left_members[left_idx], left_value, right_value
                            )));
                        }
                    }
                }

                Ok(())
            }
            (TransformedData::Vanilla(left_dataset), TransformedData::Vanilla(right_dataset)) => {
                if left_dataset.len() != right_dataset.len() {
                    return Err(TestError(
                        "Vanilla datasets have different lengths".to_string(),
                    ));
                }

                for (i, (left_record, right_record)) in
                    left_dataset.iter().zip(right_dataset.iter()).enumerate()
                {
                    if left_record.len() != right_record.len() {
                        return Err(TestError(format!(
                            "Vanilla dataset records at index {} have different numbers of keys",
                            i
                        )));
                    }

                    for (key, left_value) in left_record {
                        if let Some(right_value) = right_record.get(key) {
                            if left_value != right_value {
                                return Err(TestError(format!(
                                    "Values at index {} for key '{}' do not match: {:?} != {:?}",
                                    i, key, left_value, right_value
                                )));
                            }
                        } else {
                            return Err(TestError(format!(
                                "Key '{}' not found in right record at index {}",
                                key, i
                            )));
                        }
                    }
                }

                Ok(())
            }
            _ => Err(TestError("Mismatched TransformedData types".to_string())),
        }
    }

    #[test]
    fn test_transform_value_string_to_time_valid_rfc3339() {
        let value = DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string());
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_wo_t_to_time_valid_rfc3339() {
        let value = DBResponsePrimitive::String("2024-01-01 12:30:15.123".to_string());
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_wo_mssec_to_time_valid_rfc3339() {
        let value = DBResponsePrimitive::String("2024-01-01 12:30:15".to_string());
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.000".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_wo_mssec_w_t_to_time_valid_rfc3339() {
        let value = DBResponsePrimitive::String("2024-01-01T12:30:15".to_string());
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.000".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_with_tz_offset_to_time_valid_rfc3339() {
        let value = DBResponsePrimitive::String("2024-01-01 12:30:15.123 +00:00".to_string());
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_with_tz_to_time_valid_rfc3339() {
        let value = DBResponsePrimitive::String("2024-01-01 12:30:15.123 UTC".to_string());
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T12:30:15.123".to_string())
        );
    }

    #[test]
    fn test_transform_value_string_to_time_invalid_rfc3339() {
        let value = DBResponsePrimitive::String("invalid-date".to_string());
        let result = transform_value(value, "time");

        assert_eq!(
            result,
            DBResponsePrimitive::String("invalid-date".to_string())
        );
    }

    #[test]
    fn test_transform_value_primitive_string_type_not_time() {
        let value = DBResponsePrimitive::String("some-string".to_string());
        let result = transform_value(value, "other");

        assert_eq!(
            result,
            DBResponsePrimitive::String("some-string".to_string())
        );
    }

    #[test]
    fn test_get_date_range_value_valid_range() -> Result<()> {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "some-dim".to_string(),
            date_range: Some(vec![
                "2024-01-01T00:00:00Z".to_string(),
                "2024-01-31T23:59:59Z".to_string(),
            ]),
            compare_date_range: None,
            granularity: None,
        }];

        let result = get_date_range_value(Some(&time_dimensions))?;
        assert_eq!(
            result,
            DBResponsePrimitive::String("2024-01-01T00:00:00Z - 2024-01-31T23:59:59Z".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_get_date_range_value_no_time_dimensions() {
        let result = get_date_range_value(None);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the compare date range query."
        );
    }

    #[test]
    fn test_get_date_range_value_empty_time_dimensions() {
        let time_dimensions: Vec<QueryTimeDimension> = vec![];

        let result = get_date_range_value(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "No time dimension provided."
        );
    }

    #[test]
    fn test_get_date_range_value_missing_date_range() {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "dim".to_string(),
            date_range: None,
            compare_date_range: None,
            granularity: None,
        }];

        let result = get_date_range_value(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Inconsistent QueryTimeDimension configuration: dateRange required."
        );
    }

    #[test]
    fn test_get_date_range_value_single_date_range() {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "dim".to_string(),
            date_range: Some(vec!["2024-01-01T00:00:00Z".to_string()]),
            compare_date_range: None,
            granularity: None,
        }];

        let result = get_date_range_value(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Inconsistent dateRange configuration for the compare date range query: 2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn test_get_blending_query_key_valid_granularity() -> Result<()> {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "dim".to_string(),
            granularity: Some("day".to_string()),
            date_range: None,
            compare_date_range: None,
        }];

        let result = get_blending_query_key(Some(&time_dimensions))?;
        assert_eq!(result, "time.day");
        Ok(())
    }

    #[test]
    fn test_get_blending_query_key_no_time_dimensions() {
        let result = get_blending_query_key(None);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the blending query."
        );
    }

    #[test]
    fn test_get_blending_query_key_empty_time_dimensions() {
        let time_dimensions: Vec<QueryTimeDimension> = vec![];

        let result = get_blending_query_key(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the blending query."
        );
    }

    #[test]
    fn test_get_blending_query_key_missing_granularity() {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "dim".to_string(),
            granularity: None,
            date_range: None,
            compare_date_range: None,
        }];

        let result = get_blending_query_key(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Inconsistent QueryTimeDimension configuration for the blending query, granularity required: {:?}",
                QueryTimeDimension {
                    dimension: "dim".to_string(),
                    granularity: None,
                    date_range: None,
                    compare_date_range: None,
                }
            )
        );
    }

    #[test]
    fn test_get_blending_response_key_valid_dimension_and_granularity() -> Result<()> {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "orders.created_at".to_string(),
            granularity: Some("day".to_string()),
            date_range: None,
            compare_date_range: None,
        }];

        let result = get_blending_response_key(Some(&time_dimensions))?;
        assert_eq!(result, "orders.created_at.day");
        Ok(())
    }

    #[test]
    fn test_get_blending_response_key_no_time_dimensions() {
        let result = get_blending_response_key(None);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the blending query."
        );
    }

    #[test]
    fn test_get_blending_response_key_empty_time_dimensions() {
        let time_dimensions: Vec<QueryTimeDimension> = vec![];

        let result = get_blending_response_key(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "QueryTimeDimension should be specified for the blending query."
        );
    }

    #[test]
    fn test_get_blending_response_key_missing_granularity() {
        let time_dimensions = vec![QueryTimeDimension {
            dimension: "orders.created_at".to_string(),
            granularity: None,
            date_range: None,
            compare_date_range: None,
        }];

        let result = get_blending_response_key(Some(&time_dimensions));
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Inconsistent QueryTimeDimension configuration for the blending query, granularity required: {:?}",
                QueryTimeDimension {
                    dimension: "orders.created_at".to_string(),
                    granularity: None,
                    date_range: None,
                    compare_date_range: None,
                }
            )
        );
    }

    #[test]
    fn test_regular_profit_by_postal_code_compact() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_compare_date_range_count_by_order_date() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_compare_date_range_count_by_order_date2() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date2".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_regular_discount_by_city() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_regular_discount_by_city_to_fail() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data
            .request
            .alias_to_member_name_map
            .remove("e_commerce_records_us2021__avg_discount");
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        match TransformedData::transform(&test_data.request, &raw_data) {
            Ok(_) => Err(TestError("regular_discount_by_city should fail ".to_string()).into()),
            Err(_) => Ok(()), // Should throw an error
        }
    }

    #[test]
    fn test_blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode(
    ) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode2(
    ) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_regular_discount_by_city_default_to_fail() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data
            .request
            .alias_to_member_name_map
            .remove("e_commerce_records_us2021__avg_discount");
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        match TransformedData::transform(&test_data.request, &raw_data) {
            Ok(_) => Err(TestError("regular_discount_by_city should fail ".to_string()).into()),
            Err(_) => Ok(()), // Should throw an error
        }
    }

    #[test]
    fn test_regular_discount_by_city_default() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_regular_profit_by_postal_code_default() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode_default(
    ) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode2_default(
    ) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode2"
                    .to_string(),
            )
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_blending_query_multiple_granularities_default() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"blending_query_multiple_granularities".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Default);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_default.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_blending_query_multiple_granularities_compact() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"blending_query_multiple_granularities".to_string())
            .unwrap()
            .clone();
        test_data.request.res_type = Some(ResultType::Compact);
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let transformed = TransformedData::transform(&test_data.request, &raw_data)?;
        compare_transformed_data(&transformed, &test_data.final_result_compact.unwrap())?;
        Ok(())
    }

    #[test]
    fn test_get_members_no_alias_to_member_name_map() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        test_data.request.alias_to_member_name_map = HashMap::new();
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        match get_members(
            query_type,
            query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        ) {
            Ok(_) => Err(TestError("get_members() should fail ".to_string()).into()),
            Err(err) => {
                assert!(err.to_string().contains("Member name not found for alias"));
                Ok(())
            }
        }
    }

    #[test]
    fn test_get_members_empty_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &QueryResult::empty(),
            alias_to_member_name_map,
            annotation,
        )?;
        assert_eq!(members_to_alias_map.len(), 0);
        assert_eq!(members.len(), 0);
        Ok(())
    }

    #[test]
    fn test_get_members_empty_dataset_with_hidden_member() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();

        // Remove a measure from annotation to simulate RBAC-hidden member
        let hidden_member = test_data
            .request
            .query
            .measures
            .as_ref()
            .and_then(|m| m.first())
            .and_then(|m| match m {
                MemberOrMemberExpression::Member(s) => Some(s.clone()),
                _ => None,
            })
            .expect("Test data should have at least one measure");
        test_data.request.annotation.remove(&hidden_member);

        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        match get_members(
            query_type,
            query,
            &QueryResult::empty(),
            alias_to_member_name_map,
            annotation,
        ) {
            Ok(_) => Err(TestError(
                "get_members() should fail for hidden member with empty dataset".to_string(),
            )
            .into()),
            Err(err) => {
                assert!(err.to_string().contains("You requested hidden member"));
                Ok(())
            }
        }
    }

    #[test]
    fn test_get_members_empty_dataset_with_filter_only_time_dimension() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();

        // Add a filter-only time dimension (no granularity) to the query.
        // This simulates a dateRange filter like:
        //   timeDimensions: [{ dimension: "Orders.created_at", dateRange: [...] }]
        // These dimensions are NOT in the annotation because they don't produce
        // result columns — they're only used for filtering.
        test_data.request.query.time_dimensions = Some(vec![QueryTimeDimension {
            dimension: "ECommerceRecordsUs2021.order_date".to_string(),
            date_range: Some(vec!["2025-01-01".to_string(), "2025-12-31".to_string()]),
            compare_date_range: None,
            granularity: None,
        }]);

        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        // Should succeed — filter-only time dimensions must not be checked
        // against the annotation map.
        let result = get_members(
            query_type,
            query,
            &QueryResult::empty(),
            alias_to_member_name_map,
            annotation,
        );
        assert!(
            result.is_ok(),
            "Filter-only time dimension (no granularity) should not trigger hidden member error, got: {:?}",
            result.err()
        );
        Ok(())
    }

    #[test]
    fn test_get_members_filled_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let members_map_expected: MembersMap = IndexMap::from([
            (
                "ECommerceRecordsUs2021.postalCode".to_string(),
                "e_commerce_records_us2021__postal_code".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.avg_profit".to_string(),
                "e_commerce_records_us2021__avg_profit".to_string(),
            ),
        ]);
        assert_eq!(members_to_alias_map, members_map_expected);
        assert_eq!(members.len(), 2);
        Ok(())
    }

    #[test]
    fn test_get_members_compare_date_range_empty_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date".to_string())
            .unwrap()
            .clone();
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &QueryResult::empty(),
            alias_to_member_name_map,
            annotation,
        )?;
        assert_eq!(members_to_alias_map.len(), 0);
        assert_eq!(members.len(), 0);
        Ok(())
    }

    #[test]
    fn test_get_members_compare_date_range_filled_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let members_map_expected: MembersMap = IndexMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.day".to_string(),
                "e_commerce_records_us2021__order_date_day".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                "e_commerce_records_us2021__order_date_day".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.count".to_string(),
                "e_commerce_records_us2021__count".to_string(),
            ),
            (
                "compareDateRange".to_string(),
                "compareDateRangeQuery".to_string(),
            ),
        ]);
        assert_eq!(members_to_alias_map, members_map_expected);
        assert_eq!(members.len(), 4);
        Ok(())
    }

    #[test]
    fn test_get_members_blending_query_empty_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &QueryResult::empty(),
            alias_to_member_name_map,
            annotation,
        )?;
        assert_eq!(members_to_alias_map.len(), 0);
        assert_eq!(members.len(), 0);
        Ok(())
    }

    #[test]
    fn test_get_members_blending_query_filled_dataset() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = &test_data.request.query;
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let (members_to_alias_map, members) = get_members(
            query_type,
            query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let members_map_expected: MembersMap = IndexMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.month".to_string(),
                "e_commerce_records_us2021__order_date_month".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                "e_commerce_records_us2021__order_date_month".to_string(),
            ),
            (
                "ECommerceRecordsUs2021.avg_discount".to_string(),
                "e_commerce_records_us2021__avg_discount".to_string(),
            ),
            (
                "time.month".to_string(),
                "e_commerce_records_us2021__order_date_month".to_string(),
            ),
        ]);
        assert_eq!(members_to_alias_map, members_map_expected);
        assert_eq!(members.len(), 4);
        Ok(())
    }

    #[test]
    fn test_get_compact_row_regular_profit_by_postal_code() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_profit_by_postal_code".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();
        let time_dimensions = &test_data.request.query.time_dimensions.unwrap();

        let (members_to_alias_map, members) = get_members(
            query_type,
            &query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let plan = build_compact_plan(
            &members,
            &members_to_alias_map,
            annotation,
            &raw_data,
            query_type,
            Some(time_dimensions),
        )?;
        let res = get_compact_row(&plan, 0);

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.postalCode".to_string(),
                DBResponsePrimitive::String("95823".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.avg_profit".to_string(),
                DBResponsePrimitive::String("646.1258666666666667".to_string()),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }

        Ok(())
    }

    #[test]
    fn test_get_compact_row_regular_discount_by_city() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();
        let time_dimensions = &test_data.request.query.time_dimensions.unwrap();

        let (members_to_alias_map, members) = get_members(
            query_type,
            &query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let plan = build_compact_plan(
            &members,
            &members_to_alias_map,
            annotation,
            &raw_data,
            query_type,
            Some(time_dimensions),
        )?;
        let res = get_compact_row(&plan, 0);

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.city".to_string(),
                DBResponsePrimitive::String("Missouri City".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.avg_discount".to_string(),
                DBResponsePrimitive::String("0.80000000000000000000".to_string()),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }

        Ok(())
    }

    #[test]
    fn test_get_compact_row_compare_date_range_count_by_order_date() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"compare_date_range_count_by_order_date".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();
        let time_dimensions = &test_data.request.query.time_dimensions.unwrap();

        let (members_to_alias_map, members) = get_members(
            query_type,
            &query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let plan = build_compact_plan(
            &members,
            &members_to_alias_map,
            annotation,
            &raw_data,
            query_type,
            Some(time_dimensions),
        )?;
        let res = get_compact_row(&plan, 0);

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.day".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.count".to_string(),
                DBResponsePrimitive::String("10".to_string()),
            ),
            (
                "compareDateRange".to_string(),
                DBResponsePrimitive::String(
                    "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999".to_string(),
                ),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }

        let res = get_compact_row(&plan, 1);

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.day".to_string(),
                DBResponsePrimitive::Null,
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                DBResponsePrimitive::Null,
            ),
            (
                "ECommerceRecordsUs2021.count".to_string(),
                DBResponsePrimitive::Null,
            ),
            (
                "compareDateRange".to_string(),
                DBResponsePrimitive::String(
                    "2020-01-01T00:00:00.000 - 2020-01-31T23:59:59.999".to_string(),
                ),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }

        Ok(())
    }

    #[test]
    fn test_get_compact_row_blending_query_avg_discount() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(
                &"blending_query_avg_discount_by_date_range_for_the_first_and_standard_ship_mode"
                    .to_string(),
            )
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();
        let time_dimensions = &test_data.request.query.time_dimensions.unwrap();

        let (members_to_alias_map, members) = get_members(
            query_type,
            &query,
            &raw_data,
            alias_to_member_name_map,
            annotation,
        )?;
        let plan = build_compact_plan(
            &members,
            &members_to_alias_map,
            annotation,
            &raw_data,
            query_type,
            Some(time_dimensions),
        )?;
        let res = get_compact_row(&plan, 0);

        let members_map_expected = HashMap::from([
            (
                "ECommerceRecordsUs2021.orderDate.month".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.orderDate".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
            (
                "ECommerceRecordsUs2021.avg_discount".to_string(),
                DBResponsePrimitive::String("0.15638297872340425532".to_string()),
            ),
            (
                "time.month".to_string(),
                DBResponsePrimitive::String("2020-01-01T00:00:00.000".to_string()),
            ),
        ]);

        assert_eq!(res.len(), members_map_expected.len());
        for (i, val) in members.iter().enumerate() {
            assert_eq!(res[i], members_map_expected.get(val).unwrap().clone());
        }
        Ok(())
    }

    #[test]
    fn test_get_vanilla_row_regular_discount_by_city() -> Result<()> {
        let test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        let plan = build_vanilla_plan(
            &raw_data,
            alias_to_member_name_map,
            annotation,
            &query,
            query_type,
        )?;
        let res = get_vanilla_row(&plan, 0)?;

        let mut expected: VanillaRow = empty_vanilla_row(2);
        expected.insert(
            Arc::new(InternedKey::new("ECommerceRecordsUs2021.city")),
            DBResponsePrimitive::String("Missouri City".to_string()),
        );
        expected.insert(
            Arc::new(InternedKey::new("ECommerceRecordsUs2021.avg_discount")),
            DBResponsePrimitive::String("0.80000000000000000000".to_string()),
        );
        assert_eq!(res, expected);
        Ok(())
    }

    #[test]
    fn test_get_vanilla_row_regular_discount_by_city_to_fail_member() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data
            .request
            .alias_to_member_name_map
            .remove("e_commerce_records_us2021__avg_discount");
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        match build_vanilla_plan(
            &raw_data,
            alias_to_member_name_map,
            annotation,
            &query,
            query_type,
        ) {
            Ok(_) => Err(TestError("build_vanilla_plan() should fail ".to_string()).into()),
            Err(err) => {
                assert!(err.to_string().contains("Missing member name for alias"));
                Ok(())
            }
        }
    }

    #[test]
    fn test_get_vanilla_row_regular_discount_by_city_to_fail_annotation() -> Result<()> {
        let mut test_data = TEST_SUITE_DATA
            .get(&"regular_discount_by_city".to_string())
            .unwrap()
            .clone();
        test_data
            .request
            .annotation
            .remove("ECommerceRecordsUs2021.avg_discount");
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;
        let alias_to_member_name_map = &test_data.request.alias_to_member_name_map;
        let annotation = &test_data.request.annotation;
        let query = test_data.request.query.clone();
        let query_type = &test_data.request.query_type.clone().unwrap_or_default();

        match build_vanilla_plan(
            &raw_data,
            alias_to_member_name_map,
            annotation,
            &query,
            query_type,
        ) {
            Ok(_) => Err(TestError("build_vanilla_plan() should fail ".to_string()).into()),
            Err(err) => {
                assert!(err.to_string().contains("You requested hidden member"));
                Ok(())
            }
        }
    }

    /// Run the same fixture through both `Compact` and `Columnar` transforms and
    /// assert that the columnar columns are the column-major transpose of the
    /// compact dataset. This pins the contract: same data, different orientation.
    fn assert_columnar_matches_compact(fixture: &str) -> Result<()> {
        let mut test_data = TEST_SUITE_DATA.get(fixture).unwrap().clone();
        let raw_data = QueryResult::from_js_raw_data(test_data.query_result.clone())?;

        test_data.request.res_type = Some(ResultType::Compact);
        let compact = TransformedData::transform(&test_data.request, &raw_data)?;
        let (compact_members, compact_dataset) = match compact {
            TransformedData::Compact { members, dataset } => (members, dataset),
            _ => panic!("expected Compact"),
        };

        test_data.request.res_type = Some(ResultType::Columnar);
        let columnar = TransformedData::transform(&test_data.request, &raw_data)?;
        let (columnar_members, columnar_columns) = match columnar {
            TransformedData::Columnar { members, columns } => (members, columns),
            _ => panic!("expected Columnar"),
        };

        assert_eq!(
            compact_members, columnar_members,
            "members must match across formats"
        );
        assert_eq!(
            columnar_columns.len(),
            compact_members.len(),
            "one column per member"
        );
        for (col_idx, column) in columnar_columns.iter().enumerate() {
            assert_eq!(
                column.len(),
                compact_dataset.len(),
                "column {} length must equal row count",
                col_idx
            );
            for (row_idx, expected_row) in compact_dataset.iter().enumerate() {
                assert_eq!(
                    &column[row_idx], &expected_row[col_idx],
                    "value at column {} row {} must match compact dataset",
                    col_idx, row_idx
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_regular_discount_by_city_columnar() -> Result<()> {
        assert_columnar_matches_compact("regular_discount_by_city")
    }

    #[test]
    fn test_regular_profit_by_postal_code_columnar() -> Result<()> {
        assert_columnar_matches_compact("regular_profit_by_postal_code")
    }

    #[test]
    fn test_compare_date_range_count_by_order_date_columnar() -> Result<()> {
        assert_columnar_matches_compact("compare_date_range_count_by_order_date")
    }

    #[test]
    fn test_blending_query_multiple_granularities_columnar() -> Result<()> {
        assert_columnar_matches_compact("blending_query_multiple_granularities")
    }

    fn make_query_with_dims(dimensions: Option<Vec<MemberOrMemberExpression>>) -> NormalizedQuery {
        NormalizedQuery {
            measures: None,
            dimensions,
            time_dimensions: None,
            segments: None,
            limit: None,
            offset: None,
            total: None,
            total_query: None,
            timezone: None,
            ungrouped: None,
            response_format: None,
            filters: None,
            row_limit: None,
            order: None,
            query_type: None,
        }
    }

    #[test]
    fn test_compute_vanilla_granularity_track_none() {
        let q = make_query_with_dims(None);
        assert!(compute_vanilla_granularity_track("nodots", &q).is_none());

        let q = make_query_with_dims(None);
        assert!(compute_vanilla_granularity_track("Cube.dim", &q).is_none());

        let q = make_query_with_dims(None);
        assert!(compute_vanilla_granularity_track("Cube.dim.day.extra", &q).is_none());
    }

    #[test]
    fn test_compute_vanilla_granularity_track_known_granularity() {
        let q = make_query_with_dims(None);
        let track = compute_vanilla_granularity_track("Cube.orderDate.day", &q)
            .expect("should produce a track");
        assert_eq!(track.base_member, "Cube.orderDate");
        assert_eq!(track.level, 4);
    }

    #[test]
    fn test_compute_vanilla_granularity_track_levels_for_all_known_granularities() {
        let q = make_query_with_dims(None);
        let cases: &[(&str, u8)] = &[
            ("Cube.t.second", 1),
            ("Cube.t.minute", 2),
            ("Cube.t.hour", 3),
            ("Cube.t.day", 4),
            ("Cube.t.week", 5),
            ("Cube.t.month", 6),
            ("Cube.t.quarter", 7),
            ("Cube.t.year", 8),
        ];
        for (member, expected_level) in cases {
            let track = compute_vanilla_granularity_track(member, &q)
                .unwrap_or_else(|| panic!("expected Some for {}", member));
            assert_eq!(
                track.level, *expected_level,
                "level mismatch for {}",
                member
            );
            assert_eq!(track.base_member, "Cube.t");
        }
    }

    #[test]
    fn test_compute_vanilla_granularity_track_skips_when_base_in_dimensions() {
        let q = make_query_with_dims(Some(vec![MemberOrMemberExpression::Member(
            "Cube.orderDate".to_string(),
        )]));
        assert!(compute_vanilla_granularity_track("Cube.orderDate.day", &q).is_none());
    }

    #[test]
    fn test_compute_vanilla_granularity_track_proceeds_when_other_dims_present() {
        let q = make_query_with_dims(Some(vec![MemberOrMemberExpression::Member(
            "Cube.other".to_string(),
        )]));
        let track = compute_vanilla_granularity_track("Cube.orderDate.day", &q)
            .expect("should produce a track");
        assert_eq!(track.base_member, "Cube.orderDate");
    }

    fn make_config_item(member_type: &str) -> ConfigItem {
        ConfigItem {
            title: None,
            short_title: None,
            description: None,
            member_type: Some(member_type.to_string()),
            format: None,
            currency: None,
            meta: None,
            drill_members: None,
            drill_members_grouped: None,
            granularities: None,
            granularity: None,
        }
    }

    /// When all candidates are present, the bare key picks the finest level.
    #[test]
    fn test_get_vanilla_row_minimal_granularity_picks_finest_when_all_present() -> Result<()> {
        let mut alias_to_member_name_map: HashMap<String, String> = HashMap::new();
        alias_to_member_name_map.insert("t_day".to_string(), "Cube.t.day".to_string());
        alias_to_member_name_map.insert("t_month".to_string(), "Cube.t.month".to_string());

        let mut annotation: HashMap<String, ConfigItem> = HashMap::new();
        annotation.insert("Cube.t.day".to_string(), make_config_item("time"));
        annotation.insert("Cube.t.month".to_string(), make_config_item("time"));

        let query = make_query_with_dims(None);
        let raw_data = QueryResult::try_new(
            vec!["t_day".to_string(), "t_month".to_string()],
            vec![
                ColumnarArray::from(vec![DBResponsePrimitive::String(
                    "2024-06-15T00:00:00.000".to_string(),
                )]),
                ColumnarArray::from(vec![DBResponsePrimitive::String(
                    "2024-06-01T00:00:00.000".to_string(),
                )]),
            ],
        )?;
        let plan = build_vanilla_plan(
            &raw_data,
            &alias_to_member_name_map,
            &annotation,
            &query,
            &QueryType::RegularQuery,
        )?;
        let res = get_vanilla_row(&plan, 0)?;

        let day_transformed = transform_value(
            DBResponsePrimitive::String("2024-06-15T00:00:00.000".to_string()),
            "time",
        );
        assert_eq!(
            res.get(&InternedKey::new("Cube.t")),
            Some(&day_transformed),
            "bare base key must use the finest (day) candidate, not month"
        );
        Ok(())
    }
}
