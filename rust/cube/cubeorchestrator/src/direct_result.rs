//! Serializing a response straight from the source columns.
//!
//! [`crate::query_result_transform::TransformedData::transform`] builds the whole
//! output in memory before anything is written. For the callers that only ever
//! serialize the result and drop it (`getFinalQueryResult` and its multi
//! variant), [`DirectData`] renders the `data` member while serializing instead:
//! one cell is materialized at a time, read from the source column — Arrow memory
//! included — so neither the intermediate primitives nor the output dataset are
//! ever fully materialized.
//!
//! Values are still rendered by `Serialize for DBResponsePrimitive` and
//! [`transform_value`], so the JSON is byte-for-byte what the materializing path
//! produces. `test_direct_matches_transformed_*` asserts exactly that.

use crate::{
    query_message_parser::QueryResult,
    query_result_transform::{
        build_columnar_plan, build_compact_plan, get_members, ColumnarColumnPlan,
        ColumnarColumnSource, CompactPlan, CompactPlanEntry, DBResponsePrimitive, TransformedData,
    },
    transport::{QueryType, ResultType, TransformDataRequest},
};
use serde::{
    ser::{Error as SerError, SerializeSeq, SerializeStruct},
    Serialize, Serializer,
};

/// The `data` member of a response, rendered from `source` as it is serialized.
pub struct DirectData<'a> {
    request: &'a TransformDataRequest,
    source: &'a QueryResult,
}

impl<'a> DirectData<'a> {
    pub fn new(request: &'a TransformDataRequest, source: &'a QueryResult) -> Self {
        Self { request, source }
    }

    fn query_type(&self) -> QueryType {
        self.request.query_type.clone().unwrap_or_default()
    }

    fn serialize_compact<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let query_type = self.query_type();
        let request = self.request;

        let (members_to_alias_map, members) = get_members(
            &query_type,
            &request.query,
            self.source,
            &request.alias_to_member_name_map,
            &request.annotation,
        )
        .map_err(S::Error::custom)?;

        let plan = build_compact_plan(
            &members,
            &members_to_alias_map,
            &request.annotation,
            self.source,
            &query_type,
            request.query.time_dimensions.as_ref(),
        )
        .map_err(S::Error::custom)?;

        let mut out = serializer.serialize_struct("TransformedData", 2)?;
        out.serialize_field("members", &members)?;
        out.serialize_field(
            "dataset",
            &CompactDataset {
                plan: &plan,
                row_count: self.source.row_count(),
            },
        )?;
        out.end()
    }

    fn serialize_columnar<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let query_type = self.query_type();
        let request = self.request;

        let (members_to_alias_map, members) = get_members(
            &query_type,
            &request.query,
            self.source,
            &request.alias_to_member_name_map,
            &request.annotation,
        )
        .map_err(S::Error::custom)?;

        let plan = build_columnar_plan(
            &members,
            &members_to_alias_map,
            &request.annotation,
            &self.source.columns_pos,
            &query_type,
            request.query.time_dimensions.as_ref(),
        )
        .map_err(S::Error::custom)?;

        let mut out = serializer.serialize_struct("TransformedData", 2)?;
        out.serialize_field("members", &members)?;
        out.serialize_field(
            "columns",
            &ColumnarColumns {
                plan: &plan,
                source: self.source,
            },
        )?;
        out.end()
    }
}

impl Serialize for DirectData<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.request.res_type {
            Some(ResultType::Compact) => self.serialize_compact(serializer),
            Some(ResultType::Columnar) => self.serialize_columnar(serializer),
            // The vanilla format keeps materializing: its rows are maps, and the
            // deprecated-granularity and blending keys rely on the row map
            // deduplicating them.
            _ => TransformedData::transform(self.request, self.source)
                .map_err(S::Error::custom)?
                .serialize(serializer),
        }
    }
}

/// `dataset` of a compact result: one row at a time, no row ever kept.
struct CompactDataset<'a> {
    plan: &'a CompactPlan<'a>,
    row_count: usize,
}

impl Serialize for CompactDataset<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.row_count))?;
        for row_idx in 0..self.row_count {
            seq.serialize_element(&CompactRow {
                plan: self.plan,
                row_idx,
            })?;
        }
        seq.end()
    }
}

struct CompactRow<'a> {
    plan: &'a CompactPlan<'a>,
    row_idx: usize,
}

impl Serialize for CompactRow<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let entries = &self.plan.entries;
        let mut seq = serializer.serialize_seq(Some(entries.len()))?;

        for entry in entries {
            match entry {
                CompactPlanEntry::Cell {
                    column,
                    member_type,
                } => column.with_transformed(self.row_idx, member_type, |value| {
                    seq.serialize_element(value)
                })?,
                CompactPlanEntry::Constant(value) => seq.serialize_element(value)?,
            }
        }

        seq.end()
    }
}

/// `columns` of a columnar result: one column at a time, read column-major.
struct ColumnarColumns<'a> {
    plan: &'a [ColumnarColumnPlan<'a>],
    source: &'a QueryResult,
}

impl Serialize for ColumnarColumns<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.plan.len()))?;
        for entry in self.plan {
            seq.serialize_element(&ColumnarColumn {
                entry,
                source: self.source,
            })?;
        }
        seq.end()
    }
}

struct ColumnarColumn<'a> {
    entry: &'a ColumnarColumnPlan<'a>,
    source: &'a QueryResult,
}

impl Serialize for ColumnarColumn<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let row_count = self.source.row_count();
        let mut seq = serializer.serialize_seq(Some(row_count))?;

        match &self.entry.source {
            ColumnarColumnSource::DbColumn { index } => {
                // Column-major, so the reader keeps its type dispatch outside the
                // row loop — the same tight fill the materializing path uses.
                let reader = self.source.reader(*index).map_err(S::Error::custom)?;
                reader.for_each_transformed(self.entry.member_type, |value| {
                    seq.serialize_element(value.as_ref())
                })?;
            }
            ColumnarColumnSource::Constant(value) => {
                for _ in 0..row_count {
                    seq.serialize_element(value)?;
                }
            }
            ColumnarColumnSource::NullFilled => {
                for _ in 0..row_count {
                    seq.serialize_element(&DBResponsePrimitive::Null)?;
                }
            }
        }

        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_result_transform::tests::{
        make_result_head, StorageFixture, ALL_RES_TYPES, TEST_SUITE_DATA,
    };
    use crate::query_result_transform::RequestResultDataMulti;
    use anyhow::Result;
    use std::sync::Arc;

    /// The streamed response must be byte-identical to the materialized one.
    fn assert_direct_matches_transformed(
        request: &TransformDataRequest,
        source: &QueryResult,
        context: &str,
    ) -> Result<()> {
        let head = make_result_head(request.query.clone());

        let materialized = head
            .clone()
            .with_data(TransformedData::transform(request, source)?);
        let direct = head.with_data(DirectData::new(request, source));

        assert_eq!(
            serde_json::to_string(&materialized)?,
            serde_json::to_string(&direct)?,
            "{context}: streamed JSON must match the materialized JSON"
        );

        Ok(())
    }

    /// Covers the regular, compare-date-range and blending query shapes — the
    /// three that build different plans — in every response format.
    #[test]
    fn test_direct_matches_transformed_for_all_fixtures() -> Result<()> {
        for (name, test_data) in TEST_SUITE_DATA.iter() {
            let source = QueryResult::from_js_raw_data(test_data.query_result.clone())?;

            for res_type in ALL_RES_TYPES {
                let mut request = test_data.request.clone();
                request.res_type = res_type.clone();

                assert_direct_matches_transformed(
                    &request,
                    &source,
                    &format!("{name} / {res_type:?}"),
                )?;
            }
        }

        Ok(())
    }

    /// Same check with Arrow-backed columns, which decode cells while serializing.
    #[test]
    fn test_direct_matches_transformed_for_arrow_source() -> Result<()> {
        let fixture = StorageFixture::new()?;

        for res_type in ALL_RES_TYPES {
            let request = fixture.request(res_type.clone());
            assert_direct_matches_transformed(&request, &fixture.arrow, &format!("{res_type:?}"))?;
            assert_direct_matches_transformed(
                &request,
                &fixture.columnar,
                &format!("{res_type:?}"),
            )?;
        }

        Ok(())
    }

    /// The multi envelope must match too, `pivotQuery` included.
    #[test]
    fn test_direct_matches_transformed_multi() -> Result<()> {
        let fixture = StorageFixture::new()?;
        let requests = [
            fixture.request(Some(ResultType::Compact)),
            fixture.request(Some(ResultType::Columnar)),
        ];
        let sources = [&fixture.arrow, &fixture.columnar];

        let envelope = RequestResultDataMulti {
            query_type: QueryType::RegularQuery,
            results: requests
                .iter()
                .map(|request| make_result_head(request.query.clone()))
                .collect(),
            pivot_query: None,
            slow_query: false,
        };

        let mut materialized = envelope.clone();
        let owned_sources: Vec<_> = sources.iter().map(|s| Arc::new((*s).clone())).collect();
        materialized.prepare_results(&requests, &owned_sources)?;

        let mut direct = envelope;
        direct.prepare_pivot_query()?;
        let direct = direct.with_data(
            requests
                .iter()
                .zip(sources)
                .map(|(request, source)| DirectData::new(request, source))
                .collect(),
        )?;

        assert_eq!(
            serde_json::to_string(&materialized)?,
            serde_json::to_string(&direct)?,
            "streamed multi JSON must match the materialized multi JSON"
        );

        Ok(())
    }

    /// A plan that cannot be built still fails on the streaming path — it just
    /// surfaces as a serializer error instead of before serialization.
    #[test]
    fn test_direct_reports_plan_errors() -> Result<()> {
        let fixture = StorageFixture::new()?;
        let mut request = fixture.request(Some(ResultType::Compact));
        request.alias_to_member_name_map.clear();

        assert!(
            TransformedData::transform(&request, &fixture.arrow).is_err(),
            "materializing path must reject an unmapped alias"
        );

        let head = make_result_head(request.query.clone());
        let err = serde_json::to_string(&head.with_data(DirectData::new(&request, &fixture.arrow)))
            .expect_err("streaming path must reject an unmapped alias");
        assert!(
            err.to_string().contains("Member name not found for alias"),
            "unexpected error: {err}"
        );

        Ok(())
    }
}
