use cubeclient::models::{
    V1LoadRequestQuery, V1LoadRequestQueryFilterItem, V1LoadRequestQueryTimeDimension,
};
use itertools::Itertools;
use serde::{ser::SerializeStruct, Serialize, Serializer};

use crate::sql::ColumnType;

use super::CompiledQuery;

#[derive(Debug, PartialEq, Clone)]
pub struct CompiledQueryFieldMeta {
    pub column_from: String,
    pub column_to: String,
    pub column_type: ColumnType,
}

impl Serialize for CompiledQueryFieldMeta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CompiledQueryFieldMeta", 3)?;
        state.serialize_field("column_from", &self.column_from)?;
        state.serialize_field("column_to", &self.column_to)?;
        state.serialize_field("column_type", &format!("{:?}", self.column_type))?;
        state.end()
    }
}

#[derive(Debug)]
pub struct QueryBuilder {
    // query body
    measures: Vec<String>,
    dimensions: Vec<String>,
    segments: Vec<String>,
    time_dimensions: Vec<V1LoadRequestQueryTimeDimension>,
    filters: Vec<V1LoadRequestQueryFilterItem>,
    order: Vec<Vec<String>>,
    limit: Option<i32>,
    offset: Option<i32>,
    // query meta for response hydration
    meta: Vec<CompiledQueryFieldMeta>,
}

impl QueryBuilder {
    pub fn new() -> QueryBuilder {
        QueryBuilder {
            measures: vec![],
            dimensions: vec![],
            segments: vec![],
            meta: vec![],
            order: vec![],
            time_dimensions: vec![],
            filters: vec![],
            limit: None,
            offset: None,
        }
    }

    pub fn with_time_dimension(
        &mut self,
        td: V1LoadRequestQueryTimeDimension,
        meta: CompiledQueryFieldMeta,
    ) {
        self.time_dimensions.push(td);
        self.meta.push(meta);
    }

    pub fn push_date_range_for_time_dimension(
        &mut self,
        member: &String,
        date_range: serde_json::Value,
    ) -> bool {
        let tdm_member_cnt = self
            .time_dimensions
            .iter()
            .filter(|tdm| tdm.dimension.eq(member))
            .count();
        if tdm_member_cnt == 0 {
            // If we didnt find any TD for member, let's push a new one without granularity (supported by Cube.js)
            self.time_dimensions.push(V1LoadRequestQueryTimeDimension {
                dimension: member.clone(),
                date_range: Some(date_range),
                granularity: None,
            });

            true
        } else if tdm_member_cnt == 1 {
            for tdm in self.time_dimensions.iter_mut() {
                if tdm.dimension.eq(member) {
                    tdm.date_range = Some(date_range);

                    return true;
                }
            }

            false
        } else {
            // We found multiple TD filters for specific filter and we cannot select which filter we should update
            false
        }
    }

    pub fn with_measure(&mut self, name: String, meta: CompiledQueryFieldMeta) {
        self.measures.push(name);
        self.meta.push(meta);
    }

    pub fn with_segment(&mut self, name: String) {
        self.segments.push(name);
    }

    pub fn with_dimension(&mut self, name: String, meta: CompiledQueryFieldMeta) {
        self.dimensions.push(name);
        self.meta.push(meta);
    }

    pub fn with_limit(&mut self, limit: i32) {
        self.limit = Some(limit);
    }

    pub fn with_offset(&mut self, offset: i32) {
        self.offset = Some(offset);
    }

    pub fn with_order(&mut self, order: Vec<String>) {
        self.order.push(order);
    }

    pub fn with_filters(&mut self, filters: Vec<V1LoadRequestQueryFilterItem>) {
        self.filters = filters;
    }

    pub fn with_filter(&mut self, filter: V1LoadRequestQueryFilterItem) {
        self.filters.push(filter);
    }

    pub fn build(self) -> super::CompiledQuery {
        CompiledQuery {
            request: V1LoadRequestQuery {
                measures: Some(self.measures.into_iter().unique().collect()),
                dimensions: Some(self.dimensions.into_iter().unique().collect()),
                segments: Some(self.segments.into_iter().unique().collect()),
                time_dimensions: if !self.time_dimensions.is_empty() {
                    Some(self.time_dimensions)
                } else {
                    None
                },
                order: if !self.order.is_empty() {
                    Some(self.order)
                } else {
                    None
                },
                limit: self.limit,
                offset: self.offset,
                filters: if !self.filters.is_empty() {
                    Some(self.filters)
                } else {
                    None
                },
            },
            meta: self.meta,
        }
    }
}
