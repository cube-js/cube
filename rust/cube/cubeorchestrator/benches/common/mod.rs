#![allow(dead_code)]

use cubeorchestrator::query_result_transform::DBResponsePrimitive;
use cubeorchestrator::transport::JsRawColumnarData;

pub const ROW_COUNTS: &[usize] = &[1_000, 10_000, 50_000, 100_000];
pub const COLUMN_COUNTS: &[usize] = &[8, 16, 32, 64];

#[derive(Clone)]
pub struct TimeColumn {
    pub member: String,
    pub alias: String,
}

/// Split a target column count into ~60% dimensions and ~40% measures.
pub fn split_dim_measure(col_count: usize) -> (usize, usize) {
    let dim_count = (col_count * 6) / 10;
    let measure_count = col_count - dim_count;
    (dim_count, measure_count)
}

pub fn make_member_aliases(prefix: &str, count: usize) -> Vec<(String, String)> {
    (0..count)
        .map(|i| {
            (
                format!("Sales.{}{}", prefix, i),
                format!("sales__{}{}", prefix, i),
            )
        })
        .collect()
}

pub fn build_dataset(
    row_count: usize,
    dimensions: &[(String, String)],
    measures: &[(String, String)],
    time_dims: &[TimeColumn],
) -> JsRawColumnarData {
    let total_cols = dimensions.len() + measures.len() + time_dims.len();
    let mut members = Vec::with_capacity(total_cols);
    let mut columns: Vec<Vec<DBResponsePrimitive>> = Vec::with_capacity(total_cols);

    for (j, (_, alias)) in dimensions.iter().enumerate() {
        members.push(alias.clone());
        let mut col = Vec::with_capacity(row_count);
        for i in 0..row_count {
            col.push(DBResponsePrimitive::String(format!(
                "dim_{}_{}",
                j,
                i % 1000
            )));
        }
        columns.push(col);
    }
    for (j, (_, alias)) in measures.iter().enumerate() {
        members.push(alias.clone());
        let mut col = Vec::with_capacity(row_count);
        for i in 0..row_count {
            col.push(DBResponsePrimitive::Number(((i * (j + 1)) as f64) * 0.5));
        }
        columns.push(col);
    }
    for (j, td) in time_dims.iter().enumerate() {
        members.push(td.alias.clone());
        let mut col = Vec::with_capacity(row_count);
        for i in 0..row_count {
            // Format mirrors typical CubeStore output: ISO-8601 with millisecond
            // fractional and no timezone.
            let month = ((i + j) % 12) + 1;
            let day = ((i / 12) % 28) + 1;
            col.push(DBResponsePrimitive::String(format!(
                "2024-{:02}-{:02}T00:00:00.000",
                month, day
            )));
        }
        columns.push(col);
    }

    JsRawColumnarData { members, columns }
}
