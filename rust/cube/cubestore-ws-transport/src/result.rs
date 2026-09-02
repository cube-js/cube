use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

/// Wire encoding the server actually used for this result set. The client may
/// request `Arrow` but an older server can still answer with the legacy
/// row-format envelope, so this reflects what was decoded, not what was asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResponseFormat {
    #[default]
    Legacy,
    Arrow,
    /// The command completed without a result set (zero columns). This is a
    /// decoded-from-the-wire descriptor only — clients can never request it.
    Completed,
}

impl std::fmt::Display for ResponseFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseFormat::Legacy => f.write_str("Legacy"),
            ResponseFormat::Arrow => f.write_str("Arrow"),
            ResponseFormat::Completed => f.write_str("Completed"),
        }
    }
}

/// Decoded payload, preserving the original on-wire shape. Renderers can
/// format-aware-render (e.g. iterate arrow arrays directly) or fall through
/// to the legacy string rows. Column names live inside the variant so they can
/// be derived from the data — there's no separate "header" on the wire.
#[derive(Debug, Clone)]
pub enum ResultData {
    /// Per-cell stringified rows, as carried in the legacy FlatBuffers envelope.
    Legacy {
        columns: Vec<String>,
        rows: Vec<Vec<Option<String>>>,
    },
    /// Raw Arrow record batches decoded from the IPC stream. The schema is
    /// always present — the IPC stream writes it before any data batch — so
    /// column names survive even when `batches` is empty.
    Arrow {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    },
    /// The command completed without a result set (zero columns) — e.g. CREATE
    /// TABLE/INSERT or queue/cache writes. Carries no columns and no rows.
    Completed,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub data: ResultData,
}

impl QueryResult {
    pub fn is_empty(&self) -> bool {
        self.row_count() == 0
    }

    pub fn row_count(&self) -> usize {
        match &self.data {
            ResultData::Legacy { rows, .. } => rows.len(),
            ResultData::Arrow { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
            ResultData::Completed => 0,
        }
    }

    /// Wire encoding the server used, derived from the decoded payload variant.
    pub fn get_format(&self) -> ResponseFormat {
        match &self.data {
            ResultData::Legacy { .. } => ResponseFormat::Legacy,
            ResultData::Arrow { .. } => ResponseFormat::Arrow,
            ResultData::Completed => ResponseFormat::Completed,
        }
    }

    /// Column names derived from the payload — the legacy variant stores them
    /// alongside the row vector, the arrow variant pulls them from the schema.
    pub fn get_columns(&self) -> Vec<String> {
        match &self.data {
            ResultData::Legacy { columns, .. } => columns.clone(),
            ResultData::Arrow { schema, .. } => {
                schema.fields().iter().map(|f| f.name().clone()).collect()
            }
            ResultData::Completed => Vec::new(),
        }
    }
}
