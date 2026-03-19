use super::pg_service;
use super::TestContext;
use crate::test_fixtures::cube_bridge::MockSchema;
use tokio_postgres::Client;

pub struct IntegrationTestContext {
    test_context: TestContext,
    client: Client,
}

impl IntegrationTestContext {
    pub async fn new(schema: MockSchema, seed_file: &str) -> Self {
        let client = pg_service::connect().await;
        pg_service::run_seed(&client, seed_file).await;
        let test_context = TestContext::new(schema).expect("Failed to create TestContext");
        Self {
            test_context,
            client,
        }
    }

    pub async fn execute_query(&self, query_yaml: &str) -> String {
        let sql = self
            .test_context
            .build_sql(query_yaml)
            .expect("Failed to build SQL");

        let messages = self
            .client
            .simple_query(&sql)
            .await
            .unwrap_or_else(|e| panic!("SQL execution failed:\n{}\n\nError: {}", sql, e));

        format_query_results(&messages)
    }
}

fn format_query_results(messages: &[tokio_postgres::SimpleQueryMessage]) -> String {
    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();

    for msg in messages {
        match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                if columns.is_empty() {
                    columns = (0..row.columns().len())
                        .map(|i| row.columns()[i].name().to_string())
                        .collect();
                }
                let values: Vec<String> = (0..row.columns().len())
                    .map(|i| row.try_get(i).unwrap_or(None).unwrap_or("NULL").to_string())
                    .collect();
                rows.push(values);
            }
            tokio_postgres::SimpleQueryMessage::CommandComplete(_) => {}
            _ => {}
        }
    }

    if columns.is_empty() {
        return "(empty result)".to_string();
    }

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in &rows {
        for (i, val) in row.iter().enumerate() {
            if val.len() > widths[i] {
                widths[i] = val.len();
            }
        }
    }

    let mut result = String::new();

    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
        .collect();
    result.push_str(&header.join(" | "));
    result.push('\n');

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    result.push_str(&sep.join("-+-"));
    result.push('\n');

    for row in &rows {
        let formatted: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| format!("{:width$}", v, width = widths[i]))
            .collect();
        result.push_str(&formatted.join(" | "));
        result.push('\n');
    }

    result
}
