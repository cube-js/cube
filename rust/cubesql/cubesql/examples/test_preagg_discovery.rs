/// Test pre-aggregation table discovery from CubeStore metastore
///
/// This example demonstrates how to query system.tables from CubeStore
/// to discover pre-aggregation table names.
///
/// Prerequisites:
/// 1. CubeStore must be running on ws://127.0.0.1:3030/ws
///
/// Run with:
///   cd ~/projects/learn_erl/cube/rust/cubesql
///   cargo run --example test_preagg_discovery
use cubesql::cubestore::client::CubeStoreClient;
use datafusion::arrow::array::StringArray;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Pre-aggregation Table Discovery Test ===\n");

    let cubestore_url = std::env::var("CUBESQL_CUBESTORE_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3030/ws".to_string());

    println!("Connecting to CubeStore at: {}", cubestore_url);

    let client = CubeStoreClient::new(cubestore_url);

    // Query system.tables from CubeStore metastore
    let sql = r#"
        SELECT
            table_schema,
            table_name,
            is_ready,
            has_data
        FROM system.tables
        WHERE
            table_schema NOT IN ('information_schema', 'system', 'mysql')
        ORDER BY table_schema, table_name
    "#;

    println!("\nExecuting query:\n{}\n", sql);

    match client.query(sql.to_string()).await {
        Ok(batches) => {
            println!("✅ Successfully queried system.tables\n");

            let mut total_rows = 0;
            for (batch_idx, batch) in batches.iter().enumerate() {
                println!("Batch {}: {} rows", batch_idx + 1, batch.num_rows());
                total_rows += batch.num_rows();

                if batch.num_rows() > 0 {
                    let schema_col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    let table_col = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();

                    println!("\nPre-aggregation tables found:");
                    println!("{:-<60}", "");
                    println!("{:<30} {:<30}", "Schema", "Table");
                    println!("{:-<60}", "");

                    for i in 0..batch.num_rows() {
                        let schema = schema_col.value(i);
                        let table = table_col.value(i);
                        println!("{:<30} {:<30}", schema, table);
                    }
                }
            }

            println!("\n{:-<60}", "");
            println!("Total tables found: {}\n", total_rows);

            if total_rows == 0 {
                println!("⚠️  No pre-aggregation tables found.");
                println!("This might mean:");
                println!("  1. Pre-aggregations haven't been built yet");
                println!("  2. CubeStore is empty");
                println!("  3. Tables are in a different schema");
            } else {
                println!("✅ Table discovery successful!");
            }
        }
        Err(e) => {
            println!("❌ Failed to query system.tables: {}", e);
            println!("\nPossible causes:");
            println!("  1. CubeStore not running");
            println!("  2. Connection refused");
            println!("  3. system.tables not available");
            return Err(e.into());
        }
    }

    Ok(())
}
