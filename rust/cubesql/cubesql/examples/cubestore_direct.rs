use cubesql::cubestore::client::CubeStoreClient;
use datafusion::arrow;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let cubestore_url = env::var("CUBESQL_CUBESTORE_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3030/ws".to_string());

    println!("==========================================");
    println!("CubeStore Direct Connection Test");
    println!("==========================================");
    println!("Connecting to CubeStore at: {}", cubestore_url);
    println!();

    let client = CubeStoreClient::new(cubestore_url);

    // Test 1: Query information schema
    println!("Test 1: Querying information schema");
    println!("------------------------------------------");
    let sql = "SELECT * FROM information_schema.tables LIMIT 5";
    println!("SQL: {}", sql);
    println!();

    match client.query(sql.to_string()).await {
        Ok(batches) => {
            println!("✓ Query successful!");
            println!("  Results: {} batches", batches.len());
            println!();

            for (batch_idx, batch) in batches.iter().enumerate() {
                println!("  Batch {}: {} rows × {} columns",
                    batch_idx, batch.num_rows(), batch.num_columns());

                // Print schema
                println!("  Schema:");
                for field in batch.schema().fields() {
                    println!("    - {} ({})", field.name(), field.data_type());
                }
                println!();

                // Print first few rows
                if batch.num_rows() > 0 {
                    println!("  Data (first 3 rows):");
                    let num_rows = batch.num_rows().min(3);
                    for row_idx in 0..num_rows {
                        print!("    Row {}: [", row_idx);
                        for col_idx in 0..batch.num_columns() {
                            let column = batch.column(col_idx);

                            // Format value based on type
                            let value_str = if column.is_null(row_idx) {
                                "NULL".to_string()
                            } else {
                                match column.data_type() {
                                    arrow::datatypes::DataType::Utf8 => {
                                        let array = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::StringArray>()
                                            .unwrap();
                                        format!("\"{}\"", array.value(row_idx))
                                    }
                                    arrow::datatypes::DataType::Int64 => {
                                        let array = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::Int64Array>()
                                            .unwrap();
                                        format!("{}", array.value(row_idx))
                                    }
                                    arrow::datatypes::DataType::Float64 => {
                                        let array = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::Float64Array>()
                                            .unwrap();
                                        format!("{}", array.value(row_idx))
                                    }
                                    arrow::datatypes::DataType::Boolean => {
                                        let array = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::BooleanArray>()
                                            .unwrap();
                                        format!("{}", array.value(row_idx))
                                    }
                                    _ => format!("{:?}", column.slice(row_idx, 1)),
                                }
                            };

                            print!("{}", value_str);
                            if col_idx < batch.num_columns() - 1 {
                                print!(", ");
                            }
                        }
                        println!("]");
                    }
                    println!();
                }
            }
        }
        Err(e) => {
            println!("✗ Query failed: {}", e);
            return Err(e.into());
        }
    }

    // Test 2: Simple SELECT query
    println!();
    println!("Test 2: Simple SELECT");
    println!("------------------------------------------");
    let sql2 = "SELECT 1 as num, 'hello' as text, true as flag";
    println!("SQL: {}", sql2);
    println!();

    match client.query(sql2.to_string()).await {
        Ok(batches) => {
            println!("✓ Query successful!");
            println!("  Results: {} batches", batches.len());
            println!();

            for (batch_idx, batch) in batches.iter().enumerate() {
                println!("  Batch {}: {} rows × {} columns",
                    batch_idx, batch.num_rows(), batch.num_columns());

                println!("  Schema:");
                for field in batch.schema().fields() {
                    println!("    - {} ({})", field.name(), field.data_type());
                }
                println!();

                if batch.num_rows() > 0 {
                    println!("  Data:");
                    for row_idx in 0..batch.num_rows() {
                        print!("    Row {}: [", row_idx);
                        for col_idx in 0..batch.num_columns() {
                            let column = batch.column(col_idx);
                            let value_str = if column.is_null(row_idx) {
                                "NULL".to_string()
                            } else {
                                match column.data_type() {
                                    arrow::datatypes::DataType::Utf8 => {
                                        let array = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::StringArray>()
                                            .unwrap();
                                        format!("\"{}\"", array.value(row_idx))
                                    }
                                    arrow::datatypes::DataType::Int64 => {
                                        let array = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::Int64Array>()
                                            .unwrap();
                                        format!("{}", array.value(row_idx))
                                    }
                                    arrow::datatypes::DataType::Float64 => {
                                        let array = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::Float64Array>()
                                            .unwrap();
                                        format!("{}", array.value(row_idx))
                                    }
                                    arrow::datatypes::DataType::Boolean => {
                                        let array = column
                                            .as_any()
                                            .downcast_ref::<arrow::array::BooleanArray>()
                                            .unwrap();
                                        format!("{}", array.value(row_idx))
                                    }
                                    _ => format!("{:?}", column.slice(row_idx, 1)),
                                }
                            };
                            print!("{}", value_str);
                            if col_idx < batch.num_columns() - 1 {
                                print!(", ");
                            }
                        }
                        println!("]");
                    }
                }
            }
        }
        Err(e) => {
            println!("✗ Query failed: {}", e);
            return Err(e.into());
        }
    }

    println!();
    println!("==========================================");
    println!("✓ All tests passed!");
    println!("==========================================");

    Ok(())
}
