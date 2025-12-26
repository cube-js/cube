/// Test enhanced pre-aggregation matching with Cube API metadata
///
/// This demonstrates how we use Cube API metadata to accurately parse
/// pre-aggregation table names, even when they contain ambiguous patterns.
///
/// Run with:
///   cd ~/projects/learn_erl/cube/rust/cubesql
///   CUBESQL_CUBESTORE_DIRECT=true \
///   CUBESQL_CUBE_URL=http://localhost:4008/cubejs-api \
///   CUBESQL_CUBESTORE_URL=ws://127.0.0.1:3030/ws \
///   cargo run --example test_enhanced_matching

use cubesql::cubestore::client::CubeStoreClient;
use cubeclient::apis::{configuration::Configuration, default_api as cube_api};
use datafusion::arrow::array::StringArray;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Enhanced Pre-aggregation Matching Test ===\n");

    let cube_url = std::env::var("CUBESQL_CUBE_URL")
        .unwrap_or_else(|_| "http://localhost:4008/cubejs-api".to_string());
    let cubestore_url = std::env::var("CUBESQL_CUBESTORE_URL")
        .unwrap_or_else(|_| "ws://127.0.0.1:3030/ws".to_string());

    // Step 1: Fetch cube names from Cube API
    println!("📡 Fetching cube metadata from: {}", cube_url);

    let mut config = Configuration::default();
    config.base_path = cube_url.clone();

    let meta_response = cube_api::meta_v1(&config, true).await?;
    let cubes = meta_response.cubes.unwrap_or_else(Vec::new);
    let cube_names: Vec<String> = cubes.iter().map(|c| c.name.clone()).collect();

    println!("\n✅ Found {} cubes:", cube_names.len());
    for (idx, name) in cube_names.iter().enumerate() {
        println!("   {}. {}", idx + 1, name);
    }

    // Step 2: Query CubeStore for pre-aggregation tables
    println!("\n📊 Querying CubeStore metastore: {}", cubestore_url);

    let client = CubeStoreClient::new(cubestore_url);

    let sql = r#"
        SELECT
            table_schema,
            table_name
        FROM system.tables
        WHERE
            table_schema NOT IN ('information_schema', 'system', 'mysql')
            AND is_ready = true
            AND has_data = true
        ORDER BY table_name
    "#;

    let batches = client.query(sql.to_string()).await?;

    println!("\n✅ Pre-aggregation tables with enhanced parsing:\n");
    println!("{:-<120}", "");
    println!("{:<60} {:<30} {:<30}", "Table Name", "Cube", "Pre-agg");
    println!("{:-<120}", "");

    let mut total_tables = 0;
    let mut parsed_count = 0;

    for batch in batches {
        let schema_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let table_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();

        for i in 0..batch.num_rows() {
            total_tables += 1;
            let table_name = table_col.value(i);

            // Simulate the parsing logic (simplified version)
            let parts: Vec<&str> = table_name.split('_').collect();

            // Find hash start
            let hash_start = parts.iter()
                .position(|p| p.len() >= 8 && p.chars().all(|c| c.is_alphanumeric()))
                .unwrap_or(parts.len() - 3);

            // Try to match cube names (longest first)
            let mut sorted_cubes = cube_names.clone();
            sorted_cubes.sort_by_key(|c| std::cmp::Reverse(c.len()));

            let mut matched = false;
            for cube_name in &sorted_cubes {
                let cube_parts: Vec<&str> = cube_name.split('_').collect();

                if parts.len() >= cube_parts.len() && parts[..cube_parts.len()] == cube_parts[..] {
                    let preagg_parts = &parts[cube_parts.len()..hash_start];
                    if !preagg_parts.is_empty() {
                        let preagg_name = preagg_parts.join("_");
                        println!("{:<60} {:<30} {:<30}", table_name, cube_name, preagg_name);
                        parsed_count += 1;
                        matched = true;
                        break;
                    }
                }
            }

            if !matched {
                println!("{:<60} {:<30} {:<30}", table_name, "⚠️ UNKNOWN", "⚠️ FAILED");
            }
        }
    }

    println!("{:-<120}", "");
    println!("\n📈 Results:");
    println!("   Total tables: {}", total_tables);
    println!("   Successfully parsed: {}", parsed_count);
    println!("   Failed: {}", total_tables - parsed_count);

    if parsed_count == total_tables {
        println!("\n✅ All tables successfully matched to cube names!");
    } else {
        println!("\n⚠️  Some tables could not be matched. Check cube name patterns.");
    }

    Ok(())
}
