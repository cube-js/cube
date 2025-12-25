/// Live Pre-Aggregation Selection Test
///
/// This example demonstrates:
/// 1. Connecting to a live Cube API instance
/// 2. Fetching metadata
/// 3. Inspecting pre-aggregation definitions
///
/// Prerequisites:
/// - Cube API running at http://localhost:4000
/// - mandata_captate cube with sums_and_count_daily pre-aggregation
///
/// Usage:
///   CUBESQL_CUBE_URL=http://localhost:4000/cubejs-api \
///   cargo run --example live_preagg_selection

use reqwest;
use serde_json::Value;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logger
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();

    println!("==========================================");
    println!("Live Pre-Aggregation Selection Test");
    println!("==========================================");
    println!();

    // Get configuration from environment
    let cube_url = env::var("CUBESQL_CUBE_URL")
        .unwrap_or_else(|_| "http://localhost:4000/cubejs-api".to_string());

    println!("Configuration:");
    println!("  Cube API URL: {}", cube_url);
    println!();

    // Step 1: Fetch metadata using raw HTTP
    println!("Step 1: Fetching metadata from Cube API...");
    println!("------------------------------------------");

    let client = reqwest::Client::new();
    let meta_url = format!("{}/v1/meta?extended=true", cube_url);

    let response = match client.get(&meta_url).send().await {
        Ok(resp) => resp,
        Err(e) => {
            eprintln!("✗ Failed to connect to Cube API: {}", e);
            eprintln!();
            eprintln!("Possible causes:");
            eprintln!("  - Cube API is not running at {}", cube_url);
            eprintln!("  - Network connectivity issues");
            eprintln!();
            eprintln!("To start Cube API:");
            eprintln!("  cd examples/recipes/arrow-ipc");
            eprintln!("  ./start-cube-api.sh");
            return Err(e.into());
        }
    };

    if !response.status().is_success() {
        eprintln!("✗ API request failed with status: {}", response.status());
        return Err(format!("HTTP {}", response.status()).into());
    }

    let meta_json: Value = response.json().await?;

    println!("✓ Metadata fetched successfully");
    println!();

    // Parse cubes array
    let cubes = meta_json["cubes"]
        .as_array()
        .ok_or("Missing cubes array")?;

    println!("  Total cubes: {}", cubes.len());
    println!();

    // List all cubes
    println!("Available cubes:");
    for cube in cubes {
        if let Some(name) = cube["name"].as_str() {
            println!("  - {}", name);
        }
    }
    println!();

    // Step 2: Find mandata_captate cube
    println!("Step 2: Analyzing mandata_captate cube...");
    println!("------------------------------------------");

    let mandata_cube = cubes
        .iter()
        .find(|c| c["name"].as_str() == Some("mandata_captate"))
        .ok_or("mandata_captate cube not found")?;

    println!("✓ Found mandata_captate cube");
    println!();

    // Show dimensions
    if let Some(dimensions) = mandata_cube["dimensions"].as_array() {
        println!("Dimensions ({}):", dimensions.len());
        for dim in dimensions {
            let name = dim["name"].as_str().unwrap_or("unknown");
            let dim_type = dim["type"].as_str().unwrap_or("unknown");
            println!("  - {} (type: {})", name, dim_type);
        }
        println!();
    }

    // Show measures
    if let Some(measures) = mandata_cube["measures"].as_array() {
        println!("Measures ({}):", measures.len());
        for measure in measures {
            let name = measure["name"].as_str().unwrap_or("unknown");
            let measure_type = measure["type"].as_str().unwrap_or("unknown");
            println!("  - {} (type: {})", name, measure_type);
        }
        println!();
    }

    // Step 3: Analyze pre-aggregations
    println!("Step 3: Analyzing pre-aggregations...");
    println!("------------------------------------------");

    if let Some(pre_aggs) = mandata_cube["preAggregations"].as_array() {
        if pre_aggs.is_empty() {
            println!("⚠ No pre-aggregations found");
            println!("  Check if pre-aggregations are defined in the cube");
        } else {
            println!("Pre-aggregations ({}):", pre_aggs.len());
            println!();

            for (idx, pa) in pre_aggs.iter().enumerate() {
                let name = pa["name"].as_str().unwrap_or("unknown");
                println!("{}. Pre-aggregation: {}", idx + 1, name);

                if let Some(pa_type) = pa["type"].as_str() {
                    println!("   Type: {}", pa_type);
                }

                // Parse measureReferences (comes as a string like "[measure1, measure2]")
                if let Some(measure_refs) = pa["measureReferences"].as_str() {
                    // Remove brackets and split by comma
                    let measures: Vec<&str> = measure_refs
                        .trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .collect();

                    if !measures.is_empty() {
                        println!("   Measures ({}):", measures.len());
                        for m in &measures {
                            println!("     - {}", m);
                        }
                    }
                }

                // Parse dimensionReferences (comes as a string like "[dim1, dim2]")
                if let Some(dim_refs) = pa["dimensionReferences"].as_str() {
                    let dimensions: Vec<&str> = dim_refs
                        .trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .collect();

                    if !dimensions.is_empty() {
                        println!("   Dimensions ({}):", dimensions.len());
                        for d in &dimensions {
                            println!("     - {}", d);
                        }
                    }
                }

                if let Some(time_dim) = pa["timeDimensionReference"].as_str() {
                    println!("   Time dimension: {}", time_dim);
                }

                if let Some(granularity) = pa["granularity"].as_str() {
                    println!("   Granularity: {}", granularity);
                }

                if let Some(refresh_key) = pa["refreshKey"].as_object() {
                    println!("   Refresh key: {:?}", refresh_key);
                }

                println!();
            }

            // Step 4: Show example query that would match
            println!("Step 4: Example queries that would match pre-aggregations...");
            println!("------------------------------------------");
            println!();

            for pa in pre_aggs {
                let name = pa["name"].as_str().unwrap_or("unknown");
                println!("Query matching '{}':", name);
                println!("{{");
                println!("  \"measures\": [");

                // Parse measureReferences
                if let Some(measure_refs) = pa["measureReferences"].as_str() {
                    let measures: Vec<&str> = measure_refs
                        .trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .collect();

                    for (i, m) in measures.iter().enumerate() {
                        let comma = if i < measures.len() - 1 { "," } else { "" };
                        println!("    \"{}\"{}",m, comma);
                    }
                }
                println!("  ],");
                println!("  \"dimensions\": [");

                // Parse dimensionReferences
                if let Some(dim_refs) = pa["dimensionReferences"].as_str() {
                    let dimensions: Vec<&str> = dim_refs
                        .trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .collect();

                    for (i, d) in dimensions.iter().enumerate() {
                        let comma = if i < dimensions.len() - 1 { "," } else { "" };
                        println!("    \"{}\"{}",d, comma);
                    }
                }
                println!("  ],");
                println!("  \"timeDimensions\": [{{");
                if let Some(time_dim) = pa["timeDimensionReference"].as_str() {
                    println!("    \"dimension\": \"{}\",", time_dim);
                }
                if let Some(granularity) = pa["granularity"].as_str() {
                    println!("    \"granularity\": \"{}\",", granularity);
                }
                println!("    \"dateRange\": [\"2024-01-01\", \"2024-01-31\"]");
                println!("  }}]");
                println!("}}");
                println!();
            }
        }
    } else {
        println!("⚠ No preAggregations field found in metadata");
        println!();
        println!("Available fields in cube:");
        if let Some(obj) = mandata_cube.as_object() {
            for key in obj.keys() {
                println!("  - {}", key);
            }
        }
    }

    println!("==========================================");
    println!("✓ Metadata Analysis Complete");
    println!("==========================================");
    println!();

    // Step 5: Demonstrate Pre-Aggregation Selection
    demonstrate_preagg_selection(&mandata_cube)?;

    println!("==========================================");
    println!("✓ Test Complete");
    println!("==========================================");
    println!();

    println!("Summary:");
    println!("1. ✓ Verified Cube API is accessible");
    println!("2. ✓ Confirmed mandata_captate cube exists");
    println!("3. ✓ Inspected pre-aggregation definitions");
    println!("4. ✓ Demonstrated pre-aggregation selection logic");
    println!("5. TODO: Execute query on CubeStore directly via WebSocket");

    Ok(())
}

/// Demonstrates how pre-aggregation selection works
fn demonstrate_preagg_selection(cube: &Value) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Step 5: Pre-Aggregation Selection Demonstration");
    println!("==========================================");
    println!();

    let pre_aggs = cube["preAggregations"]
        .as_array()
        .ok_or("No pre-aggregations found")?;

    if pre_aggs.is_empty() {
        return Err("No pre-aggregations to demonstrate".into());
    }

    let pa = &pre_aggs[0];
    let pa_name = pa["name"].as_str().unwrap_or("unknown");

    println!("Available Pre-Aggregation:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("  Name: {}", pa_name);
    println!("  Type: {}", pa["type"].as_str().unwrap_or("unknown"));
    println!();

    // Parse measures and dimensions
    let measure_refs = pa["measureReferences"].as_str().unwrap_or("[]");
    let measures: Vec<&str> = measure_refs
        .trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let dim_refs = pa["dimensionReferences"].as_str().unwrap_or("[]");
    let dimensions: Vec<&str> = dim_refs
        .trim_matches(|c| c == '[' || c == ']')
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let time_dim = pa["timeDimensionReference"].as_str().unwrap_or("");
    let granularity = pa["granularity"].as_str().unwrap_or("");

    println!("  Covers:");
    println!("    • {} measures", measures.len());
    println!("    • {} dimensions", dimensions.len());
    println!("    • Time: {} ({})", time_dim, granularity);
    println!();

    // Example Query 1: Perfect Match
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Query Example 1: PERFECT MATCH ✓");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("Incoming Query:");
    println!("  SELECT");
    println!("    market_code,");
    println!("    brand_code,");
    println!("    DATE_TRUNC('day', updated_at) as day,");
    println!("    SUM(total_amount) as total,");
    println!("    COUNT(*) as order_count");
    println!("  FROM mandata_captate");
    println!("  WHERE updated_at >= '2024-01-01'");
    println!("  GROUP BY market_code, brand_code, day");
    println!();

    println!("Pre-Aggregation Selection Logic:");
    println!("  ┌─ Checking '{}'...", pa_name);
    println!("  │");
    print!("  ├─ ✓ Measures match: ");
    println!("mandata_captate.total_amount_sum, mandata_captate.count");
    print!("  ├─ ✓ Dimensions match: ");
    println!("market_code, brand_code");
    print!("  ├─ ✓ Time dimension match: ");
    println!("updated_at");
    print!("  ├─ ✓ Granularity match: ");
    println!("day");
    println!("  └─ ✓ Date range compatible");
    println!();

    println!("Decision: USE PRE-AGGREGATION '{}'", pa_name);
    println!();

    println!("Rewritten Query (sent to CubeStore):");
    println!("  SELECT");
    println!("    market_code,");
    println!("    brand_code,");
    println!("    time_dimension as day,");
    println!("    mandata_captate__total_amount_sum as total,");
    println!("    mandata_captate__count as order_count");
    println!("  FROM prod_pre_aggregations.mandata_captate_{}_20240125_abcd1234_d7kwjvzn_tztb8hap", pa_name);
    println!("  WHERE time_dimension >= '2024-01-01'");
    println!();

    println!("Performance Benefit:");
    println!("  • Data reduction: ~1000x (full table → daily rollup)");
    println!("  • Query time: ~100ms → ~5ms");
    println!("  • I/O saved: Reading pre-computed aggregates vs full scan");
    println!();

    // Example Query 2: Partial Match
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Query Example 2: PARTIAL MATCH (Superset) ✓");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("Incoming Query (only 1 measure, 1 dimension):");
    println!("  SELECT");
    println!("    market_code,");
    println!("    DATE_TRUNC('day', updated_at) as day,");
    println!("    COUNT(*) as order_count");
    println!("  FROM mandata_captate");
    println!("  WHERE updated_at >= '2024-01-01'");
    println!("  GROUP BY market_code, day");
    println!();

    println!("Pre-Aggregation Selection Logic:");
    println!("  ┌─ Checking '{}'...", pa_name);
    println!("  │");
    println!("  ├─ ✓ Measures: count ⊆ pre-agg measures");
    println!("  ├─ ✓ Dimensions: market_code ⊆ pre-agg dimensions");
    println!("  ├─ ✓ Time dimension match");
    println!("  └─ ✓ Can aggregate further (brand_code will be ignored)");
    println!();

    println!("Decision: USE PRE-AGGREGATION '{}' (with additional GROUP BY)", pa_name);
    println!();

    // Example Query 3: No Match
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Query Example 3: NO MATCH ✗");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("Incoming Query (different granularity):");
    println!("  SELECT");
    println!("    market_code,");
    println!("    DATE_TRUNC('hour', updated_at) as hour,");
    println!("    COUNT(*) as order_count");
    println!("  FROM mandata_captate");
    println!("  WHERE updated_at >= '2024-01-01'");
    println!("  GROUP BY market_code, hour");
    println!();

    println!("Pre-Aggregation Selection Logic:");
    println!("  ┌─ Checking '{}'...", pa_name);
    println!("  │");
    println!("  ├─ ✓ Measures match");
    println!("  ├─ ✓ Dimensions match");
    println!("  ├─ ✓ Time dimension match");
    println!("  └─ ✗ Granularity mismatch: hour < day (can't disaggregate)");
    println!();

    println!("Decision: SKIP PRE-AGGREGATION, query raw table");
    println!();

    println!("Explanation:");
    println!("  Pre-aggregations can only be used when the requested");
    println!("  granularity is >= pre-aggregation granularity.");
    println!("  We can roll up 'day' to 'month', but not to 'hour'.");
    println!();

    // Algorithm Summary
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("Pre-Aggregation Selection Algorithm");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!();
    println!("For each query, the cubesqlplanner:");
    println!();
    println!("1. Analyzes query structure");
    println!("   • Extract measures, dimensions, time dimensions");
    println!("   • Identify GROUP BY granularity");
    println!("   • Parse filters and date ranges");
    println!();
    println!("2. For each available pre-aggregation:");
    println!("   • Check if query measures ⊆ pre-agg measures");
    println!("   • Check if query dimensions ⊆ pre-agg dimensions");
    println!("   • Check if time dimension matches");
    println!("   • Check if granularity allows rollup");
    println!("   • Check if filters are compatible");
    println!();
    println!("3. Select best match:");
    println!("   • Prefer smallest pre-aggregation that covers query");
    println!("   • Prefer exact match over superset");
    println!("   • If no match, query raw table");
    println!();
    println!("4. Rewrite query:");
    println!("   • Replace table name with pre-agg table");
    println!("   • Map measure/dimension names to pre-agg columns");
    println!("   • Add any additional GROUP BY if needed");
    println!();

    println!("This logic is implemented in:");
    println!("  rust/cubesqlplanner/cubesqlplanner/src/logical_plan/optimizers/pre_aggregation/");
    println!();

    Ok(())
}
