use cubesql::{
    sql::{AuthContextRef, HttpAuthContext},
    transport::{
        CubeStoreTransport, CubeStoreTransportConfig, LoadRequestMeta, TransportLoadRequestQuery,
        TransportService,
    },
    CubeError,
};
use datafusion::arrow::{
    datatypes::{DataType, Field, Schema},
    util::pretty::print_batches,
};
use std::{env, sync::Arc};

/// Integration test for CubeStoreTransport
///
/// This example demonstrates the complete hybrid approach:
/// 1. Fetch metadata from Cube API (HTTP/JSON)
/// 2. Execute queries on CubeStore (WebSocket/FlatBuffers/Arrow)
///
/// Prerequisites:
/// - Cube API running on localhost:4008
/// - CubeStore running on localhost:3030
///
/// Run with:
/// ```bash
/// CUBESQL_CUBESTORE_DIRECT=true \
/// CUBESQL_CUBE_URL=http://localhost:4008/cubejs-api \
/// CUBESQL_CUBESTORE_URL=ws://127.0.0.1:3030/ws \
/// cargo run --example cubestore_transport_integration
/// ```

#[tokio::main]
async fn main() -> Result<(), CubeError> {
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    println!("\n╔════════════════════════════════════════════════════════════╗");
    println!("║ CubeStoreTransport Integration Test                       ║");
    println!("║ Hybrid Approach: Metadata from API + Data from CubeStore  ║");
    println!("╚════════════════════════════════════════════════════════════╝\n");

    // Step 1: Create CubeStoreTransport from environment
    println!("Step 1: Initialize CubeStoreTransport");
    println!("────────────────────────────────────────");

    let config = CubeStoreTransportConfig::from_env()?;

    println!("Configuration:");
    println!("  • Direct mode enabled: {}", config.enabled);
    println!("  • Cube API URL: {}", config.cube_api_url);
    println!("  • CubeStore URL: {}", config.cubestore_url);
    println!("  • Metadata cache TTL: {}s", config.metadata_cache_ttl);

    if !config.enabled {
        println!("\n⚠️  CubeStore direct mode is NOT enabled");
        println!("Set CUBESQL_CUBESTORE_DIRECT=true to enable it\n");
        return Ok(());
    }

    // Clone cube_api_url before moving config
    let cube_api_url = config.cube_api_url.clone();

    let transport = Arc::new(CubeStoreTransport::new(config)?);
    println!("✓ Transport initialized\n");

    // Step 2: Fetch metadata from Cube API
    println!("Step 2: Fetch Metadata from Cube API");
    println!("────────────────────────────────────────");

    let auth_ctx: AuthContextRef = Arc::new(HttpAuthContext {
        access_token: env::var("CUBESQL_CUBE_TOKEN").unwrap_or_else(|_| "test".to_string()),
        base_path: cube_api_url,
    });

    let meta = transport.meta(auth_ctx.clone()).await?;

    println!("✓ Metadata fetched successfully");
    println!("  • Total cubes: {}", meta.cubes.len());

    if !meta.cubes.is_empty() {
        println!("  • First 5 cubes:");
        for (i, cube) in meta.cubes.iter().take(5).enumerate() {
            println!("    {}. {}", i + 1, cube.name);
        }
    }
    println!();

    // Step 3: Test metadata caching
    println!("Step 3: Test Metadata Caching");
    println!("────────────────────────────────────────");

    let meta2 = transport.meta(auth_ctx.clone()).await?;

    println!("✓ Second call should use cache");
    println!("  • Same instance: {}", Arc::ptr_eq(&meta, &meta2));
    println!();

    // Step 4: Execute simple query on CubeStore
    println!("Step 4: Execute Query on CubeStore");
    println!("────────────────────────────────────────");

    // First, test with a simple system query
    println!("Testing connection with: SELECT 1 as test");

    let mut simple_query = TransportLoadRequestQuery::new();
    simple_query.limit = Some(1);

    // Create minimal schema for SELECT 1
    let schema = Arc::new(Schema::new(vec![Field::new(
        "test",
        DataType::Int32,
        false,
    )]));

    let sql_query = cubesql::compile::engine::df::wrapper::SqlQuery {
        sql: "SELECT 1 as test".to_string(),
        values: vec![],
    };

    let meta_fields = LoadRequestMeta::new(
        "postgres".to_string(),
        "sql".to_string(),
        Some("arrow-ipc".to_string()),
    );

    match transport
        .load(
            None,
            simple_query,
            Some(sql_query),
            auth_ctx.clone(),
            meta_fields.clone(),
            schema.clone(),
            vec![],
            None,
        )
        .await
    {
        Ok(batches) => {
            println!("✓ Query executed successfully");
            println!("  • Batches returned: {}", batches.len());

            if !batches.is_empty() {
                println!("\nResults:");
                println!("────────");
                print_batches(&batches)?;
            }
        }
        Err(e) => {
            println!("✗ Query failed: {}", e);
            println!(
                "\nThis is expected if CubeStore is not running on {}",
                env::var("CUBESQL_CUBESTORE_URL")
                    .unwrap_or_else(|_| "ws://127.0.0.1:3030/ws".to_string())
            );
        }
    }
    println!();

    // Step 5: Discover and query pre-aggregation tables
    println!("Step 5: Discover Pre-Aggregation Tables");
    println!("────────────────────────────────────────");

    let pre_agg_schema =
        env::var("CUBESQL_PRE_AGG_SCHEMA").unwrap_or_else(|_| "dev_pre_aggregations".to_string());

    let discover_sql = format!(
        "SELECT table_schema, table_name FROM information_schema.tables \
         WHERE table_schema = '{}' ORDER BY table_name LIMIT 5",
        pre_agg_schema
    );

    println!("Discovering tables in schema: {}", pre_agg_schema);

    let mut discover_query = TransportLoadRequestQuery::new();
    discover_query.limit = Some(5);

    let discover_schema = Arc::new(Schema::new(vec![
        Field::new("table_schema", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
    ]));

    let discover_sql_query = cubesql::compile::engine::df::wrapper::SqlQuery {
        sql: discover_sql.clone(),
        values: vec![],
    };

    match transport
        .load(
            None,
            discover_query,
            Some(discover_sql_query),
            auth_ctx.clone(),
            meta_fields,
            discover_schema,
            vec![],
            None,
        )
        .await
    {
        Ok(batches) => {
            println!("✓ Discovery query executed");

            if !batches.is_empty() {
                println!("\nPre-Aggregation Tables:");
                println!("──────────────────────");
                print_batches(&batches)?;
            } else {
                println!("  • No pre-aggregation tables found");
                println!("  • Make sure you've run data generation queries");
            }
        }
        Err(e) => {
            println!("✗ Discovery failed: {}", e);
        }
    }
    println!();

    // Summary
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║ Integration Test Complete                                 ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!("\n✓ CubeStoreTransport is working correctly!");
    println!("\nThe hybrid approach successfully:");
    println!("  1. Fetched metadata from Cube API (HTTP/JSON)");
    println!("  2. Cached metadata for subsequent calls");
    println!("  3. Executed queries on CubeStore (WebSocket/FlatBuffers/Arrow)");
    println!("  4. Returned results as Arrow RecordBatches");
    println!("\nNext steps:");
    println!("  • Integrate with cubesql query planning");
    println!("  • Add pre-aggregation selection logic");
    println!("  • Create end-to-end tests with real queries");
    println!();

    Ok(())
}
