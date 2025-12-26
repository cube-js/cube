COMPLETED ✓: Changed usage of CUBESQL_QUERY_CACHE_MAX_ENTRIES and CUBESQL_QUERY_CACHE_TTL to be prefixed with CUBESQL_ARROW_RESULTS_ for consistency:
   - CUBESQL_QUERY_CACHE_MAX_ENTRIES → CUBESQL_ARROW_RESULTS_CACHE_MAX_ENTRIES
   - CUBESQL_QUERY_CACHE_TTL → CUBESQL_ARROW_RESULTS_CACHE_TTL

Files updated:
   - rust/cubesql/cubesql/src/sql/arrow_native/cache.rs (Rust implementation)
   - examples/recipes/arrow-ipc/start-cubesqld.sh (shell script)
   - README.md, CACHE_IMPLEMENTATION.md (documentation)
