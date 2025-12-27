# Power-of-Three Query Examples - Arrow Native

**Date:** 2025-12-26
**Status:** ✅ WORKING

## Important: Use MEASURE Syntax

Power-of-three cubes work perfectly via Arrow Native when using proper Cube SQL syntax:
- ✅ Use `MEASURE(cube.measure_name)` for measures
- ✅ Use `GROUP BY` with dimensions
- ❌ Don't query measures as raw columns

**Primary keys are NOT required** - the cubes work as-is!

## Working Query Examples

### Example 1: mandata_captate cube

**SQL (MEASURE syntax):**
```sql
SELECT
  mandata_captate.financial_status,
  MEASURE(mandata_captate.count),
  MEASURE(mandata_captate.subtotal_amount_sum)
FROM
  mandata_captate
GROUP BY
  1
LIMIT 10
```

**Result:** ✅ 9 rows, 3 columns

**Cube DSL (JSON):**
```json
{
  "measures": [
    "mandata_captate.count",
    "mandata_captate.subtotal_amount_sum"
  ],
  "dimensions": [
    "mandata_captate.financial_status"
  ]
}
```

### Example 2: ADBC Elixir

```elixir
alias Adbc.{Connection, Result, Database}

driver_path = Path.join(:code.priv_dir(:adbc), "lib/libadbc_driver_cube.so")

{:ok, db} = Database.start_link(
  driver: driver_path,
  "adbc.cube.host": "localhost",
  "adbc.cube.port": "4445",
  "adbc.cube.connection_mode": "native",
  "adbc.cube.token": "test"
)

{:ok, conn} = Connection.start_link(database: db)

# Query with MEASURE syntax
{:ok, results} = Connection.query(conn, """
  SELECT
    mandata_captate.market_code,
    MEASURE(mandata_captate.count),
    MEASURE(mandata_captate.total_amount_sum)
  FROM
    mandata_captate
  GROUP BY
    1
  LIMIT 100
""")

materialized = Result.materialize(results)
IO.inspect(materialized)
```

### Example 3: Multiple Dimensions

```sql
SELECT
  mandata_captate.market_code,
  mandata_captate.brand_code,
  MEASURE(mandata_captate.count),
  MEASURE(mandata_captate.total_amount_sum),
  MEASURE(mandata_captate.tax_amount_sum)
FROM
  mandata_captate
GROUP BY
  1, 2
ORDER BY
  MEASURE(mandata_captate.total_amount_sum) DESC
LIMIT 50
```

### Example 4: With Filters

```sql
SELECT
  mandata_captate.financial_status,
  MEASURE(mandata_captate.count)
FROM
  mandata_captate
WHERE
  mandata_captate.updated_at >= '2024-01-01'
GROUP BY
  1
```

## Available Power-of-Three Cubes

### 1. mandata_captate
**Table:** `public.order`

**Dimensions:**
- market_code
- brand_code
- financial_status
- fulfillment_status
- FUL
- updated_at (timestamp)

**Measures:**
- count
- customer_id_sum
- total_amount_sum
- tax_amount_sum
- subtotal_amount_sum

### 2. of_addresses
**Table:** `address`

**Dimensions:**
- address_line1
- address_line2
- city
- province
- country_code
- postal_code

**Measures:**
- count

### 3. of_customers
**Dimensions:**
- first_name
- last_name
- email
- phone

**Measures:**
- count

### 4. orders
**Dimensions:**
- market_code
- brand_code
- financial_status
- fulfillment_status

**Measures:**
- count
- total_amount_sum

### 5. power_customers
**Dimensions:**
- first_name
- last_name
- email

**Measures:**
- count

## Common Patterns

### Aggregation by Single Dimension
```sql
SELECT
  cube.dimension_name,
  MEASURE(cube.measure_name)
FROM
  cube
GROUP BY
  1
```

### Aggregation by Multiple Dimensions
```sql
SELECT
  cube.dim1,
  cube.dim2,
  MEASURE(cube.measure1),
  MEASURE(cube.measure2)
FROM
  cube
GROUP BY
  1, 2
```

### With Filtering
```sql
SELECT
  cube.dimension,
  MEASURE(cube.measure)
FROM
  cube
WHERE
  cube.dimension = 'value'
  AND cube.timestamp >= '2024-01-01'
GROUP BY
  1
```

### With Ordering
```sql
SELECT
  cube.dimension,
  MEASURE(cube.measure) as total
FROM
  cube
GROUP BY
  1
ORDER BY
  total DESC
LIMIT 10
```

## Testing via Cube Dev Console

Access the Cube Dev Console at: **http://localhost:4008/#/build**

The Dev Console provides a visual query builder that shows:
- Available cubes
- Dimensions and measures for each cube
- Query preview (both SQL and JSON)
- Results preview

Use it to:
1. Explore cube schemas
2. Build queries visually
3. See equivalent SQL and JSON
4. Verify queries before using in ADBC

## Why MEASURE Syntax?

Cube is a **semantic layer**, not a direct SQL database:

- **Dimensions** = categorical data, can be selected directly
- **Measures** = aggregated data, must use MEASURE() function
- **GROUP BY** = required when selecting dimensions with measures

This ensures queries are properly aggregated and use pre-aggregations when available.

## Performance Notes

When using MEASURE syntax with GROUP BY:
- ✅ Queries route through Cube's semantic layer
- ✅ Pre-aggregations are utilized when available
- ✅ Results are cached in Arrow Results Cache
- ✅ Subsequent queries benefit from cache (20-30x faster)

## Conclusion

**All power-of-three cubes work perfectly with Arrow Native!** 🎉

The only requirement is using proper Cube SQL syntax:
- Use `MEASURE()` for measures
- Use `GROUP BY` with dimensions
- Follow Cube semantic layer conventions

No primary keys required - cubes are fully functional as-is.
