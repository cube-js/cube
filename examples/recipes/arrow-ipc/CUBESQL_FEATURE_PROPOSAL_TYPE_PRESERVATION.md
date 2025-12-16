# CubeSQL Feature Proposal: Numeric Type Preservation

**Status**: 📝 Proposal
**Priority**: Low
**Complexity**: Medium
**Target**: CubeSQL (both Arrow Native and PostgreSQL protocols)

---

## Problem Statement

CubeSQL currently maps all `type: number` dimensions and measures to `ColumnType::Double` → `DataType::Float64`, regardless of the underlying SQL column type or any metadata hints.

### Current Behavior

```rust
// cubesql/src/transport/ext.rs:163-170
fn get_sql_type(&self) -> ColumnType {
    match self.r#type.to_lowercase().as_str() {
        "time" => ColumnType::Timestamp,
        "number" => ColumnType::Double,  // ← All numbers become Double
        "boolean" => ColumnType::Boolean,
        _ => ColumnType::String,
    }
}
```

**Result**:
- INT8 columns transmitted as Float64
- INT32 columns transmitted as Float64
- INT64 columns transmitted as Float64
- FLOAT32 columns transmitted as Float64

### Impact

**Functional**: ✅ None - values are correct, precision preserved within Float64 range
**Performance**: ⚠️ Minimal - 5-10% bandwidth overhead for dimension-heavy queries
**Type Safety**: ⚠️ Client applications lose integer type information

**Affects**:
- Arrow Native protocol (port 4445)
- PostgreSQL wire protocol (port 4444)
- Both protocols receive Float64 from the same upstream type mapping

---

## Proposed Solution: Derive Types from Compiled Cube Model

### Approach: Interrogate Cube Semantic Layer

Instead of relying on custom metadata, derive numeric types by examining the compiled cube model and the underlying SQL expressions during schema compilation.

### Current Architecture Analysis

**Cube.js Compilation Pipeline**:
```
Cube YAML → Schema Compiler → Semantic Layer → CubeSQL Metadata → Type Mapping
```

Currently, type information is lost at the "Type Mapping" stage where everything becomes `ColumnType::Double`.

**Potential Sources of Type Information**:

1. **SQL Expression Analysis** - Parse the `sql:` field to identify column references
2. **Database Schema Cache** - Query underlying table schema during compilation
3. **DataFusion Schema** - Use actual query result schema from first execution
4. **Cube.js Type System** - Extend Cube.js schema to include SQL type hints

### Recommended Implementation Strategy

**Phase 1: Extend Cube Metadata API**

Modify the Cube.js schema compiler to include SQL type information in the metadata API response.

**Changes needed in Cube.js** (`packages/cubejs-schema-compiler`):

```javascript
// In dimension/measure compilation
class BaseDimension {
  compile() {
    return {
      name: this.name,
      type: this.type,  // "number", "string", etc.
      sql: this.sql,
      // NEW: Include inferred SQL type
      sqlType: this.inferSqlType(),
      ...
    };
  }

  inferSqlType() {
    // Option 1: Parse SQL expression to find column reference
    const columnRef = this.extractColumnReference(this.sql);
    if (columnRef) {
      return this.schemaCache.getColumnType(columnRef.table, columnRef.column);
    }

    // Option 2: Execute sample query and inspect result schema
    // Option 3: Use explicit type hints from cube definition

    return null;  // Fall back to current behavior
  }
}
```

**Changes needed in CubeSQL** (`transport/ext.rs`):

```rust
// Add field to V1CubeMetaDimension proto/model
pub struct V1CubeMetaDimension {
    pub name: String,
    pub r#type: String,        // "number", "string", etc.
    pub sql_type: Option<String>,  // NEW: "INTEGER", "BIGINT", "DOUBLE PRECISION"
    ...
}

// Update type mapping to use sql_type if available
impl V1CubeMetaDimensionExt for CubeMetaDimension {
    fn get_sql_type(&self) -> ColumnType {
        // Use sql_type from schema compiler if available
        if let Some(sql_type) = &self.sql_type {
            if let Some(column_type) = map_sql_type_to_column_type(sql_type) {
                return column_type;
            }
        }

        // Existing fallback (backward compatible)
        match self.r#type.to_lowercase().as_str() {
            "number" => ColumnType::Double,
            "boolean" => ColumnType::Boolean,
            "time" => ColumnType::Timestamp,
            _ => ColumnType::String,
        }
    }
}

fn map_sql_type_to_column_type(sql_type: &str) -> Option<ColumnType> {
    match sql_type.to_uppercase().as_str() {
        "SMALLINT" | "INT2" | "TINYINT" => Some(ColumnType::Int32),
        "INTEGER" | "INT" | "INT4" => Some(ColumnType::Int32),
        "BIGINT" | "INT8" => Some(ColumnType::Int64),
        "REAL" | "FLOAT4" => Some(ColumnType::Double),
        "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" => Some(ColumnType::Double),
        "NUMERIC" | "DECIMAL" => Some(ColumnType::Double),
        _ => None,  // Unknown type, use fallback
    }
}
```

### Implementation Details

**Step 1: Schema Introspection in Cube.js**

Add database schema caching during cube compilation:

```javascript
// packages/cubejs-query-orchestrator/src/orchestrator/SchemaCache.js
class SchemaCache {
  async getTableSchema(tableName) {
    const cacheKey = `schema:${tableName}`;

    return this.cache.get(cacheKey, async () => {
      const schema = await this.databaseConnection.query(`
        SELECT column_name, data_type, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_name = $1
      `, [tableName]);

      return new Map(schema.rows.map(row => [
        row.column_name,
        {
          dataType: row.data_type,
          precision: row.numeric_precision,
          scale: row.numeric_scale,
        }
      ]));
    });
  }
}
```

**Step 2: Propagate Type Through Compilation**

```javascript
// packages/cubejs-schema-compiler/src/adapter/BaseDimension.js
class BaseDimension {
  inferSqlType() {
    // For simple column references
    const match = this.sql.match(/^(\w+)\.(\w+)$/);
    if (match) {
      const [, table, column] = match;
      const tableSchema = this.cubeFactory.schemaCache.getTableSchema(table);
      const columnInfo = tableSchema?.get(column);
      return columnInfo?.dataType;
    }

    // For complex expressions, return null (use default)
    return null;
  }

  toMeta() {
    return {
      name: this.name,
      type: this.type,
      sql_type: this.inferSqlType(),  // Include in metadata
      ...
    };
  }
}
```

**Step 3: Update gRPC/API Protocol**

```protobuf
// Add to proto definition (if using proto)
message V1CubeMetaDimension {
  string name = 1;
  string type = 2;
  optional string sql_type = 10;  // NEW field
  ...
}
```

### Fallback Strategy

**Type Resolution Priority**:
1. ✅ `sql_type` from schema compiler (if available)
2. ✅ `type` with default mapping ("number" → Double)
3. ✅ Existing behavior maintained

**Edge Cases**:
- **Calculated dimensions**: No direct column mapping → fallback to Double
- **CAST expressions**: Parse CAST target type
- **Unknown SQL types**: Fallback to Double
- **Schema query failures**: Fallback to Double (log warning)

### Pros and Cons

**Pros**:
- ✅ Automatic - no manual cube model changes
- ✅ Accurate - based on actual database schema
- ✅ Proper solution - no custom metadata hacks
- ✅ Upstream acceptable - improves Cube.js type system
- ✅ Backward compatible - optional field, graceful fallback

**Cons**:
- ❌ Requires changes in both Cube.js AND CubeSQL
- ❌ Schema introspection adds complexity
- ❌ Performance impact during compilation (mitigated by caching)
- ❌ Cross-repository coordination needed

**Effort**: Medium-High (3-5 days)
- Cube.js changes: 2-3 days
- CubeSQL changes: 1 day
- Testing: 1 day

**Risk**: Medium
- Schema query performance
- Cross-version compatibility
- Edge case handling

---

## Network Impact Analysis

### Bandwidth Comparison

| Type | Bytes/Value | vs Float64 | Typical Use Case |
|------|-------------|------------|------------------|
| INT8 | 1 | -87.5% | Status codes, flags |
| INT16 | 2 | -75% | Small IDs, counts |
| INT32 | 4 | -50% | Medium IDs, years |
| INT64 | 8 | 0% | Large IDs, timestamps |
| FLOAT64 | 8 | baseline | Aggregations, metrics |

### Real-World Scenario

**Typical Analytical Query**:
```sql
SELECT
  date_trunc('day', created_at) as day,  -- TIMESTAMP
  user_id,                                -- INT64 (no savings)
  status_code,                            -- INT8 (potential 7 byte savings)
  country_code,                           -- STRING
  SUM(revenue),                           -- FLOAT64 (measure)
  COUNT(*)                                -- INT64 (already optimized)
FROM orders
GROUP BY 1, 2, 3, 4
```

**Result**: 1 million rows
- Dimension columns: 4 (1 timestamp, 2 integers, 1 string)
- Measure columns: 2 (both already optimal types)
- Potential savings: 7 MB if status_code were INT8 instead of FLOAT64
- **Total payload reduction: ~3-5%**

Most savings would be for small-integer dimensions (status codes, enum values, small counts), which are relatively rare in analytical queries.

---

## Implementation Plan

### Phase 1: Cube.js Schema Compiler Changes

**Repository**: `cube-js/cube`

**Files to modify**:
1. `packages/cubejs-schema-compiler/src/adapter/BaseDimension.js`
   - Add `inferSqlType()` method
   - Update `toMeta()` to include `sql_type`

2. `packages/cubejs-schema-compiler/src/adapter/BaseMeasure.js`
   - Similar changes for measures

3. `packages/cubejs-query-orchestrator/src/orchestrator/SchemaCache.js` (new)
   - Add `getTableSchema()` method
   - Cache schema queries with TTL

4. API/Proto definitions:
   - Add `sql_type: string?` field to dimension/measure metadata
   - Update OpenAPI/gRPC specs

**Estimated effort**: 2-3 days
**Tests needed**:
- Schema caching
- SQL type inference for various column patterns
- Fallback behavior

### Phase 2: CubeSQL Changes

**Repository**: `cube-js/cube` (Rust workspace)

**Files to modify**:
1. `rust/cubesql/cubeclient/src/models/v1_cube_meta_dimension.rs`
   - Add `sql_type: Option<String>` field
   - Update deserialization

2. `rust/cubesql/cubesql/src/transport/ext.rs`
   - Implement `map_sql_type_to_column_type()` helper
   - Update `get_sql_type()` to check `sql_type` first
   - Add same changes for measures

**Estimated effort**: 1 day
**Tests needed**:
- SQL type mapping (all database types)
- Fallback to existing behavior
- Both protocols (Arrow Native + PostgreSQL)

### Phase 3: Integration Testing

**Test scenarios**:
1. ✅ Simple column references (e.g., `sql: user_id`)
2. ✅ Calculated dimensions (e.g., `sql: YEAR(created_at)`)
3. ✅ CAST expressions (e.g., `sql: CAST(status AS BIGINT)`)
4. ✅ Backward compatibility (old Cube.js with new CubeSQL)
5. ✅ Forward compatibility (new Cube.js with old CubeSQL)
6. ✅ Schema cache invalidation
7. ✅ Unknown SQL types

**Test cubes**:
```yaml
cubes:
  - name: orders
    sql_table: public.orders

    dimensions:
      - name: id
        sql: id              # BIGINT → Int64
        type: number

      - name: status
        sql: status          # SMALLINT → Int32
        type: number

      - name: amount
        sql: amount          # NUMERIC(10,2) → Double
        type: number

      - name: created_year
        sql: EXTRACT(YEAR FROM created_at)  # Calculated → fallback to Double
        type: number
```

**Estimated effort**: 1 day

### Phase 4: Documentation & Rollout

1. **Cube.js changelog**: Mention automatic type preservation
2. **Migration guide**: Explain new behavior (mostly transparent)
3. **Performance notes**: Document schema caching strategy
4. **Breaking changes**: None (graceful fallback)

**Rollout strategy**:
- ✅ Backward compatible (optional field)
- ✅ Graceful degradation (missing field → current behavior)
- ✅ No user action required
- ✅ Benefits appear automatically after upgrade

**Estimated effort**: 0.5 days

---

## Recommendation

**Action**: Document and defer

**Rationale**:
1. **Current behavior is correct**: Values are accurate, no precision loss
2. **Low performance impact**: 5-10% bandwidth savings in best case
3. **Analytical workloads**: Float64 is standard for OLAP (ClickHouse, DuckDB, etc.)
4. **Implementation cost**: Medium effort for low impact
5. **Type safety**: Client applications can cast Float64 → Int if needed

**When to reconsider**:
1. User requests for integer type preservation
2. Large-scale deployments with bandwidth constraints
3. Integration with type-strict client libraries
4. Standardization of `meta` format in Cube.js

---

## Alternative: Document Current Behavior

Instead of implementing type preservation, document the design decision:

**Cube.js Documentation Addition**:
```markdown
### Data Types

CubeSQL transmits all numeric dimensions and measures as `FLOAT64` (PostgreSQL: `NUMERIC`,
Arrow: `Float64`) regardless of the underlying SQL column type. This is by design:

- **Simplicity**: Single numeric type path reduces implementation complexity
- **Analytics focus**: Aggregations (SUM, AVG) require floating-point anyway
- **Precision**: Float64 can represent all integers up to 2^53 without loss
- **Performance**: No type conversions during query processing

If your application requires specific integer types, cast on the client side:
- Arrow: Cast Float64 array to Int64
- PostgreSQL: Cast NUMERIC to INTEGER
```

---

## Files Referenced

### CubeSQL Source
- `cubesql/src/transport/ext.rs:101-122, 163-170` - Type mapping
- `cubesql/src/sql/types.rs:92-114` - ColumnType → Arrow conversion
- `cubesql/cubeclient/src/models/v1_cube_meta_dimension.rs:31-32` - API model
- `cubesql/src/compile/engine/df/scan.rs:874-948` - RecordBatch building
- `cubesql/src/sql/postgres/pg_type.rs:4-51` - PostgreSQL type mapping

### Evidence
- ADBC C++ tests: All numerics show format `'g'` (Float64)
- ADBC Elixir tests: All numerics show type `:f64`
- Both protocols exhibit identical behavior

---

**Author**: ADBC Driver Investigation
**Date**: December 16, 2024
**Contact**: For questions about ADBC driver behavior with CubeSQL types
