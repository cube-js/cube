// Tests for PostgreSQL type mapping in information_schema
// These tests verify that Cube correctly maps internal column types to PostgreSQL types

#[cfg(test)]
mod tests {
    use crate::sql::ColumnType;
    use crate::transport::ext::CubeColumn;
    use crate::compile::engine::information_schema::postgres::ext::CubeColumnPostgresExt;
    use datafusion::arrow::datatypes::{DataType, Field};

    #[test]
    fn test_string_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::String, true);

        assert_eq!(col.get_data_type(), "text");
        assert_eq!(col.get_udt_name(), "text");
        assert_eq!(col.is_nullable(), "YES");
        assert_eq!(col.get_numeric_precision(), None);
        assert_eq!(col.numeric_scale(), None);
    }

    #[test]
    fn test_varchar_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::VarStr, false);

        assert_eq!(col.get_data_type(), "character varying");
        assert_eq!(col.get_udt_name(), "varchar");
        assert_eq!(col.is_nullable(), "NO");
    }

    #[test]
    fn test_int8_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Int8, true);

        assert_eq!(col.get_data_type(), "smallint");
        assert_eq!(col.get_udt_name(), "int2");
        // PostgreSQL int types don't have numeric_precision
        assert_eq!(col.get_numeric_precision(), None);
        assert_eq!(col.numeric_precision_radix(), None);
        assert_eq!(col.numeric_scale(), None);
    }

    #[test]
    fn test_int32_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Int32, true);

        assert_eq!(col.get_data_type(), "integer");
        assert_eq!(col.get_udt_name(), "int4");
        assert_eq!(col.get_numeric_precision(), None);
    }

    #[test]
    fn test_int64_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Int64, true);

        assert_eq!(col.get_data_type(), "bigint");
        assert_eq!(col.get_udt_name(), "int8");
        // In PostgreSQL, bigint doesn't have numeric_precision
        // (numeric_precision is for NUMERIC/DECIMAL types only)
        assert_eq!(col.get_numeric_precision(), None);
        assert_eq!(col.numeric_scale(), None);
    }

    #[test]
    fn test_double_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Double, true);

        assert_eq!(col.get_data_type(), "double precision");
        assert_eq!(col.get_udt_name(), "float8");
        // Float8 has binary precision of 53 bits
        assert_eq!(col.get_numeric_precision(), Some(53));
        // Radix 2 for binary representation
        assert_eq!(col.numeric_precision_radix(), Some(2));
    }

    #[test]
    fn test_decimal_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Decimal(38, 10), true);

        assert_eq!(col.get_data_type(), "numeric");
        assert_eq!(col.get_udt_name(), "numeric");
        // Decimal should report its precision and scale
        assert_eq!(col.get_numeric_precision(), Some(38));
        assert_eq!(col.numeric_scale(), Some(10));
        // Decimal uses base 10
        assert_eq!(col.numeric_precision_radix(), Some(10));
    }

    #[test]
    fn test_boolean_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Boolean, true);

        assert_eq!(col.get_data_type(), "boolean");
        assert_eq!(col.get_udt_name(), "bool");
        assert_eq!(col.get_numeric_precision(), None);
    }

    #[test]
    fn test_timestamp_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Timestamp, true);

        assert_eq!(col.get_data_type(), "timestamp without time zone");
        assert_eq!(col.get_udt_name(), "timestamp");
        // Timestamp has precision of 6 (microseconds)
        assert_eq!(col.datetime_precision(), Some(6));
    }

    #[test]
    fn test_date_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Date(false), true);

        assert_eq!(col.get_data_type(), "date");
        assert_eq!(col.get_udt_name(), "date");
    }

    #[test]
    fn test_blob_type_mapping() {
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Blob, true);

        assert_eq!(col.get_data_type(), "bytea");
        assert_eq!(col.get_udt_name(), "bytea");
    }

    #[test]
    fn test_interval_type_mapping() {
        let col = CubeColumn::new(
            "cube.column",
            "column",
            None,
            ColumnType::Interval(
                datafusion::arrow::datatypes::IntervalUnit::MonthDayNano
            ),
            true,
        );

        assert_eq!(col.get_data_type(), "interval");
        assert_eq!(col.get_udt_name(), "interval");
    }

    #[test]
    fn test_list_int64_type_mapping() {
        let col = CubeColumn::new(
            "cube.column",
            "column",
            None,
            ColumnType::List(Box::new(
                Field::new("item", DataType::Int64, true)
            )),
            true,
        );

        assert_eq!(col.get_data_type(), "bigint[]");
        assert_eq!(col.get_udt_name(), "_int8");
    }

    #[test]
    fn test_list_string_type_mapping() {
        let col = CubeColumn::new(
            "cube.column",
            "column",
            None,
            ColumnType::List(Box::new(
                Field::new("item", DataType::Utf8, true)
            )),
            true,
        );

        assert_eq!(col.get_data_type(), "text[]");
        assert_eq!(col.get_udt_name(), "_text");
    }

    #[test]
    fn test_udt_schema_is_pg_catalog() {
        // Built-in types should report pg_catalog schema
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Int64, true);

        assert_eq!(col.udt_schema(), "pg_catalog");
    }

    #[test]
    fn test_decimal_precision_extraction() {
        // Test various decimal precisions are correctly extracted
        let test_cases = vec![
            (10, 2),   // NUMERIC(10,2)
            (38, 10),  // NUMERIC(38,10)
            (5, 0),    // NUMERIC(5,0)
        ];

        for (precision, scale) in test_cases {
            let col = CubeColumn::new(
                "cube.column",
                "column",
                None,
                ColumnType::Decimal(precision, scale),
                true,
            );

            assert_eq!(col.get_numeric_precision(), Some(precision as u32));
            assert_eq!(col.numeric_scale(), Some(scale as u32));
            assert_eq!(col.numeric_precision_radix(), Some(10));
        }
    }

    #[test]
    fn test_nullable_vs_not_nullable() {
        let nullable_col = CubeColumn::new("cube.column", "column", None, ColumnType::String, true);
        let not_nullable_col = CubeColumn::new("cube.column", "column", None, ColumnType::String, false);

        assert_eq!(nullable_col.is_nullable(), "YES");
        assert_eq!(not_nullable_col.is_nullable(), "NO");
    }

    #[test]
    fn test_all_column_types_have_data_types() {
        // This ensures we don't have any ColumnType variants that default to "text"
        // All explicit mappings should be in place

        let test_cases: Vec<(&str, ColumnType, &str)> = vec![
            ("String", ColumnType::String, "text"),
            ("VarStr", ColumnType::VarStr, "character varying"),
            ("Double", ColumnType::Double, "double precision"),
            ("Boolean", ColumnType::Boolean, "boolean"),
            ("Int8", ColumnType::Int8, "smallint"),
            ("Int32", ColumnType::Int32, "integer"),
            ("Int64", ColumnType::Int64, "bigint"),
            ("Blob", ColumnType::Blob, "bytea"),
            ("Date(false)", ColumnType::Date(false), "date"),
            ("Date(true)", ColumnType::Date(true), "date"),
            ("Timestamp", ColumnType::Timestamp, "timestamp without time zone"),
            ("Decimal(10,2)", ColumnType::Decimal(10, 2), "numeric"),
        ];

        for (name, col_type, expected_data_type) in test_cases {
            let col = CubeColumn::new("cube.column", "column", None, col_type, true);
            let actual = col.get_data_type();
            assert_eq!(
                actual, expected_data_type,
                "Expected data_type for {} to be '{}' but got '{}'",
                name, expected_data_type, actual
            );
        }
    }

    #[test]
    fn test_all_column_types_have_udt_names() {
        // Verify all ColumnType variants map to proper UDT names
        let test_cases: Vec<(&str, ColumnType, &str)> = vec![
            ("String", ColumnType::String, "text"),
            ("VarStr", ColumnType::VarStr, "varchar"),
            ("Double", ColumnType::Double, "float8"),
            ("Boolean", ColumnType::Boolean, "bool"),
            ("Int8", ColumnType::Int8, "int2"),
            ("Int32", ColumnType::Int32, "int4"),
            ("Int64", ColumnType::Int64, "int8"),
            ("Blob", ColumnType::Blob, "bytea"),
            ("Date(false)", ColumnType::Date(false), "date"),
            ("Date(true)", ColumnType::Date(true), "date"),
            ("Timestamp", ColumnType::Timestamp, "timestamp"),
            ("Decimal(10,2)", ColumnType::Decimal(10, 2), "numeric"),
        ];

        for (name, col_type, expected_udt_name) in test_cases {
            let col = CubeColumn::new("cube.column", "column", None, col_type, true);
            let actual = col.get_udt_name();
            assert_eq!(
                actual, expected_udt_name,
                "Expected udt_name for {} to be '{}' but got '{}'",
                name, expected_udt_name, actual
            );
        }
    }

    #[test]
    fn test_character_set_name_for_text_columns() {
        // Text columns should report UTF8 character set (via information_schema columns view)
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::String, true);

        // Character set information is handled in columns.rs
        // This test verifies the column type is properly classified as text
        assert_eq!(col.get_data_type(), "text");
    }

    #[test]
    fn test_double_precision_is_binary() {
        // Double precision (float8) uses binary radix, not decimal
        let col = CubeColumn::new("cube.column", "column", None, ColumnType::Double, true);

        assert_eq!(col.numeric_precision_radix(), Some(2),
            "Double precision should use binary radix (2)");
        assert_eq!(col.get_numeric_precision(), Some(53),
            "Double precision has 53 bits of precision");
    }

    #[test]
    fn test_int_types_have_no_precision_or_scale() {
        // In PostgreSQL, INTEGER types don't report numeric_precision or numeric_scale
        // Only NUMERIC/DECIMAL types do
        let int_types = vec![
            ColumnType::Int8,
            ColumnType::Int32,
            ColumnType::Int64,
        ];

        for int_type in int_types {
            let col = CubeColumn::new("cube.column", "column", None, int_type.clone(), true);
            assert_eq!(col.get_numeric_precision(), None,
                "Integer type {:?} should not have numeric_precision", int_type);
            assert_eq!(col.numeric_scale(), None,
                "Integer type {:?} should not have numeric_scale", int_type);
        }
    }
}
