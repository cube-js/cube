use crate::cube_bridge::driver_tools::DriverTools;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::test_fixtures::cube_bridge::MockSqlTemplatesRender;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Mock implementation of DriverTools for testing
///
/// This mock provides implementations based on PostgresQuery.ts and BaseQuery.js
/// from packages/cubejs-schema-compiler/src/adapter/
///
/// # Example
///
/// ```
/// use cubesqlplanner::test_fixtures::cube_bridge::MockDriverTools;
///
/// let tools = MockDriverTools::new();
/// let result = tools.time_grouped_column("day".to_string(), "created_at".to_string()).unwrap();
/// assert_eq!(result, "date_trunc('day', created_at)");
/// ```
#[derive(Clone)]
pub struct MockDriverTools {
    timezone: String,
    timestamp_precision: u32,
    sql_templates: Rc<MockSqlTemplatesRender>,
}

impl MockDriverTools {
    /// Creates a new MockDriverTools with default settings (UTC timezone)
    pub fn new() -> Self {
        Self {
            timezone: "UTC".to_string(),
            timestamp_precision: 3,
            sql_templates: Rc::new(MockSqlTemplatesRender::default_templates()),
        }
    }

    /// Creates a new MockDriverTools with a specific timezone
    pub fn with_timezone(timezone: String) -> Self {
        Self {
            timezone,
            timestamp_precision: 3,
            sql_templates: Rc::new(MockSqlTemplatesRender::default_templates()),
        }
    }

    /// Creates a new MockDriverTools with custom SQL templates
    #[allow(dead_code)]
    pub fn with_sql_templates(sql_templates: MockSqlTemplatesRender) -> Self {
        Self {
            timezone: "UTC".to_string(),
            timestamp_precision: 3,
            sql_templates: Rc::new(sql_templates),
        }
    }
}

impl Default for MockDriverTools {
    fn default() -> Self {
        Self::new()
    }
}

impl DriverTools for MockDriverTools {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }

    /// Convert timezone - based on PostgresQuery.ts:26-28
    /// Returns: `(field::timestamptz AT TIME ZONE 'timezone')`
    fn convert_tz(&self, field: String) -> Result<String, CubeError> {
        Ok(format!(
            "({}::timestamptz AT TIME ZONE '{}')",
            field, self.timezone
        ))
    }

    /// Time grouped column - based on PostgresQuery.ts:30-32
    /// Uses date_trunc function with granularity mapping
    fn time_grouped_column(
        &self,
        granularity: String,
        dimension: String,
    ) -> Result<String, CubeError> {
        // Map granularity to Postgres interval (from PostgresQuery.ts:4-13)
        let interval = match granularity.as_str() {
            "day" => "day",
            "week" => "week",
            "hour" => "hour",
            "minute" => "minute",
            "second" => "second",
            "month" => "month",
            "quarter" => "quarter",
            "year" => "year",
            _ => {
                return Err(CubeError::user(format!(
                    "Unsupported granularity: {}",
                    granularity
                )))
            }
        };

        Ok(format!("date_trunc('{}', {})", interval, dimension))
    }

    /// Returns SQL templates renderer
    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        Ok(self.sql_templates.clone())
    }

    /// Timestamp precision - based on BaseQuery.js:3834-3836
    fn timestamp_precision(&self) -> Result<u32, CubeError> {
        Ok(self.timestamp_precision)
    }

    /// Timestamp cast - based on BaseQuery.js:2101-2103
    /// Returns: `value::timestamptz`
    fn time_stamp_cast(&self, field: String) -> Result<String, CubeError> {
        Ok(format!("{}::timestamptz", field))
    }

    /// DateTime cast - based on BaseQuery.js:2105-2107
    /// Returns: `value::timestamp`
    fn date_time_cast(&self, field: String) -> Result<String, CubeError> {
        Ok(format!("{}::timestamp", field))
    }

    /// Convert date to DB timezone - based on BaseQuery.js:3820-3822
    /// This is a simplified version that returns the date as-is
    /// The full implementation would use localTimestampToUtc utility
    fn in_db_time_zone(&self, date: String) -> Result<String, CubeError> {
        // In real implementation this calls localTimestampToUtc(timezone, timestampFormat(), date)
        // For mock we just return the date as-is
        Ok(date)
    }

    /// Get allocated parameters - returns empty vec for mock
    fn get_allocated_params(&self) -> Result<Vec<String>, CubeError> {
        Ok(Vec::new())
    }

    /// Subtract interval - based on BaseQuery.js:1166-1169
    /// Returns: `date - interval 'interval'`
    fn subtract_interval(&self, date: String, interval: String) -> Result<String, CubeError> {
        let interval_str = self.interval_string(interval)?;
        Ok(format!("{} - interval {}", date, interval_str))
    }

    /// Add interval - based on BaseQuery.js:1176-1179
    /// Returns: `date + interval 'interval'`
    fn add_interval(&self, date: String, interval: String) -> Result<String, CubeError> {
        let interval_str = self.interval_string(interval)?;
        Ok(format!("{} + interval {}", date, interval_str))
    }

    /// Format interval string - based on BaseQuery.js:1190-1192
    /// Returns: `'interval'`
    fn interval_string(&self, interval: String) -> Result<String, CubeError> {
        Ok(format!("'{}'", interval))
    }

    /// Add timestamp interval - based on BaseQuery.js:1199-1201
    /// Delegates to add_interval
    fn add_timestamp_interval(&self, date: String, interval: String) -> Result<String, CubeError> {
        self.add_interval(date, interval)
    }

    /// Get interval and minimal time unit - based on BaseQuery.js:2116-2119
    /// Returns: [interval, minimal_time_unit]
    /// The minimal time unit is the lowest unit in the interval (e.g., "day" for "5 days")
    fn interval_and_minimal_time_unit(&self, interval: String) -> Result<Vec<String>, CubeError> {
        // Parse minimal granularity from interval
        // This is a simplified version - full implementation would call diffTimeUnitForInterval
        let min_unit = if interval.contains("second") {
            "second"
        } else if interval.contains("minute") {
            "minute"
        } else if interval.contains("hour") {
            "hour"
        } else if interval.contains("day") {
            "day"
        } else if interval.contains("week") {
            "week"
        } else if interval.contains("month") {
            "month"
        } else if interval.contains("quarter") {
            "quarter"
        } else if interval.contains("year") {
            "year"
        } else {
            "day" // default
        };

        Ok(vec![interval, min_unit.to_string()])
    }

    /// HLL init - based on PostgresQuery.ts:48-50
    /// Returns: `hll_add_agg(hll_hash_any(sql))`
    fn hll_init(&self, sql: String) -> Result<String, CubeError> {
        Ok(format!("hll_add_agg(hll_hash_any({}))", sql))
    }

    /// HLL merge - based on PostgresQuery.ts:52-54
    /// Returns: `round(hll_cardinality(hll_union_agg(sql)))`
    fn hll_merge(&self, sql: String) -> Result<String, CubeError> {
        Ok(format!("round(hll_cardinality(hll_union_agg({})))", sql))
    }

    /// HLL cardinality merge - based on BaseQuery.js:3734-3736
    /// Delegates to hll_merge
    fn hll_cardinality_merge(&self, sql: String) -> Result<String, CubeError> {
        self.hll_merge(sql)
    }

    /// Count distinct approx - based on PostgresQuery.ts:56-58
    /// Returns: `round(hll_cardinality(hll_add_agg(hll_hash_any(sql))))`
    fn count_distinct_approx(&self, sql: String) -> Result<String, CubeError> {
        Ok(format!(
            "round(hll_cardinality(hll_add_agg(hll_hash_any({}))))",
            sql
        ))
    }

    /// Support generated series for custom time dimensions - based on PostgresQuery.ts:60-62
    /// Postgres supports this, so returns true
    fn support_generated_series_for_custom_td(&self) -> Result<bool, CubeError> {
        Ok(true)
    }

    /// Date bin function - based on PostgresQuery.ts:40-46
    /// Returns sql for source expression floored to timestamps aligned with
    /// intervals relative to origin timestamp point
    fn date_bin(
        &self,
        interval: String,
        source: String,
        origin: String,
    ) -> Result<String, CubeError> {
        Ok(format!(
            "('{}' ::timestamp + INTERVAL '{}' * FLOOR(EXTRACT(EPOCH FROM ({} - '{}'::timestamp)) / EXTRACT(EPOCH FROM INTERVAL '{}')))",
            origin, interval, source, origin, interval
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_tz() {
        let tools = MockDriverTools::new();
        let result = tools.convert_tz("created_at".to_string()).unwrap();
        assert_eq!(result, "(created_at::timestamptz AT TIME ZONE 'UTC')");
    }

    #[test]
    fn test_convert_tz_with_custom_timezone() {
        let tools = MockDriverTools::with_timezone("America/Los_Angeles".to_string());
        let result = tools.convert_tz("created_at".to_string()).unwrap();
        assert_eq!(
            result,
            "(created_at::timestamptz AT TIME ZONE 'America/Los_Angeles')"
        );
    }

    #[test]
    fn test_time_grouped_column() {
        let tools = MockDriverTools::new();

        // Test various granularities
        assert_eq!(
            tools
                .time_grouped_column("day".to_string(), "created_at".to_string())
                .unwrap(),
            "date_trunc('day', created_at)"
        );

        assert_eq!(
            tools
                .time_grouped_column("month".to_string(), "updated_at".to_string())
                .unwrap(),
            "date_trunc('month', updated_at)"
        );

        assert_eq!(
            tools
                .time_grouped_column("year".to_string(), "timestamp".to_string())
                .unwrap(),
            "date_trunc('year', timestamp)"
        );
    }

    #[test]
    fn test_time_grouped_column_invalid_granularity() {
        let tools = MockDriverTools::new();
        let result = tools.time_grouped_column("invalid".to_string(), "created_at".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_timestamp_precision() {
        let tools = MockDriverTools::new();
        assert_eq!(tools.timestamp_precision().unwrap(), 3);
    }

    #[test]
    fn test_time_stamp_cast() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools.time_stamp_cast("?".to_string()).unwrap(),
            "?::timestamptz"
        );
    }

    #[test]
    fn test_date_time_cast() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools.date_time_cast("date_from".to_string()).unwrap(),
            "date_from::timestamp"
        );
    }

    #[test]
    fn test_subtract_interval() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools
                .subtract_interval("NOW()".to_string(), "1 day".to_string())
                .unwrap(),
            "NOW() - interval '1 day'"
        );
    }

    #[test]
    fn test_add_interval() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools
                .add_interval("created_at".to_string(), "7 days".to_string())
                .unwrap(),
            "created_at + interval '7 days'"
        );
    }

    #[test]
    fn test_interval_string() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools.interval_string("1 hour".to_string()).unwrap(),
            "'1 hour'"
        );
    }

    #[test]
    fn test_add_timestamp_interval() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools
                .add_timestamp_interval("timestamp".to_string(), "5 minutes".to_string())
                .unwrap(),
            "timestamp + interval '5 minutes'"
        );
    }

    #[test]
    fn test_interval_and_minimal_time_unit() {
        let tools = MockDriverTools::new();

        let result = tools
            .interval_and_minimal_time_unit("5 days".to_string())
            .unwrap();
        assert_eq!(result, vec!["5 days", "day"]);

        let result = tools
            .interval_and_minimal_time_unit("2 hours".to_string())
            .unwrap();
        assert_eq!(result, vec!["2 hours", "hour"]);

        let result = tools
            .interval_and_minimal_time_unit("30 seconds".to_string())
            .unwrap();
        assert_eq!(result, vec!["30 seconds", "second"]);
    }

    #[test]
    fn test_hll_init() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools.hll_init("user_id".to_string()).unwrap(),
            "hll_add_agg(hll_hash_any(user_id))"
        );
    }

    #[test]
    fn test_hll_merge() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools.hll_merge("hll_column".to_string()).unwrap(),
            "round(hll_cardinality(hll_union_agg(hll_column)))"
        );
    }

    #[test]
    fn test_hll_cardinality_merge() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools.hll_cardinality_merge("hll_data".to_string()).unwrap(),
            "round(hll_cardinality(hll_union_agg(hll_data)))"
        );
    }

    #[test]
    fn test_count_distinct_approx() {
        let tools = MockDriverTools::new();
        assert_eq!(
            tools
                .count_distinct_approx("visitor_id".to_string())
                .unwrap(),
            "round(hll_cardinality(hll_add_agg(hll_hash_any(visitor_id))))"
        );
    }

    #[test]
    fn test_support_generated_series_for_custom_td() {
        let tools = MockDriverTools::new();
        assert!(tools.support_generated_series_for_custom_td().unwrap());
    }

    #[test]
    fn test_date_bin() {
        let tools = MockDriverTools::new();
        let result = tools
            .date_bin(
                "1 day".to_string(),
                "created_at".to_string(),
                "2024-01-01".to_string(),
            )
            .unwrap();

        assert_eq!(
            result,
            "('2024-01-01' ::timestamp + INTERVAL '1 day' * FLOOR(EXTRACT(EPOCH FROM (created_at - '2024-01-01'::timestamp)) / EXTRACT(EPOCH FROM INTERVAL '1 day')))"
        );
    }

    #[test]
    fn test_in_db_time_zone() {
        let tools = MockDriverTools::new();
        let result = tools
            .in_db_time_zone("2024-01-01T00:00:00".to_string())
            .unwrap();
        assert_eq!(result, "2024-01-01T00:00:00");
    }

    #[test]
    fn test_get_allocated_params() {
        let tools = MockDriverTools::new();
        let result = tools.get_allocated_params().unwrap();
        assert_eq!(result, Vec::<String>::new());
    }

    #[test]
    fn test_sql_templates() {
        let tools = MockDriverTools::new();
        let templates = tools.sql_templates().unwrap();

        // Verify it returns a valid SqlTemplatesRender
        assert!(templates.contains_template("filters/equals"));
        assert!(templates.contains_template("functions/SUM"));
    }
}
