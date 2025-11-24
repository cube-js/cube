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
#[derive(Clone)]
pub struct MockDriverTools {
    timezone: String,
    timestamp_precision: u32,
    sql_templates: Rc<MockSqlTemplatesRender>,
}

impl MockDriverTools {
    pub fn new() -> Self {
        Self {
            timezone: "UTC".to_string(),
            timestamp_precision: 3,
            sql_templates: Rc::new(MockSqlTemplatesRender::default_templates()),
        }
    }

    #[allow(dead_code)]
    pub fn with_timezone(timezone: String) -> Self {
        Self {
            timezone,
            timestamp_precision: 3,
            sql_templates: Rc::new(MockSqlTemplatesRender::default_templates()),
        }
    }

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

    fn convert_tz(&self, field: String) -> Result<String, CubeError> {
        Ok(format!(
            "({}::timestamptz AT TIME ZONE '{}')",
            field, self.timezone
        ))
    }

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

    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        Ok(self.sql_templates.clone())
    }

    fn timestamp_precision(&self) -> Result<u32, CubeError> {
        Ok(self.timestamp_precision)
    }

    fn time_stamp_cast(&self, field: String) -> Result<String, CubeError> {
        Ok(format!("{}::timestamptz", field))
    }

    fn date_time_cast(&self, field: String) -> Result<String, CubeError> {
        Ok(format!("{}::timestamp", field))
    }

    fn in_db_time_zone(&self, date: String) -> Result<String, CubeError> {
        Ok(date)
    }

    fn get_allocated_params(&self) -> Result<Vec<String>, CubeError> {
        Ok(Vec::new())
    }

    fn subtract_interval(&self, date: String, interval: String) -> Result<String, CubeError> {
        let interval_str = self.interval_string(interval)?;
        Ok(format!("{} - interval {}", date, interval_str))
    }

    fn add_interval(&self, date: String, interval: String) -> Result<String, CubeError> {
        let interval_str = self.interval_string(interval)?;
        Ok(format!("{} + interval {}", date, interval_str))
    }

    fn interval_string(&self, interval: String) -> Result<String, CubeError> {
        Ok(format!("'{}'", interval))
    }

    fn add_timestamp_interval(&self, date: String, interval: String) -> Result<String, CubeError> {
        self.add_interval(date, interval)
    }

    fn interval_and_minimal_time_unit(&self, interval: String) -> Result<Vec<String>, CubeError> {
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

    fn hll_init(&self, sql: String) -> Result<String, CubeError> {
        Ok(format!("hll_add_agg(hll_hash_any({}))", sql))
    }

    fn hll_merge(&self, sql: String) -> Result<String, CubeError> {
        Ok(format!("round(hll_cardinality(hll_union_agg({})))", sql))
    }

    fn hll_cardinality_merge(&self, sql: String) -> Result<String, CubeError> {
        self.hll_merge(sql)
    }

    fn count_distinct_approx(&self, sql: String) -> Result<String, CubeError> {
        Ok(format!(
            "round(hll_cardinality(hll_add_agg(hll_hash_any({}))))",
            sql
        ))
    }

    fn support_generated_series_for_custom_td(&self) -> Result<bool, CubeError> {
        Ok(true)
    }

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
