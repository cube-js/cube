use cubenativeutils::CubeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregationType {
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
    CountDistinctApprox,
    NumberAgg,
    RunningTotal,
}

impl AggregationType {
    pub fn from_str(s: &str) -> Result<Self, CubeError> {
        match s {
            "sum" => Ok(Self::Sum),
            "avg" => Ok(Self::Avg),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "countDistinct" | "count_distinct" => Ok(Self::CountDistinct),
            "countDistinctApprox" | "count_distinct_approx" => Ok(Self::CountDistinctApprox),
            "numberAgg" | "number_agg" => Ok(Self::NumberAgg),
            "runningTotal" | "running_total" => Ok(Self::RunningTotal),
            other => Err(CubeError::user(format!(
                "Unknown aggregation type: '{}'",
                other
            ))),
        }
    }

    pub fn is_additive(&self) -> bool {
        matches!(
            self,
            Self::Sum | Self::Min | Self::Max | Self::CountDistinctApprox | Self::RunningTotal
        )
    }

    pub fn is_distinct(&self) -> bool {
        matches!(self, Self::CountDistinct | Self::CountDistinctApprox)
    }

    pub fn sql_function_name(&self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::CountDistinct => "count_distinct",
            Self::CountDistinctApprox => "count_distinct_approx",
            Self::NumberAgg => "number_agg",
            Self::RunningTotal => "sum",
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::CountDistinct => "countDistinct",
            Self::CountDistinctApprox => "countDistinctApprox",
            Self::NumberAgg => "numberAgg",
            Self::RunningTotal => "runningTotal",
        }
    }
}

impl TryFrom<&str> for AggregationType {
    type Error = CubeError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_str(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_camel_case() {
        assert_eq!(
            AggregationType::from_str("sum").unwrap(),
            AggregationType::Sum
        );
        assert_eq!(
            AggregationType::from_str("avg").unwrap(),
            AggregationType::Avg
        );
        assert_eq!(
            AggregationType::from_str("min").unwrap(),
            AggregationType::Min
        );
        assert_eq!(
            AggregationType::from_str("max").unwrap(),
            AggregationType::Max
        );
        assert_eq!(
            AggregationType::from_str("countDistinct").unwrap(),
            AggregationType::CountDistinct
        );
        assert_eq!(
            AggregationType::from_str("countDistinctApprox").unwrap(),
            AggregationType::CountDistinctApprox
        );
        assert_eq!(
            AggregationType::from_str("numberAgg").unwrap(),
            AggregationType::NumberAgg
        );
        assert_eq!(
            AggregationType::from_str("runningTotal").unwrap(),
            AggregationType::RunningTotal
        );
    }

    #[test]
    fn test_from_str_snake_case() {
        assert_eq!(
            AggregationType::from_str("count_distinct").unwrap(),
            AggregationType::CountDistinct
        );
        assert_eq!(
            AggregationType::from_str("count_distinct_approx").unwrap(),
            AggregationType::CountDistinctApprox
        );
        assert_eq!(
            AggregationType::from_str("number_agg").unwrap(),
            AggregationType::NumberAgg
        );
        assert_eq!(
            AggregationType::from_str("running_total").unwrap(),
            AggregationType::RunningTotal
        );
    }

    #[test]
    fn test_unknown_type_error() {
        assert!(AggregationType::from_str("unknown").is_err());
    }

    #[test]
    fn test_is_additive() {
        assert!(AggregationType::Sum.is_additive());
        assert!(AggregationType::Min.is_additive());
        assert!(AggregationType::Max.is_additive());
        assert!(!AggregationType::Avg.is_additive());
        assert!(!AggregationType::CountDistinct.is_additive());
        assert!(AggregationType::CountDistinctApprox.is_additive());
        assert!(!AggregationType::NumberAgg.is_additive());
        assert!(AggregationType::RunningTotal.is_additive());
    }

    #[test]
    fn test_is_distinct() {
        assert!(AggregationType::CountDistinct.is_distinct());
        assert!(AggregationType::CountDistinctApprox.is_distinct());
        assert!(!AggregationType::Sum.is_distinct());
        assert!(!AggregationType::Avg.is_distinct());
        assert!(!AggregationType::Min.is_distinct());
        assert!(!AggregationType::Max.is_distinct());
        assert!(!AggregationType::NumberAgg.is_distinct());
        assert!(!AggregationType::RunningTotal.is_distinct());
    }

    #[test]
    fn test_sql_function_name() {
        assert_eq!(AggregationType::Sum.sql_function_name(), "sum");
        assert_eq!(AggregationType::Avg.sql_function_name(), "avg");
        assert_eq!(AggregationType::Min.sql_function_name(), "min");
        assert_eq!(AggregationType::Max.sql_function_name(), "max");
        assert_eq!(
            AggregationType::CountDistinct.sql_function_name(),
            "count_distinct"
        );
        assert_eq!(
            AggregationType::CountDistinctApprox.sql_function_name(),
            "count_distinct_approx"
        );
        assert_eq!(AggregationType::NumberAgg.sql_function_name(), "number_agg");
        assert_eq!(AggregationType::RunningTotal.sql_function_name(), "sum");
    }

    #[test]
    fn test_as_str_round_trip() {
        let variants = [
            AggregationType::Sum,
            AggregationType::Avg,
            AggregationType::Min,
            AggregationType::Max,
            AggregationType::CountDistinct,
            AggregationType::CountDistinctApprox,
            AggregationType::NumberAgg,
            AggregationType::RunningTotal,
        ];
        for v in &variants {
            let s = v.as_str();
            let parsed = AggregationType::from_str(s).unwrap();
            assert_eq!(*v, parsed);
        }
    }

    #[test]
    fn test_try_from() {
        let result: Result<AggregationType, _> = "sum".try_into();
        assert_eq!(result.unwrap(), AggregationType::Sum);

        let result: Result<AggregationType, _> = "unknown".try_into();
        assert!(result.is_err());
    }
}
