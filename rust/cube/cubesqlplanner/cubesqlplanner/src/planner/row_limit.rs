use std::fmt;

/// Placeholder the JS side passes for the rollupLambda source query limit. The orchestrator
/// substitutes `preAggregationsOptions.maxSourceRowLimit` for it at execution time, so it has to
/// reach the SQL as a bound param rather than as a number.
pub const MAX_SOURCE_ROW_LIMIT: &str = "__MAX_SOURCE_ROW_LIMIT";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowLimit {
    Value(usize),
    /// Rendered as a param carrying this name, for the orchestrator to substitute.
    Param(String),
}

impl RowLimit {
    pub fn parse(value: &str) -> Option<Self> {
        if value == MAX_SOURCE_ROW_LIMIT {
            return Some(Self::Param(value.to_string()));
        }
        value.parse::<usize>().ok().map(Self::Value)
    }
}

impl fmt::Display for RowLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(value) => write!(f, "{}", value),
            Self::Param(name) => write!(f, "{}", name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_numbers_and_the_source_row_limit_placeholder() {
        assert_eq!(RowLimit::parse("0"), Some(RowLimit::Value(0)));
        assert_eq!(RowLimit::parse("10"), Some(RowLimit::Value(10)));
        assert_eq!(
            RowLimit::parse(MAX_SOURCE_ROW_LIMIT),
            Some(RowLimit::Param(MAX_SOURCE_ROW_LIMIT.to_string()))
        );
        assert_eq!(RowLimit::parse("abc"), None);
    }
}
