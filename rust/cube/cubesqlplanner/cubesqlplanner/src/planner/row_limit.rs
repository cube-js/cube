use std::fmt;

/// Placeholder the JS side passes for the rollupLambda source query limit. The orchestrator
/// substitutes `preAggregationsOptions.maxSourceRowLimit` for it at execution time, so it has to
/// reach the SQL as a bound param rather than as a number.
pub const MAX_SOURCE_ROW_LIMIT: &str = "__MAX_SOURCE_ROW_LIMIT";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowLimit {
    Value(usize),
    /// Rendered as a `MAX_SOURCE_ROW_LIMIT` param, for the orchestrator to substitute.
    MaxSourceRowLimit,
}

impl RowLimit {
    pub fn parse(value: &str) -> Option<Self> {
        if value == MAX_SOURCE_ROW_LIMIT {
            return Some(Self::MaxSourceRowLimit);
        }
        value.parse::<usize>().ok().map(Self::Value)
    }
}

impl fmt::Display for RowLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(value) => write!(f, "{}", value),
            Self::MaxSourceRowLimit => write!(f, "{}", MAX_SOURCE_ROW_LIMIT),
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
            Some(RowLimit::MaxSourceRowLimit)
        );
        assert_eq!(RowLimit::parse("abc"), None);
    }
}
