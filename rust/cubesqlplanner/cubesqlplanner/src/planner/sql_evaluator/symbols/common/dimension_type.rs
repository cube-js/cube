use cubenativeutils::CubeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DimensionType {
    String,
    Number,
    Boolean,
    Time,
}

impl DimensionType {
    pub fn from_str(s: &str) -> Result<Self, CubeError> {
        match s {
            "string" => Ok(Self::String),
            "number" => Ok(Self::Number),
            "boolean" => Ok(Self::Boolean),
            "time" => Ok(Self::Time),
            other => Err(CubeError::user(format!(
                "Unknown dimension type: '{}'",
                other
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::Time => "time",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_round_trip() {
        for s in &["string", "number", "boolean", "time"] {
            let dt = DimensionType::from_str(s).unwrap();
            assert_eq!(dt.as_str(), *s);
        }
    }

    #[test]
    fn test_unknown_type_error() {
        let result = DimensionType::from_str("unknown");
        assert!(result.is_err());
    }
}
