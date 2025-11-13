use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrimitiveType {
    Number,
    String,
    Boolean,
    Time,
}

impl PrimitiveType {
    /// Try to construct PrimitiveType from string
    pub fn try_from_str(s: &str) -> Option<Self> {
        match s {
            "number" => Some(Self::Number),
            "string" => Some(Self::String),
            "boolean" => Some(Self::Boolean),
            "time" => Some(Self::Time),
            _ => None,
        }
    }

    /// Get string representation of the type
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Number => "number",
            Self::String => "string",
            Self::Boolean => "boolean",
            Self::Time => "time",
        }
    }
}

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<&str> for PrimitiveType {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_from_str(value).ok_or(())
    }
}

impl TryFrom<String> for PrimitiveType {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from_str(&value).ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_str() {
        assert_eq!(
            PrimitiveType::try_from_str("number"),
            Some(PrimitiveType::Number)
        );
        assert_eq!(
            PrimitiveType::try_from_str("string"),
            Some(PrimitiveType::String)
        );
        assert_eq!(
            PrimitiveType::try_from_str("boolean"),
            Some(PrimitiveType::Boolean)
        );
        assert_eq!(
            PrimitiveType::try_from_str("time"),
            Some(PrimitiveType::Time)
        );
        assert_eq!(PrimitiveType::try_from_str("unknown"), None);
    }

    #[test]
    fn test_as_str() {
        assert_eq!(PrimitiveType::Number.as_str(), "number");
        assert_eq!(PrimitiveType::String.as_str(), "string");
        assert_eq!(PrimitiveType::Boolean.as_str(), "boolean");
        assert_eq!(PrimitiveType::Time.as_str(), "time");
    }

    #[test]
    fn test_try_from() {
        assert_eq!(PrimitiveType::try_from("number"), Ok(PrimitiveType::Number));
        assert_eq!(PrimitiveType::try_from("invalid"), Err(()));

        assert_eq!(
            PrimitiveType::try_from("string".to_string()),
            Ok(PrimitiveType::String)
        );
        assert_eq!(PrimitiveType::try_from("invalid".to_string()), Err(()));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", PrimitiveType::Number), "number");
        assert_eq!(format!("{}", PrimitiveType::String), "string");
        assert_eq!(format!("{}", PrimitiveType::Boolean), "boolean");
        assert_eq!(format!("{}", PrimitiveType::Time), "time");
    }
}
