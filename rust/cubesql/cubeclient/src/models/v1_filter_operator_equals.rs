/// V1FilterOperatorEquals : Use it when you need an exact match. It supports multiple values. - Applied to measures. - Dimension types: `string`, `number`, `time`.

/// Use it when you need an exact match. It supports multiple values. - Applied to measures. - Dimension types: `string`, `number`, `time`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorEquals {
    #[serde(rename = "equals")]
    Equals,
}

impl ToString for V1FilterOperatorEquals {
    fn to_string(&self) -> String {
        match self {
            Self::Equals => String::from("equals"),
        }
    }
}

impl Default for V1FilterOperatorEquals {
    fn default() -> V1FilterOperatorEquals {
        Self::Equals
    }
}
