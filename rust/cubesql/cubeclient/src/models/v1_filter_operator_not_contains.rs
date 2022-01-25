/// V1FilterOperatorNotContains : The opposite operator of `contains`. It supports multiple values. - Dimension types: `string`.

/// The opposite operator of `contains`. It supports multiple values. - Dimension types: `string`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorNotContains {
    #[serde(rename = "notContains")]
    NotContains,
}

impl ToString for V1FilterOperatorNotContains {
    fn to_string(&self) -> String {
        match self {
            Self::NotContains => String::from("notContains"),
        }
    }
}

impl Default for V1FilterOperatorNotContains {
    fn default() -> V1FilterOperatorNotContains {
        Self::NotContains
    }
}
