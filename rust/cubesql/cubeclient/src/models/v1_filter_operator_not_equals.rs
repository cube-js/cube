/// V1FilterOperatorNotEquals : The opposite operator of `equals`. It supports multiple values. - Applied to measures. - Dimension types: `string`, `number`, `time`.

/// The opposite operator of `equals`. It supports multiple values. - Applied to measures. - Dimension types: `string`, `number`, `time`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorNotEquals {
    #[serde(rename = "notEquals")]
    NotEquals,
}

impl ToString for V1FilterOperatorNotEquals {
    fn to_string(&self) -> String {
        match self {
            Self::NotEquals => String::from("notEquals"),
        }
    }
}

impl Default for V1FilterOperatorNotEquals {
    fn default() -> V1FilterOperatorNotEquals {
        Self::NotEquals
    }
}
