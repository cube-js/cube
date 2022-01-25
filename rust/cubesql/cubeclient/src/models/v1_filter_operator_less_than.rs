/// V1FilterOperatorLessThan : The `lt` operator means **less than** and is used with measures or dimensions of type `number`. - Applied to measures. - Dimension types: `number`.

/// The `lt` operator means **less than** and is used with measures or dimensions of type `number`. - Applied to measures. - Dimension types: `number`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorLessThan {
    #[serde(rename = "lt")]
    Lt,
}

impl ToString for V1FilterOperatorLessThan {
    fn to_string(&self) -> String {
        match self {
            Self::Lt => String::from("lt"),
        }
    }
}

impl Default for V1FilterOperatorLessThan {
    fn default() -> V1FilterOperatorLessThan {
        Self::Lt
    }
}
