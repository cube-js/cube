/// V1FilterOperatorGreaterThan : The `gt` operator means **greater than** and is used with measures or dimensions of type `number`. - Applied to measures. - Dimension types: `number`.

/// The `gt` operator means **greater than** and is used with measures or dimensions of type `number`. - Applied to measures. - Dimension types: `number`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorGreaterThan {
    #[serde(rename = "gt")]
    Gt,
}

impl ToString for V1FilterOperatorGreaterThan {
    fn to_string(&self) -> String {
        match self {
            Self::Gt => String::from("gt"),
        }
    }
}

impl Default for V1FilterOperatorGreaterThan {
    fn default() -> V1FilterOperatorGreaterThan {
        Self::Gt
    }
}
