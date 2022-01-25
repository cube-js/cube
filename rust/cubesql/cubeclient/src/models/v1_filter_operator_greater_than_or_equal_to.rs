/// V1FilterOperatorGreaterThanOrEqualTo : The `gte` operator means **greater than or equal to** and is used with measures or dimensions of type `number`. - Applied to measures. - Dimension types: `number`.

/// The `gte` operator means **greater than or equal to** and is used with measures or dimensions of type `number`. - Applied to measures. - Dimension types: `number`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorGreaterThanOrEqualTo {
    #[serde(rename = "gte")]
    Gte,
}

impl ToString for V1FilterOperatorGreaterThanOrEqualTo {
    fn to_string(&self) -> String {
        match self {
            Self::Gte => String::from("gte"),
        }
    }
}

impl Default for V1FilterOperatorGreaterThanOrEqualTo {
    fn default() -> V1FilterOperatorGreaterThanOrEqualTo {
        Self::Gte
    }
}
