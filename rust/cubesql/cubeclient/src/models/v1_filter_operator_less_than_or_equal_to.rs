/// V1FilterOperatorLessThanOrEqualTo : The `lte` operator means **less than or equal to** and is used with measures or dimensions of type `number`. - Applied to measures. - Dimension types: `number`.

/// The `lte` operator means **less than or equal to** and is used with measures or dimensions of type `number`. - Applied to measures. - Dimension types: `number`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorLessThanOrEqualTo {
    #[serde(rename = "lte")]
    Lte,
}

impl ToString for V1FilterOperatorLessThanOrEqualTo {
    fn to_string(&self) -> String {
        match self {
            Self::Lte => String::from("lte"),
        }
    }
}

impl Default for V1FilterOperatorLessThanOrEqualTo {
    fn default() -> V1FilterOperatorLessThanOrEqualTo {
        Self::Lte
    }
}
