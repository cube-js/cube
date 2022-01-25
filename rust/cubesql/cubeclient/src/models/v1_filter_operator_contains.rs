///
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorContains {
    #[serde(rename = "contains")]
    Contains,
}

impl ToString for V1FilterOperatorContains {
    fn to_string(&self) -> String {
        match self {
            Self::Contains => String::from("contains"),
        }
    }
}

impl Default for V1FilterOperatorContains {
    fn default() -> V1FilterOperatorContains {
        Self::Contains
    }
}
