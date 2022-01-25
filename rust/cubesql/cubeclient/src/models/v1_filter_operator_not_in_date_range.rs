/// V1FilterOperatorNotInDateRange : The opposite operator to `inDateRange`, use it when you want to exclude specific dates. The values format is the same as for `inDateRange`. - Dimension types: `time`.

/// The opposite operator to `inDateRange`, use it when you want to exclude specific dates. The values format is the same as for `inDateRange`. - Dimension types: `time`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorNotInDateRange {
    #[serde(rename = "notInDateRange")]
    NotInDateRange,
}

impl ToString for V1FilterOperatorNotInDateRange {
    fn to_string(&self) -> String {
        match self {
            Self::NotInDateRange => String::from("notInDateRange"),
        }
    }
}

impl Default for V1FilterOperatorNotInDateRange {
    fn default() -> V1FilterOperatorNotInDateRange {
        Self::NotInDateRange
    }
}
