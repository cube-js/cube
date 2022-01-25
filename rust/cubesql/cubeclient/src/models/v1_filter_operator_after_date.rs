/// V1FilterOperatorAfterDate : The same as `beforeDate`, but is used to get all results after a specific date. - Dimension types: `time`.

/// The same as `beforeDate`, but is used to get all results after a specific date. - Dimension types: `time`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorAfterDate {
    #[serde(rename = "afterDate")]
    AfterDate,
}

impl ToString for V1FilterOperatorAfterDate {
    fn to_string(&self) -> String {
        match self {
            Self::AfterDate => String::from("afterDate"),
        }
    }
}

impl Default for V1FilterOperatorAfterDate {
    fn default() -> V1FilterOperatorAfterDate {
        Self::AfterDate
    }
}
