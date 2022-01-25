/// V1FilterOperatorBeforeDate : Use it when you want to retrieve all results before some specific date. The values should be an array of one element in `YYYY-MM-DD` format. - Dimension types: `time`.

/// Use it when you want to retrieve all results before some specific date. The values should be an array of one element in `YYYY-MM-DD` format. - Dimension types: `time`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorBeforeDate {
    #[serde(rename = "beforeDate")]
    BeforeDate,
}

impl ToString for V1FilterOperatorBeforeDate {
    fn to_string(&self) -> String {
        match self {
            Self::BeforeDate => String::from("beforeDate"),
        }
    }
}

impl Default for V1FilterOperatorBeforeDate {
    fn default() -> V1FilterOperatorBeforeDate {
        Self::BeforeDate
    }
}
