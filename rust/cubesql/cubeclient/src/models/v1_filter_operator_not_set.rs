/// V1FilterOperatorNotSet : An opposite to the `set` operator. It checks whether the value of the member **is** `NULL`. You don't need to pass `values` for this operator. - Applied to measures. - Dimension types: `number`, `string`, `time`.

/// An opposite to the `set` operator. It checks whether the value of the member **is** `NULL`. You don't need to pass `values` for this operator. - Applied to measures. - Dimension types: `number`, `string`, `time`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorNotSet {
    #[serde(rename = "notSet")]
    NotSet,
}

impl ToString for V1FilterOperatorNotSet {
    fn to_string(&self) -> String {
        match self {
            Self::NotSet => String::from("notSet"),
        }
    }
}

impl Default for V1FilterOperatorNotSet {
    fn default() -> V1FilterOperatorNotSet {
        Self::NotSet
    }
}
