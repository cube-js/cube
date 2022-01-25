/// V1FilterOperatorSet : Operator `set` checks whether the value of the member **is not** `NULL`. You don't need to pass `values` for this operator. - Applied to measures. - Dimension types: `number`, `string`, `time`.

/// Operator `set` checks whether the value of the member **is not** `NULL`. You don't need to pass `values` for this operator. - Applied to measures. - Dimension types: `number`, `string`, `time`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum V1FilterOperatorSet {
    #[serde(rename = "set")]
    Set,
}

impl ToString for V1FilterOperatorSet {
    fn to_string(&self) -> String {
        match self {
            Self::Set => String::from("set"),
        }
    }
}

impl Default for V1FilterOperatorSet {
    fn default() -> V1FilterOperatorSet {
        Self::Set
    }
}
