/// V1FilterOperator : Only some operators are available for measures. For dimensions, the available operators depend on the [type of the dimension](/schema/reference/types-and-formats#types).

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1FilterOperator {}

impl V1FilterOperator {
    /// Only some operators are available for measures. For dimensions, the available operators depend on the [type of the dimension](/schema/reference/types-and-formats#types).
    pub fn new() -> V1FilterOperator {
        V1FilterOperator {}
    }
}
