/// V1LoadRequestQueryOrderObject : An object where the keys are measures or dimensions to order by and their corresponding values are either `asc` or `desc`. The order of the keys in the object is used to order the final results.

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct V1LoadRequestQueryOrderObject {
    #[serde(rename = "member", skip_serializing_if = "Option::is_none")]
    pub member: Option<String>,
    #[serde(rename = "order", skip_serializing_if = "Option::is_none")]
    pub order: Option<Order>,
}

impl V1LoadRequestQueryOrderObject {
    /// An object where the keys are measures or dimensions to order by and their corresponding values are either `asc` or `desc`. The order of the keys in the object is used to order the final results.
    pub fn new() -> V1LoadRequestQueryOrderObject {
        V1LoadRequestQueryOrderObject {
            member: None,
            order: None,
        }
    }
}

///
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Order {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

impl Default for Order {
    fn default() -> Order {
        Self::Asc
    }
}
