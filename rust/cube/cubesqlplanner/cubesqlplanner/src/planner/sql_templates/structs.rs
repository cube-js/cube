use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateProjectionColumn {
    pub expr: String,
    pub alias: String,
    pub aliased: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateGroupByColumn {
    pub expr: String,
    pub index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateOrderByColumn {
    pub expr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateCalcGroup {
    pub name: String,
    pub alias: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateCalcSingleValue {
    pub name: String,
    pub value: String,
}
