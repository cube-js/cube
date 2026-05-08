use crate::physical_plan::QualifiedColumnName;
use std::collections::HashMap;

#[derive(Clone)]
pub struct RawReferenceValue(pub String);

#[derive(Clone)]
pub enum RenderReferencesType {
    QualifiedColumnName(QualifiedColumnName),
    LiteralValue(String),
    RawReferenceValue(String),
}

impl From<QualifiedColumnName> for RenderReferencesType {
    fn from(value: QualifiedColumnName) -> Self {
        Self::QualifiedColumnName(value)
    }
}

impl From<String> for RenderReferencesType {
    fn from(value: String) -> Self {
        Self::LiteralValue(value)
    }
}

impl From<RawReferenceValue> for RenderReferencesType {
    fn from(value: RawReferenceValue) -> Self {
        Self::RawReferenceValue(value.0)
    }
}

#[derive(Default, Clone)]
pub struct RenderReferences {
    references: HashMap<String, RenderReferencesType>,
}

impl RenderReferences {
    pub fn insert<T: Into<RenderReferencesType>>(&mut self, name: String, value: T) {
        self.references.insert(name, value.into());
    }

    pub fn get(&self, name: &str) -> Option<&RenderReferencesType> {
        self.references.get(name)
    }

    pub fn is_empty(&self) -> bool {
        self.references.is_empty()
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.references.contains_key(name)
    }
}
