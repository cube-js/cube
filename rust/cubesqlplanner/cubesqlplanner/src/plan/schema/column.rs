use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct QualifiedColumnName {
    source: Option<String>,
    name: String,
}

impl QualifiedColumnName {
    pub fn new(source: Option<String>, name: String) -> Self {
        Self { source, name }
    }

    pub fn source(&self) -> &Option<String> {
        &self.source
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn set_source(&mut self, source: Option<String>) {
        self.source = source;
    }

    pub fn set_source_if_none(&mut self, source: &str) {
        if self.source.is_none() {
            self.source = Some(source.to_string());
        }
    }
}

impl Display for QualifiedColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(source) = &self.source {
            write!(f, "{}.", source)?
        }
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Clone)]
pub struct SchemaColumn {
    name: String,
    origin_member: Option<String>,
}

impl SchemaColumn {
    pub fn new(name: String, origin_member: Option<String>) -> Self {
        Self {
            name,
            origin_member,
        }
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn origin_member(&self) -> &Option<String> {
        &self.origin_member
    }
}
