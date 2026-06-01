use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CubeName(String);

impl CubeName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CubeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for CubeName {
    fn from(value: String) -> Self {
        Self(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MemberPath {
    cube: CubeName,
    name: String,
}

impl MemberPath {
    pub fn new(cube: CubeName, name: impl Into<String>) -> Self {
        Self {
            cube,
            name: name.into(),
        }
    }

    pub fn cube(&self) -> &CubeName {
        &self.cube
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.cube, self.name)
    }

    /// Parses a `Cube.member` reference. Returns an error for paths
    /// that do not split into exactly two segments (we'll grow this
    /// to support view-style join chains later).
    pub fn parse(path: &str) -> Result<Self, cubenativeutils::CubeError> {
        match path.split_once('.') {
            Some((cube, name)) if !cube.is_empty() && !name.is_empty() => {
                Ok(MemberPath::new(CubeName::new(cube), name.to_string()))
            }
            _ => Err(cubenativeutils::CubeError::user(format!(
                "Invalid member path: {path}"
            ))),
        }
    }
}

impl fmt::Display for MemberPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.cube, self.name)
    }
}
