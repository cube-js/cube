use super::expression::Expression;
use super::path::CubeName;

/// Join relationship — normalized to one of three by the schema-compiler.
/// Input forms: `belongs_to`/`many_to_one`/`manyToOne` → `BelongsTo`;
/// `has_many`/`one_to_many`/`oneToMany` → `HasMany`;
/// `has_one`/`one_to_one`/`oneToOne` → `HasOne`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Relationship {
    BelongsTo,
    HasMany,
    HasOne,
}

impl Relationship {
    /// `prepareJoins` in JS normalizes to camelCase `belongsTo` /
    /// `hasMany` / `hasOne` before reaching Rust. YAML fixtures still
    /// use the source forms (`many_to_one` etc), so we accept those too.
    pub fn parse(raw: &str) -> Result<Self, cubenativeutils::CubeError> {
        match raw {
            "belongsTo" | "belongs_to" | "many_to_one" | "manyToOne" => Ok(Self::BelongsTo),
            "hasMany" | "has_many" | "one_to_many" | "oneToMany" => Ok(Self::HasMany),
            "hasOne" | "has_one" | "one_to_one" | "oneToOne" => Ok(Self::HasOne),
            other => Err(cubenativeutils::CubeError::user(format!(
                "Unknown join relationship: {other}"
            ))),
        }
    }
}

#[derive(Clone)]
pub struct Join {
    /// Cube on the "from" side — the one that owns this join entry.
    pub from: CubeName,
    pub to: CubeName,
    pub relationship: Relationship,
    pub sql: Expression,
}
