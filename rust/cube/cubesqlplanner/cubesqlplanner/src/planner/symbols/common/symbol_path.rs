use std::hash::{Hash, Hasher};
use std::rc::Rc;

use cubenativeutils::CubeError;

use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::planner::{CubeId, MemberId};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SymbolPathType {
    Dimension,
    Measure,
    Segment,
    CubeName,
    CubeTable,
}

/// Resolved path of a member or cube reference in the data model:
/// parsed from a dotted string (`cube.member`, `cube.cube2.dim`,
/// `cube.time_dim.day`, `CUBE`, `CUBE.__sql_fn`), with its
/// `SymbolPathType` and the chain of cubes traversed along the way.
#[derive(Debug, Clone)]
pub struct SymbolPath {
    path_type: SymbolPathType,
    path: Vec<CubeId>,
    cube_id: CubeId,
    symbol_name: String,
    granularity: Option<String>,
}

impl SymbolPath {
    fn new(
        path_type: SymbolPathType,
        path: Vec<CubeId>,
        cube_id: CubeId,
        symbol_name: String,
        granularity: Option<String>,
    ) -> Self {
        Self {
            path_type,
            path,
            cube_id,
            symbol_name,
            granularity,
        }
    }

    /// Parses a dotted path string by walking the data model through
    /// the provided `CubeEvaluator`.
    pub fn parse(cube_evaluator: Rc<dyn CubeEvaluator>, path: &str) -> Result<Self, CubeError> {
        let parts: Vec<String> = path.split('.').map(|s| s.to_string()).collect();
        Self::parse_parts(cube_evaluator, None, &parts)
    }

    /// Parses pre-split path segments resolved relative to
    /// `current_cube` — so the first segment may refer to a member
    /// of `current_cube` directly.
    pub fn parse_parts(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        current_cube: Option<&CubeId>,
        parts: &[String],
    ) -> Result<Self, CubeError> {
        Self::resolve_parts(cube_evaluator, current_cube, parts, vec![])
    }

    fn resolve_parts(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        current_cube: Option<&CubeId>,
        parts: &[String],
        path: Vec<CubeId>,
    ) -> Result<Self, CubeError> {
        if parts.is_empty() {
            return Err(CubeError::user("Empty path".to_string()));
        }

        // Step 1: If current_cube set, try resolving parts[0] as member
        if let Some(cube_id) = current_cube {
            if let Some(result) =
                Self::try_resolve_as_member(cube_evaluator.clone(), cube_id, parts, &path)?
            {
                return Ok(result);
            }
        }

        // Step 2: Try resolving parts[0] as cube reference
        let resolved = Self::resolve_cube_name(cube_evaluator.clone(), current_cube, &parts[0])?;

        if let Some(cube_id) = resolved {
            let mut new_path = path;
            new_path.push(cube_id.clone());

            if parts.len() == 1 {
                return Ok(Self::new(
                    SymbolPathType::CubeName,
                    new_path,
                    cube_id,
                    String::new(),
                    None,
                ));
            }
            if parts.len() >= 2 && parts[1] == "__sql_fn" {
                return Ok(Self::new(
                    SymbolPathType::CubeTable,
                    new_path,
                    cube_id,
                    String::new(),
                    None,
                ));
            }
            return Self::resolve_parts(cube_evaluator, Some(&cube_id), &parts[1..], new_path);
        }

        Err(CubeError::user(format!("Cannot resolve: {}", parts[0])))
    }

    fn try_resolve_as_member(
        evaluator: Rc<dyn CubeEvaluator>,
        cube_id: &CubeId,
        parts: &[String],
        path: &[CubeId],
    ) -> Result<Option<Self>, CubeError> {
        let check_path = vec![cube_id.target().to_string(), parts[0].clone()];

        if evaluator.is_dimension(check_path.clone())? {
            if parts.len() == 1 {
                return Ok(Some(Self::new(
                    SymbolPathType::Dimension,
                    path.to_vec(),
                    cube_id.clone(),
                    parts[0].clone(),
                    None,
                )));
            }
            if parts.len() == 2 {
                let dim =
                    evaluator.dimension_by_path(format!("{}.{}", cube_id.target(), parts[0]))?;
                if dim.static_data().dimension_type == "time" {
                    return Ok(Some(Self::new(
                        SymbolPathType::Dimension,
                        path.to_vec(),
                        cube_id.clone(),
                        parts[0].clone(),
                        Some(parts[1].clone()),
                    )));
                }
            }
            // Dimension with extra parts (non-time) — not a member match
            return Ok(None);
        }

        // Measures/segments can't have extra parts
        if parts.len() > 1 {
            return Ok(None);
        }

        if evaluator.is_measure(check_path.clone())? {
            return Ok(Some(Self::new(
                SymbolPathType::Measure,
                path.to_vec(),
                cube_id.clone(),
                parts[0].clone(),
                None,
            )));
        }
        if evaluator.is_segment(check_path)? {
            return Ok(Some(Self::new(
                SymbolPathType::Segment,
                path.to_vec(),
                cube_id.clone(),
                parts[0].clone(),
                None,
            )));
        }
        Ok(None)
    }

    fn resolve_cube_name(
        evaluator: Rc<dyn CubeEvaluator>,
        current_cube: Option<&CubeId>,
        name: &str,
    ) -> Result<Option<CubeId>, CubeError> {
        if matches!(name, "CUBE" | "TABLE") {
            return Ok(current_cube.cloned());
        }
        if evaluator.cube_exists(name.to_string())? {
            return Ok(Some(CubeId::cube(name)));
        }
        Ok(None)
    }

    pub fn path_type(&self) -> &SymbolPathType {
        &self.path_type
    }

    pub fn path(&self) -> &Vec<CubeId> {
        &self.path
    }

    pub fn cube_id(&self) -> &CubeId {
        &self.cube_id
    }

    pub fn symbol_name(&self) -> &String {
        &self.symbol_name
    }

    /// The rendered name: the member's full name, or the cube's for a cube path.
    pub fn full_name(&self) -> String {
        if self.symbol_name.is_empty() {
            self.cube_id.to_string()
        } else {
            self.member_id().full_name().clone()
        }
    }

    /// Identity of the member this path resolves to.
    pub fn member_id(&self) -> MemberId {
        MemberId::member(self.cube_id.clone(), self.symbol_name.clone())
    }

    pub fn cache_name(&self) -> String {
        if let Some(granularity) = &self.granularity {
            format!("{}.{}", self.full_name(), granularity)
        } else {
            self.full_name()
        }
    }

    pub fn granularity(&self) -> &Option<String> {
        &self.granularity
    }
}

impl PartialEq for SymbolPath {
    fn eq(&self, other: &Self) -> bool {
        self.path_type == other.path_type
            && self.cube_id == other.cube_id
            && self.symbol_name == other.symbol_name
            && self.granularity == other.granularity
            && self.path == other.path
    }
}

impl Eq for SymbolPath {}

impl Hash for SymbolPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path_type.hash(state);
        self.cube_id.hash(state);
        self.symbol_name.hash(state);
        self.granularity.hash(state);
        self.path.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::{MockCubeEvaluator, MockSchema};
    use indoc::indoc;

    fn create_test_evaluator() -> Rc<MockCubeEvaluator> {
        let schema = MockSchema::from_yaml(indoc! {"
            cubes:
              - name: users
                sql: SELECT * FROM users
                dimensions:
                  - name: created_at
                    type: time
                    sql: created_at
                  - name: id
                    type: number
                    sql: id
                  - name: source
                    type: string
                    sql: source
                measures:
                  - name: count
                    type: count
                segments:
                  - name: google
                    sql: \"{CUBE.source} = 'google'\"
              - name: orders
                sql: SELECT * FROM orders
                dimensions:
                  - name: id
                    type: number
                    sql: id
                measures:
                  - name: total
                    type: sum
                    sql: amount
        "})
        .unwrap();
        Rc::new(MockCubeEvaluator::new(schema))
    }

    #[test]
    fn test_parse_simple_dimension() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(evaluator.clone(), "users.created_at").unwrap();
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.full_name(), "users.created_at");
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
        assert_eq!(result.granularity(), &None);
        assert_eq!(result.path_type(), &SymbolPathType::Dimension);
    }

    #[test]
    fn test_parse_cross_cube_dimension() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(evaluator.clone(), "orders.users.created_at").unwrap();
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.full_name(), "users.created_at");
        assert_eq!(
            result.path(),
            &vec![CubeId::cube("orders"), CubeId::cube("users")]
        );
        assert_eq!(result.granularity(), &None);
        assert_eq!(result.path_type(), &SymbolPathType::Dimension);
    }

    #[test]
    fn test_parse_time_dimension_with_granularity() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(evaluator.clone(), "users.created_at.day").unwrap();
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.full_name(), "users.created_at");
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
        assert_eq!(result.granularity(), &Some("day".to_string()));
        assert_eq!(result.path_type(), &SymbolPathType::Dimension);
    }

    #[test]
    fn test_parse_cross_cube_time_dimension_with_granularity() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(evaluator.clone(), "orders.users.created_at.day").unwrap();
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.full_name(), "users.created_at");
        assert_eq!(
            result.path(),
            &vec![CubeId::cube("orders"), CubeId::cube("users")]
        );
        assert_eq!(result.granularity(), &Some("day".to_string()));
        assert_eq!(result.path_type(), &SymbolPathType::Dimension);
    }

    #[test]
    fn test_parse_measure() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(evaluator.clone(), "users.count").unwrap();
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "count");
        assert_eq!(result.full_name(), "users.count");
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
        assert_eq!(result.granularity(), &None);
        assert_eq!(result.path_type(), &SymbolPathType::Measure);
    }

    #[test]
    fn test_parse_cross_cube_measure() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(evaluator.clone(), "users.orders.total").unwrap();
        assert_eq!(result.cube_id(), &CubeId::cube("orders"));
        assert_eq!(result.symbol_name(), "total");
        assert_eq!(result.full_name(), "orders.total");
        assert_eq!(
            result.path(),
            &vec![CubeId::cube("users"), CubeId::cube("orders")]
        );
        assert_eq!(result.granularity(), &None);
        assert_eq!(result.path_type(), &SymbolPathType::Measure);
    }

    #[test]
    fn test_parse_parts_cube_alias() {
        let evaluator = create_test_evaluator();
        let parts = vec!["CUBE".to_string()];
        let result =
            SymbolPath::parse_parts(evaluator.clone(), Some(&CubeId::cube("users")), &parts)
                .unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::CubeName);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
    }

    #[test]
    fn test_parse_parts_table_alias() {
        let evaluator = create_test_evaluator();
        let parts = vec!["TABLE".to_string()];
        let result =
            SymbolPath::parse_parts(evaluator.clone(), Some(&CubeId::cube("users")), &parts)
                .unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::CubeName);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
    }

    #[test]
    fn test_parse_parts_cube_table() {
        let evaluator = create_test_evaluator();
        let parts = vec!["CUBE".to_string(), "__sql_fn".to_string()];
        let result =
            SymbolPath::parse_parts(evaluator.clone(), Some(&CubeId::cube("users")), &parts)
                .unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::CubeTable);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
    }

    #[test]
    fn test_parse_parts_simple_member() {
        let evaluator = create_test_evaluator();
        let parts = vec!["source".to_string()];
        let result =
            SymbolPath::parse_parts(evaluator.clone(), Some(&CubeId::cube("users")), &parts)
                .unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::Dimension);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "source");
        assert_eq!(result.path(), &Vec::<CubeId>::new());
    }

    #[test]
    fn test_parse_parts_time_dimension_with_granularity() {
        let evaluator = create_test_evaluator();
        let parts = vec!["created_at".to_string(), "day".to_string()];
        let result =
            SymbolPath::parse_parts(evaluator.clone(), Some(&CubeId::cube("users")), &parts)
                .unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::Dimension);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.granularity(), &Some("day".to_string()));
        assert_eq!(result.path(), &Vec::<CubeId>::new());
    }

    #[test]
    fn test_parse_parts_cross_cube_member() {
        let evaluator = create_test_evaluator();
        let parts = vec!["orders".to_string(), "total".to_string()];
        let result =
            SymbolPath::parse_parts(evaluator.clone(), Some(&CubeId::cube("users")), &parts)
                .unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::Measure);
        assert_eq!(result.cube_id(), &CubeId::cube("orders"));
        assert_eq!(result.symbol_name(), "total");
        assert_eq!(result.path(), &vec![CubeId::cube("orders")]);
    }

    #[test]
    fn test_parse_parts_measure() {
        let evaluator = create_test_evaluator();
        let parts = vec!["count".to_string()];
        let result =
            SymbolPath::parse_parts(evaluator.clone(), Some(&CubeId::cube("users")), &parts)
                .unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::Measure);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "count");
        assert_eq!(result.path(), &Vec::<CubeId>::new());
    }

    #[test]
    fn test_parse_parts_segment() {
        let evaluator = create_test_evaluator();
        let parts = vec!["google".to_string()];
        let result =
            SymbolPath::parse_parts(evaluator.clone(), Some(&CubeId::cube("users")), &parts)
                .unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::Segment);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "google");
        assert_eq!(result.path(), &Vec::<CubeId>::new());
    }

    #[test]
    fn test_cache_name_without_granularity() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(evaluator.clone(), "users.count").unwrap();
        assert_eq!(result.cache_name(), "users.count");
    }

    #[test]
    fn test_cache_name_with_granularity() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(evaluator.clone(), "users.created_at.day").unwrap();
        assert_eq!(result.cache_name(), "users.created_at.day");
    }
}
