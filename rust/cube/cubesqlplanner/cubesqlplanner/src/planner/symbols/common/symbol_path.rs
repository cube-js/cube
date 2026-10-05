use std::hash::{Hash, Hasher};
use std::rc::Rc;

use cubenativeutils::CubeError;

use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::planner::{CubeId, MemberId, ModelCubes};

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

    /// Parses a dotted path string by walking the data model.
    pub fn parse(cubes: &ModelCubes, path: &str) -> Result<Self, CubeError> {
        let parts: Vec<String> = path.split('.').map(|s| s.to_string()).collect();
        Self::parse_parts(cubes, None, &parts)
    }

    /// Parses pre-split path segments resolved relative to
    /// `current_cube` — so the first segment may refer to a member
    /// of `current_cube` directly.
    pub fn parse_parts(
        cubes: &ModelCubes,
        current_cube: Option<&CubeId>,
        parts: &[String],
    ) -> Result<Self, CubeError> {
        Self::resolve_parts(cubes, current_cube, None, parts, vec![])
    }

    /// Like `parse_parts`, inside the ON sql of the join `joined` is reached
    /// through: there the joined cube's own name means `joined`.
    pub fn parse_parts_joined_to(
        cubes: &ModelCubes,
        current_cube: Option<&CubeId>,
        joined: Option<&CubeId>,
        parts: &[String],
    ) -> Result<Self, CubeError> {
        Self::resolve_parts(cubes, current_cube, joined, parts, vec![])
    }

    fn resolve_parts(
        cubes: &ModelCubes,
        current_cube: Option<&CubeId>,
        joined: Option<&CubeId>,
        parts: &[String],
        path: Vec<CubeId>,
    ) -> Result<Self, CubeError> {
        if parts.is_empty() {
            return Err(CubeError::user("Empty path".to_string()));
        }
        let evaluator = cubes.evaluator().clone();

        // Step 1: If current_cube set, try resolving parts[0] as member
        if let Some(cube_id) = current_cube {
            if let Some(result) =
                Self::try_resolve_as_member(evaluator.clone(), cube_id, parts, &path)?
            {
                return Ok(result);
            }
        }

        // Step 2: Try resolving parts[0] as cube reference
        let resolved = Self::resolve_cube_name(cubes, current_cube, joined, parts)?;

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
            return Self::resolve_parts(cubes, Some(&cube_id), None, &parts[1..], new_path);
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

    // Inside an instance the joined cube's own name means the instance and only
    // the cubes it joins directly are in reach; any join below an alias is an
    // instance. A cube's own name always means the cube itself.
    fn resolve_cube_name(
        cubes: &ModelCubes,
        current_cube: Option<&CubeId>,
        joined: Option<&CubeId>,
        parts: &[String],
    ) -> Result<Option<CubeId>, CubeError> {
        let name = parts[0].as_str();
        if matches!(name, "CUBE" | "TABLE") {
            return Ok(current_cube.cloned());
        }
        let own_name = current_cube.is_some_and(|current| current.target() == name);
        if let Some(joined) = joined.filter(|joined| joined.target() == name && !own_name) {
            return Ok(Some(joined.clone()));
        }
        if let Some(current) = current_cube {
            if current.is_joined() && current.target() == name {
                return Ok(Some(current.clone()));
            }
            if let Some(join) = cubes.find_join(current.target(), name)? {
                if join.alias().is_some() || current.is_joined() {
                    return Ok(Some(CubeId::joined(
                        current.clone(),
                        name,
                        join.name().clone(),
                    )));
                }
            }
            let is_table_ref = parts.get(1).is_some_and(|part| part == "__sql_fn");
            if current.is_joined()
                && !is_table_ref
                && cubes.evaluator().cube_exists(name.to_string())?
            {
                return Err(CubeError::user(format!(
                    "Cube `{}` is not joined to `{}`, so it can't be referenced from `{}`",
                    name,
                    current.target(),
                    current
                )));
            }
        }
        if cubes.evaluator().cube_exists(name.to_string())? {
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
            format!("{}.{}", self.cube_id, self.symbol_name)
        }
    }

    /// Identity of the member this path resolves to. Only a dimension,
    /// measure or segment path names a member.
    pub fn member_id(&self) -> Result<MemberId, CubeError> {
        if self.symbol_name.is_empty() {
            return Err(CubeError::user(format!(
                "Cube `{}` is not a member",
                self.cube_id
            )));
        }
        Ok(MemberId::member(
            self.cube_id.clone(),
            self.symbol_name.clone(),
        ))
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

    fn create_test_evaluator() -> Rc<ModelCubes> {
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
        ModelCubes::new(Rc::new(MockCubeEvaluator::new(schema)))
    }

    fn create_aliases_evaluator() -> Rc<ModelCubes> {
        let schema = MockSchema::from_yaml(indoc! {r#"
            cubes:
              - name: orders
                sql: SELECT * FROM orders
                joins:
                  - name: users
                    alias: customer
                    sql: "{CUBE}.customer_id = {users}.id"
                    relationship: many_to_one
                  - name: users
                    alias: manager
                    sql: "{CUBE}.manager_id = {users}.id"
                    relationship: many_to_one
                  - name: products
                    sql: "{CUBE}.product_id = {products}.id"
                    relationship: many_to_one
                dimensions:
                  - name: id
                    type: number
                    sql: id
              - name: users
                sql: SELECT * FROM users
                joins:
                  - name: departments
                    sql: "{CUBE}.department_id = {departments}.id"
                    relationship: many_to_one
                dimensions:
                  - name: id
                    type: number
                    sql: id
                  - name: city
                    type: string
                    sql: city
              - name: departments
                sql: SELECT * FROM departments
                dimensions:
                  - name: name
                    type: string
                    sql: name
              - name: products
                sql: SELECT * FROM products
                dimensions:
                  - name: name
                    type: string
                    sql: name
        "#})
        .unwrap();
        ModelCubes::new(Rc::new(MockCubeEvaluator::new(schema)))
    }

    fn parts(path: &str) -> Vec<String> {
        path.split('.').map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_parse_member_through_alias() {
        let cubes = create_aliases_evaluator();
        let result = SymbolPath::parse(&cubes, "orders.customer.city").unwrap();
        let orders = CubeId::cube("orders");
        let customer = CubeId::joined(orders.clone(), "customer", "users");
        assert_eq!(result.path_type(), &SymbolPathType::Dimension);
        assert_eq!(result.cube_id(), &customer);
        assert_eq!(result.path(), &vec![orders, customer]);
        assert_eq!(result.full_name(), "orders.customer.city");

        let manager = SymbolPath::parse(&cubes, "orders.manager.city").unwrap();
        assert_ne!(result, manager);
    }

    #[test]
    fn test_parse_member_below_alias() {
        let cubes = create_aliases_evaluator();
        let result = SymbolPath::parse(&cubes, "orders.manager.departments.name").unwrap();
        let manager = CubeId::joined(CubeId::cube("orders"), "manager", "users");
        let departments = CubeId::joined(manager.clone(), "departments", "departments");
        assert_eq!(result.cube_id(), &departments);
        assert_eq!(
            result.path(),
            &vec![CubeId::cube("orders"), manager, departments]
        );
        assert_eq!(result.full_name(), "orders.manager.departments.name");
    }

    #[test]
    fn test_parse_unaliased_join_keeps_the_cube() {
        let cubes = create_aliases_evaluator();
        let result = SymbolPath::parse(&cubes, "orders.products.name").unwrap();
        assert_eq!(result.cube_id(), &CubeId::cube("products"));
        assert_eq!(
            result.path(),
            &vec![CubeId::cube("orders"), CubeId::cube("products")]
        );
    }

    #[test]
    fn test_parse_inside_instance() {
        let cubes = create_aliases_evaluator();
        let customer = CubeId::joined(CubeId::cube("orders"), "customer", "users");

        for path in ["CUBE.city", "users.city", "city"] {
            let result = SymbolPath::parse_parts(&cubes, Some(&customer), &parts(path)).unwrap();
            assert_eq!(result.cube_id(), &customer, "{path}");
        }

        let result =
            SymbolPath::parse_parts(&cubes, Some(&customer), &parts("departments.name")).unwrap();
        assert_eq!(
            result.cube_id(),
            &CubeId::joined(customer.clone(), "departments", "departments")
        );

        let err =
            SymbolPath::parse_parts(&cubes, Some(&customer), &parts("products.name")).unwrap_err();
        assert!(
            err.message.contains("is not joined to `users`"),
            "{}",
            err.message
        );

        let result =
            SymbolPath::parse_parts(&cubes, Some(&customer), &parts("products.__sql_fn")).unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::CubeTable);
        assert_eq!(result.cube_id(), &CubeId::cube("products"));
    }

    #[test]
    fn test_parse_alias_from_declaring_cube() {
        let cubes = create_aliases_evaluator();
        let orders = CubeId::cube("orders");
        let result =
            SymbolPath::parse_parts(&cubes, Some(&orders), &parts("customer.city")).unwrap();
        assert_eq!(
            result.cube_id(),
            &CubeId::joined(orders.clone(), "customer", "users")
        );

        let result = SymbolPath::parse_parts(&cubes, Some(&orders), &parts("users")).unwrap();
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
    }

    #[test]
    fn test_parse_join_condition_of_instance() {
        let cubes = create_aliases_evaluator();
        let orders = CubeId::cube("orders");
        let customer = CubeId::joined(orders.clone(), "customer", "users");
        for path in ["users.id", "customer.id"] {
            let result = SymbolPath::parse_parts_joined_to(
                &cubes,
                Some(&orders),
                Some(&customer),
                &parts(path),
            )
            .unwrap();
            assert_eq!(result.cube_id(), &customer, "{path}");
        }
        let result = SymbolPath::parse_parts_joined_to(
            &cubes,
            Some(&orders),
            Some(&customer),
            &parts("CUBE"),
        )
        .unwrap();
        assert_eq!(result.cube_id(), &orders);
    }

    #[test]
    fn test_parse_bare_alias_is_not_a_cube() {
        let cubes = create_aliases_evaluator();
        let err = SymbolPath::parse(&cubes, "customer.city").unwrap_err();
        assert!(
            err.message.contains("Cannot resolve: customer"),
            "{}",
            err.message
        );
    }

    #[test]
    fn test_parse_simple_dimension() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(&evaluator, "users.created_at").unwrap();
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
        let result = SymbolPath::parse(&evaluator, "orders.users.created_at").unwrap();
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
        let result = SymbolPath::parse(&evaluator, "users.created_at.day").unwrap();
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
        let result = SymbolPath::parse(&evaluator, "orders.users.created_at.day").unwrap();
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
        let result = SymbolPath::parse(&evaluator, "users.count").unwrap();
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
        let result = SymbolPath::parse(&evaluator, "users.orders.total").unwrap();
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
            SymbolPath::parse_parts(&evaluator, Some(&CubeId::cube("users")), &parts).unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::CubeName);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
    }

    #[test]
    fn test_parse_parts_table_alias() {
        let evaluator = create_test_evaluator();
        let parts = vec!["TABLE".to_string()];
        let result =
            SymbolPath::parse_parts(&evaluator, Some(&CubeId::cube("users")), &parts).unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::CubeName);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
    }

    #[test]
    fn test_parse_parts_cube_table() {
        let evaluator = create_test_evaluator();
        let parts = vec!["CUBE".to_string(), "__sql_fn".to_string()];
        let result =
            SymbolPath::parse_parts(&evaluator, Some(&CubeId::cube("users")), &parts).unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::CubeTable);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.path(), &vec![CubeId::cube("users")]);
    }

    #[test]
    fn test_parse_parts_simple_member() {
        let evaluator = create_test_evaluator();
        let parts = vec!["source".to_string()];
        let result =
            SymbolPath::parse_parts(&evaluator, Some(&CubeId::cube("users")), &parts).unwrap();
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
            SymbolPath::parse_parts(&evaluator, Some(&CubeId::cube("users")), &parts).unwrap();
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
            SymbolPath::parse_parts(&evaluator, Some(&CubeId::cube("users")), &parts).unwrap();
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
            SymbolPath::parse_parts(&evaluator, Some(&CubeId::cube("users")), &parts).unwrap();
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
            SymbolPath::parse_parts(&evaluator, Some(&CubeId::cube("users")), &parts).unwrap();
        assert_eq!(result.path_type(), &SymbolPathType::Segment);
        assert_eq!(result.cube_id(), &CubeId::cube("users"));
        assert_eq!(result.symbol_name(), "google");
        assert_eq!(result.path(), &Vec::<CubeId>::new());
    }

    #[test]
    fn test_cache_name_without_granularity() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(&evaluator, "users.count").unwrap();
        assert_eq!(result.cache_name(), "users.count");
    }

    #[test]
    fn test_cache_name_with_granularity() {
        let evaluator = create_test_evaluator();
        let result = SymbolPath::parse(&evaluator, "users.created_at.day").unwrap();
        assert_eq!(result.cache_name(), "users.created_at.day");
    }
}
