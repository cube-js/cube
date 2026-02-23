use std::rc::Rc;

use cubenativeutils::CubeError;

use crate::cube_bridge::evaluator::CubeEvaluator;

#[derive(Debug, Clone)]
pub enum SymbolPathType {
    Dimension,
    Measure,
    Segment,
}

#[derive(Debug, Clone)]
pub struct SymbolPath {
    path_type: SymbolPathType,
    path: Vec<String>,
    cube_name: String,
    symbol_name: String,
    full_name: String,
    granularity: Option<String>,
}

impl SymbolPath {
    fn new(
        path_type: SymbolPathType,
        path: Vec<String>,
        cube_name: String,
        symbol_name: String,
        granularity: Option<String>,
    ) -> Self {
        let full_name = format!("{}.{}", cube_name, symbol_name);
        Self {
            path_type,
            path,
            cube_name,
            symbol_name,
            full_name,
            granularity,
        }
    }

    pub fn parse(cube_evaluator: Rc<dyn CubeEvaluator>, path: &str) -> Result<Self, CubeError> {
        let parts = path.split(".").collect::<Vec<&str>>();
        if parts.len() < 2 {
            return Err(CubeError::user(format!("Invalid symbol path: {}", path)));
        }

        if let Some(dim_path) =
            Self::try_parse_as_dimension_with_granularity(cube_evaluator.clone(), &parts)?
        {
            return Ok(dim_path);
        }

        let path_to_check = vec![
            parts[parts.len() - 2].to_string(),
            parts[parts.len() - 1].to_string(),
        ];

        let path_type = if cube_evaluator.is_dimension(path_to_check.clone())? {
            SymbolPathType::Dimension
        } else if cube_evaluator.is_measure(path_to_check.clone())? {
            SymbolPathType::Measure
        } else if cube_evaluator.is_segment(path_to_check.clone())? {
            SymbolPathType::Segment
        } else {
            return Err(CubeError::user(format!(
                "Symbol path doesn't refer to a dimension, measure or segment: {}",
                path
            )));
        };

        let path = parts[0..parts.len() - 2]
            .iter()
            .map(|s| s.to_string())
            .collect();
        Ok(Self::new(
            path_type,
            path,
            path_to_check[0].clone(),
            path_to_check[1].clone(),
            None,
        ))
    }

    pub fn path_type(&self) -> &SymbolPathType {
        &self.path_type
    }

    pub fn path(&self) -> &Vec<String> {
        &self.path
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    pub fn symbol_name(&self) -> &String {
        &self.symbol_name
    }

    pub fn full_name(&self) -> &String {
        &self.full_name
    }

    pub fn cache_name(&self) -> String {
        if let Some(granularity) = &self.granularity {
            format!("{}.{}", self.full_name, granularity)
        } else {
            self.full_name.clone()
        }
    }

    pub fn granularity(&self) -> &Option<String> {
        &self.granularity
    }

    fn try_parse_as_dimension_with_granularity(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        parts: &[&str],
    ) -> Result<Option<Self>, CubeError> {
        if parts.len() > 2 {
            let path_to_check = vec![
                parts[parts.len() - 3].to_string(),
                parts[parts.len() - 2].to_string(),
            ];
            if cube_evaluator.is_dimension(path_to_check.clone())? {
                let path_type = SymbolPathType::Dimension;
                let path = parts[0..parts.len() - 3]
                    .iter()
                    .map(|s| s.to_string())
                    .collect();

                let granularity = Some(parts[parts.len() - 1].to_string());
                return Ok(Some(Self::new(
                    path_type,
                    path,
                    path_to_check[0].clone(),
                    path_to_check[1].clone(),
                    granularity,
                )));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::cube_bridge::{MockCubeEvaluator, MockSchema};
    use indoc::indoc;

    #[test]
    fn test_symbol_path_parsing() {
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
                measures:
                  - name: count
                    type: count
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

        let evaluator = Rc::new(MockCubeEvaluator::new(schema));

        let result = SymbolPath::parse(evaluator.clone(), "users.created_at").unwrap();
        assert_eq!(result.cube_name(), "users");
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.full_name(), "users.created_at");
        assert_eq!(result.path(), &Vec::<String>::new());
        assert_eq!(result.granularity(), &None);
        assert!(matches!(result.path_type, SymbolPathType::Dimension));

        let result = SymbolPath::parse(evaluator.clone(), "orders.users.created_at").unwrap();
        assert_eq!(result.cube_name(), "users");
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.full_name(), "users.created_at");
        assert_eq!(result.path(), &vec!["orders".to_string()]);
        assert_eq!(result.granularity(), &None);
        assert!(matches!(result.path_type, SymbolPathType::Dimension));

        let result = SymbolPath::parse(evaluator.clone(), "users.created_at.day").unwrap();
        assert_eq!(result.cube_name(), "users");
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.full_name(), "users.created_at");
        assert_eq!(result.path(), &Vec::<String>::new());
        assert_eq!(result.granularity(), &Some("day".to_string()));
        assert!(matches!(result.path_type, SymbolPathType::Dimension));

        let result = SymbolPath::parse(evaluator.clone(), "orders.users.created_at.day").unwrap();
        assert_eq!(result.cube_name(), "users");
        assert_eq!(result.symbol_name(), "created_at");
        assert_eq!(result.full_name(), "users.created_at");
        assert_eq!(result.path(), &vec!["orders".to_string()]);
        assert_eq!(result.granularity(), &Some("day".to_string()));
        assert!(matches!(result.path_type, SymbolPathType::Dimension));

        let result = SymbolPath::parse(evaluator.clone(), "users.count").unwrap();
        assert_eq!(result.cube_name(), "users");
        assert_eq!(result.symbol_name(), "count");
        assert_eq!(result.full_name(), "users.count");
        assert_eq!(result.path(), &Vec::<String>::new());
        assert_eq!(result.granularity(), &None);
        assert!(matches!(result.path_type, SymbolPathType::Measure));

        let result = SymbolPath::parse(evaluator.clone(), "users.orders.total").unwrap();
        assert_eq!(result.cube_name(), "orders");
        assert_eq!(result.symbol_name(), "total");
        assert_eq!(result.full_name(), "orders.total");
        assert_eq!(result.path(), &vec!["users".to_string()]);
        assert_eq!(result.granularity(), &None);
        assert!(matches!(result.path_type, SymbolPathType::Measure));
    }
}
