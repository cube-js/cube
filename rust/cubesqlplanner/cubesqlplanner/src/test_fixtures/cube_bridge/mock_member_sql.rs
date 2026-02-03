use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::member_sql::{MemberSql, SqlTemplate, SqlTemplateArgs};
use crate::cube_bridge::security_context::SecurityContext;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Mock implementation of MemberSql for testing
/// Parses template strings like "{CUBE.field} / {other_cube.field} + {revenue}"
/// and converts them to "{arg:0} / {arg:1} + {arg:2}" with extracted symbol paths
#[derive(Debug)]
pub struct MockMemberSql {
    template: SqlTemplate,
    args: SqlTemplateArgs,
    args_names: Vec<String>,
}

impl MockMemberSql {
    /// Create a new MockMemberSql from a template string
    /// Example: "{CUBE.field} / {other_cube.cube2.field} + {revenue}"
    pub fn new(template: impl Into<String>) -> Result<Self, CubeError> {
        let template_str = template.into();
        let (parsed_template, args, args_names) = Self::parse_template(&template_str)?;

        Ok(Self {
            template: SqlTemplate::String(parsed_template),
            args,
            args_names,
        })
    }

    /// Pre-aggregation single reference: "orders.created_at" → (orders) => orders.created_at → "orders.created_at"
    pub fn pre_agg_single_ref(member_path: impl Into<String>) -> Result<Self, CubeError> {
        let path = member_path.into();
        let path_parts: Vec<String> = path.split('.').map(|s| s.to_string()).collect();

        if path_parts.is_empty() || path_parts.iter().any(|p| p.is_empty()) {
            return Err(CubeError::user(format!("Invalid path in pre-aggregation: {}", path)));
        }

        let mut args = SqlTemplateArgs::default();
        let arg_name = path_parts[0].clone();
        let index = args.insert_symbol_path(path_parts);

        Ok(Self {
            template: SqlTemplate::String(format!("{{arg:{}}}", index)),
            args,
            args_names: vec![arg_name],
        })
    }

    /// Pre-aggregation array references: ["orders.status", "line_items.id"] → (orders, line_items) => [orders.status, line_items.id]
    pub fn pre_agg_array_refs(member_paths: Vec<impl Into<String>>) -> Result<Rc<Self>, CubeError> {
        let paths: Vec<String> = member_paths.into_iter().map(|p| p.into()).collect();

        if paths.is_empty() {
            return Err(CubeError::user("Pre-aggregation array references cannot be empty".to_string()));
        }

        let mut all_args = SqlTemplateArgs::default();
        let mut all_args_names = Vec::new();
        let mut template_elements = Vec::new();

        for path in &paths {
            let path_parts: Vec<String> = path.split('.').map(|s| s.to_string()).collect();

            if path_parts.is_empty() || path_parts.iter().any(|p| p.is_empty()) {
                return Err(CubeError::user(format!("Invalid path in pre-aggregation: {}", path)));
            }

            let arg_name = path_parts[0].clone();
            if !all_args_names.contains(&arg_name) {
                all_args_names.push(arg_name);
            }

            let index = all_args.insert_symbol_path(path_parts);
            template_elements.push(format!("{{arg:{}}}", index));
        }

        Ok(Rc::new(Self {
            template: SqlTemplate::StringVec(template_elements),
            args: all_args,
            args_names: all_args_names,
        }))
    }

    /// Parse the template string and extract symbol paths
    /// Converts "{path.to.symbol}" to "{arg:N}" and collects paths
    fn parse_template(template: &str) -> Result<(String, SqlTemplateArgs, Vec<String>), CubeError> {
        let mut result = String::new();
        let mut args = SqlTemplateArgs::default();
        let mut args_names = Vec::new();

        let mut chars = template.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '{' {
                // Check if it's an escaped brace
                if chars.peek() == Some(&'{') {
                    chars.next(); // consume second '{'
                    result.push_str("{{");
                    continue;
                }

                // Extract content until closing '}'
                let mut path = String::new();
                let mut found_closing = false;

                for ch in chars.by_ref() {
                    if ch == '}' {
                        found_closing = true;
                        break;
                    }
                    path.push(ch);
                }

                if !found_closing {
                    return Err(CubeError::user(format!(
                        "Unclosed brace in template: {}",
                        template
                    )));
                }

                // Parse the path and add to symbol_paths
                let path_parts: Vec<String> = path.split('.').map(|s| s.to_string()).collect();

                if path_parts.is_empty() || path_parts.iter().any(|p| p.is_empty()) {
                    return Err(CubeError::user(format!(
                        "Invalid path in template: {}",
                        path
                    )));
                }

                // Extract top-level arg name
                let arg_name = path_parts[0].clone();
                if !args_names.contains(&arg_name) {
                    args_names.push(arg_name);
                }

                let index = args.insert_symbol_path(path_parts);
                result.push_str(&format!("{{arg:{}}}", index));
            } else if ch == '}' {
                // Check if it's an escaped brace
                if chars.peek() == Some(&'}') {
                    chars.next(); // consume second '}'
                    result.push_str("}}");
                    continue;
                }
                result.push(ch);
            } else {
                result.push(ch);
            }
        }

        Ok((result, args, args_names))
    }
}

impl MemberSql for MockMemberSql {
    fn args_names(&self) -> &Vec<String> {
        &self.args_names
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }

    fn compile_template_sql(
        &self,
        _base_tools: Rc<dyn BaseTools>,
        _security_context: Rc<dyn SecurityContext>,
    ) -> Result<(SqlTemplate, SqlTemplateArgs), CubeError> {
        Ok((self.template.clone(), self.args.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_path() {
        let mock = MockMemberSql::new("{CUBE.field}").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{arg:0}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_multiple_paths() {
        let mock = MockMemberSql::new("{CUBE.field} / {other_cube.field}").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{arg:0} / {arg:1}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 2);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(mock.args.symbol_paths[1], vec!["other_cube", "field"]);
        assert_eq!(mock.args_names, vec!["CUBE", "other_cube"]);
    }

    #[test]
    fn test_nested_path() {
        let mock = MockMemberSql::new("{other_cube.cube2.field}").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{arg:0}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(
            mock.args.symbol_paths[0],
            vec!["other_cube", "cube2", "field"]
        );
        assert_eq!(mock.args_names, vec!["other_cube"]);
    }

    #[test]
    fn test_complex_expression() {
        let mock =
            MockMemberSql::new("{CUBE.field} / {other_cube.cube2.field} + {revenue}").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{arg:0} / {arg:1} + {arg:2}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 3);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(
            mock.args.symbol_paths[1],
            vec!["other_cube", "cube2", "field"]
        );
        assert_eq!(mock.args.symbol_paths[2], vec!["revenue"]);
        assert_eq!(mock.args_names, vec!["CUBE", "other_cube", "revenue"]);
    }

    #[test]
    fn test_duplicate_paths() {
        let mock = MockMemberSql::new("{CUBE.field} + {CUBE.field}").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{arg:0} + {arg:0}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_same_top_level_different_paths() {
        let mock = MockMemberSql::new("{CUBE.field1} + {CUBE.field2}").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{arg:0} + {arg:1}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 2);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field1"]);
        assert_eq!(mock.args.symbol_paths[1], vec!["CUBE", "field2"]);
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_with_text() {
        let mock = MockMemberSql::new("SUM({CUBE.amount}) * 100").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("SUM({arg:0}) * 100".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "amount"]);
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_escaped_braces() {
        let mock = MockMemberSql::new("{{literal}} {CUBE.field}").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{{literal}} {arg:0}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_unclosed_brace_error() {
        let result = MockMemberSql::new("{CUBE.field");

        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Unclosed brace"));
    }

    #[test]
    fn test_empty_path_error() {
        let result = MockMemberSql::new("{}");

        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Invalid path"));
    }

    #[test]
    fn test_compile_template_sql() {
        let mock = Rc::new(MockMemberSql::new("{CUBE.field} / {other.field}").unwrap());
        let (template, args) = mock
            .compile_template_sql(
                Rc::new(crate::test_fixtures::cube_bridge::MockBaseTools::default()),
                Rc::new(crate::test_fixtures::cube_bridge::MockSecurityContext),
            )
            .unwrap();

        match template {
            SqlTemplate::String(s) => {
                assert_eq!(s, "{arg:0} / {arg:1}");
            }
            _ => panic!("Expected String template"),
        }

        assert_eq!(args.symbol_paths.len(), 2);
        assert_eq!(args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(args.symbol_paths[1], vec!["other", "field"]);
    }

    #[test]
    fn test_pre_agg_single_ref() {
        let mock = MockMemberSql::pre_agg_single_ref("orders.created_at").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{arg:0}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["orders", "created_at"]);
        assert_eq!(mock.args_names, vec!["orders"]);
    }

    #[test]
    fn test_pre_agg_single_ref_with_joins() {
        let mock = MockMemberSql::pre_agg_single_ref("users.orders.created_at").unwrap();

        assert_eq!(mock.template, SqlTemplate::String("{arg:0}".to_string()));
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["users", "orders", "created_at"]);
        assert_eq!(mock.args_names, vec!["users"]);
    }

    #[test]
    fn test_pre_agg_array_refs_simple() {
        let mock = MockMemberSql::pre_agg_array_refs(vec![
            "orders.status",
            "orders.amount"
        ]).unwrap();

        assert_eq!(mock.template, SqlTemplate::StringVec(vec![
            "{arg:0}".to_string(),
            "{arg:1}".to_string()
        ]));
        assert_eq!(mock.args.symbol_paths.len(), 2);
        assert_eq!(mock.args.symbol_paths[0], vec!["orders", "status"]);
        assert_eq!(mock.args.symbol_paths[1], vec!["orders", "amount"]);
        assert_eq!(mock.args_names, vec!["orders"]);
    }

    #[test]
    fn test_pre_agg_array_refs_multiple_cubes() {
        let mock = MockMemberSql::pre_agg_array_refs(vec![
            "orders.status",
            "line_items.product_id",
            "orders.amount"
        ]).unwrap();

        assert_eq!(mock.template, SqlTemplate::StringVec(vec![
            "{arg:0}".to_string(),
            "{arg:1}".to_string(),
            "{arg:2}".to_string()
        ]));
        assert_eq!(mock.args.symbol_paths.len(), 3);
        assert_eq!(mock.args.symbol_paths[0], vec!["orders", "status"]);
        assert_eq!(mock.args.symbol_paths[1], vec!["line_items", "product_id"]);
        assert_eq!(mock.args.symbol_paths[2], vec!["orders", "amount"]);
        assert_eq!(mock.args_names, vec!["orders", "line_items"]);
    }

    #[test]
    fn test_pre_agg_array_refs_with_joins() {
        let mock = MockMemberSql::pre_agg_array_refs(vec![
            "visitors.aaa.dim_1",
            "visitors.bbb.dim2"
        ]).unwrap();

        assert_eq!(mock.template, SqlTemplate::StringVec(vec![
            "{arg:0}".to_string(),
            "{arg:1}".to_string()
        ]));
        assert_eq!(mock.args.symbol_paths.len(), 2);
        assert_eq!(mock.args.symbol_paths[0], vec!["visitors", "aaa", "dim_1"]);
        assert_eq!(mock.args.symbol_paths[1], vec!["visitors", "bbb", "dim2"]);
        assert_eq!(mock.args_names, vec!["visitors"]);
    }

    #[test]
    fn test_pre_agg_array_refs_empty_error() {
        let empty_vec: Vec<String> = vec![];
        let result = MockMemberSql::pre_agg_array_refs(empty_vec);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("cannot be empty"));
    }

    #[test]
    fn test_pre_agg_array_refs_compile_to_string_vec() {
        let mock = MockMemberSql::pre_agg_array_refs(vec![
            "orders.status",
            "line_items.product_id"
        ]).unwrap();

        let (template, args) = mock.compile_template_sql(
            Rc::new(crate::test_fixtures::cube_bridge::MockBaseTools::default()),
            Rc::new(crate::test_fixtures::cube_bridge::MockSecurityContext),
        ).unwrap();

        match template {
            SqlTemplate::StringVec(vec) => {
                assert_eq!(vec.len(), 2);
                assert_eq!(vec[0], "{arg:0}");
                assert_eq!(vec[1], "{arg:1}");
            }
            _ => panic!("Expected StringVec template"),
        }

        assert_eq!(args.symbol_paths.len(), 2);
        assert_eq!(args.symbol_paths[0], vec!["orders", "status"]);
        assert_eq!(args.symbol_paths[1], vec!["line_items", "product_id"]);
    }
}
