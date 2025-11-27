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
    template: String,
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
            template: parsed_template,
            args,
            args_names,
        })
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
        Ok((
            SqlTemplate::String(self.template.clone()),
            self.args.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_path() {
        let mock = MockMemberSql::new("{CUBE.field}").unwrap();

        assert_eq!(mock.template, "{arg:0}");
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_multiple_paths() {
        let mock = MockMemberSql::new("{CUBE.field} / {other_cube.field}").unwrap();

        assert_eq!(mock.template, "{arg:0} / {arg:1}");
        assert_eq!(mock.args.symbol_paths.len(), 2);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(mock.args.symbol_paths[1], vec!["other_cube", "field"]);
        assert_eq!(mock.args_names, vec!["CUBE", "other_cube"]);
    }

    #[test]
    fn test_nested_path() {
        let mock = MockMemberSql::new("{other_cube.cube2.field}").unwrap();

        assert_eq!(mock.template, "{arg:0}");
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

        assert_eq!(mock.template, "{arg:0} / {arg:1} + {arg:2}");
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

        // UniqueVector should deduplicate, so both references use arg:0
        assert_eq!(mock.template, "{arg:0} + {arg:0}");
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field"]);
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_same_top_level_different_paths() {
        let mock = MockMemberSql::new("{CUBE.field1} + {CUBE.field2}").unwrap();

        assert_eq!(mock.template, "{arg:0} + {arg:1}");
        assert_eq!(mock.args.symbol_paths.len(), 2);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "field1"]);
        assert_eq!(mock.args.symbol_paths[1], vec!["CUBE", "field2"]);
        // Top-level arg name appears only once
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_with_text() {
        let mock = MockMemberSql::new("SUM({CUBE.amount}) * 100").unwrap();

        assert_eq!(mock.template, "SUM({arg:0}) * 100");
        assert_eq!(mock.args.symbol_paths.len(), 1);
        assert_eq!(mock.args.symbol_paths[0], vec!["CUBE", "amount"]);
        assert_eq!(mock.args_names, vec!["CUBE"]);
    }

    #[test]
    fn test_escaped_braces() {
        let mock = MockMemberSql::new("{{literal}} {CUBE.field}").unwrap();

        assert_eq!(mock.template, "{{literal}} {arg:0}");
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
}
