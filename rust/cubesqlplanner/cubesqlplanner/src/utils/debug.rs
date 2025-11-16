/// Trait for generating human-readable SQL representation
pub trait DebugSql {
    /// Generate SQL string representation
    ///
    /// # Arguments
    /// * `expand_deps` - If true, recursively expand dependencies; if false, show dependency names
    fn debug_sql(&self, expand_deps: bool) -> String;
}

/// Helper function to indent multi-line strings with custom indentation
///
/// # Arguments
/// * `text` - The text to indent
/// * `indent` - The indentation string to prepend to each non-empty line
///
/// # Examples
/// ```
/// use cubesqlplanner::utils::debug::indent_lines;
///
/// let text = "line1\nline2\n\nline3";
/// let result = indent_lines(text, "  ");
/// assert_eq!(result, "  line1\n  line2\n\n  line3");
/// ```
pub fn indent_lines(text: &str, indent: &str) -> String {
    text.lines()
        .map(|line| {
            if line.trim().is_empty() {
                line.to_string()
            } else {
                format!("{}{}", indent, line)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Indent multi-line string by specified number of levels (each level = 2 spaces)
///
/// # Arguments
/// * `text` - The text to indent
/// * `levels` - Number of indentation levels (each level adds 2 spaces)
///
/// # Examples
/// ```
/// use cubesqlplanner::utils::debug::indent_by;
///
/// let text = "line1\nline2";
/// let result = indent_by(text, 2);
/// assert_eq!(result, "    line1\n    line2"); // 2 levels = 4 spaces
/// ```
pub fn indent_by(text: &str, levels: usize) -> String {
    let indent = "  ".repeat(levels);
    indent_lines(text, &indent)
}

/// Indent multi-line string by 2 spaces (convenience function)
///
/// # Arguments
/// * `text` - The text to indent
///
/// # Examples
/// ```
/// use cubesqlplanner::utils::debug::indent;
///
/// let text = "line1\nline2";
/// let result = indent(text);
/// assert_eq!(result, "  line1\n  line2");
/// ```
pub fn indent(text: &str) -> String {
    indent_by(text, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indent_lines_basic() {
        let text = "line1\nline2";
        let result = indent_lines(text, "  ");
        assert_eq!(result, "  line1\n  line2");
    }

    #[test]
    fn test_indent_lines_preserves_empty() {
        let text = "line1\n\nline2";
        let result = indent_lines(text, "  ");
        assert_eq!(result, "  line1\n\n  line2");
    }

    #[test]
    fn test_indent_lines_custom_indent() {
        let text = "line1\nline2";
        let result = indent_lines(text, ">>> ");
        assert_eq!(result, ">>> line1\n>>> line2");
    }

    #[test]
    fn test_indent_by_zero() {
        let text = "line1\nline2";
        let result = indent_by(text, 0);
        assert_eq!(result, "line1\nline2");
    }

    #[test]
    fn test_indent_by_one() {
        let text = "line1\nline2";
        let result = indent_by(text, 1);
        assert_eq!(result, "  line1\n  line2");
    }

    #[test]
    fn test_indent_by_multiple() {
        let text = "line1\nline2";
        let result = indent_by(text, 3);
        assert_eq!(result, "      line1\n      line2");
    }

    #[test]
    fn test_indent() {
        let text = "line1\nline2";
        let result = indent(text);
        assert_eq!(result, "  line1\n  line2");
    }

    #[test]
    fn test_indent_empty_lines() {
        let text = "line1\n\n\nline2";
        let result = indent(text);
        assert_eq!(result, "  line1\n\n\n  line2");
    }
}
