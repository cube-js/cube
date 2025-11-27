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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub fn indent_by(text: &str, levels: usize) -> String {
    let indent = "  ".repeat(levels);
    indent_lines(text, &indent)
}

/// Indent multi-line string by 2 spaces (convenience function)
///
/// # Arguments
/// * `text` - The text to indent
///
#[allow(dead_code)]
pub fn indent(text: &str) -> String {
    indent_by(text, 1)
}
