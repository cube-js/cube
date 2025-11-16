/// Trait for generating human-readable SQL representation
pub trait DebugSql {
    /// Generate SQL string representation
    ///
    /// # Arguments
    /// * `expand_deps` - If true, recursively expand dependencies; if false, show dependency names
    fn debug_sql(&self, expand_deps: bool) -> String;
}

/// Helper function to indent multi-line strings
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
