//! Options of the `COPY ... FROM STDIN` command.
//!
//! Only loading from STDIN into a temporary table is supported: cubes are a
//! read-only data source, so a temporary table is the only place data can go.
//! The options below mirror PostgreSQL semantics, including its defaults and
//! validation rules.

use crate::compile::{router::normalize_ident, CompilationError, CompilationResult};
use sqlparser::ast;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    pub format: CopyFormat,
    /// Column separator
    pub delimiter: char,
    /// Representation of a NULL value
    pub null_string: String,
    /// The data starts with a line of column names, which is discarded
    pub header: bool,
    /// Quoting character, CSV only
    pub quote: char,
    /// Character which escapes the quoting character inside a quoted value, CSV only
    pub escape: char,
    /// Columns whose values are never matched against the NULL representation, CSV only
    pub force_not_null: Vec<String>,
    /// Columns where a quoted value matching the NULL representation is still NULL, CSV only
    pub force_null: Vec<String>,
}

impl CopyOptions {
    pub fn new(format: CopyFormat) -> Self {
        let (delimiter, null_string) = match format {
            CopyFormat::Text => ('\t', "\\N".to_string()),
            CopyFormat::Csv => (',', "".to_string()),
        };

        Self {
            format,
            delimiter,
            null_string,
            header: false,
            quote: '"',
            escape: '"',
            force_not_null: vec![],
            force_null: vec![],
        }
    }

    /// Build options from both the modern `WITH (...)` and the legacy (pre-9.0) syntax.
    pub fn parse(
        options: &[ast::CopyOption],
        legacy_options: &[ast::CopyLegacyOption],
    ) -> CompilationResult<Self> {
        let mut collected = CollectedOptions::default();
        collected.collect(options)?;
        collected.collect_legacy(legacy_options)?;
        collected.finish()
    }
}

/// Raw options as they were specified, before defaults are applied. Defaults depend
/// on the format, which can be specified after any other option.
#[derive(Debug, Default)]
struct CollectedOptions {
    format: Option<CopyFormat>,
    delimiter: Option<char>,
    null_string: Option<String>,
    header: Option<bool>,
    quote: Option<char>,
    escape: Option<char>,
    force_not_null: Option<Vec<String>>,
    force_null: Option<Vec<String>>,
}

impl CollectedOptions {
    fn collect(&mut self, options: &[ast::CopyOption]) -> CompilationResult<()> {
        for option in options {
            match option {
                ast::CopyOption::Format(name) => {
                    self.format = Some(match name.value.to_lowercase().as_str() {
                        "text" => CopyFormat::Text,
                        "csv" => CopyFormat::Csv,
                        "binary" => {
                            return Err(CompilationError::unsupported(
                                "COPY BINARY format is not supported, use TEXT or CSV".to_string(),
                            ))
                        }
                        other => {
                            return Err(CompilationError::user(format!(
                                "COPY format \"{}\" not recognized",
                                other
                            )))
                        }
                    })
                }
                ast::CopyOption::Delimiter(delimiter) => self.delimiter = Some(*delimiter),
                ast::CopyOption::Null(null_string) => self.null_string = Some(null_string.clone()),
                ast::CopyOption::Header(header) => self.header = Some(*header),
                ast::CopyOption::Quote(quote) => self.quote = Some(*quote),
                ast::CopyOption::Escape(escape) => self.escape = Some(*escape),
                ast::CopyOption::ForceNotNull(columns) => {
                    self.force_not_null = Some(column_names(columns))
                }
                ast::CopyOption::ForceNull(columns) => {
                    self.force_null = Some(column_names(columns))
                }
                // FORCE_QUOTE applies to COPY TO, FREEZE to loading into a real table
                ast::CopyOption::ForceQuote(_) | ast::CopyOption::Freeze(_) => {
                    return Err(CompilationError::unsupported(format!(
                        "COPY option is not supported for COPY FROM: {}",
                        option
                    )))
                }
                ast::CopyOption::Encoding(encoding) => {
                    if !is_utf8_encoding(encoding) {
                        return Err(CompilationError::unsupported(format!(
                            "COPY ENCODING is only supported for UTF8, actual: {}",
                            encoding
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    fn collect_legacy(&mut self, options: &[ast::CopyLegacyOption]) -> CompilationResult<()> {
        for option in options {
            match option {
                ast::CopyLegacyOption::Delimiter(delimiter) => self.delimiter = Some(*delimiter),
                ast::CopyLegacyOption::Null(null_string) => {
                    self.null_string = Some(null_string.clone())
                }
                ast::CopyLegacyOption::Header => self.header = Some(true),
                ast::CopyLegacyOption::Csv(csv_options) => {
                    self.format = Some(CopyFormat::Csv);

                    for csv_option in csv_options {
                        match csv_option {
                            ast::CopyLegacyCsvOption::Header => self.header = Some(true),
                            ast::CopyLegacyCsvOption::Quote(quote) => self.quote = Some(*quote),
                            ast::CopyLegacyCsvOption::Escape(escape) => self.escape = Some(*escape),
                            ast::CopyLegacyCsvOption::ForceNotNull(columns) => {
                                self.force_not_null = Some(column_names(columns))
                            }
                            ast::CopyLegacyCsvOption::ForceQuote(_) => {
                                return Err(CompilationError::unsupported(format!(
                                    "COPY option is not supported for COPY FROM: {}",
                                    csv_option
                                )))
                            }
                        }
                    }
                }
                ast::CopyLegacyOption::Binary => {
                    return Err(CompilationError::unsupported(
                        "COPY BINARY format is not supported, use TEXT or CSV".to_string(),
                    ))
                }
                // Redshift-specific options: they describe loading from S3, which has
                // no meaning for COPY ... FROM STDIN
                other => {
                    return Err(CompilationError::unsupported(format!(
                        "COPY option is not supported: {}",
                        other
                    )))
                }
            }
        }

        Ok(())
    }

    fn finish(self) -> CompilationResult<CopyOptions> {
        let format = self.format.unwrap_or(CopyFormat::Text);
        let mut options = CopyOptions::new(format);

        // The parser works on bytes, and so does PostgreSQL
        for (name, char) in [
            ("delimiter", self.delimiter),
            ("quote", self.quote),
            ("escape", self.escape),
        ] {
            if let Some(char) = char {
                if !char.is_ascii() {
                    return Err(CompilationError::unsupported(format!(
                        "COPY {} must be a single one-byte character",
                        name
                    )));
                }
            }
        }

        if format != CopyFormat::Csv {
            for (name, specified) in [
                ("QUOTE", self.quote.is_some()),
                ("ESCAPE", self.escape.is_some()),
                ("FORCE_NOT_NULL", self.force_not_null.is_some()),
                ("FORCE_NULL", self.force_null.is_some()),
            ] {
                if specified {
                    return Err(CompilationError::user(format!(
                        "COPY {} available only in CSV mode",
                        name
                    )));
                }
            }
        }

        if let Some(delimiter) = self.delimiter {
            if delimiter == '\r' || delimiter == '\n' {
                return Err(CompilationError::user(
                    "COPY delimiter cannot be newline or carriage return".to_string(),
                ));
            }

            if delimiter == '\\' {
                return Err(CompilationError::user(
                    "COPY delimiter cannot be backslash".to_string(),
                ));
            }

            options.delimiter = delimiter;
        }

        if let Some(null_string) = self.null_string {
            if null_string.contains('\r') || null_string.contains('\n') {
                return Err(CompilationError::user(
                    "COPY null representation cannot use newline or carriage return".to_string(),
                ));
            }

            options.null_string = null_string;
        }

        if let Some(quote) = self.quote {
            options.quote = quote;
            // ESCAPE defaults to the quoting character
            options.escape = quote;
        }

        if let Some(escape) = self.escape {
            options.escape = escape;
        }

        if let Some(header) = self.header {
            options.header = header;
        }

        if let Some(force_not_null) = self.force_not_null {
            options.force_not_null = force_not_null;
        }

        if let Some(force_null) = self.force_null {
            options.force_null = force_null;
        }

        if options.null_string.contains(options.delimiter) {
            return Err(CompilationError::user(
                "COPY delimiter must not appear in the NULL specification".to_string(),
            ));
        }

        if format == CopyFormat::Csv {
            if options.delimiter == options.quote {
                return Err(CompilationError::user(
                    "COPY delimiter and quote must be different".to_string(),
                ));
            }

            if options.null_string.contains(options.quote) {
                return Err(CompilationError::user(
                    "CSV quote character must not appear in the NULL specification".to_string(),
                ));
            }
        }

        Ok(options)
    }
}

/// Column names of an option, folded the way the names of the table are, so that
/// FORCE_NOT_NULL (A) and a column declared as `a` are the same column.
fn column_names(columns: &[ast::Ident]) -> Vec<String> {
    columns.iter().map(normalize_ident).collect()
}

fn is_utf8_encoding(encoding: &str) -> bool {
    let encoding = encoding.to_lowercase().replace(['-', '_'], "");

    // The names PostgreSQL accepts for the encoding
    encoding == "utf8" || encoding == "unicode"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{parser::parse_sql_to_statement, DatabaseProtocol};

    fn parse_options(sql: &str) -> CompilationResult<CopyOptions> {
        let statement =
            parse_sql_to_statement(&sql.to_string(), DatabaseProtocol::PostgreSQL, &mut None)
                .expect("COPY statement must be parsed");

        match statement {
            ast::Statement::Copy {
                options,
                legacy_options,
                ..
            } => CopyOptions::parse(&options, &legacy_options),
            other => panic!("expected COPY statement, got: {}", other),
        }
    }

    fn parse_options_err(sql: &str) -> String {
        match parse_options(sql) {
            Ok(options) => panic!("expected an error, got options: {:?}", options),
            Err(err) => err.message(),
        }
    }

    #[test]
    fn test_text_defaults() {
        let options = parse_options("COPY t FROM STDIN").unwrap();

        assert_eq!(options, CopyOptions::new(CopyFormat::Text));
        assert_eq!(options.delimiter, '\t');
        assert_eq!(options.null_string, "\\N");
        assert!(!options.header);
    }

    #[test]
    fn test_csv_options() {
        let options = parse_options(
            "COPY t FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL 'nil', HEADER, QUOTE '~', FORCE_NOT_NULL (a, b))",
        )
        .unwrap();

        assert_eq!(options.format, CopyFormat::Csv);
        assert_eq!(options.delimiter, ';');
        assert_eq!(options.null_string, "nil");
        assert!(options.header);
        assert_eq!(options.quote, '~');
        // ESCAPE follows QUOTE unless specified
        assert_eq!(options.escape, '~');
        assert_eq!(
            options.force_not_null,
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn test_legacy_options() {
        let options = parse_options("COPY t FROM STDIN CSV HEADER QUOTE '~'").unwrap();

        assert_eq!(options.format, CopyFormat::Csv);
        assert!(options.header);
        assert_eq!(options.quote, '~');

        let options = parse_options("COPY t FROM STDIN DELIMITER '|'").unwrap();

        assert_eq!(options.format, CopyFormat::Text);
        assert_eq!(options.delimiter, '|');
    }

    #[test]
    fn test_option_validation() {
        assert_eq!(
            parse_options_err("COPY t FROM STDIN WITH (FORMAT parquet)"),
            "COPY format \"parquet\" not recognized"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN WITH (FORMAT binary)"),
            "COPY BINARY format is not supported, use TEXT or CSV"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN BINARY"),
            "COPY BINARY format is not supported, use TEXT or CSV"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN WITH (QUOTE '~')"),
            "COPY QUOTE available only in CSV mode"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN WITH (FORMAT csv, DELIMITER '\"')"),
            "COPY delimiter and quote must be different"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN WITH (DELIMITER '\\')"),
            "COPY delimiter cannot be backslash"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN WITH (NULL 'a\tb')"),
            "COPY delimiter must not appear in the NULL specification"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN WITH (FORCE_QUOTE (a))"),
            "COPY option is not supported for COPY FROM: FORCE_QUOTE (a)"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN WITH (ENCODING 'LATIN1')"),
            "COPY ENCODING is only supported for UTF8, actual: LATIN1"
        );
        assert_eq!(
            parse_options_err("COPY t FROM STDIN GZIP"),
            "COPY option is not supported: GZIP"
        );
    }
}
