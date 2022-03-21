use sqlparser::{ast::Statement, dialect::Dialect, dialect::PostgreSqlDialect, parser::Parser};

use crate::{compile::CompilationError, sql::DatabaseProtocol};

use super::CompilationResult;

#[derive(Debug)]
pub struct MySqlDialectWithBackTicks {}

impl Dialect for MySqlDialectWithBackTicks {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://dev.mysql.com/doc/refman/8.0/en/identifiers.html.
        // We don't yet support identifiers beginning with numbers, as that
        // makes it hard to distinguish numeric literals.
        ('a'..='z').contains(&ch)
            || ('A'..='Z').contains(&ch)
            || ch == '_'
            || ch == '$'
            || ch == '@'
            || ('\u{0080}'..='\u{ffff}').contains(&ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ('0'..='9').contains(&ch)
    }
}

pub fn parse_sql_to_statement(
    query: &String,
    protocol: DatabaseProtocol,
) -> CompilationResult<Statement> {
    let parse_result = match protocol {
        DatabaseProtocol::MySQL => Parser::parse_sql(&MySqlDialectWithBackTicks {}, query.as_str()),
        DatabaseProtocol::PostgreSQL => Parser::parse_sql(&PostgreSqlDialect {}, query.as_str()),
    };

    match parse_result {
        Err(error) => Err(CompilationError::User(format!(
            "Unable to parse: {:?}",
            error
        ))),
        Ok(stmts) => {
            if stmts.len() == 1 {
                Ok(stmts[0].clone())
            } else if stmts.is_empty() {
                Err(CompilationError::User(format!(
                    "Invalid query, no statements was specified: {}",
                    &query
                )))
            } else {
                Err(CompilationError::Unsupported(format!(
                    "Multiple statements was specified in one query: {}",
                    &query
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_statements_mysql() {
        let result = parse_sql_to_statement(
            &"-- 6dcd92a04feb50f14bbcf07c661680ba SELECT NOW".to_string(),
            DatabaseProtocol::MySQL,
        );
        match result {
            Ok(_) => panic!("This test should throw an error"),
            Err(err) => assert_eq!(
                true,
                err.to_string()
                    .contains("Invalid query, no statements was specified")
            ),
        }
    }

    #[test]
    fn test_multiple_statements_mysql() {
        let result = parse_sql_to_statement(
            &"SELECT NOW(); SELECT NOW();".to_string(),
            DatabaseProtocol::MySQL,
        );
        match result {
            Ok(_) => panic!("This test should throw an error"),
            Err(err) => assert_eq!(
                true,
                err.to_string()
                    .contains("Multiple statements was specified in one query")
            ),
        }
    }

    #[test]
    fn test_single_line_comments_mysql() {
        let result = parse_sql_to_statement(
            &"-- 6dcd92a04feb50f14bbcf07c661680ba
            SELECT DATE(`createdAt`) AS __timestamp,
                   COUNT(*) AS count
            FROM db.`Orders`
            GROUP BY DATE(`createdAt`)
            ORDER BY count DESC
            LIMIT 10000
            -- 6dcd92a04feb50f14bbcf07c661680ba
        "
            .to_string(),
            DatabaseProtocol::MySQL,
        );
        match result {
            Ok(_) => {}
            Err(err) => panic!("{}", err),
        }
    }

    #[test]
    fn test_no_statements_postgres() {
        let result = parse_sql_to_statement(
            &"-- 6dcd92a04feb50f14bbcf07c661680ba SELECT NOW".to_string(),
            DatabaseProtocol::PostgreSQL,
        );
        match result {
            Ok(_) => panic!("This test should throw an error"),
            Err(err) => assert_eq!(
                true,
                err.to_string()
                    .contains("Invalid query, no statements was specified")
            ),
        }
    }

    #[test]
    fn test_multiple_statements_postgres() {
        let result = parse_sql_to_statement(
            &"SELECT NOW(); SELECT NOW();".to_string(),
            DatabaseProtocol::PostgreSQL,
        );
        match result {
            Ok(_) => panic!("This test should throw an error"),
            Err(err) => assert_eq!(
                true,
                err.to_string()
                    .contains("Multiple statements was specified in one query")
            ),
        }
    }

    #[test]
    fn test_single_line_comments_postgres() {
        let result = parse_sql_to_statement(
            &"-- 6dcd92a04feb50f14bbcf07c661680ba
            SELECT createdAt AS __timestamp,
                   COUNT(*) AS count
            FROM Orders
            GROUP BY createdAt
            ORDER BY count DESC
            LIMIT 10000
            -- 6dcd92a04feb50f14bbcf07c661680ba
        "
            .to_string(),
            DatabaseProtocol::PostgreSQL,
        );
        match result {
            Ok(_) => {}
            Err(err) => panic!("{}", err),
        }
    }
}
