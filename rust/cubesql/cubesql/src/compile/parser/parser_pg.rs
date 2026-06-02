use std::{collections::HashMap, sync::LazyLock};

use crate::compile::{qtrace::Qtrace, CompilationError, CompilationResult, DatabaseProtocol};
use regex::Regex;
use sqlparser::{
    ast::Statement,
    dialect::PostgreSqlDialect,
    parser::{Parser, ParserError},
    tokenizer::Tokenizer,
};

use super::sql_snippet;

static SIGMA_WORKAROUND: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?s)^\s*with\s+nsp\sas\s\(.*nspname\s=\s.*\),\s+tbl\sas\s\(.*relname\s=\s.*\).*select\s+attname.*from\spg_attribute.*$"#).unwrap()
});

pub fn parse_sql_to_statements(
    query: &str,
    protocol: DatabaseProtocol,
    qtrace: &mut Option<Qtrace>,
) -> CompilationResult<Vec<Statement>> {
    let original_query = query;

    log::debug!("Parsing SQL: {}", query);

    // DBeaver
    let query = query.replace(
        "LEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum and dep.classid = dep.refclassid",
        "LEFT OUTER JOIN pg_depend dep on dep.refobjid = a.attrelid AND dep.deptype = 'i' and dep.refobjsubid = a.attnum",
    );

    // TODO Superset introspection: LEFT JOIN by ANY() is not supported
    let query = query.replace(
        "on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)",
        "on t.oid = a.attrelid",
    );

    // TODO: Quick workaround for Tableau Desktop (ODBC), waiting for DF rebase...
    // LEFT JOIN by Boolean is not supported
    let query = query.replace(
        "left outer join pg_attrdef d on a.atthasdef and",
        "left outer join pg_attrdef d on",
    );

    // TODO: Likely for Superset with JOINs
    let query = query.replace("a.attnum = ANY(cons.conkey)", "1 = 1");
    let query = query.replace("pg_get_constraintdef(cons.oid) as src", "NULL as src");

    // ThoughtSpot (Redshift)
    // Subquery must have alias, It's a default Postgres behaviour, but Redshift is based on top of old Postgres version...
    let query = query.replace(
        // Subquery must have alias
        "AS REF_GENERATION  FROM svv_tables) WHERE true  AND current_database() = ",
        "AS REF_GENERATION  FROM svv_tables) as svv_tables WHERE current_database() =",
    );
    let query = query.replace(
        // Subquery must have alias
        // Incorrect alias for subquery
        "FROM (select lbv_cols.schemaname, lbv_cols.tablename, lbv_cols.columnname,REGEXP_REPLACE(REGEXP_REPLACE(lbv_cols.columntype,'\\\\(.*\\\\)'),'^_.+','ARRAY') as columntype_rep,columntype, lbv_cols.columnnum from pg_get_late_binding_view_cols() lbv_cols( schemaname name, tablename name, columnname name, columntype text, columnnum int)) lbv_columns   WHERE",
        "FROM (select schemaname, tablename, columnname,REGEXP_REPLACE(REGEXP_REPLACE(columntype,'\\\\(.*\\\\)'),'^_.+','ARRAY') as columntype_rep,columntype, columnnum from get_late_binding_view_cols_unpacked) as lbv_columns   WHERE",
    );
    let query = query.replace(
        // Subquery must have alias
        "ORDER BY TABLE_SCHEM,c.relname,attnum )  UNION ALL SELECT current_database()::VARCHAR(128) AS TABLE_CAT",
        "ORDER BY TABLE_SCHEM,c.relname,attnum ) as t  UNION ALL SELECT current_database()::VARCHAR(128) AS TABLE_CAT",
    );
    let query = query.replace(
        // Reusage of new column in another column
        "END AS IS_AUTOINCREMENT, IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN",
        "END AS IS_AUTOINCREMENT, false AS IS_GENERATEDCOLUMN",
    );

    // Sigma Computing WITH query workaround
    // TODO: remove workaround when subquery is supported in JOIN ON conditions
    let query = if SIGMA_WORKAROUND.is_match(&query) {
        static RELNAMESPACE_RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"(?s)from\spg_catalog\.pg_class\s+where\s+relname\s=\s(?P<relname>'(?:[^']|'')+'|\$\d+)\s+and\s+relnamespace\s=\s\(select\soid\sfrom\snsp\)"#).unwrap()
        });
        static ATTRELID_RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"(?s)left\sjoin\spg_description\son\s+attrelid\s=\sobjoid\sand\s+attnum\s=\sobjsubid\s+where\s+attnum\s>\s0\s+and\s+attrelid\s=\s\(select\soid\sfrom\stbl\)"#).unwrap()
        });

        let relnamespace_replaced = RELNAMESPACE_RE.replace(
            &query,
            "from pg_catalog.pg_class join nsp on relnamespace = nsp.oid where relname = $relname",
        );
        let attrelid_replaced = ATTRELID_RE.replace(&relnamespace_replaced, "left join pg_description on attrelid = objoid and attnum = objsubid join tbl on attrelid = tbl.oid where attnum > 0");
        attrelid_replaced.to_string()
    } else {
        query
    };

    // Holistics.io
    // TODO: Waiting for rebase DF
    // Right now, our fork of DF doesn't support ON conditions with this filter
    let query = query.replace(
        "ON c.conrelid=ta.attrelid AND ta.attnum=c.conkey[o.ord]",
        "ON c.conrelid=ta.attrelid",
    );

    // Holistics.io
    // TODO: Waiting for rebase DF
    // Right now, our fork of DF doesn't support ON conditions with this filter
    let query = query.replace(
        "ON c.confrelid=fa.attrelid AND fa.attnum=c.confkey[o.ord]",
        "ON c.confrelid=fa.attrelid",
    );

    // Grafana
    // TODO: PostgreSQL accepts any function in FROM as table, even scalars
    // string_to_array is *NOT* a UDTF! It returns one row of type list even in FROM!
    let query = query.replace(
        "WHERE quote_ident(table_schema) NOT IN ('information_schema', 'pg_catalog', '_timescaledb_cache', '_timescaledb_catalog', '_timescaledb_internal', '_timescaledb_config', 'timescaledb_information', 'timescaledb_experimental') AND table_type = 'BASE TABLE' AND quote_ident(table_schema) IN (SELECT CASE WHEN TRIM(s[i]) = '\"$user\"' THEN user ELSE TRIM(s[i]) END FROM generate_series(array_lower(string_to_array(current_setting('search_path'), ','), 1), array_upper(string_to_array(current_setting('search_path'), ','), 1)) AS i, string_to_array(current_setting('search_path'), ',') AS s)",
        "WHERE quote_ident(table_schema) IN (current_user, current_schema()) AND table_type = 'BASE TABLE'"
    );
    let query = query.replace(
        "where quote_ident(table_schema) not in ('information_schema',\
\n                             'pg_catalog',\
\n                             '_timescaledb_cache',\
\n                             '_timescaledb_catalog',\
\n                             '_timescaledb_internal',\
\n                             '_timescaledb_config',\
\n                             'timescaledb_information',\
\n                             'timescaledb_experimental')\
\n      and \
\n          quote_ident(table_schema) IN (\
\n          SELECT\
\n            CASE WHEN trim(s[i]) = '\"$user\"' THEN user ELSE trim(s[i]) END\
\n          FROM\
\n            generate_series(\
\n              array_lower(string_to_array(current_setting('search_path'),','),1),\
\n              array_upper(string_to_array(current_setting('search_path'),','),1)\
\n            ) as i,\
\n            string_to_array(current_setting('search_path'),',') s\
\n          )",
        "WHERE quote_ident(table_schema) IN (current_user, current_schema())",
    );

    // Work around an issue with lowercase table name when queried as uppercase,
    // an uncommon way of casting literals, and skip a few funcs along the way
    let query = query.replace(
        "(CASE\
        \n  WHEN c.reltuples < 0 THEN NULL\
        \n  WHEN c.relpages = 0 THEN float8 '0'\
        \n  ELSE c.reltuples / c.relpages END\
        \n  * (pg_relation_size(c.oid) / pg_catalog.current_setting('block_size')::int)\
        \n)::bigint",
        "NULL::bigint",
    );

    // Work around an aliasing issue (lowercase-uppercase). This is fine
    // since it must be equivalent in PostgreSQL
    let query = query.replace(
        "c.relname AS PARTITION_NAME,",
        "c.relname AS partition_name,",
    );

    // Quicksight workarounds
    // subquery must have an alias
    let query = query.replace(
        "ORDER BY nspname,c.relname,attnum  ) UNION ALL",
        "ORDER BY nspname,c.relname,attnum  ) _internal_unaliased_1 UNION ALL",
    );
    // SELECT expression referencing column aliased above
    let query = query.replace(
        "AS IS_AUTOINCREMENT, IS_AUTOINCREMENT AS IS_GENERATEDCOLUMN",
        "AS IS_AUTOINCREMENT, 'NO' AS IS_GENERATEDCOLUMN",
    );
    // WHERE expressions referencing SELECT aliases
    let query = {
        static WHERE_TABLE_SCHEMA_NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"\slbv_columns\s+WHERE\s+table_schema\sLIKE\s(?P<tableschema>[^\s]+)\s+AND\stable_name\sLIKE\s(?P<tablename>[^\s]+)\s*$"#).unwrap()
        });
        WHERE_TABLE_SCHEMA_NAME_RE
            .replace(
                &query,
                " lbv_columns WHERE schemaname LIKE $tableschema AND tablename LIKE $tablename",
            )
            .to_string()
    };

    // DataGrip CTID workaround
    let query = query.replace("SELECT t.*, CTID\nFROM ", "SELECT t.*, NULL AS ctid\nFROM ");

    // Talend pg_encoding_to_char workaround
    let query = query.replace(
        "SELECT pg_encoding_to_char(encoding) FROM pg_database",
        "SELECT pg_encoding_to_char(encoding) AS pg_encoding_to_char FROM pg_database",
    );

    if let Some(qtrace) = qtrace {
        qtrace.set_replaced_query(&query)
    }

    let dialect = match protocol {
        DatabaseProtocol::PostgreSQL => PostgreSqlDialect {},
        DatabaseProtocol::Extension(_) => unimplemented!(),
    };

    let tokens = match Tokenizer::new(&dialect, query.as_str()).tokenize_with_location() {
        Ok(d) => d,
        Err(err) => {
            let mut message = format!("Unable to parse: {}", err);

            let snippet = sql_snippet::snippet_for_tokenizer_error(query.as_str(), &err);
            if let Some(snippet) = snippet {
                message.push_str("\n\n");
                message.push_str(&snippet);
            }

            return Err(CompilationError::SqlParser(
                message,
                Some(HashMap::from([(
                    "query".to_string(),
                    original_query.to_string(),
                )])),
            ));
        }
    };

    let parse_result = Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements();

    parse_result.map_err(|err| {
        // We don't need a prefix "sql parser error: "
        let body = match &err {
            ParserError::TokenizerError(message) | ParserError::ParserError(message) => {
                message.as_str()
            }
            ParserError::RecursionLimitExceeded => &"Recursion limit exceeded",
        };
        let mut message = format!("Unable to parse: {}", body);

        // The parser consumed the tokens above, so re-tokenize
        let snippet = Tokenizer::new(&dialect, query.as_str())
            .tokenize_with_location()
            .ok()
            .and_then(|tokens| {
                sql_snippet::snippet_for_parser_error(query.as_str(), &tokens, &err.to_string())
            });

        if let Some(snippet) = snippet {
            message.push_str("\n\n");
            message.push_str(&snippet);
        }

        CompilationError::SqlParser(
            message,
            Some(HashMap::from([(
                "query".to_string(),
                original_query.to_string(),
            )])),
        )
    })
}

pub fn parse_sql_to_statement(
    query: &str,
    protocol: DatabaseProtocol,
    qtrace: &mut Option<Qtrace>,
) -> CompilationResult<Statement> {
    match parse_sql_to_statements(query, protocol, qtrace)? {
        stmts => {
            if stmts.len() == 1 {
                Ok(stmts[0].clone())
            } else {
                let err = if stmts.is_empty() {
                    CompilationError::user(format!(
                        "Invalid query, no statements was specified: {}",
                        &query
                    ))
                } else {
                    CompilationError::unsupported(format!(
                        "Multiple statements was specified in one query: {}",
                        &query
                    ))
                };

                Err(err.with_meta(Some(HashMap::from([(
                    "query".to_string(),
                    query.to_string(),
                )]))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_statements_postgres() {
        let result = parse_sql_to_statement(
            &"-- 6dcd92a04feb50f14bbcf07c661680ba SELECT NOW".to_string(),
            DatabaseProtocol::PostgreSQL,
            &mut None,
        );
        match result {
            Ok(_) => panic!("This test should throw an error"),
            Err(err) => assert!(err
                .to_string()
                .contains("Invalid query, no statements was specified")),
        }
    }

    fn parse_err(sql: &str) -> String {
        match parse_sql_to_statement(&sql.to_string(), DatabaseProtocol::PostgreSQL, &mut None) {
            Ok(_) => panic!("expected a parse error for: {}", sql),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn test_syntax_error_in_large_query_stays_within_context_window() {
        // The error is buried ~300 lines into a ~500-line query; the snippet must
        // stay bounded to the error line plus two leading context lines and never
        // dump the whole query (note the ~200 trailing lines the parser, stopping
        // at the first error, never even reaches).
        let sql = {
            let mut sql = String::from("SELECT\n");

            for col in 2..=300u64 {
                sql.push_str(&format!("    col{},\n", col));
            }

            sql.push_str("    status MEASURE(orders.count)\n");

            for col in 302..=499u64 {
                sql.push_str(&format!("    col{},\n", col));
            }

            sql.push_str("FROM orders");

            sql
        };

        assert_eq!(
            parse_err(&sql),
            "SQL Parser Error: Unable to parse: \
Expected: end of statement, found: ( at Line: 301, Column: 19\n\
\n\
299 |     col299,
300 |     col300,
301 |     status MEASURE(orders.count)
    |            ^"
        );
    }

    #[test]
    fn test_syntax_error_in_single_wide_line_is_truncated() {
        // One very wide line, far past the 64-char limit. The snippet crops to a
        // horizontal window centered on the caret, trimming both sides.
        let sql = format!(
            "SELECT {a} MEASURE(orders.count) {z} FROM orders",
            a = "a".repeat(100),
            z = "z".repeat(100),
        );
        assert_eq!(
            parse_err(&sql),
            "SQL Parser Error: Unable to parse: \
Expected: end of statement, found: ( at Line: 1, Column: 116\n\
\n\
1 | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa MEASURE(orders.count) zzzzzzzzzz
  |                                 ^"
        );
    }

    #[test]
    fn test_syntax_error_in_wide_line_truncates_right_only() {
        // `status MEASURE(...)` makes the `(` the real error; a long trailing run
        // pushes the line past the limit while the caret stays near the start, so
        // only the right side is cropped.
        let sql = format!(
            "SELECT status MEASURE(orders.count) {z}",
            z = "z".repeat(300)
        );
        assert_eq!(
            parse_err(&sql),
            "SQL Parser Error: Unable to parse: \
Expected: end of statement, found: ( at Line: 1, Column: 22\n\
\n\
1 | SELECT status MEASURE(orders.count) zzzzzzzzzzzzzzzzzzzzzzzzzzzz
  |               ^"
        );
    }

    #[test]
    fn test_syntax_error_on_wide_numeric_token() {
        // A number cannot be an alias, so the long numeric literal after
        // `MEASURE(orders.count)` is itself the offending token. The huge digit run
        // in the error message must not confuse `at Line:/Column:` location parsing.
        let sql = format!(
            "SELECT MEASURE(orders.count) {n} FROM orders",
            n = "1".repeat(100)
        );
        assert_eq!(
            parse_err(&sql),
            "SQL Parser Error: Unable to parse: \
Expected: end of statement, found: 1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111 at Line: 1, Column: 30\n\
\n\
1 | SELECT MEASURE(orders.count) 11111111111111111111111111111111111
  |                              ^"
        );
    }

    #[test]
    fn test_multiple_statements_postgres() {
        let result = parse_sql_to_statement(
            &"SELECT NOW(); SELECT NOW();".to_string(),
            DatabaseProtocol::PostgreSQL,
            &mut None,
        );
        match result {
            Ok(_) => panic!("This test should throw an error"),
            Err(err) => assert!(err
                .to_string()
                .contains("Multiple statements was specified in one query")),
        }
    }

    #[test]
    fn test_syntax_error_missing_comma_postgres() {
        // Missing comma between the two projection items (`status` and `MEASURE(...)`).
        // The parser stops on the `(` (column 24) after parsing the un-separated
        // `MEASURE` as an alias, but the snippet caret snaps back to the start of
        // the offending `MEASURE` token.
        assert_eq!(
            parse_err(
                "SELECT DISTINCT
                orders_transactions.status
                MEASURE(orders_transactions.count)
            FROM
                orders_transactions
            GROUP BY
                1
            LIMIT
                5000"
            ),
            "SQL Parser Error: Unable to parse: \
Expected: end of statement, found: ( at Line: 3, Column: 24
\n\
1 | SELECT DISTINCT
2 |                 orders_transactions.status
3 |                 MEASURE(orders_transactions.count)
  |                 ^"
        );
    }

    #[test]
    fn test_syntax_error_dangling_from_postgres() {
        // The parser reports no location for an unexpected EOF, so the caret is
        // anchored at the end of the last token (`FROM`).
        assert_eq!(
            parse_err("SELECT FROM"),
            "SQL Parser Error: Unable to parse: \
Expected: identifier, found: EOF\n\
\n\
1 | SELECT FROM
  |           ^"
        );
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
            &mut None,
        );
        match result {
            Ok(_) => {}
            Err(err) => panic!("{}", err),
        }
    }

    #[test]
    fn test_syntax_error_trailing_comma() {
        // Dangling comma in the projection list: caret points at the `FROM` that
        // the parser found where it expected another expression.
        assert_eq!(
            parse_err("SELECT a, FROM t"),
            "SQL Parser Error: Unable to parse: \
Expected an expression, found: FROM at Line: 1, Column: 16\n\
\n\
1 | SELECT a, FROM t
  |                ^"
        );
    }

    #[test]
    fn test_syntax_error_double_comma() {
        // The extra comma is the offending token; it is preceded by another
        // comma (not a word), so the caret stays on it.
        assert_eq!(
            parse_err("SELECT a,, b FROM t"),
            "SQL Parser Error: Unable to parse: \
Expected: an expression, found: , at Line: 1, Column: 10\n\
\n\
1 | SELECT a,, b FROM t
  |          ^"
        );
    }

    #[test]
    fn test_syntax_error_misspelled_keyword() {
        assert_eq!(
            parse_err("SELET a FROM t"),
            "SQL Parser Error: Unable to parse: \
Expected: an SQL statement, found: SELET at Line: 1, Column: 1\n\
\n\
1 | SELET a FROM t
  | ^"
        );
    }

    #[test]
    fn test_syntax_error_unclosed_paren() {
        assert_eq!(
            parse_err("SELECT count(a FROM t"),
            "SQL Parser Error: Unable to parse: \
Expected: ), found: FROM at Line: 1, Column: 16\n\
\n\
1 | SELECT count(a FROM t
  |                ^"
        );
    }

    #[test]
    fn test_syntax_error_where_without_expr() {
        // Trailing `WHERE` hits EOF; the caret is anchored at the end of `WHERE`.
        assert_eq!(
            parse_err("SELECT a FROM t WHERE"),
            "SQL Parser Error: Unable to parse: \
Expected: an expression, found: EOF\n\
\n\
1 | SELECT a FROM t WHERE
  |                     ^"
        );
    }

    #[test]
    fn test_syntax_error_unterminated_string() {
        // The whole visible literal `'abc` is underlined, not just the quote.
        assert_eq!(
            parse_err("SELECT 'abc FROM t"),
            "SQL Parser Error: Unable to parse: \
Unterminated string literal at Line: 1, Column: 8\n\
\n\
1 | SELECT 'abc FROM t
  |        ^^^^"
        );
    }

    #[test]
    fn test_syntax_error_dangling_from_semicolon() {
        // `SELECT FROM;` stops on the `;`; the caret sits on the end of `FROM`.
        assert_eq!(
            parse_err("SELECT FROM;"),
            "SQL Parser Error: Unable to parse: \
Expected: identifier, found: ; at Line: 1, Column: 12\n\
\n\
1 | SELECT FROM;
  |           ^"
        );
    }

    #[test]
    fn test_syntax_error_group_by_without_expr() {
        assert_eq!(
            parse_err("SELECT a FROM t GROUP BY"),
            "SQL Parser Error: Unable to parse: \
Expected: an expression, found: EOF\n\
\n\
1 | SELECT a FROM t GROUP BY
  |                        ^"
        );
    }
}
