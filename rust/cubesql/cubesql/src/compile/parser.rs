use std::{collections::HashMap, sync::LazyLock};

use regex::Regex;
use sqlparser::{
    ast::Statement,
    dialect::{Dialect, PostgreSqlDialect},
    parser::Parser,
};

use super::{qtrace::Qtrace, CompilationError, DatabaseProtocol};

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
        ch.is_ascii_lowercase()
            || ch.is_ascii_uppercase()
            || ch == '_'
            || ch == '$'
            || ch == '@'
            || ('\u{0080}'..='\u{ffff}').contains(&ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || ch.is_ascii_digit()
    }
}

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
    // @todo Support without workarounds
    // metabase
    let query = query.replace("IF(TABLE_TYPE='BASE TABLE' or TABLE_TYPE='SYSTEM VERSIONED', 'TABLE', TABLE_TYPE) as TABLE_TYPE", "TABLE_TYPE");
    let query = query.replace("ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME", "");
    // @todo Implement CONVERT function
    let query = query.replace("CONVERT (CASE DATA_TYPE WHEN 'year' THEN NUMERIC_SCALE WHEN 'tinyint' THEN 0 ELSE NUMERIC_SCALE END, UNSIGNED INTEGER)", "0");
    // @todo problem with parser, space in types
    let query = query.replace("signed integer", "bigint");
    let query = query.replace("SIGNED INTEGER", "bigint");
    let query = query.replace("unsigned integer", "bigint");
    let query = query.replace("UNSIGNED INTEGER", "bigint");

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

    if let Some(qtrace) = qtrace {
        qtrace.set_replaced_query(&query)
    }

    let parse_result = match protocol {
        DatabaseProtocol::MySQL => Parser::parse_sql(&MySqlDialectWithBackTicks {}, query.as_str()),
        DatabaseProtocol::PostgreSQL => Parser::parse_sql(&PostgreSqlDialect {}, query.as_str()),
        DatabaseProtocol::Extension(_) => unimplemented!(),
    };

    parse_result.map_err(|err| {
        CompilationError::user(format!("Unable to parse: {:?}", err)).with_meta(Some(
            HashMap::from([("query".to_string(), original_query.to_string())]),
        ))
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
    fn test_no_statements_mysql() {
        let result = parse_sql_to_statement(
            &"-- 6dcd92a04feb50f14bbcf07c661680ba SELECT NOW".to_string(),
            DatabaseProtocol::MySQL,
            &mut None,
        );
        match result {
            Ok(_) => panic!("This test should throw an error"),
            Err(err) => assert!(err
                .to_string()
                .contains("Invalid query, no statements was specified")),
        }
    }

    #[test]
    fn test_multiple_statements_mysql() {
        let result = parse_sql_to_statement(
            &"SELECT NOW(); SELECT NOW();".to_string(),
            DatabaseProtocol::MySQL,
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
            &mut None,
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
            &mut None,
        );
        match result {
            Ok(_) => panic!("This test should throw an error"),
            Err(err) => assert!(err
                .to_string()
                .contains("Invalid query, no statements was specified")),
        }
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
}
