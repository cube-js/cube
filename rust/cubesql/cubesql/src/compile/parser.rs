use sqlparser::{
    ast::Statement,
    dialect::{Dialect, PostgreSqlDialect},
    parser::Parser,
};

use crate::{compile::CompilationError, sql::session::DatabaseProtocol};

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

lazy_static! {
    static ref SIGMA_WORKAROUND: regex::Regex = regex::Regex::new(r#"(?s)^\s*with\s+nsp\sas\s\(.*nspname\s=\s(?P<nspname>'[^']+'|\$\d+).*\),\s+tbl\sas\s\(.*relname\s=\s(?P<relname>'[^']+'|\$\d+).*\).*$"#).unwrap();
}

pub fn parse_sql_to_statements(
    query: &String,
    protocol: DatabaseProtocol,
) -> CompilationResult<Vec<Statement>> {
    log::debug!("Parsing SQL: {}", query);
    // @todo Support without workarounds
    // metabase
    let query = query.clone().replace("IF(TABLE_TYPE='BASE TABLE' or TABLE_TYPE='SYSTEM VERSIONED', 'TABLE', TABLE_TYPE) as TABLE_TYPE", "TABLE_TYPE");
    let query = query.replace("ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME", "");
    // @todo Implement CONVERT function
    let query = query.replace("CONVERT (CASE DATA_TYPE WHEN 'year' THEN NUMERIC_SCALE WHEN 'tinyint' THEN 0 ELSE NUMERIC_SCALE END, UNSIGNED INTEGER)", "0");
    // @todo problem with parser, space in types
    let query = query.replace("signed integer", "bigint");
    let query = query.replace("SIGNED INTEGER", "bigint");
    let query = query.replace("unsigned integer", "bigint");
    let query = query.replace("UNSIGNED INTEGER", "bigint");

    // DBEver
    let query = query.replace(
        "SELECT db.oid,db.* FROM pg_catalog.pg_database db",
        "SELECT db.oid as _oid,db.* FROM pg_catalog.pg_database db",
    );
    let query = query.replace(
        "SELECT t.oid,t.*,c.relkind",
        "SELECT t.oid as _oid,t.*,c.relkind",
    );
    let query = query.replace(
        "SELECT n.oid,n.*,d.description FROM",
        "SELECT n.oid as _oid,n.*,d.description FROM",
    );

    // TODO support these introspection Superset queries
    let query = query.replace(
        "(SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)\
\n                FROM pg_catalog.pg_attrdef d\
\n               WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum\
\n               AND a.atthasdef)\
\n              AS DEFAULT",
        "NULL AS DEFAULT",
    );

    let query = query.replace(
        "SELECT\
\n                  i.relname as relname,\
\n                  ix.indisunique, ix.indexprs, ix.indpred,\
\n                  a.attname, a.attnum, c.conrelid, ix.indkey::varchar,\
\n                  ix.indoption::varchar, i.reloptions, am.amname,\
\n                  ix.indnkeyatts as indnkeyatts\
\n              FROM\
\n                  pg_class t\
\n                        join pg_index ix on t.oid = ix.indrelid\
\n                        join pg_class i on i.oid = ix.indexrelid\
\n                        left outer join\
\n                            pg_attribute a\
\n                            on t.oid = a.attrelid and a.attnum = ANY(ix.indkey)\
\n                        left outer join\
\n                            pg_constraint c\
\n                            on (ix.indrelid = c.conrelid and\
\n                                ix.indexrelid = c.conindid and\
\n                                c.contype in ('p', 'u', 'x'))\
\n                        left outer join\
\n                            pg_am am\
\n                            on i.relam = am.oid\
\n              WHERE\
\n                  t.relkind IN ('r', 'v', 'f', 'm', 'p')",
        "SELECT\
\n                  i.relname as relname,\
\n                  ix.indisunique, ix.indexprs, ix.indpred,\
\n                  a.attname, a.attnum, c.conrelid, ix.indkey,\
\n                  ix.indoption, i.reloptions, am.amname,\
\n                  ix.indnkeyatts as indnkeyatts\
\n              FROM\
\n                  pg_class t\
\n                        join pg_index ix on t.oid = ix.indrelid\
\n                        join pg_class i on i.oid = ix.indexrelid\
\n                        left outer join\
\n                            pg_attribute a\
\n                            on t.oid = a.attrelid\
\n                        left outer join\
\n                            pg_constraint c\
\n                            on (ix.indrelid = c.conrelid and\
\n                                ix.indexrelid = c.conindid and\
\n                                c.contype in ('p', 'u', 'x'))\
\n                        left outer join\
\n                            pg_am am\
\n                            on i.relam = am.oid\
\n              WHERE\
\n                  t.relkind IN ('r', 'v', 'f', 'm', 'p')",
    );

    let query = query.replace(
        "and ix.indisprimary = 'f'\
\n              ORDER BY\
\n                  t.relname,\
\n                  i.relname",
        "and ix.indisprimary = false",
    );

    // TODO: Quick workaround for Tableau Desktop (ODBC), waiting for DF rebase...
    // Right now, our fork of DF doesn't support ON conditions with this filter
    let query = query.replace(
        "left outer join pg_attrdef d on a.atthasdef and",
        "left outer join pg_attrdef d on",
    );

    let query = query.replace("a.attnum = ANY(cons.conkey)", "1 = 1");
    let query = query.replace("pg_get_constraintdef(cons.oid) as src", "NULL as src");

    // ThoughtSpot (Redshift)
    // Subquery must have alias, It's a default Postgres behaviour, but Redshift is based on top of old Postgres version...
    let query = query.replace(
        "AS REF_GENERATION  FROM svv_tables) WHERE true  AND current_database() = ",
        "AS REF_GENERATION  FROM svv_tables) as r WHERE true  AND current_database() =",
    );

    // Sigma Computing WITH query workaround
    let query = match SIGMA_WORKAROUND.captures(&query) {
        Some(c) => {
            let nspname = c.name("nspname").unwrap().as_str();
            let relname = c.name("relname").unwrap().as_str();
            format!(
                "
                select
                    attname,
                    typname,
                    description
                from pg_attribute a
                join pg_type on atttypid = pg_type.oid
                left join pg_description on
                    attrelid = objoid and
                    attnum = objsubid
                join pg_catalog.pg_namespace nsp ON nspname = {}
                join pg_catalog.pg_class tbl ON relname = {} and relnamespace = nsp.oid
                where
                    attnum > 0 and
                    attrelid = tbl.oid
                order by attnum
                ;
                ",
                nspname, relname
            )
        }
        None => query,
    };

    let parse_result = match protocol {
        DatabaseProtocol::MySQL => Parser::parse_sql(&MySqlDialectWithBackTicks {}, query.as_str()),
        DatabaseProtocol::PostgreSQL => Parser::parse_sql(&PostgreSqlDialect {}, query.as_str()),
    };

    parse_result.map_err(|err| CompilationError::user(format!("Unable to parse: {:?}", err)))
}

pub fn parse_sql_to_statement(
    query: &String,
    protocol: DatabaseProtocol,
) -> CompilationResult<Statement> {
    match parse_sql_to_statements(query, protocol)? {
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

                Err(err)
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
