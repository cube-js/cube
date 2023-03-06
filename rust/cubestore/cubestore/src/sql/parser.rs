use crate::cachestore::QueueItemStatus;
use sqlparser::ast::{
    HiveDistributionStyle, Ident, ObjectName, Query, SqlOption, Statement as SQLStatement, Value,
};
use sqlparser::dialect::keywords::Keyword;
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Debug)]
pub struct MySqlDialectWithBackTicks {}

impl Dialect for MySqlDialectWithBackTicks {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"' || ch == '`'
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || ch == '_'
            || ch == '$'
            || (ch >= '\u{0080}' && ch <= '\u{ffff}')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.is_identifier_start(ch) || (ch >= '0' && ch <= '9')
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedIndexRef {
    pub name: ObjectName,
    pub columns: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Statement(SQLStatement),
    CreateTable {
        create_table: SQLStatement,
        partitioned_index: Option<PartitionedIndexRef>,
        indexes: Vec<SQLStatement>,
        locations: Option<Vec<String>>,
        unique_key: Option<Vec<Ident>>,
        aggregates: Option<Vec<(Ident, Ident)>>,
    },
    CreateSchema {
        schema_name: ObjectName,
        if_not_exists: bool,
    },
    CreateSource {
        name: Ident,
        source_type: String,
        credentials: Vec<SqlOption>,
        or_update: bool,
    },
    Cache(CacheCommand),
    Queue(QueueCommand),
    System(SystemCommand),
    Dump(Box<Query>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RocksStoreName {
    Meta,
    Cache,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CacheCommand {
    Set {
        key: Ident,
        value: String,
        ttl: Option<u32>,
        nx: bool,
    },
    Get {
        key: Ident,
    },
    Keys {
        prefix: Ident,
    },
    Remove {
        key: Ident,
    },
    Truncate {},
    Incr {
        path: Ident,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueueCommand {
    Add {
        priority: i64,
        orphaned: Option<u32>,
        key: Ident,
        value: String,
    },
    Get {
        key: Ident,
    },
    ToCancel {
        prefix: Ident,
        heartbeat_timeout: Option<u32>,
        orphaned_timeout: Option<u32>,
    },
    List {
        prefix: Ident,
        with_payload: bool,
        status_filter: Option<QueueItemStatus>,
        sort_by_priority: bool,
    },
    Cancel {
        key: Ident,
    },
    Heartbeat {
        key: Ident,
    },
    Ack {
        key: Ident,
        result: Option<String>,
    },
    MergeExtra {
        key: Ident,
        payload: String,
    },
    Retrieve {
        key: Ident,
        concurrency: u32,
        extended: bool,
    },
    Result {
        key: Ident,
    },
    ResultBlocking {
        key: Ident,
        timeout: u64,
    },
    Truncate {},
}

#[derive(Debug, Clone, PartialEq)]
pub enum SystemCommand {
    KillAllJobs,
    Repartition { partition_id: u64 },
    Drop(DropCommand),
    PanicWorker,
    MetaStore(MetaStoreCommand),
    CacheStore(CacheStoreCommand),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DropCommand {
    DropQueryCache,
    DropAllCache,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetaStoreCommand {
    SetCurrent { id: u128 },
    Compaction,
    Healthcheck,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CacheStoreCommand {
    Compaction,
    Healthcheck,
}

pub struct CubeStoreParser<'a> {
    parser: Parser<'a>,
}

impl<'a> CubeStoreParser<'a> {
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &MySqlDialectWithBackTicks {};
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;
        Ok(CubeStoreParser {
            parser: Parser::new(tokens, dialect),
        })
    }

    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => match w.keyword {
                _ if w.value.eq_ignore_ascii_case("sys") => {
                    self.parser.next_token();
                    self.parse_system()
                }
                _ if w.value.eq_ignore_ascii_case("queue") => {
                    self.parser.next_token();
                    self.parse_queue()
                }
                Keyword::CACHE => {
                    self.parser.next_token();
                    self.parse_cache()
                }
                Keyword::CREATE => {
                    self.parser.next_token();
                    self.parse_create()
                }
                _ if w.value.eq_ignore_ascii_case("dump") => {
                    self.parser.next_token();
                    let s = self.parser.parse_statement()?;
                    let q = match s {
                        SQLStatement::Query(q) => q,
                        _ => {
                            return Err(ParserError::ParserError(
                                "Expected select query after 'dump'".to_string(),
                            ))
                        }
                    };
                    Ok(Statement::Dump(q))
                }
                _ => Ok(Statement::Statement(self.parser.parse_statement()?)),
            },
            _ => Ok(Statement::Statement(self.parser.parse_statement()?)),
        }
    }

    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if self.parser.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema()
        } else if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_create_table()
        } else if self.parser.consume_token(&Token::make_keyword("SOURCE"))
            || self.parser.consume_token(&Token::make_keyword("source"))
        {
            self.parse_create_source()
        } else {
            Ok(Statement::Statement(self.parser.parse_create()?))
        }
    }

    fn parse_cache(&mut self) -> Result<Statement, ParserError> {
        let method = match self.parser.next_token() {
            Token::Word(w) => w.value.to_ascii_lowercase(),
            other => {
                return Err(ParserError::ParserError(format!(
                    "Invalid token: {}, expected Word (command)",
                    other
                )))
            }
        };

        let command = match method.as_str() {
            "set" => {
                let nx = self.parse_custom_token(&"nx");
                let ttl = if self.parse_custom_token(&"ttl") {
                    Some(self.parse_integer("ttl", false)?)
                } else {
                    None
                };

                CacheCommand::Set {
                    key: self.parser.parse_identifier()?,
                    value: self.parser.parse_literal_string()?,
                    ttl,
                    nx,
                }
            }
            "get" => CacheCommand::Get {
                key: self.parser.parse_identifier()?,
            },
            "keys" => CacheCommand::Keys {
                prefix: self.parser.parse_identifier()?,
            },
            "incr" => CacheCommand::Incr {
                path: self.parser.parse_identifier()?,
            },
            "remove" => CacheCommand::Remove {
                key: self.parser.parse_identifier()?,
            },
            "truncate" => CacheCommand::Truncate {},
            other => {
                return Err(ParserError::ParserError(format!(
                    "Unknown cache command: {}, available: SET|GET|KEYS|INC|REMOVE|TRUNCATE",
                    other
                )))
            }
        };

        Ok(Statement::Cache(command))
    }

    fn parse_integer<R: num::Integer + std::str::FromStr>(
        &mut self,
        var_name: &str,
        allow_negative: bool,
    ) -> Result<R, ParserError>
    where
        <R as std::str::FromStr>::Err: std::fmt::Display,
    {
        let is_negative = match self.parser.peek_token() {
            Token::Minus => {
                self.parser.next_token();
                true
            }
            _ => false,
        };

        match self.parser.parse_number_value()? {
            Value::Number(var, false) => {
                let value = if is_negative {
                    "-".to_string() + &var
                } else {
                    var
                };

                if is_negative && !allow_negative {
                    return Err(ParserError::ParserError(format!(
                        "{} must be a positive integer, actual: {}",
                        var_name, value
                    )));
                }

                value.parse::<R>().map_err(|err| {
                    ParserError::ParserError(format!(
                        "{} must be a valid integer, error: {}",
                        var_name, err
                    ))
                })
            }
            x => {
                return Err(ParserError::ParserError(format!(
                    "{} must be a valid integer, actual: {:?}",
                    var_name, x
                )))
            }
        }
    }

    pub fn parse_drop(&mut self) -> Result<Statement, ParserError> {
        if self.parse_custom_token("query") && self.parse_custom_token("cache") {
            Ok(Statement::System(SystemCommand::Drop(
                DropCommand::DropQueryCache,
            )))
        } else if self.parse_custom_token("cache") {
            Ok(Statement::System(SystemCommand::Drop(
                DropCommand::DropAllCache,
            )))
        } else {
            Err(ParserError::ParserError("Unknown drop command".to_string()))
        }
    }

    pub fn parse_cachestore(&mut self) -> Result<Statement, ParserError> {
        let command = if self.parse_custom_token("compaction") {
            CacheStoreCommand::Compaction
        } else if self.parse_custom_token("healthcheck") {
            CacheStoreCommand::Healthcheck
        } else {
            return Err(ParserError::ParserError(
                "Unknown cachestore command".to_string(),
            ));
        };

        Ok(Statement::System(SystemCommand::CacheStore(command)))
    }

    pub fn parse_metastore(&mut self) -> Result<Statement, ParserError> {
        let command = if self.parse_custom_token("set_current") {
            MetaStoreCommand::SetCurrent {
                id: self.parse_integer("metastore snapshot id", false)?,
            }
        } else if self.parse_custom_token("compaction") {
            MetaStoreCommand::Compaction
        } else if self.parse_custom_token("healthcheck") {
            MetaStoreCommand::Healthcheck
        } else {
            return Err(ParserError::ParserError(
                "Unknown metastore command".to_string(),
            ));
        };

        Ok(Statement::System(SystemCommand::MetaStore(command)))
    }

    fn parse_queue(&mut self) -> Result<Statement, ParserError> {
        let method = match self.parser.next_token() {
            Token::Word(w) => w.value.to_ascii_lowercase(),
            other => {
                return Err(ParserError::ParserError(format!(
                    "Invalid token: {}, expected Word (command)",
                    other
                )))
            }
        };

        let command = match method.as_str() {
            "add" => {
                let priority = if self.parse_custom_token(&"priority") {
                    self.parse_integer(&"priority", true)?
                } else {
                    0
                };

                let orphaned = if self.parse_custom_token(&"orphaned") {
                    Some(self.parse_integer("orphaned", false)?)
                } else {
                    None
                };

                QueueCommand::Add {
                    priority,
                    orphaned,
                    key: self.parser.parse_identifier()?,
                    value: self.parser.parse_literal_string()?,
                }
            }
            "cancel" => QueueCommand::Cancel {
                key: self.parser.parse_identifier()?,
            },
            "heartbeat" => QueueCommand::Heartbeat {
                key: self.parser.parse_identifier()?,
            },
            "ack" => {
                let key = self.parser.parse_identifier()?;
                let result = if self.parser.parse_keyword(Keyword::NULL) {
                    None
                } else {
                    Some(self.parser.parse_literal_string()?)
                };

                QueueCommand::Ack { key, result }
            }
            "merge_extra" => QueueCommand::MergeExtra {
                key: self.parser.parse_identifier()?,
                payload: self.parser.parse_literal_string()?,
            },
            "get" => QueueCommand::Get {
                key: self.parser.parse_identifier()?,
            },
            "stalled" => {
                let heartbeat_timeout = Some(self.parse_integer("heartbeat timeout", false)?);

                QueueCommand::ToCancel {
                    prefix: self.parser.parse_identifier()?,
                    orphaned_timeout: None,
                    heartbeat_timeout,
                }
            }
            "orphaned" => {
                let orphaned_timeout = Some(self.parse_integer("orphaned timeout", false)?);

                QueueCommand::ToCancel {
                    prefix: self.parser.parse_identifier()?,
                    heartbeat_timeout: None,
                    orphaned_timeout,
                }
            }
            "to_cancel" => {
                let heartbeat_timeout = Some(self.parse_integer("heartbeat timeout", false)?);
                let orphaned_timeout = Some(self.parse_integer("orphaned timeout", false)?);

                QueueCommand::ToCancel {
                    prefix: self.parser.parse_identifier()?,
                    heartbeat_timeout,
                    orphaned_timeout,
                }
            }
            "pending" => {
                let with_payload = self.parse_custom_token(&"with_payload");

                QueueCommand::List {
                    prefix: self.parser.parse_identifier()?,
                    with_payload,
                    status_filter: Some(QueueItemStatus::Pending),
                    sort_by_priority: true,
                }
            }
            "active" => {
                let with_payload = self.parse_custom_token(&"with_payload");

                QueueCommand::List {
                    prefix: self.parser.parse_identifier()?,
                    with_payload,
                    status_filter: Some(QueueItemStatus::Active),
                    sort_by_priority: false,
                }
            }
            "list" => {
                let with_payload = self.parse_custom_token(&"with_payload");

                QueueCommand::List {
                    prefix: self.parser.parse_identifier()?,
                    with_payload,
                    status_filter: None,
                    sort_by_priority: true,
                }
            }
            "retrieve" => {
                // for backward compatibility
                let extended = self.parse_custom_token("extended");
                let concurrency = if self.parse_custom_token(&"concurrency") {
                    self.parse_integer("concurrency", false)?
                } else {
                    1
                };

                QueueCommand::Retrieve {
                    key: self.parser.parse_identifier()?,
                    extended,
                    concurrency,
                }
            }
            "result" => QueueCommand::Result {
                key: self.parser.parse_identifier()?,
            },
            "result_blocking" => {
                let timeout = self.parse_integer(&"timeout", false)?;

                QueueCommand::ResultBlocking {
                    timeout,
                    key: self.parser.parse_identifier()?,
                }
            }
            "truncate" => QueueCommand::Truncate {},
            other => {
                return Err(ParserError::ParserError(format!(
                    "Unknown queue command: {}",
                    other
                )))
            }
        };

        Ok(Statement::Queue(command))
    }

    fn parse_system(&mut self) -> Result<Statement, ParserError> {
        if self.parse_custom_token("kill")
            && self.parser.parse_keywords(&[Keyword::ALL])
            && self.parse_custom_token("jobs")
        {
            Ok(Statement::System(SystemCommand::KillAllJobs))
        } else if self.parse_custom_token("repartition") {
            Ok(Statement::System(SystemCommand::Repartition {
                partition_id: self.parse_integer("partition id", false)?,
            }))
        } else if self.parse_custom_token("drop") {
            self.parse_drop()
        } else if self.parse_custom_token("metastore") {
            self.parse_metastore()
        } else if self.parse_custom_token("cachestore") {
            self.parse_cachestore()
        } else if self.parse_custom_token("panic") && self.parse_custom_token("worker") {
            Ok(Statement::System(SystemCommand::PanicWorker))
        } else {
            Err(ParserError::ParserError(
                "Unknown system command".to_string(),
            ))
        }
    }

    fn parse_custom_token(&mut self, token: &str) -> bool {
        if let Token::Word(w) = self.parser.peek_token() {
            if w.value.eq_ignore_ascii_case(token) {
                self.parser.next_token();
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn parse_create_table(&mut self) -> Result<Statement, ParserError> {
        // Note that we disable hive extensions as they clash with `location`.
        let statement = self.parser.parse_create_table_ext(false, false, false)?;
        if let SQLStatement::CreateTable {
            name,
            columns,
            constraints,
            with_options,
            if_not_exists,
            file_format,
            query,
            without_rowid,
            or_replace,
            table_properties,
            like,
            ..
        } = statement
        {
            let unique_key = if self.parser.parse_keywords(&[Keyword::UNIQUE, Keyword::KEY]) {
                self.parser.expect_token(&Token::LParen)?;
                let res = Some(
                    self.parser
                        .parse_comma_separated(|p| p.parse_identifier())?,
                );
                self.parser.expect_token(&Token::RParen)?;
                res
            } else {
                None
            };

            let aggregates = if self.parse_custom_token("aggregations") {
                self.parser.expect_token(&Token::LParen)?;
                let res = self.parser.parse_comma_separated(|p| {
                    let func = p.parse_identifier()?;
                    p.expect_token(&Token::LParen)?;
                    let column = p.parse_identifier()?;
                    p.expect_token(&Token::RParen)?;
                    Ok((func, column))
                })?;
                self.parser.expect_token(&Token::RParen)?;
                Some(res)
            } else {
                None
            };

            let mut indexes = Vec::new();

            loop {
                if self.parse_custom_token("aggregate") {
                    self.parser.expect_keyword(Keyword::INDEX)?;
                    indexes.push(self.parse_with_index(name.clone(), true)?);
                } else if self.parser.parse_keyword(Keyword::INDEX) {
                    indexes.push(self.parse_with_index(name.clone(), false)?);
                } else {
                    break;
                }
            }

            let partitioned_index = if self.parser.parse_keywords(&[
                Keyword::ADD,
                Keyword::TO,
                Keyword::PARTITIONED,
                Keyword::INDEX,
            ]) {
                let name = self.parser.parse_object_name()?;
                self.parser.expect_token(&Token::LParen)?;
                let columns = self
                    .parser
                    .parse_comma_separated(Parser::parse_identifier)?;
                self.parser.expect_token(&Token::RParen)?;
                Some(PartitionedIndexRef { name, columns })
            } else {
                None
            };

            let locations = if self.parser.parse_keyword(Keyword::LOCATION) {
                Some(
                    self.parser
                        .parse_comma_separated(|p| p.parse_literal_string())?,
                )
            } else {
                None
            };

            Ok(Statement::CreateTable {
                create_table: SQLStatement::CreateTable {
                    or_replace,
                    name,
                    columns,
                    constraints,
                    hive_distribution: HiveDistributionStyle::NONE,
                    hive_formats: None,
                    table_properties,
                    with_options,
                    if_not_exists,
                    external: locations.is_some(),
                    file_format,
                    location: None,
                    query,
                    without_rowid,
                    temporary: false,
                    like,
                },
                indexes,
                aggregates,
                partitioned_index,
                locations,
                unique_key,
            })
        } else {
            Ok(Statement::Statement(statement))
        }
    }

    pub fn parse_with_index(
        &mut self,
        table_name: ObjectName,
        is_aggregate: bool,
    ) -> Result<SQLStatement, ParserError> {
        let index_name = self.parser.parse_object_name()?;
        self.parser.expect_token(&Token::LParen)?;
        let columns = self
            .parser
            .parse_comma_separated(Parser::parse_order_by_expr)?;
        self.parser.expect_token(&Token::RParen)?;
        //TODO I use unique flag for aggregate index for reusing CreateIndex struct. When adding another type of index, we will need to parse it into a custom structure
        Ok(SQLStatement::CreateIndex {
            name: index_name,
            table_name,
            columns,
            unique: is_aggregate,
            if_not_exists: false,
        })
    }

    fn parse_create_schema(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let schema_name = self.parser.parse_object_name()?;
        Ok(Statement::CreateSchema {
            schema_name,
            if_not_exists,
        })
    }

    fn parse_create_source(&mut self) -> Result<Statement, ParserError> {
        let or_update = self.parser.parse_keywords(&[Keyword::OR, Keyword::UPDATE]);
        let name = self.parser.parse_identifier()?;
        self.parser.expect_keyword(Keyword::AS)?;
        let source_type = self.parser.parse_literal_string()?;
        let credentials = self.parser.parse_options(Keyword::VALUES)?;
        Ok(Statement::CreateSource {
            name,
            or_update,
            credentials,
            source_type,
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use sqlparser::ast::Statement as SQLStatement;

    #[test]
    fn parse_aggregate_index() {
        let query = "CREATE TABLE foo.Orders (
            id int,
            platform varchar(255),
            age int,
            gender varchar(2),
            count int,
            max_id int
            )
            UNIQUE KEY (id, platform, age, gender)
            AGGREGATIONS(sum(count), max(max_id))
            INDEX index1 (platform, age)
            AGGREGATE INDEX aggr_index (platform, age)
            INDEX index2 (age, platform )
            ;";
        let mut parser = CubeStoreParser::new(&query).unwrap();
        let res = parser.parse_statement().unwrap();
        match res {
            Statement::CreateTable {
                indexes,
                aggregates,
                ..
            } => {
                assert_eq!(aggregates.as_ref().unwrap()[0].0.value, "sum".to_string());
                assert_eq!(aggregates.as_ref().unwrap()[0].1.value, "count".to_string());
                assert_eq!(aggregates.as_ref().unwrap()[1].0.value, "max".to_string());
                assert_eq!(
                    aggregates.as_ref().unwrap()[1].1.value,
                    "max_id".to_string()
                );

                assert_eq!(indexes.len(), 3);

                let ind = &indexes[0];
                if let SQLStatement::CreateIndex {
                    columns, unique, ..
                } = ind
                {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(unique, &false);
                } else {
                    assert!(false);
                }

                let ind = &indexes[1];
                if let SQLStatement::CreateIndex {
                    columns, unique, ..
                } = ind
                {
                    assert_eq!(columns.len(), 2);
                    assert_eq!(unique, &true);
                } else {
                    assert!(false);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn parse_metastore_set_current() {
        let query = "sys MeTasTore SEt_Current 1671235558783";
        let mut parser = CubeStoreParser::new(&query).unwrap();
        let res = parser.parse_statement().unwrap();
        match res {
            Statement::System(SystemCommand::MetaStore(MetaStoreCommand::SetCurrent { id })) => {
                assert_eq!(id, 1671235558783);
            }
            _ => {
                assert!(false)
            }
        }
    }
}
