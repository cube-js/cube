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
    CacheSet {
        key: Ident,
        value: String,
        ttl: Option<u32>,
        nx: bool,
    },
    CacheGet {
        key: Ident,
    },
    CacheKeys {
        prefix: Ident,
    },
    CacheRemove {
        key: Ident,
    },
    CacheTruncate {},
    CacheIncr {
        path: Ident,
    },
    // queue
    QueueAdd {
        priority: i64,
        key: Ident,
        value: String,
    },
    QueueGet {
        key: Ident,
    },
    QueueToCancel {
        prefix: Ident,
        orphaned_timeout: Option<u32>,
        stalled_timeout: Option<u32>,
    },
    QueueList {
        prefix: Ident,
        with_payload: bool,
        status_filter: Option<QueueItemStatus>,
        sort_by_priority: bool,
    },
    QueueCancel {
        key: Ident,
    },
    QueueHeartbeat {
        key: Ident,
    },
    QueueAck {
        key: Ident,
        result: String,
    },
    QueueMergeExtra {
        key: Ident,
        payload: String,
    },
    QueueRetrieve {
        key: Ident,
        concurrency: u32,
    },
    QueueResult {
        key: Ident,
    },
    QueueResultBlocking {
        key: Ident,
        timeout: u64,
    },
    QueueTruncate {},
    System(SystemCommand),
    Dump(Box<Query>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RocksStoreName {
    Meta,
    Cache,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SystemCommand {
    Compaction { store: Option<RocksStoreName> },
    KillAllJobs,
    Repartition { partition_id: u64 },
    PanicWorker,
    Metastore(MetastoreCommand),
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetastoreCommand {
    SetCurrent { id: u128 },
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
        let command = match self.parser.next_token() {
            Token::Word(w) => w.value.to_ascii_lowercase(),
            _ => {
                return Err(ParserError::ParserError(
                    "Unknown cache command, available: SET|GET|KEYS|INC|REMOVE|TRUNCATE"
                        .to_string(),
                ))
            }
        };

        match command.as_str() {
            "set" => {
                let nx = self.parse_custom_token(&"nx");
                let ttl = if self.parse_custom_token(&"ttl") {
                    Some(self.parse_number("ttl")?)
                } else {
                    None
                };

                Ok(Statement::CacheSet {
                    key: self.parser.parse_identifier()?,
                    value: self.parser.parse_literal_string()?,
                    ttl,
                    nx,
                })
            }
            "get" => Ok(Statement::CacheGet {
                key: self.parser.parse_identifier()?,
            }),
            "keys" => Ok(Statement::CacheKeys {
                prefix: self.parser.parse_identifier()?,
            }),
            "incr" => Ok(Statement::CacheIncr {
                path: self.parser.parse_identifier()?,
            }),
            "remove" => Ok(Statement::CacheRemove {
                key: self.parser.parse_identifier()?,
            }),
            "truncate" => Ok(Statement::CacheTruncate {}),
            command => Err(ParserError::ParserError(format!(
                "Unknown cache command: {}",
                command
            ))),
        }
    }

    fn parse_number(&mut self, var_name: &str) -> Result<u32, ParserError> {
        match self.parser.parse_number_value()? {
            Value::Number(var, false) => var.parse::<u32>().map_err(|err| {
                ParserError::ParserError(format!(
                    "{} must be a positive integer, error: {}",
                    var_name, err
                ))
            }),
            x => {
                return Err(ParserError::ParserError(format!(
                    "{} must be a positive integer, actual: {:?}",
                    var_name, x
                )))
            }
        }
    }

    pub fn parse_metastore(&mut self) -> Result<Statement, ParserError> {
        if self.parse_custom_token("set_current") {
            match self.parser.parse_number_value()? {
                Value::Number(id, _) => Ok(Statement::System(SystemCommand::Metastore(
                    MetastoreCommand::SetCurrent {
                        id: id.parse::<u128>().map_err(|e| {
                            ParserError::ParserError(format!(
                                "Can't parse metastore snapshot id: {}",
                                e
                            ))
                        })?,
                    },
                ))),
                x => Err(ParserError::ParserError(format!(
                    "Snapshot id expected but {:?} found",
                    x
                ))),
            }
        } else {
            Err(ParserError::ParserError(
                "Unknown metastore command".to_string(),
            ))
        }
    }

    fn parse_queue(&mut self) -> Result<Statement, ParserError> {
        let command = match self.parser.next_token() {
            Token::Word(w) => w.value.to_ascii_lowercase(),
            _ => {
                return Err(ParserError::ParserError(
                    "Unknown queue command, available: ADD|TRUNCATE".to_string(),
                ))
            }
        };

        match command.as_str() {
            "add" => {
                let priority = if self.parse_custom_token(&"priority") {
                    match self.parser.parse_number_value()? {
                        Value::Number(priority, _) => {
                            let r = priority.parse::<i64>().map_err(|err| {
                                ParserError::ParserError(format!(
                                    "priority must be a positive integer, error: {}",
                                    err
                                ))
                            })?;

                            r
                        }
                        x => {
                            return Err(ParserError::ParserError(format!(
                                "priority must be a positive integer, actual: {:?}",
                                x
                            )))
                        }
                    }
                } else {
                    0
                };

                Ok(Statement::QueueAdd {
                    priority,
                    key: self.parser.parse_identifier()?,
                    value: self.parser.parse_literal_string()?,
                })
            }
            "cancel" => Ok(Statement::QueueCancel {
                key: self.parser.parse_identifier()?,
            }),
            "heartbeat" => Ok(Statement::QueueHeartbeat {
                key: self.parser.parse_identifier()?,
            }),
            "ack" => Ok(Statement::QueueAck {
                key: self.parser.parse_identifier()?,
                result: self.parser.parse_literal_string()?,
            }),
            "merge_extra" => Ok(Statement::QueueMergeExtra {
                key: self.parser.parse_identifier()?,
                payload: self.parser.parse_literal_string()?,
            }),
            "get" => Ok(Statement::QueueGet {
                key: self.parser.parse_identifier()?,
            }),
            "stalled" => {
                let stalled_timeout = self.parse_number("stalled timeout")?;

                Ok(Statement::QueueToCancel {
                    prefix: self.parser.parse_identifier()?,
                    orphaned_timeout: None,
                    stalled_timeout: Some(stalled_timeout),
                })
            }
            "orphaned" => {
                let orphaned_timeout = self.parse_number("orphaned timeout")?;

                Ok(Statement::QueueToCancel {
                    prefix: self.parser.parse_identifier()?,
                    orphaned_timeout: Some(orphaned_timeout),
                    stalled_timeout: None,
                })
            }
            "to_cancel" => {
                let stalled_timeout = self.parse_number("stalled timeout")?;
                let orphaned_timeout = self.parse_number("orphaned timeout")?;

                Ok(Statement::QueueToCancel {
                    prefix: self.parser.parse_identifier()?,
                    orphaned_timeout: Some(stalled_timeout),
                    stalled_timeout: Some(orphaned_timeout),
                })
            }
            "pending" => {
                let with_payload = self.parse_custom_token(&"with_payload");

                Ok(Statement::QueueList {
                    prefix: self.parser.parse_identifier()?,
                    with_payload,
                    status_filter: Some(QueueItemStatus::Pending),
                    sort_by_priority: true,
                })
            }
            "active" => {
                let with_payload = self.parse_custom_token(&"with_payload");

                Ok(Statement::QueueList {
                    prefix: self.parser.parse_identifier()?,
                    with_payload,
                    status_filter: Some(QueueItemStatus::Active),
                    sort_by_priority: false,
                })
            }
            "list" => {
                let with_payload = self.parse_custom_token(&"with_payload");

                Ok(Statement::QueueList {
                    prefix: self.parser.parse_identifier()?,
                    with_payload,
                    status_filter: None,
                    sort_by_priority: true,
                })
            }
            "retrieve" => {
                let concurrency = if self.parse_custom_token(&"concurrency") {
                    self.parse_number("concurrency")?
                } else {
                    1
                };

                Ok(Statement::QueueRetrieve {
                    key: self.parser.parse_identifier()?,
                    concurrency,
                })
            }
            "result" => Ok(Statement::QueueResult {
                key: self.parser.parse_identifier()?,
            }),
            "result_blocking" => {
                let timeout = match self.parser.parse_number_value()? {
                    Value::Number(concurrency, false) => {
                        let r = concurrency.parse::<u64>().map_err(|err| {
                            ParserError::ParserError(format!(
                                "TIMEOUT must be a positive integer, error: {}",
                                err
                            ))
                        })?;

                        r
                    }
                    x => {
                        return Err(ParserError::ParserError(format!(
                            "TIMEOUT must be a positive integer, actual: {:?}",
                            x
                        )))
                    }
                };

                Ok(Statement::QueueResultBlocking {
                    timeout,
                    key: self.parser.parse_identifier()?,
                })
            }
            "truncate" => Ok(Statement::QueueTruncate {}),
            command => Err(ParserError::ParserError(format!(
                "Unknown queue command: {}",
                command
            ))),
        }
    }

    fn parse_system(&mut self) -> Result<Statement, ParserError> {
        if self.parse_custom_token("kill")
            && self.parser.parse_keywords(&[Keyword::ALL])
            && self.parse_custom_token("jobs")
        {
            Ok(Statement::System(SystemCommand::KillAllJobs))
        } else if self.parse_custom_token("repartition") {
            match self.parser.parse_number_value()? {
                Value::Number(id, _) => Ok(Statement::System(SystemCommand::Repartition {
                    partition_id: id.parse::<u64>().map_err(|e| {
                        ParserError::ParserError(format!("Can't parse partition id: {}", e))
                    })?,
                })),
                x => Err(ParserError::ParserError(format!(
                    "Partition id expected but {:?} found",
                    x
                ))),
            }
        } else if self.parse_custom_token("metastore") {
            self.parse_metastore()
        } else if self.parse_custom_token("panic") && self.parse_custom_token("worker") {
            Ok(Statement::System(SystemCommand::PanicWorker))
        } else if self.parse_custom_token("compaction") {
            let store = if let Token::Word(w) = self.parser.peek_token() {
                if w.value.eq_ignore_ascii_case("cache") {
                    self.parser.next_token();
                    Some(RocksStoreName::Cache)
                } else if w.value.eq_ignore_ascii_case("meta") {
                    self.parser.next_token();
                    Some(RocksStoreName::Meta)
                } else {
                    return Err(ParserError::ParserError(format!(
                        "Unknown store name, expected CACHE or META, found: {}",
                        w
                    )));
                }
            } else {
                None
            };

            Ok(Statement::System(SystemCommand::Compaction { store }))
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
            Statement::System(SystemCommand::Metastore(MetastoreCommand::SetCurrent { id })) => {
                assert_eq!(id, 1671235558783);
            }
            _ => {
                assert!(false)
            }
        }
    }
}
