use sqlparser::dialect::Dialect;
use sqlparser::ast::{ObjectName, Statement as SQLStatement};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Tokenizer, Token};
use sqlparser::dialect::keywords::Keyword;

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
pub enum Statement {
    Statement(SQLStatement),
    CreateSchema { schema_name: ObjectName, if_not_exists: bool },
}

pub struct CubeStoreParser {
    parser: Parser,
}

impl CubeStoreParser {
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &MySqlDialectWithBackTicks {};
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;
        Ok(CubeStoreParser {
            parser: Parser::new(tokens),
        })
    }

    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token() {
            Token::Word(w) => match w.keyword {
                Keyword::CREATE => {
                    self.parser.next_token();
                    self.parse_create()
                }
                _ => Ok(Statement::Statement(self.parser.parse_statement()?))
            },
            _ => Ok(Statement::Statement(self.parser.parse_statement()?))
        }
    }

    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if self.parser.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema()
        } else if self.parser.parse_keyword(Keyword::TABLE) {
            self.parse_create_table()
        } else {
            Ok(Statement::Statement(self.parser.parse_create()?))
        }
    }

    pub fn parse_create_table(&mut self) -> Result<Statement, ParserError> {
        let statement = self.parser.parse_create_table()?;
        if let SQLStatement::CreateTable {
            name,
            columns,
            constraints,
            with_options,
            if_not_exists,
            file_format,
            query,
            without_rowid,
            ..
        } = statement {
            let location = if self.parser.parse_keyword(Keyword::LOCATION) {
                Some(self.parser.parse_literal_string()?)
            } else {
                None
            };

            Ok(Statement::Statement(SQLStatement::CreateTable {
                name,
                columns,
                constraints,
                with_options,
                if_not_exists,
                external: location.is_some(),
                file_format,
                location,
                query,
                without_rowid,
            }))
        } else {
            Ok(Statement::Statement(statement))
        }
    }

    fn parse_create_schema(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self
            .parser
            .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let schema_name = self.parser.parse_object_name()?;
        Ok(Statement::CreateSchema {
            schema_name,
            if_not_exists
        })
    }
}