//! Decoding of the `COPY ... FROM STDIN` data stream into Arrow record batches.
//!
//! The data arrives as a byte stream split into CopyData messages at arbitrary
//! points, so rows are assembled across message boundaries.
//!
//! The parsing rules follow `CopyReadLineText`, `CopyReadAttributesText` and
//! `CopyReadAttributesCSV` of PostgreSQL, down to the error messages and codes.

use crate::compile::copy::{CopyFormat, CopyOptions};
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use datafusion::arrow::{
    array::{
        ArrayRef, BooleanBuilder, Date32Builder, DecimalBuilder, Float32Builder, Float64Builder,
        Int16Builder, Int32Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
    },
    datatypes::{DataType, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use pg_srv::{
    protocol::{ErrorCode, ErrorResponse},
    ProtocolError,
};
use std::{convert::TryFrom, sync::Arc};

const UNIX_EPOCH_DAY: i64 = 719_163;

/// Field metadata holding the declared width of a character column.
pub const MAX_LENGTH_METADATA: &str = "max_length";

/// How much of a row an error message may quote back. PostgreSQL applies the same
/// limit in `limit_printout_length`, so that a bad line cannot be echoed in full.
const MAX_PRINTOUT_LENGTH: usize = 1024;

/// Cut a value down to what an error message may show, as PostgreSQL does.
fn limit_printout(value: &str) -> String {
    let mut end = MAX_PRINTOUT_LENGTH.min(value.len());
    while end < value.len() && !value.is_char_boundary(end) {
        end -= 1;
    }

    match end < value.len() {
        true => format!("{}...", &value[..end]),
        false => value.to_string(),
    }
}

/// Line ending of the data, detected on the first line and required to stay the same.
/// PostgreSQL calls these EOL_NL, EOL_CR and EOL_CRNL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EolStyle {
    Unknown,
    /// A single line feed
    Lf,
    /// A single carriage return
    Cr,
    /// A carriage return followed by a line feed
    CrLf,
}

/// One row taken out of the byte stream, or the reason there is none yet.
enum Line {
    /// A complete row, without its line terminator
    Row(Vec<u8>),
    /// The end-of-copy marker was found, the rest of the stream is data no more
    EndOfData,
    /// More data is needed to complete the row
    Incomplete,
}

/// Assembles rows out of the incoming byte stream and turns them into a record batch
/// matching the schema of the target table.
pub struct CopyFromDecoder {
    /// Name of the target table, for the error context
    table_name: String,
    schema: SchemaRef,
    /// Target column of every field of an incoming row, by position
    column_indices: Vec<usize>,
    options: CopyOptions,
    builders: Vec<ColumnBuilder>,
    /// Bytes received but not yet split into rows
    buffer: Vec<u8>,
    /// Bytes at the front of the buffer which have become rows already. They are
    /// left in place and dropped in batches, so that taking a row does not move
    /// everything behind it
    consumed: usize,
    /// How far past the consumed bytes the row scan has already looked
    scanned: usize,
    /// The scan is inside a quoted value, CSV only
    in_quote: bool,
    eol: EolStyle,
    /// Lines of the data consumed so far, including the header
    line: u64,
    /// Rows loaded so far
    rows: usize,
    /// A header line is still to be discarded
    skip_header: bool,
    /// The end-of-copy marker has been seen
    finished: bool,
    /// Bytes of data accepted so far, to bound memory usage
    accepted_bytes: usize,
    /// Bytes the values built so far take, which is not the same as the bytes read:
    /// a two-character "1" of a bigint column becomes eight
    built_bytes: usize,
    max_bytes: usize,
}

impl CopyFromDecoder {
    /// `max_bytes` bounds both the data held while rows are assembled and the values
    /// built out of it, so that a copy cannot outgrow the table it loads into.
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        column_indices: Vec<usize>,
        options: CopyOptions,
        max_bytes: usize,
    ) -> Result<Self, ProtocolError> {
        let builders = schema
            .fields()
            .iter()
            .map(|field| {
                let max_length = field
                    .metadata()
                    .as_ref()
                    .and_then(|metadata| metadata.get(MAX_LENGTH_METADATA))
                    .and_then(|length| length.parse::<usize>().ok());

                ColumnBuilder::new(field.data_type(), max_length)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            table_name,
            schema,
            column_indices,
            skip_header: options.header,
            options,
            builders,
            buffer: vec![],
            consumed: 0,
            scanned: 0,
            in_quote: false,
            eol: EolStyle::Unknown,
            line: 0,
            rows: 0,
            finished: false,
            accepted_bytes: 0,
            built_bytes: 0,
            max_bytes,
        })
    }

    /// Whether the end-of-copy marker has been seen, after which PostgreSQL stops
    /// reading and completes the copy.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Feed the payload of one CopyData message.
    pub fn push(&mut self, chunk: &[u8]) -> Result<(), ProtocolError> {
        if self.finished {
            return Ok(());
        }

        self.accepted_bytes += chunk.len();
        self.check_memory_limit()?;

        // Rows already taken are dropped once they are worth moving the rest over,
        // which costs each byte of the stream one move at most
        if self.consumed > 0 && self.consumed >= self.buffer.len() / 2 {
            self.buffer.drain(..self.consumed);
            self.consumed = 0;
        }

        self.buffer.extend_from_slice(chunk);

        loop {
            match self.take_line(false)? {
                Line::Row(line) => self.consume_line(line)?,
                Line::EndOfData | Line::Incomplete => return Ok(()),
            }

            if self.finished {
                return Ok(());
            }
        }
    }

    /// Finish the stream and build the batch. The last row does not have to end with
    /// a line terminator.
    pub fn finish(mut self) -> Result<(RecordBatch, usize), ProtocolError> {
        while !self.finished {
            match self.take_line(true)? {
                Line::Row(line) => self.consume_line(line)?,
                Line::EndOfData | Line::Incomplete => break,
            }
        }

        let columns = self
            .builders
            .iter_mut()
            .map(|builder| builder.finish())
            .collect::<Vec<ArrayRef>>();

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|err| {
            ErrorResponse::error(
                ErrorCode::InternalError,
                format!("Unable to build COPY data: {}", err),
            )
        })?;

        Ok((batch, self.rows))
    }

    /// Split off the next row. A value can hold a line terminator when it is escaped
    /// in text format or quoted in CSV format, so the scan is aware of both.
    ///
    /// `at_end` tells the scan that no more data will arrive, which makes an
    /// unterminated last row a row of its own.
    fn take_line(&mut self, at_end: bool) -> Result<Line, ProtocolError> {
        let csv = self.options.format == CopyFormat::Csv;

        while self.consumed + self.scanned < self.buffer.len() {
            let byte = self.buffer[self.consumed + self.scanned];

            // Inside a quoted CSV value nothing terminates the line
            if csv && self.in_quote {
                if byte == self.options.escape as u8 && self.options.escape != self.options.quote {
                    if self.peek(self.scanned + 1).is_none() && !at_end {
                        return Ok(Line::Incomplete);
                    }

                    self.scanned = (self.scanned + 2).min(self.buffer.len() - self.consumed);

                    continue;
                }

                if byte == self.options.quote as u8 {
                    self.in_quote = false;
                }

                self.scanned += 1;

                continue;
            }

            if csv && byte == self.options.quote as u8 {
                self.in_quote = true;
                self.scanned += 1;

                continue;
            }

            // A backslash followed by a period ends the data. PostgreSQL looks for it
            // anywhere in text format, but only at the start of a line in CSV format:
            // `if (c == '\\' && (!cstate->opts.csv_mode || first_char_in_line))`.
            if byte == b'\\' && (!csv || self.scanned == 0) {
                let Some(next) = self.peek(self.scanned + 1) else {
                    if at_end {
                        // A backslash at the very end escapes nothing, and the row it
                        // closes is taken by the unterminated-row branch below
                        self.scanned += 1;

                        continue;
                    }

                    return Ok(Line::Incomplete);
                };

                if next != b'.' {
                    // In CSV format a backslash is an ordinary character
                    self.scanned += if csv { 1 } else { 2 };

                    continue;
                }

                return self.take_end_of_data_marker(at_end);
            }

            if byte == b'\r' {
                return self.take_carriage_return(at_end);
            }

            if byte == b'\n' {
                if matches!(self.eol, EolStyle::Cr | EolStyle::CrLf) {
                    return Err(self.newline_in_data_error().into());
                }

                self.eol = EolStyle::Lf;

                return Ok(Line::Row(self.split_line(1)));
            }

            self.scanned += 1;
        }

        // A last row without a line terminator is still a row
        if at_end && self.consumed < self.buffer.len() {
            if csv && self.in_quote {
                let row = String::from_utf8_lossy(&self.buffer[self.consumed..]).to_string();

                return Err(ErrorResponse::error(
                    ErrorCode::BadCopyFileFormat,
                    "unterminated CSV quoted field".to_string(),
                )
                .with_context(self.row_context_at(self.line + 1, &row))
                .into());
            }

            return Ok(Line::Row(self.split_line(0)));
        }

        Ok(Line::Incomplete)
    }

    /// Handle a carriage return, which either terminates the line or is data
    /// PostgreSQL refuses to guess about.
    fn take_carriage_return(&mut self, at_end: bool) -> Result<Line, ProtocolError> {
        match self.eol {
            EolStyle::Lf => Err(self.carriage_return_in_data_error().into()),
            EolStyle::Cr => Ok(Line::Row(self.split_line(1))),
            EolStyle::Unknown | EolStyle::CrLf => {
                match self.peek(self.scanned + 1) {
                    None if !at_end => Ok(Line::Incomplete),
                    // A carriage return at the very end of the data terminates the row
                    None => {
                        self.eol = EolStyle::Cr;

                        Ok(Line::Row(self.split_line(1)))
                    }
                    Some(b'\n') => {
                        self.eol = EolStyle::CrLf;

                        Ok(Line::Row(self.split_line(2)))
                    }
                    Some(_) => {
                        // A lone carriage return cannot be a terminator once the data
                        // has been seen to use CRLF
                        if self.eol == EolStyle::CrLf {
                            return Err(self.carriage_return_in_data_error().into());
                        }

                        self.eol = EolStyle::Cr;

                        Ok(Line::Row(self.split_line(1)))
                    }
                }
            }
        }
    }

    /// Handle the `\.` end-of-copy marker. It has to be followed by the line ending
    /// the rest of the data uses; whatever stands before it on the line is still a
    /// row, as PostgreSQL keeps the part of the line it has already read.
    fn take_end_of_data_marker(&mut self, at_end: bool) -> Result<Line, ProtocolError> {
        let corrupt = || {
            ErrorResponse::error(
                ErrorCode::BadCopyFileFormat,
                "end-of-copy marker corrupt".to_string(),
            )
            .with_context(self.line_context(self.line + 1))
        };

        let terminator = match self.peek(self.scanned + 2) {
            // More data may still turn up unless the client is done sending
            None if !at_end => return Ok(Line::Incomplete),
            None => return Err(corrupt().into()),
            Some(terminator) => terminator,
        };

        if terminator != b'\r' && terminator != b'\n' {
            return Err(corrupt().into());
        }

        let expected = match self.eol {
            EolStyle::Unknown | EolStyle::Lf => b'\n',
            EolStyle::Cr | EolStyle::CrLf => b'\r',
        };

        if terminator != expected
            || (self.eol == EolStyle::CrLf && self.peek(self.scanned + 3) != Some(b'\n'))
        {
            return Err(ErrorResponse::error(
                ErrorCode::BadCopyFileFormat,
                "end-of-copy marker does not match previous newline style".to_string(),
            )
            .with_context(self.line_context(self.line + 1))
            .into());
        }

        self.finished = true;

        // Data read before the marker is a row of its own
        match self.scanned {
            0 => Ok(Line::EndOfData),
            _ => Ok(Line::Row(self.split_line(0))),
        }
    }

    /// Byte `at` places past the bytes already consumed, which is None while it has
    /// not arrived yet.
    fn peek(&self, at: usize) -> Option<u8> {
        self.buffer.get(self.consumed + at).copied()
    }

    /// Take the bytes up to the scan position as a row, and step over `terminator`
    /// bytes of line terminator behind it.
    fn split_line(&mut self, terminator: usize) -> Vec<u8> {
        let start = self.consumed;
        let end = start + self.scanned;

        self.consumed = end + terminator;
        self.scanned = 0;

        self.buffer[start..end].to_vec()
    }

    fn consume_line(&mut self, line: Vec<u8>) -> Result<(), ProtocolError> {
        self.line += 1;

        let line = String::from_utf8(line).map_err(|err| {
            let byte = err.as_bytes()[err.utf8_error().valid_up_to()];

            self.error(
                ErrorCode::CharacterNotInRepertoire,
                format!(
                    "invalid byte sequence for encoding \"UTF8\": 0x{:02x}",
                    byte
                ),
            )
        })?;

        if self.skip_header {
            self.skip_header = false;

            return Ok(());
        }

        let fields = match self.options.format {
            CopyFormat::Text => split_text_line(&line, &self.options),
            CopyFormat::Csv => split_csv_line(&line, &self.options),
        };

        if fields.len() > self.column_indices.len() {
            return Err(ErrorResponse::error(
                ErrorCode::BadCopyFileFormat,
                "extra data after last expected column".to_string(),
            )
            .with_context(self.row_context(&line))
            .into());
        }

        if fields.len() < self.column_indices.len() {
            let missing = self.schema.field(self.column_indices[fields.len()]).name();

            return Err(ErrorResponse::error(
                ErrorCode::BadCopyFileFormat,
                format!("missing data for column \"{}\"", missing),
            )
            .with_context(self.row_context(&line))
            .into());
        }

        // Columns which the COPY statement did not list stay NULL
        let mut values: Vec<Option<String>> = vec![None; self.builders.len()];
        for (field, column) in fields.into_iter().zip(self.column_indices.iter()) {
            let name = self.schema.field(*column).name();

            values[*column] = match resolve_value(field, name, &self.options) {
                None => None,
                Some(Ok(value)) => Some(value),
                Some(Err(byte)) => {
                    return Err(ErrorResponse::error(
                        ErrorCode::CharacterNotInRepertoire,
                        format!(
                            "invalid byte sequence for encoding \"UTF8\": 0x{:02x}",
                            byte
                        ),
                    )
                    .with_context(self.row_context(&line))
                    .into())
                }
            };
        }

        for (column, value) in values.into_iter().enumerate() {
            let field = self.schema.field(column);
            let name = field.name().clone();

            // A column declared NOT NULL takes neither the NULL representation nor
            // the implicit NULL of a column the statement did not list
            if value.is_none() && !field.is_nullable() {
                return Err(ErrorResponse::error(
                    ErrorCode::NotNullViolation,
                    format!(
                        "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                        name, self.table_name
                    ),
                )
                .with_context(self.row_context(&line))
                .into());
            }

            self.built_bytes += self.builders[column].value_size(value.as_deref());

            if let Err(err) = self.builders[column].append(value.as_deref()) {
                let mut response =
                    ErrorResponse::error(err.code, err.message).with_context(format!(
                        "COPY {}, line {}, column {}: \"{}\"",
                        self.table_name,
                        self.line,
                        name,
                        limit_printout(&value.unwrap_or_default())
                    ));

                if let Some(detail) = err.detail {
                    response = response.with_detail(detail);
                }

                return Err(response.into());
            }
        }

        self.rows += 1;

        self.check_memory_limit()
    }

    /// Refuse data which neither the buffer nor the resulting table could hold. The
    /// values built out of the data are counted as well as the data itself, because
    /// a narrow column can widen a value several times over.
    fn check_memory_limit(&self) -> Result<(), ProtocolError> {
        if self.accepted_bytes <= self.max_bytes && self.built_bytes <= self.max_bytes {
            return Ok(());
        }

        Err(self
            .error(
                ErrorCode::ConfigurationLimitExceeded,
                format!(
                    "COPY data exceeds the temporary table memory limit ({} MiB)",
                    self.max_bytes / 1024 / 1024
                ),
            )
            .into())
    }

    fn error(&self, code: ErrorCode, message: String) -> ErrorResponse {
        ErrorResponse::error(code, message).with_context(self.line_context(self.line))
    }

    fn line_context(&self, line: u64) -> String {
        format!("COPY {}, line {}", self.table_name, line)
    }

    /// Context of an error in a row which has been read in full, quoting it the way
    /// PostgreSQL does.
    fn row_context(&self, row: &str) -> String {
        self.row_context_at(self.line, row)
    }

    fn row_context_at(&self, line: u64, row: &str) -> String {
        format!(
            "COPY {}, line {}: \"{}\"",
            self.table_name,
            line,
            limit_printout(row)
        )
    }

    fn carriage_return_in_data_error(&self) -> ErrorResponse {
        // The line is reported one further, as PostgreSQL counts the line it is
        // reading when it refuses the character
        let error = match self.options.format {
            CopyFormat::Text => self
                .error(
                    ErrorCode::BadCopyFileFormat,
                    "literal carriage return found in data".to_string(),
                )
                .with_hint("Use \"\\r\" to represent carriage return.".to_string()),
            CopyFormat::Csv => self
                .error(
                    ErrorCode::BadCopyFileFormat,
                    "unquoted carriage return found in data".to_string(),
                )
                .with_hint("Use quoted CSV field to represent carriage return.".to_string()),
        };

        error.with_context(self.line_context(self.line + 1))
    }

    fn newline_in_data_error(&self) -> ErrorResponse {
        let error = match self.options.format {
            CopyFormat::Text => self
                .error(
                    ErrorCode::BadCopyFileFormat,
                    "literal newline found in data".to_string(),
                )
                .with_hint("Use \"\\n\" to represent newline.".to_string()),
            CopyFormat::Csv => self
                .error(
                    ErrorCode::BadCopyFileFormat,
                    "unquoted newline found in data".to_string(),
                )
                .with_hint("Use quoted CSV field to represent newline.".to_string()),
        };

        error.with_context(self.line_context(self.line + 1))
    }
}

impl std::fmt::Debug for CopyFromDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "CopyFromDecoder(table: {}, options: {:?}, line: {}, rows: {})",
            self.table_name, self.options, self.line, self.rows
        ))
    }
}

/// A field of a row, as it was written in the data.
#[derive(Debug)]
struct CopyField {
    value: String,
    /// A quote was seen in the value, so it can only be a NULL through FORCE_NULL
    quoted: bool,
}

impl CopyField {
    fn plain(value: String) -> Self {
        Self {
            value,
            quoted: false,
        }
    }
}

/// Decide whether a field is a NULL value or a string. The comparison against the
/// NULL representation happens before backslash escapes are undone, so a value
/// written as `\\N` is the two-character string, not a NULL.
fn resolve_value(
    field: CopyField,
    column: &str,
    options: &CopyOptions,
) -> Option<Result<String, u8>> {
    let matches_null = !field.quoted && field.value == options.null_string;

    if matches_null && !options.force_not_null.iter().any(|c| c == column) {
        return None;
    }

    // A quoted CSV value which spells the NULL representation is a NULL only when
    // FORCE_NULL says so
    if field.quoted
        && field.value == options.null_string
        && options.force_null.iter().any(|c| c == column)
    {
        return None;
    }

    match options.format {
        CopyFormat::Text => Some(unescape_text(&field.value)),
        // CSV values are unescaped while the line is split
        CopyFormat::Csv => Some(Ok(field.value)),
    }
}

/// Split a line of the text format into fields, leaving the backslash escapes in
/// place: the NULL representation is matched against the raw text of a field.
fn split_text_line(line: &str, options: &CopyOptions) -> Vec<CopyField> {
    let mut fields = vec![];
    let mut value = String::new();
    let mut chars = line.chars();

    while let Some(char) = chars.next() {
        if char == options.delimiter {
            fields.push(CopyField::plain(std::mem::take(&mut value)));

            continue;
        }

        value.push(char);

        // An escaped character is never a delimiter
        if char == '\\' {
            if let Some(escaped) = chars.next() {
                value.push(escaped);
            }
        }
    }

    fields.push(CopyField::plain(value));

    fields
}

/// Undo the backslash escapes of the text format. Octal and hexadecimal escapes
/// spell out bytes, which do not have to form valid UTF-8: the offending byte is
/// returned when they do not.
fn unescape_text(value: &str) -> Result<String, u8> {
    let mut result: Vec<u8> = Vec::with_capacity(value.len());
    let mut chars = value.chars().peekable();

    let mut encoded = [0; 4];

    while let Some(char) = chars.next() {
        if char != '\\' {
            result.extend_from_slice(char.encode_utf8(&mut encoded).as_bytes());

            continue;
        }

        match chars.next() {
            // A backslash at the end of a value escapes nothing, and PostgreSQL
            // drops it
            None => (),
            Some('b') => result.push(0x08),
            Some('f') => result.push(0x0c),
            Some('n') => result.push(b'\n'),
            Some('r') => result.push(b'\r'),
            Some('t') => result.push(b'\t'),
            Some('v') => result.push(0x0b),
            // Up to three octal digits
            Some(digit @ '0'..='7') => {
                let mut octal = digit.to_digit(8).expect("an octal digit");

                for _ in 0..2 {
                    match chars.peek().and_then(|char| char.to_digit(8)) {
                        Some(digit) => {
                            octal = (octal << 3) + digit;
                            chars.next();
                        }
                        None => break,
                    }
                }

                result.push(octal as u8);
            }
            // Up to two hexadecimal digits
            Some('x') => match chars.peek().and_then(|char| char.to_digit(16)) {
                None => result.push(b'x'),
                Some(digit) => {
                    let mut hex = digit;
                    chars.next();

                    if let Some(digit) = chars.peek().and_then(|char| char.to_digit(16)) {
                        hex = (hex << 4) + digit;
                        chars.next();
                    }

                    result.push(hex as u8);
                }
            },
            // A backslash before anything else stands for the character itself
            Some(other) => result.extend_from_slice(other.encode_utf8(&mut encoded).as_bytes()),
        }
    }

    String::from_utf8(result).map_err(|err| err.as_bytes()[err.utf8_error().valid_up_to()])
}

/// Split a line of the CSV format, unquoting the values. As in PostgreSQL, a quote
/// can open a quoted section anywhere in a value, not only at its start.
fn split_csv_line(line: &str, options: &CopyOptions) -> Vec<CopyField> {
    let mut fields = vec![];
    let mut value = String::new();
    let mut quoted = false;
    let mut in_quote = false;
    let mut chars = line.chars().peekable();

    while let Some(char) = chars.next() {
        if in_quote {
            // The escape character escapes the quoting character and itself, which
            // are the same character unless ESCAPE says otherwise
            if char == options.escape
                && chars
                    .peek()
                    .is_some_and(|next| *next == options.quote || *next == options.escape)
            {
                value.push(chars.next().expect("peeked character must exist"));

                continue;
            }

            if char == options.quote {
                in_quote = false;

                continue;
            }

            value.push(char);

            continue;
        }

        if char == options.quote {
            in_quote = true;
            quoted = true;

            continue;
        }

        if char == options.delimiter {
            fields.push(CopyField {
                value: std::mem::take(&mut value),
                quoted: std::mem::take(&mut quoted),
            });

            continue;
        }

        value.push(char);
    }

    fields.push(CopyField { value, quoted });

    fields
}

/// Why a value could not be appended to its column.
struct AppendError {
    code: ErrorCode,
    message: String,
    detail: Option<String>,
}

impl AppendError {
    fn invalid_syntax(type_name: &str, value: &str) -> Self {
        Self {
            code: ErrorCode::InvalidTextRepresentation,
            message: format!("invalid input syntax for type {}: \"{}\"", type_name, value),
            detail: None,
        }
    }
}

/// Builds the array of one column, converting the values as PostgreSQL input
/// functions do.
enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal(DecimalBuilder, usize, usize),
    /// A character column carries the width it was declared with
    Utf8(StringBuilder, Option<usize>),
    Date32(Date32Builder),
    TimestampNanosecond(TimestampNanosecondBuilder),
}

impl ColumnBuilder {
    fn new(data_type: &DataType, max_length: Option<usize>) -> Result<Self, ProtocolError> {
        let capacity = 0;

        let builder = match data_type {
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new(capacity)),
            DataType::Int16 => ColumnBuilder::Int16(Int16Builder::new(capacity)),
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::new(capacity)),
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::new(capacity)),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::new(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::new(capacity)),
            // Arrow stores decimals in 128 bits and looks the precision up in a
            // table of 38 entries, so anything wider has to be refused here
            DataType::Decimal(precision, scale) if *precision >= 1 && *precision <= 38 => {
                ColumnBuilder::Decimal(
                    DecimalBuilder::new(capacity, *precision, *scale),
                    *precision,
                    *scale,
                )
            }
            DataType::Utf8 => ColumnBuilder::Utf8(StringBuilder::new(capacity), max_length),
            DataType::Date32 => ColumnBuilder::Date32(Date32Builder::new(capacity)),
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                ColumnBuilder::TimestampNanosecond(TimestampNanosecondBuilder::new(capacity))
            }
            other => {
                return Err(ErrorResponse::error(
                    ErrorCode::FeatureNotSupported,
                    format!("COPY does not support a column of type {}", other),
                )
                .into())
            }
        };

        Ok(builder)
    }

    /// Append one value, failing the way the PostgreSQL input function of the type
    /// would when the value does not fit it.
    fn append(&mut self, value: Option<&str>) -> Result<(), AppendError> {
        macro_rules! append {
            ($builder:expr, $type_name:expr, $parse:expr) => {{
                match value {
                    None => $builder.append_null().map_err(arrow_error)?,
                    Some(value) => {
                        let value = value.trim();
                        let parsed = $parse(value)
                            .ok_or_else(|| AppendError::invalid_syntax($type_name, value))?;

                        $builder.append_value(parsed).map_err(arrow_error)?
                    }
                }
            }};
        }

        match self {
            ColumnBuilder::Utf8(builder, max_length) => match value {
                None => builder.append_null().map_err(arrow_error)?,
                Some(value) => {
                    // PostgreSQL counts characters, not bytes
                    if let Some(max_length) = max_length {
                        if value.chars().count() > *max_length {
                            return Err(AppendError {
                                code: ErrorCode::StringDataRightTruncation,
                                message: format!(
                                    "value too long for type character varying({})",
                                    max_length
                                ),
                                detail: None,
                            });
                        }
                    }

                    builder.append_value(value).map_err(arrow_error)?
                }
            },
            ColumnBuilder::Boolean(builder) => append!(builder, "boolean", parse_bool),
            ColumnBuilder::Int16(builder) => append!(builder, "smallint", parse_number::<i16>),
            ColumnBuilder::Int32(builder) => append!(builder, "integer", parse_number::<i32>),
            ColumnBuilder::Int64(builder) => append!(builder, "bigint", parse_number::<i64>),
            ColumnBuilder::Float32(builder) => append!(builder, "real", parse_number::<f32>),
            ColumnBuilder::Float64(builder) => {
                append!(builder, "double precision", parse_number::<f64>)
            }
            ColumnBuilder::Decimal(builder, precision, scale) => {
                let (precision, scale) = (*precision, *scale);

                match value {
                    None => builder.append_null().map_err(arrow_error)?,
                    Some(value) => {
                        let value = value.trim();

                        // PostgreSQL numerics can be NaN or infinite, a 128 bit
                        // decimal cannot. Only the words are these values: a number
                        // too large to be finite as a float is still a decimal which
                        // does not fit, and is reported as one
                        let word = value.trim_start_matches(['+', '-']).to_lowercase();
                        if matches!(word.as_str(), "nan" | "inf" | "infinity") {
                            return Err(AppendError {
                                code: ErrorCode::FeatureNotSupported,
                                message: format!("NUMERIC value \"{}\" is not supported", value),
                                detail: None,
                            });
                        }

                        let parsed = parse_decimal(value, scale).map_err(|err| match err {
                            DecimalError::Syntax => AppendError::invalid_syntax(
                                &format!("numeric({},{})", precision, scale),
                                value,
                            ),
                            DecimalError::Overflow => numeric_overflow(precision, scale),
                        })?;

                        // Arrow lets a decimal of the widest precision hold anything
                        // 128 bits can, so the range of the type is checked here
                        let max = 10_i128.pow(precision as u32) - 1;
                        if parsed.unsigned_abs() > max.unsigned_abs() {
                            return Err(numeric_overflow(precision, scale));
                        }

                        builder
                            .append_value(parsed)
                            .map_err(|_| numeric_overflow(precision, scale))?
                    }
                }
            }
            ColumnBuilder::Date32(builder) => append!(builder, "date", parse_date),
            ColumnBuilder::TimestampNanosecond(builder) => {
                append!(builder, "timestamp", parse_timestamp)
            }
        };

        Ok(())
    }

    /// Bytes the value takes once it is built, including the validity bit rounded up
    /// to a byte.
    fn value_size(&self, value: Option<&str>) -> usize {
        let width = match self {
            ColumnBuilder::Boolean(_) => 1,
            ColumnBuilder::Int16(_) => 2,
            ColumnBuilder::Int32(_) | ColumnBuilder::Float32(_) | ColumnBuilder::Date32(_) => 4,
            ColumnBuilder::Int64(_)
            | ColumnBuilder::Float64(_)
            | ColumnBuilder::TimestampNanosecond(_) => 8,
            ColumnBuilder::Decimal(_, _, _) => 16,
            // The offset of the value is stored next to its bytes
            ColumnBuilder::Utf8(_, _) => value.map(|value| value.len()).unwrap_or(0) + 4,
        };

        width + 1
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Boolean(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int16(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int32(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int64(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float32(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float64(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Decimal(builder, _, _) => Arc::new(builder.finish()),
            ColumnBuilder::Utf8(builder, _) => Arc::new(builder.finish()),
            ColumnBuilder::Date32(builder) => Arc::new(builder.finish()),
            ColumnBuilder::TimestampNanosecond(builder) => Arc::new(builder.finish()),
        }
    }
}

/// What PostgreSQL says about a value which does not fit the numeric it was read for.
fn numeric_overflow(precision: usize, scale: usize) -> AppendError {
    AppendError {
        code: ErrorCode::NumericValueOutOfRange,
        message: "numeric field overflow".to_string(),
        detail: Some(format!(
            "A field with precision {}, scale {} must round to an absolute value less than 10^{}.",
            precision,
            scale,
            precision - scale,
        )),
    }
}

fn arrow_error(err: datafusion::arrow::error::ArrowError) -> AppendError {
    AppendError {
        code: ErrorCode::InternalError,
        message: err.to_string(),
        detail: None,
    }
}

fn parse_number<T: std::str::FromStr>(value: &str) -> Option<T> {
    value.parse::<T>().ok()
}

/// The spellings PostgreSQL accepts for the boolean type: any unambiguous prefix of
/// the words it knows, or a single digit. A lone "o" is ambiguous and refused.
fn parse_bool(value: &str) -> Option<bool> {
    let value = value.to_lowercase();

    // Every word starts with the empty string, which is not a spelling of anything
    if value.is_empty() {
        return None;
    }

    for (word, boolean) in [
        ("true", true),
        ("false", false),
        ("yes", true),
        ("no", false),
        ("on", true),
        ("off", false),
    ] {
        // "o" could start either of the words which begin with it
        if word.starts_with(&value) && value != "o" {
            return Some(boolean);
        }
    }

    match value.as_str() {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

fn parse_date(value: &str) -> Option<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()?;

    Some((date.num_days_from_ce() as i64 - UNIX_EPOCH_DAY) as i32)
}

fn parse_timestamp(value: &str) -> Option<i64> {
    // A column without a time zone keeps the value as it was written, and the offset
    // of the value, if it carries one, is ignored: PostgreSQL does the same
    let value = strip_timezone_offset(value);

    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .map(|date| date.and_hms_opt(0, 0, 0).expect("midnight is a valid time"))
        })
        .ok()?
        .and_utc()
        .timestamp_nanos_opt()
}

/// Cut off the time zone a timestamp may end with, such as `+03`, `-05:30` or `Z`.
fn strip_timezone_offset(value: &str) -> &str {
    if let Some(value) = value.strip_suffix(['Z', 'z']) {
        return value;
    }

    // The offset follows the time, which is where the date it cannot be confused with
    // has ended already
    let time = match value.find([' ', 'T', 't']) {
        Some(position) => position,
        None => return value,
    };

    match value[time..].find(['+', '-']) {
        Some(position) => &value[..time + position],
        None => value,
    }
}

/// Why a value could not be read as a decimal: PostgreSQL tells a value which is not
/// a number from one which does not fit the type, and so does the message it gives.
enum DecimalError {
    Syntax,
    Overflow,
}

/// Parse a decimal the way the PostgreSQL numeric input function does: a value with
/// more decimals than the type holds is rounded, not refused, and the value may be
/// written with an exponent or without an integer part. A value too wide for the
/// precision is left to the builder, which reports it as an overflow.
fn parse_decimal(value: &str, scale: usize) -> Result<i128, DecimalError> {
    let (sign, rest) = match value.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, value.strip_prefix('+').unwrap_or(value)),
    };

    // The exponent moves the decimal point, and the rest is read as if it were not there
    let (number, exponent) = match rest.split_once(['e', 'E']) {
        Some((number, exponent)) => (
            number,
            exponent.parse::<i64>().map_err(|_| DecimalError::Syntax)?,
        ),
        None => (rest, 0),
    };

    let mut parts = number.split('.');
    let integer = parts.next().unwrap_or("");
    let fraction = parts.next().unwrap_or("");
    if parts.next().is_some() || (integer.is_empty() && fraction.is_empty()) {
        return Err(DecimalError::Syntax);
    }

    if !integer.chars().all(|c| c.is_ascii_digit()) || !fraction.chars().all(|c| c.is_ascii_digit())
    {
        return Err(DecimalError::Syntax);
    }

    // Read the digits as a whole number, then move the point to where the scale of
    // the column wants it
    let all_digits = format!("{}{}", integer, fraction);
    let mut digits = all_digits.trim_start_matches('0');
    let mut shift = (scale as i64)
        .saturating_add(exponent)
        .saturating_sub(fraction.len() as i64);

    // Zero is zero at any scale, however far the exponent moves the point
    if digits.is_empty() {
        return Ok(0);
    }

    // Digits which the scale rounds away do not have to be read as a number: keep
    // the one which decides the rounding, drop the rest and move the point by as
    // many places. Deciding the magnitude first would refuse a value written with
    // more digits than 128 bits hold even when it rounds to something small
    if shift < 0 {
        let keep = (digits.len() as i64 + shift + 1).max(0) as usize;

        if keep < digits.len() {
            shift += (digits.len() - keep) as i64;
            digits = &digits[..keep];
        }
    }

    // Everything the value was made of has been rounded away
    if digits.is_empty() {
        return Ok(0);
    }

    // The trim above leaves exactly one digit to round away, and it is read on its
    // own rather than as part of the number: keeping it would make what is parsed
    // ten times the value being stored, which can be out of range when the stored
    // value is not
    let (digits, rounding) = match shift < 0 {
        true => digits.split_at(digits.len() - 1),
        false => (digits, ""),
    };

    // More digits than 128 bits hold is a value out of range, not a value which was
    // written wrongly
    let mut unscaled = match digits.is_empty() {
        true => 0,
        false => digits.parse::<i128>().map_err(|_| DecimalError::Overflow)?,
    };

    if shift >= 0 {
        let scaled = u32::try_from(shift)
            .ok()
            .and_then(|shift| 10_i128.checked_pow(shift))
            .and_then(|shift| unscaled.checked_mul(shift));

        unscaled = scaled.ok_or(DecimalError::Overflow)?;
    }

    // The digit which does not fit the scale rounds the value half away from zero,
    // as PostgreSQL rounds a numeric to its scale
    if rounding
        .as_bytes()
        .first()
        .is_some_and(|digit| *digit >= b'5')
    {
        unscaled = unscaled.checked_add(1).ok_or(DecimalError::Overflow)?;
    }

    Ok(sign * unscaled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::{
        array::{
            Array, BooleanArray, DecimalArray, Int64Array, StringArray, TimestampNanosecondArray,
        },
        datatypes::{Field, Schema},
    };

    /// Stands in for a batch in assertions which only look at the error.
    fn dummy_batch() -> RecordBatch {
        RecordBatch::new_empty(schema())
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Boolean, true),
        ]))
    }

    /// Feed the data in chunks of `chunk_size` bytes, the way CopyData messages arrive.
    fn decode_chunked(
        options: CopyOptions,
        column_indices: Vec<usize>,
        data: &str,
        chunk_size: usize,
    ) -> Result<(RecordBatch, usize), ProtocolError> {
        let mut decoder = CopyFromDecoder::new(
            "t".to_string(),
            schema(),
            column_indices,
            options,
            10 * 1024 * 1024,
        )
        .unwrap();

        for chunk in data.as_bytes().chunks(chunk_size) {
            decoder.push(chunk)?;
        }

        decoder.finish()
    }

    fn decode(
        options: CopyOptions,
        column_indices: Vec<usize>,
        data: &str,
    ) -> Result<(RecordBatch, usize), ProtocolError> {
        decode_chunked(options, column_indices, data, data.len().max(1))
    }

    fn text(data: &str) -> Result<(RecordBatch, usize), ProtocolError> {
        decode(CopyOptions::new(CopyFormat::Text), vec![0, 1, 2], data)
    }

    fn csv(data: &str) -> Result<(RecordBatch, usize), ProtocolError> {
        decode(CopyOptions::new(CopyFormat::Csv), vec![0, 1, 2], data)
    }

    /// Message and CONTEXT of the error, as a client would see them.
    fn error_of(result: Result<(RecordBatch, usize), ProtocolError>) -> (String, String) {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(ProtocolError::ErrorResponse { source, .. }) => (
                source.message.clone(),
                source.context().cloned().unwrap_or_default(),
            ),
            Err(err) => panic!("expected an ErrorResponse, got: {}", err),
        }
    }

    fn strings(batch: &RecordBatch, idx: usize) -> Vec<Option<String>> {
        let column = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column must be a string");

        (0..column.len())
            .map(|i| match column.is_null(i) {
                true => None,
                false => Some(column.value(i).to_string()),
            })
            .collect()
    }

    fn int64(batch: &RecordBatch, idx: usize) -> Vec<Option<i64>> {
        let column = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("column must be an int64");

        (0..column.len())
            .map(|i| match column.is_null(i) {
                true => None,
                false => Some(column.value(i)),
            })
            .collect()
    }

    fn booleans(batch: &RecordBatch, idx: usize) -> Vec<Option<bool>> {
        let column = batch
            .column(idx)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("column must be a boolean");

        (0..column.len())
            .map(|i| match column.is_null(i) {
                true => None,
                false => Some(column.value(i)),
            })
            .collect()
    }

    /// Build a decoder with a small budget, to check what it refuses to hold.
    fn decoder_with_budget(max_bytes: usize) -> CopyFromDecoder {
        CopyFromDecoder::new(
            "t".to_string(),
            schema(),
            vec![0, 1, 2],
            CopyOptions::new(CopyFormat::Text),
            max_bytes,
        )
        .unwrap()
    }

    #[test]
    fn test_a_large_message_is_decoded_in_one_pass() {
        // Taking a row used to move every byte behind it, which made one CopyData
        // message cost time in the square of its size
        let rows = 200_000;
        let data = "1\tone\tt\n".repeat(rows);
        let mut decoder = CopyFromDecoder::new(
            "t".to_string(),
            schema(),
            vec![0, 1, 2],
            CopyOptions::new(CopyFormat::Text),
            64 * 1024 * 1024,
        )
        .unwrap();

        let started = std::time::Instant::now();
        decoder.push(data.as_bytes()).unwrap();
        let (_, loaded) = decoder.finish().unwrap();

        assert_eq!(loaded, rows);
        // Well under a second when the work is linear, minutes when it is not
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "decoding {} rows took {:?}",
            rows,
            started.elapsed()
        );
    }

    #[test]
    fn test_data_over_the_budget_is_refused() {
        let mut decoder = decoder_with_budget(1024);
        let row = "1\tone\tt\n".repeat(20);

        // The data itself is counted, whether or not it holds complete rows
        let mut error = None;
        for _ in 0..100 {
            if let Err(err) = decoder.push(row.as_bytes()) {
                error = Some(err);
                break;
            }
        }

        assert_eq!(
            error_of(error.map(Err).unwrap_or(Ok((dummy_batch(), 0)))).0,
            "COPY data exceeds the temporary table memory limit (0 MiB)"
        );
    }

    #[test]
    fn test_values_wider_than_the_data_are_counted() {
        // Values can take more room than the data they were read from: a bigint is
        // eight bytes whatever its two characters were
        let mut decoder = decoder_with_budget(4096);
        let row = "1\t\\N\tt\n";

        let mut error = None;
        for _ in 0..1000 {
            if let Err(err) = decoder.push(row.as_bytes()) {
                error = Some(err);
                break;
            }
        }

        let (message, _) = error_of(error.map(Err).unwrap_or(Ok((dummy_batch(), 0))));
        assert_eq!(
            message,
            "COPY data exceeds the temporary table memory limit (0 MiB)"
        );
        // The values, not the data, are what filled the budget up
        assert!(decoder.accepted_bytes < decoder.max_bytes);
        assert!(decoder.built_bytes > decoder.max_bytes);
    }

    #[test]
    fn test_a_bad_row_is_not_echoed_in_full() {
        let long = "x".repeat(8192);
        let (_, context) = error_of(text(&format!("1\t{}\n", long)));

        // The row is quoted back only up to the limit PostgreSQL uses
        assert!(
            context.len() < long.len(),
            "the whole row was echoed: {} bytes",
            context.len()
        );
        assert!(
            context.ends_with("...\"") && context.len() < 1200,
            "unexpected context of {} bytes",
            context.len()
        );
    }

    #[test]
    fn test_the_scan_never_runs_past_the_data() {
        // A quoted CSV value ending with the escape character used to move the scan
        // beyond the end of the buffer
        let mut options = CopyOptions::new(CopyFormat::Csv);
        options.escape = '\\';

        assert_eq!(
            error_of(decode(options, vec![0, 1, 2], "1,\"ab\\")).0,
            "unterminated CSV quoted field"
        );
    }

    #[test]
    fn test_trailing_backslash_at_end_of_data() {
        // A backslash which escapes nothing is dropped, and the row it ends is loaded
        // instead of sending the scan around the same buffer forever
        let (batch, rows) = text("1\tone\tt\n2\ttwo\tt\\").unwrap();

        assert_eq!(rows, 2);
        assert_eq!(
            strings(&batch, 1),
            vec![Some("one".to_string()), Some("two".to_string())]
        );

        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let mut decoder = CopyFromDecoder::new(
            "t".to_string(),
            schema,
            vec![0],
            CopyOptions::new(CopyFormat::Text),
            10 * 1024 * 1024,
        )
        .unwrap();
        decoder.push(b"one\\").unwrap();
        let (batch, rows) = decoder.finish().unwrap();

        assert_eq!(rows, 1);
        assert_eq!(strings(&batch, 0), vec![Some("one".to_string())]);
    }

    #[test]
    fn test_decimal_values_are_rounded_and_measured_as_postgres_does() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal(4, 2),
            true,
        )]));
        let decode_decimal = |data: &str| {
            let mut decoder = CopyFromDecoder::new(
                "t".to_string(),
                Arc::clone(&schema),
                vec![0],
                CopyOptions::new(CopyFormat::Text),
                10 * 1024 * 1024,
            )
            .unwrap();
            decoder.push(data.as_bytes())?;

            decoder.finish()
        };

        // Digits beyond the scale are rounded away, half away from zero
        let (batch, rows) = decode_decimal("1.234\n1.235\n1.9999\n007\n0.05\n").unwrap();
        assert_eq!(rows, 5);

        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<DecimalArray>()
            .expect("column must be a decimal");
        // The unscaled values of numeric(4, 2): 1.23, 1.24, 2.00, 7.00, 0.05
        let values = (0..column.len())
            .map(|i| column.value(i))
            .collect::<Vec<_>>();
        assert_eq!(values, vec![123, 124, 200, 700, 5]);

        // A value too wide for the precision is an overflow, not a syntax error
        let error = decode_decimal("100.00\n").expect_err("must fail");
        let ProtocolError::ErrorResponse { source, .. } = &error else {
            panic!("expected an ErrorResponse, got: {}", error);
        };
        assert_eq!(source.message, "numeric field overflow");
        assert_eq!(
            source.detail().map(String::as_str),
            Some(
                "A field with precision 4, scale 2 must round to an absolute value less than 10^2."
            )
        );
    }

    #[test]
    fn test_not_null_is_enforced() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, false),
        ]));
        let decode_row = |data: &str, columns: Vec<usize>| {
            let mut decoder = CopyFromDecoder::new(
                "t".to_string(),
                Arc::clone(&schema),
                columns,
                CopyOptions::new(CopyFormat::Text),
                10 * 1024 * 1024,
            )
            .unwrap();
            decoder.push(data.as_bytes())?;

            decoder.finish()
        };

        // An explicit NULL for the column
        assert_eq!(
            error_of(decode_row("1\t\\N\n", vec![0, 1])),
            (
                "null value in column \"s\" of relation \"t\" violates not-null constraint"
                    .to_string(),
                "COPY t, line 1: \"1\t\\N\"".to_string()
            )
        );

        // And the implicit NULL of a column the statement did not list
        assert_eq!(
            error_of(decode_row("1\n", vec![0])).0,
            "null value in column \"s\" of relation \"t\" violates not-null constraint"
        );
    }

    #[test]
    fn test_timestamp_offsets_are_ignored() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let mut decoder = CopyFromDecoder::new(
            "t".to_string(),
            schema,
            vec![0],
            CopyOptions::new(CopyFormat::Text),
            10 * 1024 * 1024,
        )
        .unwrap();

        // A column without a time zone keeps the value as written, offset and all
        decoder
            .push(b"2024-03-01 10:20:30+03\n2024-03-01 10:20:30-05:30\n2024-03-01T10:20:30.5Z\n")
            .unwrap();
        let (batch, rows) = decoder.finish().unwrap();

        assert_eq!(rows, 3);
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("column must be a timestamp");
        assert_eq!(column.value(0), column.value(1));
        assert_eq!(column.value(2) - column.value(0), 500_000_000);
    }

    #[test]
    fn test_decimal_input_forms() {
        let decode_decimal = |data: &str, precision: usize, scale: usize| {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "d",
                DataType::Decimal(precision, scale),
                true,
            )]));
            let mut decoder = CopyFromDecoder::new(
                "t".to_string(),
                schema,
                vec![0],
                CopyOptions::new(CopyFormat::Text),
                10 * 1024 * 1024,
            )
            .unwrap();
            decoder.push(data.as_bytes())?;

            decoder.finish()
        };
        let unscaled = |batch: &RecordBatch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<DecimalArray>()
                .expect("column must be a decimal");

            (0..column.len())
                .map(|i| column.value(i))
                .collect::<Vec<_>>()
        };

        // A value may be written without an integer part, or with an exponent
        let (batch, rows) = decode_decimal(".5\n-.5\n1e5\n1.5e2\n1.5E-1\n", 10, 2).unwrap();
        assert_eq!(rows, 5);
        assert_eq!(unscaled(&batch), vec![50, -50, 10_000_000, 15_000, 15]);

        // A leading dot at scale zero rounds like any other value
        let (batch, _) = decode_decimal(".5\n", 4, 0).unwrap();
        assert_eq!(unscaled(&batch), vec![1]);

        // What a 128 bit decimal cannot hold is refused for what it is
        let error = decode_decimal("NaN\n", 4, 2).expect_err("must fail");
        let ProtocolError::ErrorResponse { source, .. } = &error else {
            panic!("expected an ErrorResponse, got: {}", error);
        };
        assert_eq!(source.message, "NUMERIC value \"NaN\" is not supported");

        assert_eq!(
            error_of(decode_decimal("oops\n", 4, 2)).0,
            "invalid input syntax for type numeric(4,2): \"oops\""
        );

        // Rounding away 38 digits leaves a divisor of 10^38, which is more than half
        // of what an i128 holds: the comparison must not double the remainder
        let nines = "0.".to_string() + &"9".repeat(38);
        let (batch, rows) = decode_decimal(&format!("{}\n", nines), 38, 0).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(unscaled(&batch), vec![1]);

        // A value rounded away entirely is zero, however small it was written
        let (batch, _) = decode_decimal("1e-50\n", 38, 2).unwrap();
        assert_eq!(unscaled(&batch), vec![0]);

        // Digits beyond the scale are rounded away rather than making the value too
        // large to read: only what is left of it has to fit the column
        let long = "1234567890".repeat(4);
        let (batch, _) = decode_decimal(&format!("0.{}\n", long), 10, 2).unwrap();
        assert_eq!(unscaled(&batch), vec![12]);

        let (batch, _) = decode_decimal(&format!("0.{}\n", "9".repeat(40)), 10, 2).unwrap();
        assert_eq!(unscaled(&batch), vec![100]);

        // The same value written with the point moved by an exponent instead
        let (batch, _) = decode_decimal(&format!("{}1e-40\n", long), 10, 2).unwrap();
        assert_eq!(unscaled(&batch), vec![123]);

        // Rounding looks no further than the digit which decides it
        let (batch, _) = decode_decimal(&format!("0.4{}\n", "9".repeat(39)), 38, 0).unwrap();
        assert_eq!(unscaled(&batch), vec![0]);

        // Zero is zero however far the exponent moves its point
        let (batch, rows) = decode_decimal("0e40\n-0e40\n0e999999999\n", 38, 0).unwrap();
        assert_eq!(rows, 3);
        assert_eq!(unscaled(&batch), vec![0, 0, 0]);

        // The digit rounded away is not part of what has to fit: a value of the full
        // precision with one more digit is stored, not refused
        let wide = format!("2{}", "0".repeat(37));
        let (batch, _) = decode_decimal(&format!("{}.4\n{}.5\n", wide, wide), 38, 0).unwrap();
        assert_eq!(
            unscaled(&batch),
            vec![
                20000000000000000000000000000000000000,
                20000000000000000000000000000000000001
            ]
        );

        // Rounding which carries past the precision is still out of range
        let nines = "9".repeat(38);
        assert_eq!(
            error_of(decode_decimal(&format!("{}.5\n", nines), 38, 0)).0,
            "numeric field overflow"
        );

        // A value which cannot fit is out of range, whichever way it is written, and
        // is not reported as something which was written wrongly
        for value in [
            "1e40",
            "1e999999999",
            "100000000000000000000000000000000000000",
        ] {
            let error = decode_decimal(&format!("{}\n", value), 38, 0)
                .expect_err("a value out of range must be refused");
            let ProtocolError::ErrorResponse { source, .. } = &error else {
                panic!("expected an ErrorResponse, got: {}", error);
            };

            assert_eq!(source.message, "numeric field overflow", "value {}", value);
            assert_eq!(
                source.detail().map(String::as_str),
                Some(
                    "A field with precision 38, scale 0 must round to an absolute value less than 10^38."
                ),
                "value {}",
                value
            );
        }
    }

    #[test]
    fn test_boolean_spellings() {
        let decode_bool = |data: &str| {
            let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, true)]));
            let mut decoder = CopyFromDecoder::new(
                "t".to_string(),
                schema,
                vec![0],
                CopyOptions::new(CopyFormat::Text),
                10 * 1024 * 1024,
            )
            .unwrap();
            decoder.push(data.as_bytes())?;

            decoder.finish()
        };

        // Any unambiguous prefix of the words PostgreSQL knows
        let (batch, rows) =
            decode_bool("t\ntr\ntrue\nf\nfal\nfalse\ny\nye\nn\non\noff\n1\n0\n").unwrap();
        assert_eq!(rows, 13);
        assert_eq!(
            booleans(&batch, 0),
            vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                Some(false),
            ]
        );

        // A lone "o" could be the start of either word
        assert_eq!(
            error_of(decode_bool("o\n")).0,
            "invalid input syntax for type boolean: \"o\""
        );

        // An empty field is a value, not a NULL and not a spelling of true
        assert_eq!(
            error_of(decode_bool("\n")).0,
            "invalid input syntax for type boolean: \"\""
        );
    }

    #[test]
    fn test_column_type_out_of_range_is_refused() {
        // Arrow cannot store a decimal this wide, and must not be asked to try
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Decimal(50, 2),
            true,
        )]));
        let decoder = CopyFromDecoder::new(
            "t".to_string(),
            schema,
            vec![0],
            CopyOptions::new(CopyFormat::Text),
            10 * 1024 * 1024,
        );

        assert_eq!(
            error_of(decoder.map(|_| (dummy_batch(), 0))).0,
            "COPY does not support a column of type Decimal(50, 2)"
        );
    }

    #[test]
    fn test_text_format() {
        let (batch, rows) = text("1\tone\tt\n2\t\\N\tf\n3\ttab\\there\tt\n").unwrap();

        assert_eq!(rows, 3);
        assert_eq!(int64(&batch, 0), vec![Some(1), Some(2), Some(3)]);
        assert_eq!(
            strings(&batch, 1),
            vec![Some("one".to_string()), None, Some("tab\there".to_string())]
        );
        assert_eq!(
            booleans(&batch, 2),
            vec![Some(true), Some(false), Some(true)]
        );
    }

    #[test]
    fn test_text_format_escapes() {
        // \\N is the two-character string, only a bare \N is a NULL. Octal and
        // hexadecimal escapes are supported, as in PostgreSQL
        let (batch, _) = text("1\t\\\\N\tt\n2\t\\101\\x42\\x9\tf\n3\t\\q\\\\\tt\n").unwrap();

        assert_eq!(
            strings(&batch, 1),
            vec![
                Some("\\N".to_string()),
                Some("AB\t".to_string()),
                Some("q\\".to_string()),
            ]
        );
    }

    #[test]
    fn test_text_format_end_of_data_marker() {
        let (batch, rows) = text("1\tone\tt\n\\.\n2\tignored\tt\n").unwrap();

        assert_eq!(rows, 1);
        assert_eq!(strings(&batch, 1), vec![Some("one".to_string())]);

        // In text format the marker ends the data wherever it stands, and what was
        // read before it on the line is still a row
        let (batch, rows) = text("1\tone\tt\n2\ttwo\tt\\.\n3\tthree\tt\n").unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            strings(&batch, 1),
            vec![Some("one".to_string()), Some("two".to_string())]
        );

        // The marker has to be followed by the line terminator
        assert_eq!(
            error_of(text("1\tone\tt\n2\t\\.x\tt\n")),
            (
                "end-of-copy marker corrupt".to_string(),
                "COPY t, line 2".to_string()
            )
        );

        // In CSV format the marker is only recognized at the start of a line
        let (batch, rows) = csv("1,one,t\n\\.\n2,two,f\n").unwrap();
        assert_eq!(rows, 1);
        assert_eq!(strings(&batch, 1), vec![Some("one".to_string())]);

        let (batch, rows) = csv("1,one,t\n2,x\\.y,f\n").unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            strings(&batch, 1),
            vec![Some("one".to_string()), Some("x\\.y".to_string())]
        );
    }

    #[test]
    fn test_line_endings() {
        // CRLF data
        let (batch, rows) = text("1\tone\tt\r\n2\ttwo\tf\r\n").unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            strings(&batch, 1),
            vec![Some("one".to_string()), Some("two".to_string())]
        );

        // A carriage return alone terminates the line as well
        let (_, rows) = text("1\tone\tt\r2\ttwo\tf\r").unwrap();
        assert_eq!(rows, 2);

        // But the style cannot change halfway through
        assert_eq!(
            error_of(text("1\tone\tt\n2\ttwo\tf\r\n")).0,
            "literal carriage return found in data"
        );
        assert_eq!(
            error_of(text("1\tone\tt\r\n2\ttwo\tf\n")).0,
            "literal newline found in data"
        );
        assert_eq!(
            error_of(csv("1,one,t\n2,two,f\r\n")).0,
            "unquoted carriage return found in data"
        );
    }

    #[test]
    fn test_text_format_split_across_messages() {
        let data = "1\tone\tt\n2\ttwo\tf\n\\.\n";

        for chunk_size in 1..=data.len() {
            let (batch, rows) = decode_chunked(
                CopyOptions::new(CopyFormat::Text),
                vec![0, 1, 2],
                data,
                chunk_size,
            )
            .unwrap_or_else(|err| panic!("chunk size {}: {}", chunk_size, err));

            assert_eq!(rows, 2, "chunk size {}", chunk_size);
            assert_eq!(
                strings(&batch, 1),
                vec![Some("one".to_string()), Some("two".to_string())],
                "chunk size {}",
                chunk_size
            );
        }
    }

    #[test]
    fn test_csv_format() {
        let mut options = CopyOptions::new(CopyFormat::Csv);
        options.header = true;

        // An unquoted empty value is NULL, a quoted one is an empty string, and a
        // quoted value can hold the delimiter, a doubled quote and a newline
        let data = "n,s,b\n1,\"a,b\",t\n2,,f\n3,\"\",t\n4,\"say \"\"hi\"\"\nagain\",f\n";
        let (batch, rows) = decode_chunked(options, vec![0, 1, 2], data, 7).unwrap();

        assert_eq!(rows, 4);
        assert_eq!(int64(&batch, 0), vec![Some(1), Some(2), Some(3), Some(4)]);
        assert_eq!(
            strings(&batch, 1),
            vec![
                Some("a,b".to_string()),
                None,
                Some("".to_string()),
                Some("say \"hi\"\nagain".to_string()),
            ]
        );
    }

    #[test]
    fn test_csv_quote_can_open_mid_value() {
        // As in PostgreSQL, a quote starts a quoted section wherever it appears, and
        // any quote in the value stops it from being read as a NULL
        let (batch, rows) = csv("1,ab\"c,d\",t\n2,\"\",f\n").unwrap();

        assert_eq!(rows, 2);
        assert_eq!(
            strings(&batch, 1),
            vec![Some("abc,d".to_string()), Some("".to_string())]
        );

        assert_eq!(
            error_of(csv("1,\"unterminated,t\n")).0,
            "unterminated CSV quoted field"
        );
    }

    #[test]
    fn test_csv_escape_option() {
        let mut options = CopyOptions::new(CopyFormat::Csv);
        options.escape = '\\';

        // The escape character escapes the quoting character and itself, and the
        // values come out as PostgreSQL loads them
        let (batch, rows) = decode(
            options,
            vec![0, 1],
            "1,\"a\\\\\"\n2,\"b\\\"c\"\n3,\"d\\\\e\"\n",
        )
        .unwrap();

        assert_eq!(rows, 3);
        assert_eq!(
            strings(&batch, 1),
            vec![
                Some("a\\".to_string()),
                Some("b\"c".to_string()),
                Some("d\\e".to_string()),
            ]
        );
    }

    #[test]
    fn test_csv_force_options() {
        let mut options = CopyOptions::new(CopyFormat::Csv);
        options.null_string = "NULL".to_string();
        options.force_not_null = vec!["s".to_string()];

        let (batch, _) = decode(options, vec![0, 1, 2], "1,NULL,t\n").unwrap();
        assert_eq!(strings(&batch, 1), vec![Some("NULL".to_string())]);

        let mut options = CopyOptions::new(CopyFormat::Csv);
        options.null_string = "NULL".to_string();
        options.force_null = vec!["s".to_string()];

        let (batch, _) = decode(options, vec![0, 1, 2], "1,\"NULL\",t\n").unwrap();
        assert_eq!(strings(&batch, 1), vec![None]);
    }

    #[test]
    fn test_columns_not_listed_are_null() {
        // COPY t (b, n) FROM STDIN: the s column is not loaded
        let (batch, rows) =
            decode(CopyOptions::new(CopyFormat::Text), vec![2, 0], "t\t7\n").unwrap();

        assert_eq!(rows, 1);
        assert_eq!(int64(&batch, 0), vec![Some(7)]);
        assert_eq!(strings(&batch, 1), vec![None]);
        assert_eq!(booleans(&batch, 2), vec![Some(true)]);
    }

    #[test]
    fn test_last_row_without_line_terminator() {
        let (batch, rows) = text("1\tone\tt").unwrap();

        assert_eq!(rows, 1);
        assert_eq!(strings(&batch, 1), vec![Some("one".to_string())]);
    }

    #[test]
    fn test_column_count_errors() {
        // The context quotes the row, as PostgreSQL does
        assert_eq!(
            error_of(text("1\tone\n")),
            (
                "missing data for column \"b\"".to_string(),
                "COPY t, line 1: \"1\tone\"".to_string()
            )
        );
        assert_eq!(
            error_of(text("1\tone\tt\textra\n")),
            (
                "extra data after last expected column".to_string(),
                "COPY t, line 1: \"1\tone\tt\textra\"".to_string()
            )
        );
    }

    #[test]
    fn test_value_errors_point_at_the_row() {
        assert_eq!(
            error_of(text("1\tone\tt\n2\ttwo\tf\noops\tthree\tt\n")),
            (
                "invalid input syntax for type bigint: \"oops\"".to_string(),
                "COPY t, line 3, column n: \"oops\"".to_string()
            )
        );
        assert_eq!(
            error_of(text("1\tone\tmaybe\n")),
            (
                "invalid input syntax for type boolean: \"maybe\"".to_string(),
                "COPY t, line 1, column b: \"maybe\"".to_string()
            )
        );
    }
}
