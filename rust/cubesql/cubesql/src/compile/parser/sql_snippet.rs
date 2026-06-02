//! Rendering of source-location snippets for SQL errors.
//!
//! Parsing happens in two phases (see `parse_sql_to_statements`): tokenize, then
//! parse. This module turns a failure of either phase into a human-friendly
//! fragment of the original query with a `^` pointer, e.g.:
//!
//! ```text
//!   2 |                 orders_transactions.status
//!   3 |                 MEASURE(orders_transactions.count)
//!     |                 ^
//! ```
//!
//! - Tokenizer errors carry a structured [`Location`], used directly.
//! - Parser errors only carry a message string with a `" at Line: N, Column: N"`
//!   suffix; we extract that location and map it onto the token stream to place
//!   the caret on the most relevant token.
//!
//! The renderer accepts a [`Span`] directly so it can be reused later for
//! semantic errors that obtain a span from `node.span()`.

use std::sync::LazyLock;

use regex::Regex;
use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan, TokenizerError};

/// Number of source lines shown before the line that contains the error.
const CONTEXT_LINES: u64 = 2;

/// Maximum number of source characters shown per line. Longer lines are cropped
/// to a horizontal window around the caret.
const MAX_LINE_WIDTH: usize = 64;

/// Extract the error [`Location`] from a parser error message.
pub(crate) fn location_from_parser_error(msg: &str) -> Option<Location> {
    static LOCATION_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"at Line: (\d+), Column: (\d+)").unwrap());

    let caps = LOCATION_RE.captures_iter(msg).last()?;
    let line = caps.get(1)?.as_str().parse::<u64>().ok()?;
    let column = caps.get(2)?.as_str().parse::<u64>().ok()?;

    Some(Location::new(line, column))
}

pub(crate) fn render_query_snippet(sql: &str, span: Span) -> Option<String> {
    let start = span.start;
    let end = span.end;

    if start.line == 0 || start.column == 0 {
        return None;
    }

    let lines: Vec<&str> = sql.split('\n').collect();

    // `start.line` is 1-based; bail out if it is out of range.
    let start_idx = (start.line as usize).checked_sub(1)?;
    if start_idx >= lines.len() {
        return None;
    }

    // Widest line number we will print determines the gutter width.
    let max_line_no = start.line;
    let gutter_width = max_line_no.to_string().len();

    let first_context_line = start.line.saturating_sub(CONTEXT_LINES).max(1);

    // Caret geometry on the error line, in 0-based char offsets.
    let error_chars: Vec<char> = lines[start_idx].chars().collect();
    let error_line_len = error_chars.len() as u64;

    // Clamp the start column into the line (columns are 1-based).
    let start_col = start.column.min(error_line_len + 1);
    let caret_start = (start_col - 1) as usize;

    // Caret width (span ends are inclusive here):
    // - same-line span: cover start..=end, clamped to the line
    // - multi-line span: underline from start column to the end of the line
    // - point location (start == end): a single caret
    let caret_width = if end.line == start.line && end.column >= start.column {
        let end_col = end.column.min(error_line_len.max(start_col));
        ((end_col - start.column) + 1) as usize
    } else if end.line > start.line {
        ((error_line_len + 1).saturating_sub(start_col)).max(1) as usize
    } else {
        1
    };
    let caret_end = caret_start + caret_width.max(1); // exclusive, 0-based

    // Horizontal window shared by every printed line so columns stay aligned.
    // Only crops when some displayed line is wider than MAX_LINE_WIDTH.
    let widest = (first_context_line..=start.line)
        .map(|n| lines[(n - 1) as usize].chars().count())
        .max()
        .unwrap_or(0);

    let (win_start, win_end) = if widest <= MAX_LINE_WIDTH {
        (0, widest)
    } else {
        // Keep the caret roughly centered within the window.
        let half = MAX_LINE_WIDTH / 2;
        let mut ws = caret_start.saturating_sub(half);
        let mut we = ws + MAX_LINE_WIDTH;
        if we > widest {
            we = widest;
            ws = we.saturating_sub(MAX_LINE_WIDTH);
        }
        (ws, we)
    };

    let mut out = String::new();

    for line_no in first_context_line..=start.line {
        let chars: Vec<char> = lines[(line_no - 1) as usize].chars().collect();
        let text = crop_to_window(&chars, win_start, win_end);
        out.push_str(&format!(
            "{:>width$} | {}\n",
            line_no,
            text,
            width = gutter_width
        ));
    }

    // Pointer line: blank gutter, spaces up to the caret (relative to the window
    // start), then carets.
    let visible_caret_start = caret_start.saturating_sub(win_start);
    let visible_caret_end = caret_end.min(win_end).saturating_sub(win_start);
    let visible_caret_width = visible_caret_end.saturating_sub(visible_caret_start).max(1);
    let lead = visible_caret_start;

    out.push_str(&format!(
        "{:>width$} | {}{}",
        "",
        " ".repeat(lead),
        "^".repeat(visible_caret_width),
        width = gutter_width
    ));

    Some(out)
}

/// Crop a line's characters to `[win_start, win_end)`.
fn crop_to_window(chars: &[char], win_start: usize, win_end: usize) -> String {
    let len = chars.len();
    let s = win_start.min(len);
    let e = win_end.min(len);
    chars[s..e].iter().collect()
}

/// Extend a point location into a span covering the contiguous run of
/// non-whitespace characters starting at it.
fn extend_to_token_end(sql: &str, span: Span) -> Span {
    let Some(line_idx) = (span.start.line as usize).checked_sub(1) else {
        return span;
    };

    let Some(line) = sql.split('\n').nth(line_idx) else {
        return span;
    };

    let chars: Vec<char> = line.chars().collect();
    if chars.is_empty() {
        return span;
    }

    let start_i = (span.start.column as usize)
        .saturating_sub(1)
        .min(chars.len() - 1);
    let mut end_i = start_i;
    while end_i + 1 < chars.len() && !chars[end_i + 1].is_whitespace() {
        end_i += 1;
    }
    let end = Location::new(span.start.line, (end_i + 1) as u64);
    Span::new(span.start, end)
}

fn is_whitespace_token(token: &TokenWithSpan) -> bool {
    matches!(token.token, Token::Whitespace(_))
}

fn token_end(token: &TokenWithSpan) -> Location {
    let end = token.span.end;
    Location::new(end.line, end.column.saturating_sub(1).max(1))
}

fn last_token_end(tokens: &[TokenWithSpan]) -> Option<Location> {
    tokens
        .iter()
        .rev()
        .find(|t| !is_whitespace_token(t))
        .map(token_end)
}

/// Map the location the parser reported (the start of the token it stopped on)
/// onto the most relevant token, à la Postgres' "at or near ...".
fn place_parser_caret(tokens: &[TokenWithSpan], reported: Location) -> Location {
    let Some(idx) = tokens.iter().position(|t| t.span.start == reported) else {
        return reported;
    };
    let found = &tokens[idx];

    // A statement terminator means the real problem is the missing input *before*
    // it, so point at the end of the previous significant token (e.g. the `M` of
    // a dangling `FROM;`).
    if matches!(found.token, Token::SemiColon) {
        if let Some(prev) = tokens[..idx].iter().rev().find(|t| !is_whitespace_token(t)) {
            return token_end(prev);
        }
        return reported;
    }

    // A delimiter glued to a preceding word (e.g. the `(` in `MEASURE(`): the
    // word is the offending token, so point at its start.
    if !matches!(found.token, Token::Word(_)) {
        if idx > 0 {
            let prev = &tokens[idx - 1];
            if matches!(prev.token, Token::Word(_)) {
                return prev.span.start;
            }
        }
        return found.span.start;
    }

    // Otherwise point at the start of the token the parser stopped on.
    found.span.start
}

pub(crate) fn snippet_for_parser_error(
    sql: &str,
    tokens: &[TokenWithSpan],
    err_msg: &str,
) -> Option<String> {
    let location = if let Some(reported) = location_from_parser_error(err_msg) {
        place_parser_caret(tokens, reported)
    } else if err_msg.contains("found: EOF") {
        last_token_end(tokens)?
    } else {
        return None;
    };

    render_query_snippet(sql, Span::new(location, location))
}

pub(crate) fn snippet_for_tokenizer_error(sql: &str, err: &TokenizerError) -> Option<String> {
    if err.location.line == 0 {
        return None;
    }

    let span = extend_to_token_end(sql, Span::new(err.location, err.location));
    render_query_snippet(sql, span)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::{dialect::PostgreSqlDialect, tokenizer::Tokenizer};

    fn tokens(sql: &str) -> Vec<TokenWithSpan> {
        Tokenizer::new(&PostgreSqlDialect {}, sql)
            .tokenize_with_location()
            .unwrap()
    }

    #[test]
    fn test_location_from_parser_error() {
        let loc = location_from_parser_error(
            "Expected: an expression, found: MEASURE at Line: 3, Column: 17",
        )
        .expect("should extract a location");
        assert_eq!(loc, Location::new(3, 17));
    }

    #[test]
    fn test_location_from_parser_error_takes_last() {
        // Defensive: if several locations appear, the most specific (last) wins.
        let loc = location_from_parser_error(
            "outer at Line: 1, Column: 1 -- inner at Line: 5, Column: 9",
        )
        .unwrap();
        assert_eq!(loc, Location::new(5, 9));
    }

    #[test]
    fn test_location_from_parser_error_missing() {
        assert!(location_from_parser_error("some error without a location").is_none());
    }

    #[test]
    fn test_render_point_caret() {
        let sql = "SELECT a\nFROM t\nWHERE";
        let span = Span::new(Location::new(3, 1), Location::new(3, 1));
        let snippet = render_query_snippet(sql, span).unwrap();
        let expected = "\
1 | SELECT a
2 | FROM t
3 | WHERE
  | ^";
        assert_eq!(snippet, expected);
    }

    #[test]
    fn test_render_same_line_range() {
        let sql = "SELECT foobar FROM t";
        // Underline `foobar` (columns 8..=13).
        let span = Span::new(Location::new(1, 8), Location::new(1, 13));
        let snippet = render_query_snippet(sql, span).unwrap();
        let expected = "\
1 | SELECT foobar FROM t
  |        ^^^^^^";
        assert_eq!(snippet, expected);
    }

    #[test]
    fn test_render_multiline_span() {
        let sql = "SELECT a,\n       b\nFROM t";
        // Span starting on line 1 col 8 and ending on line 2: underline to EOL of line 1.
        let span = Span::new(Location::new(1, 8), Location::new(2, 8));
        let snippet = render_query_snippet(sql, span).unwrap();
        let expected = "\
1 | SELECT a,
  |        ^^";
        assert_eq!(snippet, expected);
    }

    #[test]
    fn test_render_out_of_bounds() {
        let sql = "SELECT 1";
        let span = Span::new(Location::new(5, 1), Location::new(5, 1));
        assert!(render_query_snippet(sql, span).is_none());
    }

    #[test]
    fn test_render_empty_span() {
        let sql = "SELECT 1";
        assert!(render_query_snippet(sql, Span::empty()).is_none());
    }

    #[test]
    fn test_parser_error_snaps_delimiter_to_token_start() {
        // The parser stops on the `(` (column 12) after the un-separated
        // `MEASURE`; the caret must snap back to the start of `MEASURE`.
        let sql = "SELECT DISTINCT\n    orders.status\n    MEASURE(orders.count)\nFROM orders";
        let err = "Expected: end of statement, found: ( at Line: 3, Column: 12";
        let snippet = snippet_for_parser_error(sql, &tokens(sql), err).unwrap();
        let expected = "\
1 | SELECT DISTINCT
2 |     orders.status
3 |     MEASURE(orders.count)
  |     ^";
        assert_eq!(snippet, expected);
    }

    #[test]
    fn test_parser_error_terminator_points_at_end_of_last_token() {
        // `SELECT FROM;` stops on the `;`; the caret should sit on the end of
        // `FROM` (the `M`), not snap to its start.
        let sql = "SELECT FROM;";
        let err = "Expected: identifier, found: ; at Line: 1, Column: 12";
        let snippet = snippet_for_parser_error(sql, &tokens(sql), err).unwrap();
        let expected = "\
1 | SELECT FROM;
  |           ^";
        assert_eq!(snippet, expected);
    }

    #[test]
    fn test_parser_error_eof_points_at_end_of_last_token() {
        // `SELECT FROM` fails at EOF (no location); anchor at the end of `FROM`.
        let sql = "SELECT FROM";
        let err = "Expected: identifier, found: EOF";
        let snippet = snippet_for_parser_error(sql, &tokens(sql), err).unwrap();
        let expected = "\
1 | SELECT FROM
  |           ^";
        assert_eq!(snippet, expected);
    }

    #[test]
    fn test_tokenizer_error_underlines_literal() {
        // The tokenizer reports only the opening quote; underline the whole
        // visible literal `'abc` up to the next whitespace.
        let sql = "SELECT 'abc FROM t";
        let err = TokenizerError {
            message: "Unterminated string literal".to_string(),
            location: Location::new(1, 8),
        };
        let snippet = snippet_for_tokenizer_error(sql, &err).unwrap();
        let expected = "\
1 | SELECT 'abc FROM t
  |        ^^^^";
        assert_eq!(snippet, expected);
    }
}
