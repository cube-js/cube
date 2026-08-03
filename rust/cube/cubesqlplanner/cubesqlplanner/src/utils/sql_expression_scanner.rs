//! Minimal dialect-agnostic SQL scanner used to decide whether a SQL expression
//! needs to be wrapped in parentheses when substituted into another expression.
//!
//! Two public entry points:
//! - [`is_top_level_compound`] classifies a rendered SQL string as either atomic
//!   (safe to inline as-is) or compound (has a top-level operator and needs
//!   parentheses in arithmetic/logical contexts).
//! - [`analyze_template_arg_contexts`] analyses a `SqlCall` template and, per
//!   `{arg:N}` placeholder, reports whether the surrounding context would make
//!   a compound substitution unsafe.
//!
//! The scanner is intentionally not a full SQL parser. It tokenizes enough to
//! respect strings, comments, brackets and Cube placeholders, then decides
//! atomicity by a positive-list rule: an expression is atomic iff its top-level
//! token stream contains no operator, no operator-keyword and no top-level
//! comma outside of `CASE ... END`.

use std::collections::HashMap;

// ---------- Tokenizer ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlaceholderKind {
    Arg,
    FilterParam,
    FilterGroup,
    SecurityValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenKind {
    Word,
    QuotedIdent,
    Number,
    StringLit,
    Open(char),
    Close(char),
    Comma,
    Dot,
    Semicolon,
    CastOp,
    Operator,
    Placeholder { kind: PlaceholderKind, index: usize },
    OpaqueBraces,
    Unknown,
}

#[derive(Debug, Clone)]
struct Token<'a> {
    kind: TokenKind,
    text: &'a str,
    depth: usize,
}

struct Tokenizer<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
    depth: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            bytes: src.as_bytes(),
            pos: 0,
            depth: 0,
        }
    }

    fn peek(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    fn at_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn skip_trivia(&mut self) {
        loop {
            if self.at_eof() {
                return;
            }
            match self.peek(0).unwrap() {
                b' ' | b'\t' | b'\n' | b'\r' => {
                    self.pos += 1;
                }
                b'-' if self.peek(1) == Some(b'-') => {
                    self.pos += 2;
                    while !self.at_eof() && self.peek(0) != Some(b'\n') {
                        self.pos += 1;
                    }
                }
                b'/' if self.peek(1) == Some(b'/') => {
                    // Line comment variant in BigQuery and Snowflake.
                    self.pos += 2;
                    while !self.at_eof() && self.peek(0) != Some(b'\n') {
                        self.pos += 1;
                    }
                }
                b'/' if self.peek(1) == Some(b'*') => {
                    self.pos += 2;
                    let mut nested = 1usize;
                    while !self.at_eof() {
                        if self.peek(0) == Some(b'/') && self.peek(1) == Some(b'*') {
                            self.pos += 2;
                            nested += 1;
                        } else if self.peek(0) == Some(b'*') && self.peek(1) == Some(b'/') {
                            self.pos += 2;
                            nested -= 1;
                            if nested == 0 {
                                break;
                            }
                        } else {
                            self.pos += 1;
                        }
                    }
                }
                _ => return,
            }
        }
    }

    fn next_token(&mut self) -> Option<Token<'a>> {
        self.skip_trivia();
        if self.at_eof() {
            return None;
        }
        let offset = self.pos;
        let b = self.peek(0).unwrap();

        // String-like prefixes: N'...', E'...', B'...', R'...', X'..., also lowercased.
        if matches!(
            b,
            b'N' | b'n' | b'E' | b'e' | b'B' | b'b' | b'R' | b'r' | b'X' | b'x'
        ) && self.peek(1) == Some(b'\'')
        {
            self.pos += 1;
            return Some(self.read_quoted(offset, b'\'', true, TokenKind::StringLit));
        }

        match b {
            b'\'' => return Some(self.read_quoted(offset, b'\'', true, TokenKind::StringLit)),
            b'"' => return Some(self.read_quoted(offset, b'"', true, TokenKind::QuotedIdent)),
            b'`' => return Some(self.read_quoted(offset, b'`', false, TokenKind::QuotedIdent)),
            b'$' => {
                if let Some(tok) = self.try_read_dollar_quoted(offset) {
                    return Some(tok);
                }
                // fall through to operator handling
            }
            _ => {}
        }

        // Brackets
        if b == b'(' {
            self.pos += 1;
            self.depth += 1;
            return Some(Token {
                kind: TokenKind::Open('('),
                text: &self.src[offset..self.pos],
                depth: self.depth - 1,
            });
        }
        if b == b')' {
            self.pos += 1;
            if self.depth > 0 {
                self.depth -= 1;
            }
            return Some(Token {
                kind: TokenKind::Close(')'),
                text: &self.src[offset..self.pos],
                depth: self.depth,
            });
        }
        if b == b'[' {
            self.pos += 1;
            self.depth += 1;
            return Some(Token {
                kind: TokenKind::Open('['),
                text: &self.src[offset..self.pos],
                depth: self.depth - 1,
            });
        }
        if b == b']' {
            self.pos += 1;
            if self.depth > 0 {
                self.depth -= 1;
            }
            return Some(Token {
                kind: TokenKind::Close(']'),
                text: &self.src[offset..self.pos],
                depth: self.depth,
            });
        }
        if b == b'{' {
            if let Some(tok) = self.try_read_placeholder(offset) {
                return Some(tok);
            }
            return Some(self.read_opaque_braces(offset));
        }
        if b == b'}' {
            self.pos += 1;
            return Some(Token {
                kind: TokenKind::Unknown,
                text: &self.src[offset..self.pos],
                depth: self.depth,
            });
        }

        if b == b',' {
            self.pos += 1;
            return Some(Token {
                kind: TokenKind::Comma,
                text: &self.src[offset..self.pos],
                depth: self.depth,
            });
        }
        if b == b';' {
            self.pos += 1;
            return Some(Token {
                kind: TokenKind::Semicolon,
                text: &self.src[offset..self.pos],
                depth: self.depth,
            });
        }
        if b == b'.' {
            self.pos += 1;
            return Some(Token {
                kind: TokenKind::Dot,
                text: &self.src[offset..self.pos],
                depth: self.depth,
            });
        }

        if b == b':' && self.peek(1) == Some(b':') {
            self.pos += 2;
            return Some(Token {
                kind: TokenKind::CastOp,
                text: &self.src[offset..self.pos],
                depth: self.depth,
            });
        }

        if b.is_ascii_digit() {
            return Some(self.read_number(offset));
        }

        if is_ident_start(b) {
            return Some(self.read_word(offset));
        }

        if is_operator_byte(b) {
            while !self.at_eof() && is_operator_byte(self.peek(0).unwrap()) {
                self.pos += 1;
            }
            return Some(Token {
                kind: TokenKind::Operator,
                text: &self.src[offset..self.pos],
                depth: self.depth,
            });
        }

        // Anything else — consume one byte (or UTF-8 char) as Unknown so we keep progressing.
        let char_len = utf8_char_len(self.bytes, self.pos);
        self.pos += char_len;
        Some(Token {
            kind: TokenKind::Unknown,
            text: &self.src[offset..self.pos],
            depth: self.depth,
        })
    }

    /// Reads a quoted run delimited by `quote`. Supports the doubled-quote escape
    /// (`''`, `""`, `` `` ``) and backslash escapes. When `allow_triple` is set
    /// and the opener is tripled (`'''`/`"""`), reads until the matching triple.
    fn read_quoted(
        &mut self,
        offset: usize,
        quote: u8,
        allow_triple: bool,
        kind: TokenKind,
    ) -> Token<'a> {
        if allow_triple
            && self.peek(0) == Some(quote)
            && self.peek(1) == Some(quote)
            && self.peek(2) == Some(quote)
        {
            self.pos += 3;
            while !self.at_eof() {
                if self.peek(0) == Some(quote)
                    && self.peek(1) == Some(quote)
                    && self.peek(2) == Some(quote)
                {
                    self.pos += 3;
                    break;
                }
                self.pos += 1;
            }
            return Token {
                kind,
                text: &self.src[offset..self.pos],
                depth: self.depth,
            };
        }
        self.pos += 1;
        while !self.at_eof() {
            let c = self.peek(0).unwrap();
            if c == b'\\' {
                self.pos = (self.pos + 2).min(self.bytes.len());
                continue;
            }
            if c == quote {
                if self.peek(1) == Some(quote) {
                    self.pos += 2;
                    continue;
                }
                self.pos += 1;
                break;
            }
            self.pos += 1;
        }
        Token {
            kind,
            text: &self.src[offset..self.pos],
            depth: self.depth,
        }
    }

    fn try_read_dollar_quoted(&mut self, offset: usize) -> Option<Token<'a>> {
        // $[tag]$...$[tag]$ where tag is optional alnum/_ run.
        let mut tag_end = self.pos + 1;
        while tag_end < self.bytes.len()
            && (self.bytes[tag_end] == b'_' || self.bytes[tag_end].is_ascii_alphanumeric())
        {
            tag_end += 1;
        }
        if tag_end >= self.bytes.len() || self.bytes[tag_end] != b'$' {
            return None;
        }
        let delim_len = tag_end - self.pos + 1;
        let delim = self.src[self.pos..self.pos + delim_len].to_string();
        self.pos += delim_len;
        while self.pos + delim_len <= self.bytes.len() {
            if self.src[self.pos..self.pos + delim_len] == delim {
                self.pos += delim_len;
                return Some(Token {
                    kind: TokenKind::StringLit,
                    text: &self.src[offset..self.pos],
                    depth: self.depth,
                });
            }
            self.pos += 1;
        }
        self.pos = self.bytes.len();
        Some(Token {
            kind: TokenKind::StringLit,
            text: &self.src[offset..self.pos],
            depth: self.depth,
        })
    }

    fn try_read_placeholder(&mut self, offset: usize) -> Option<Token<'a>> {
        let mut end = self.pos + 1;
        while end < self.bytes.len() && self.bytes[end] != b'}' {
            end += 1;
        }
        if end >= self.bytes.len() {
            return None;
        }
        let inner = &self.src[self.pos + 1..end];
        let (prefix, idx_str) = inner.split_once(':')?;
        let idx: usize = idx_str.parse().ok()?;
        let kind = match prefix {
            "arg" => PlaceholderKind::Arg,
            "fp" => PlaceholderKind::FilterParam,
            "fg" => PlaceholderKind::FilterGroup,
            "sv" => PlaceholderKind::SecurityValue,
            _ => return None,
        };
        self.pos = end + 1;
        Some(Token {
            kind: TokenKind::Placeholder { kind, index: idx },
            text: &self.src[offset..self.pos],
            depth: self.depth,
        })
    }

    fn read_opaque_braces(&mut self, offset: usize) -> Token<'a> {
        self.pos += 1;
        let mut nested = 1usize;
        while self.pos < self.bytes.len() && nested > 0 {
            match self.bytes[self.pos] {
                b'{' => nested += 1,
                b'}' => nested -= 1,
                _ => {}
            }
            self.pos += 1;
        }
        Token {
            kind: TokenKind::OpaqueBraces,
            text: &self.src[offset..self.pos],
            depth: self.depth,
        }
    }

    fn read_number(&mut self, offset: usize) -> Token<'a> {
        while !self.at_eof() {
            let c = self.peek(0).unwrap();
            if c.is_ascii_digit() || c == b'.' || c == b'_' {
                self.pos += 1;
                continue;
            }
            if (c == b'e' || c == b'E')
                && matches!(self.peek(1), Some(b'+') | Some(b'-') | Some(b'0'..=b'9'))
            {
                self.pos += 1;
                if matches!(self.peek(0), Some(b'+') | Some(b'-')) {
                    self.pos += 1;
                }
                while !self.at_eof() && self.peek(0).unwrap().is_ascii_digit() {
                    self.pos += 1;
                }
                break;
            }
            break;
        }
        Token {
            kind: TokenKind::Number,
            text: &self.src[offset..self.pos],
            depth: self.depth,
        }
    }

    fn read_word(&mut self, offset: usize) -> Token<'a> {
        while !self.at_eof() && is_ident_cont(self.peek(0).unwrap()) {
            self.pos += 1;
        }
        Token {
            kind: TokenKind::Word,
            text: &self.src[offset..self.pos],
            depth: self.depth,
        }
    }
}

fn is_ident_start(b: u8) -> bool {
    b == b'_' || b.is_ascii_alphabetic() || b >= 0x80
}

fn is_ident_cont(b: u8) -> bool {
    b == b'_' || b.is_ascii_alphanumeric() || b >= 0x80
}

fn is_operator_byte(b: u8) -> bool {
    matches!(
        b,
        b'+' | b'-'
            | b'*'
            | b'/'
            | b'%'
            | b'='
            | b'<'
            | b'>'
            | b'!'
            | b'|'
            | b'&'
            | b'^'
            | b'~'
            | b'?'
            | b'@'
            | b'#'
            | b':'
    )
}

fn utf8_char_len(bytes: &[u8], pos: usize) -> usize {
    let b = bytes[pos];
    if b < 0x80 {
        1
    } else if b < 0xC0 {
        1
    } else if b < 0xE0 {
        2
    } else if b < 0xF0 {
        3
    } else {
        4
    }
    .min(bytes.len() - pos)
}

fn matches_any_keyword(word: &str, keywords: &[&str]) -> bool {
    keywords.iter().any(|kw| word.eq_ignore_ascii_case(kw))
}

fn is_operator_keyword(word: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "AND", "OR", "NOT", "IS", "LIKE", "ILIKE", "RLIKE", "BETWEEN", "IN", "SIMILAR", "OVERLAPS",
        "ESCAPE", "ANY", "ALL", "SOME", "COLLATE",
    ];
    matches_any_keyword(word, KEYWORDS)
}

fn is_case_start(word: &str) -> bool {
    word.eq_ignore_ascii_case("CASE")
}

fn is_case_end(word: &str) -> bool {
    word.eq_ignore_ascii_case("END")
}

fn is_case_keyword(word: &str) -> bool {
    const KEYWORDS: &[&str] = &["WHEN", "THEN", "ELSE", "CASE", "END"];
    matches_any_keyword(word, KEYWORDS)
}

fn tokenize_all(src: &str) -> Vec<Token<'_>> {
    let mut tokenizer = Tokenizer::new(src);
    let mut out = Vec::new();
    while let Some(t) = tokenizer.next_token() {
        out.push(t);
    }
    out
}

// ---------- Classifier: render-time atomicity ----------

/// Returns `true` if `sql` has a top-level operator (or operator-keyword) and
/// therefore needs parentheses when embedded in an operator context.
/// Atomic forms — identifier, literal, function call (optionally with
/// `OVER/FILTER/WITHIN GROUP/IGNORE NULLS/RESPECT NULLS` suffixes),
/// `CAST/EXTRACT/CASE` constructs, or an already-parenthesized expression —
/// return `false`.
pub fn is_top_level_compound(sql: &str) -> bool {
    let mut case_depth: usize = 0;
    let mut prev_significant: Option<TokenKind> = None;
    for tok in tokenize_all(sql) {
        if tok.depth != 0 {
            continue;
        }
        if let TokenKind::Word = tok.kind {
            if is_case_start(tok.text) {
                case_depth += 1;
                prev_significant = Some(tok.kind);
                continue;
            }
            if is_case_end(tok.text) && case_depth > 0 {
                case_depth -= 1;
                prev_significant = Some(tok.kind);
                continue;
            }
        }
        if case_depth > 0 {
            prev_significant = Some(tok.kind);
            continue;
        }
        match &tok.kind {
            TokenKind::Operator => return true,
            TokenKind::Comma | TokenKind::Semicolon => return true,
            TokenKind::Word => {
                if is_operator_keyword(tok.text) {
                    // Avoid treating "xxx.in" style column refs as operator.
                    if !matches!(prev_significant, Some(TokenKind::Dot)) {
                        return true;
                    }
                }
            }
            _ => {}
        }
        prev_significant = Some(tok.kind);
    }
    false
}

// ---------- Template analyzer: compile-time placeholder contexts ----------

/// Analyses an `SqlCall` template and returns, for each `{arg:N}` index present,
/// whether the surrounding context would require a compound substitution to be
/// wrapped in parentheses (`true`) or allow raw inlining (`false`).
///
/// Indices absent from the returned map were not referenced by any placeholder
/// in the template; the caller should treat them as safe by default.
pub fn analyze_template_arg_contexts(template: &str) -> HashMap<usize, bool> {
    let tokens = tokenize_all(template);
    let mut result: HashMap<usize, bool> = HashMap::new();

    for (i, tok) in tokens.iter().enumerate() {
        let idx = match &tok.kind {
            TokenKind::Placeholder {
                kind: PlaceholderKind::Arg,
                index,
            } => *index,
            _ => continue,
        };
        let unsafe_here = is_placeholder_context_unsafe(&tokens, i);
        let entry = result.entry(idx).or_insert(false);
        *entry = *entry || unsafe_here;
    }
    result
}

fn is_placeholder_context_unsafe(tokens: &[Token<'_>], idx: usize) -> bool {
    let placeholder_depth = tokens[idx].depth;

    let scan_start = match find_left_boundary(tokens, idx, placeholder_depth) {
        Some(lb) => lb + 1,
        None => 0,
    };
    let scan_end = find_right_boundary(tokens, idx, placeholder_depth).unwrap_or(tokens.len());

    let mut case_depth: usize = 0;
    for (i, tok) in tokens
        .iter()
        .enumerate()
        .skip(scan_start)
        .take(scan_end - scan_start)
    {
        if i == idx {
            continue;
        }
        if tok.depth != placeholder_depth {
            continue;
        }
        if let TokenKind::Word = tok.kind {
            if is_case_start(tok.text) {
                case_depth += 1;
                continue;
            }
            if is_case_end(tok.text) && case_depth > 0 {
                case_depth -= 1;
                continue;
            }
        }
        if case_depth > 0 {
            // Treat CASE keywords at this depth as boundaries; nothing inside a
            // sibling CASE branch can affect this placeholder.
            continue;
        }
        match &tok.kind {
            TokenKind::Operator => return true,
            TokenKind::Word => {
                if is_operator_keyword(tok.text) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// Returns the largest index `j < idx` that acts as a left boundary for the
/// placeholder's scope: an `Open` at `depth - 1`, or a `Comma/Semicolon/CASE`
/// keyword at `depth`. `None` means "scan from start of input".
fn find_left_boundary(tokens: &[Token<'_>], idx: usize, depth: usize) -> Option<usize> {
    let mut i = idx;
    while i > 0 {
        i -= 1;
        let t = &tokens[i];
        if t.depth < depth {
            return Some(i);
        }
        if t.depth == depth {
            match &t.kind {
                TokenKind::Comma | TokenKind::Semicolon => return Some(i),
                TokenKind::Word if is_case_keyword(t.text) => return Some(i),
                _ => {}
            }
        }
        if let TokenKind::Open(_) = t.kind {
            if depth > 0 && t.depth == depth - 1 {
                return Some(i);
            }
        }
    }
    None
}

/// Mirror of [`find_left_boundary`]. `None` means "scan to end of input".
fn find_right_boundary(tokens: &[Token<'_>], idx: usize, depth: usize) -> Option<usize> {
    let mut i = idx + 1;
    while i < tokens.len() {
        let t = &tokens[i];
        if t.depth < depth {
            return Some(i);
        }
        if t.depth == depth {
            match &t.kind {
                TokenKind::Comma | TokenKind::Semicolon => return Some(i),
                TokenKind::Word if is_case_keyword(t.text) => return Some(i),
                _ => {}
            }
        }
        if let TokenKind::Close(_) = t.kind {
            if depth > 0 && t.depth == depth - 1 {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    // ----- is_top_level_compound -----

    #[test]
    fn atomic_simple_identifier() {
        assert!(!is_top_level_compound("a"));
        assert!(!is_top_level_compound("users.id"));
        assert!(!is_top_level_compound("schema.table.col"));
    }

    #[test]
    fn atomic_literals() {
        assert!(!is_top_level_compound("1"));
        assert!(!is_top_level_compound("1.5"));
        assert!(!is_top_level_compound("'hello'"));
        assert!(!is_top_level_compound("NULL"));
        assert!(!is_top_level_compound("TRUE"));
        assert!(!is_top_level_compound("DATE '2020-01-01'"));
    }

    #[test]
    fn atomic_function_call() {
        assert!(!is_top_level_compound("COUNT(*)"));
        assert!(!is_top_level_compound("COALESCE(a, b)"));
        assert!(!is_top_level_compound("MAX(a + b)"));
        assert!(!is_top_level_compound("FN(a OR b, c AND d)"));
        assert!(!is_top_level_compound("schema.fn(a)"));
    }

    #[test]
    fn atomic_window_function() {
        assert!(!is_top_level_compound(
            "ROW_NUMBER() OVER (PARTITION BY x ORDER BY y)"
        ));
        assert!(!is_top_level_compound("COUNT(*) FILTER (WHERE x > 0)"));
        assert!(!is_top_level_compound(
            "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)"
        ));
        assert!(!is_top_level_compound(
            "LAST_VALUE(x IGNORE NULLS) OVER (ORDER BY y)"
        ));
    }

    #[test]
    fn atomic_cast() {
        assert!(!is_top_level_compound("CAST(x AS INT)"));
        assert!(!is_top_level_compound("EXTRACT(YEAR FROM ts)"));
        assert!(!is_top_level_compound("x::int"));
        assert!(!is_top_level_compound("x::int::text"));
    }

    #[test]
    fn atomic_case() {
        // Searched form.
        assert!(!is_top_level_compound(
            "CASE WHEN x = 1 THEN 'a' ELSE 'b' END"
        ));
        assert!(!is_top_level_compound(
            "CASE WHEN x IS NULL THEN 0 ELSE x + 1 END"
        ));
        // Simple form (expression after CASE).
        assert!(!is_top_level_compound(
            "CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END"
        ));
        assert!(!is_top_level_compound("CASE x + 1 WHEN 2 THEN 'a' END"));
    }

    #[test]
    fn atomic_parenthesized() {
        assert!(!is_top_level_compound("(a + b)"));
        assert!(!is_top_level_compound("(a OR b)"));
    }

    #[test]
    fn atomic_array_and_tuple_literal() {
        assert!(!is_top_level_compound("[1, 2, 3]"));
        assert!(!is_top_level_compound("ARRAY[1, 2, 3]"));
    }

    #[test]
    fn compound_arithmetic() {
        assert!(is_top_level_compound("a + b"));
        assert!(is_top_level_compound("a - b"));
        assert!(is_top_level_compound("a * b"));
        assert!(is_top_level_compound("a / b + c"));
    }

    #[test]
    fn compound_logical() {
        assert!(is_top_level_compound("a AND b"));
        assert!(is_top_level_compound("a OR b"));
        assert!(is_top_level_compound("NOT x"));
        assert!(is_top_level_compound("x IS NULL"));
        assert!(is_top_level_compound("x BETWEEN 1 AND 10"));
        assert!(is_top_level_compound("x LIKE '%foo%'"));
        assert!(is_top_level_compound("x IN (1, 2, 3)"));
    }

    #[test]
    fn compound_comparison() {
        assert!(is_top_level_compound("a = b"));
        assert!(is_top_level_compound("a < b"));
        assert!(is_top_level_compound("a >= b"));
        assert!(is_top_level_compound("a <> b"));
    }

    #[test]
    fn compound_string_concat() {
        assert!(is_top_level_compound("'a' || 'b'"));
    }

    #[test]
    fn strings_protect_contents() {
        assert!(!is_top_level_compound("'a OR b'"));
        assert!(!is_top_level_compound("'a + b'"));
    }

    #[test]
    fn comments_protect_contents() {
        assert!(!is_top_level_compound("users.id -- a + b"));
        assert!(!is_top_level_compound("users.id // a + b"));
        assert!(!is_top_level_compound("users.id /* a + b */"));
        assert!(!is_top_level_compound("users./* comment */id"));
    }

    #[test]
    fn dollar_quoted_strings() {
        assert!(!is_top_level_compound("$$a + b$$"));
        assert!(!is_top_level_compound("$tag$a + b$tag$"));
    }

    #[test]
    fn triple_quoted_strings() {
        assert!(!is_top_level_compound("'''a + b'''"));
        assert!(!is_top_level_compound("\"\"\"a + b\"\"\""));
    }

    #[test]
    fn mssql_bracket_identifier() {
        assert!(!is_top_level_compound("[my col]"));
        assert!(!is_top_level_compound("[a+b]"));
    }

    #[test]
    fn clickhouse_opaque_braces() {
        // {name:Type} is a CH parameter, should be treated as opaque atom.
        assert!(!is_top_level_compound("{user_id:Int64}"));
    }

    #[test]
    fn nested_case_is_atomic() {
        assert!(!is_top_level_compound(
            "CASE WHEN CASE WHEN x = 1 THEN y = 2 ELSE y = 3 END THEN 'a' ELSE 'b' END"
        ));
    }

    // ----- analyze_template_arg_contexts -----

    fn is_unsafe(template: &str, arg: usize) -> bool {
        let m = analyze_template_arg_contexts(template);
        *m.get(&arg).unwrap_or(&false)
    }

    #[test]
    fn direct_reference_is_safe() {
        assert!(!is_unsafe("{arg:0}", 0));
    }

    #[test]
    fn top_level_arithmetic_is_unsafe() {
        assert!(is_unsafe("{arg:0} + 1", 0));
        assert!(is_unsafe("1 + {arg:0}", 0));
        assert!(is_unsafe("{arg:0} * {arg:1}", 0));
        assert!(is_unsafe("{arg:0} * {arg:1}", 1));
    }

    #[test]
    fn top_level_logical_is_unsafe() {
        assert!(is_unsafe("{arg:0} AND x", 0));
        assert!(is_unsafe("{arg:0} OR {arg:1}", 0));
        assert!(is_unsafe("NOT {arg:0}", 0));
        assert!(is_unsafe("{arg:0} IS NULL", 0));
        assert!(is_unsafe("{arg:0} BETWEEN 1 AND 10", 0));
    }

    #[test]
    fn function_arg_is_safe() {
        assert!(!is_unsafe("FN({arg:0})", 0));
        assert!(!is_unsafe("FN({arg:0}, x)", 0));
        assert!(!is_unsafe("FN(x, {arg:0})", 0));
        assert!(!is_unsafe("COALESCE({arg:0}, {arg:1}, 0)", 0));
        assert!(!is_unsafe("COALESCE({arg:0}, {arg:1}, 0)", 1));
    }

    #[test]
    fn cast_arg_is_safe() {
        assert!(!is_unsafe("CAST({arg:0} AS INT)", 0));
        assert!(!is_unsafe("EXTRACT(YEAR FROM {arg:0})", 0));
    }

    #[test]
    fn function_with_inner_operator_is_unsafe() {
        assert!(is_unsafe("FN({arg:0} + 1)", 0));
        assert!(is_unsafe("FN(x, {arg:0} OR y)", 0));
    }

    #[test]
    fn join_equality_template() {
        // {arg:0} = {arg:1} — classic join condition.
        assert!(is_unsafe("{arg:0} = {arg:1}", 0));
        assert!(is_unsafe("{arg:0} = {arg:1}", 1));
    }

    #[test]
    fn case_branch_scoping() {
        // Inside a CASE branch, sibling branches should not affect scoping.
        // `{arg:0}` sits in the THEN branch; the `=` is in a sibling WHEN branch.
        assert!(!is_unsafe("CASE WHEN y = 1 THEN {arg:0} ELSE 0 END", 0));
        // But if the placeholder is inside a branch with a top-level operator,
        // that branch still produces compound context.
        assert!(is_unsafe("CASE WHEN y = 1 THEN {arg:0} + 1 ELSE 0 END", 0));
    }

    #[test]
    fn string_literals_hide_placeholders_logic() {
        // Placeholder here is inside a string literal — tokenizer swallows it.
        // The non-string template still reports correctly.
        assert!(is_unsafe("'{arg:0}' + {arg:0}", 0));
    }

    #[test]
    fn multiple_occurrences_merge_with_or() {
        // One occurrence safe, another unsafe — overall must be unsafe.
        assert!(is_unsafe("FN({arg:0}) + {arg:0}", 0));
    }
}
