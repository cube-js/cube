use std::borrow::Cow;

const REDACTED: &str = "<redacted>";

/// Replaces the userinfo and the query string of every http(s) URL in `text` with `<redacted>`:
/// presigned S3, GCS and Azure SAS URLs carry their credential and signature in the query.
/// Everything else, `s3://` and `stream://` locations included, is returned unchanged.
pub fn redact_url_credentials(text: &str) -> Cow<'_, str> {
    let bytes = text.as_bytes();
    let mut redacted: Option<String> = None;
    let mut copied = 0;
    let mut pos = 0;
    while let Some(authority_start) = find_scheme_end(bytes, pos) {
        // A path may hold a space (a blob name), so only a quote, a backslash, a line break or the
        // next URL ends it. A query never holds a raw space but may hold a raw URL, so it ends at
        // whitespace alone.
        let span_end = text[authority_start..]
            .find(|c: char| matches!(c, '"' | '\'' | '`' | '\\' | '\n' | '\r'))
            .map_or(text.len(), |len| authority_start + len);
        let span = &text[authority_start..span_end];
        let next_url = (1..span.len()).find(|&i| scheme_len(&span.as_bytes()[i..]).is_some());
        let query_start = span
            .find('?')
            .filter(|&query_start| next_url.map_or(true, |next| query_start < next));
        let url_end = match query_start {
            Some(query_start) => span[query_start..]
                .find(char::is_whitespace)
                .map_or(span.len(), |len| query_start + len),
            None => next_url.unwrap_or(span.len()),
        };
        pos = authority_start + url_end;

        let url = &span[..url_end];
        let authority_len = url
            .find(|c: char| matches!(c, '/' | '?' | '#') || c.is_whitespace())
            .unwrap_or(url.len());
        let userinfo_end = url[..authority_len].rfind('@').map(|at| at + 1);
        if userinfo_end.is_none() && query_start.is_none() {
            continue;
        }

        let out = redacted.get_or_insert_with(|| String::with_capacity(text.len()));
        out.push_str(&text[copied..authority_start]);
        let path_start = match userinfo_end {
            Some(userinfo_end) => {
                out.push_str(REDACTED);
                out.push('@');
                userinfo_end
            }
            None => 0,
        };
        copied = match query_start {
            Some(query_start) => {
                out.push_str(&url[path_start..=query_start]);
                out.push_str(REDACTED);
                pos
            }
            None => authority_start + path_start,
        };
    }

    match redacted {
        None => Cow::Borrowed(text),
        Some(mut out) => {
            out.push_str(&text[copied..]);
            Cow::Owned(out)
        }
    }
}

fn find_scheme_end(bytes: &[u8], from: usize) -> Option<usize> {
    (from..bytes.len()).find_map(|i| scheme_len(&bytes[i..]).map(|len| i + len))
}

fn scheme_len(bytes: &[u8]) -> Option<usize> {
    [&b"https://"[..], &b"http://"[..]]
        .into_iter()
        .find(|scheme| {
            bytes.len() >= scheme.len() && bytes[..scheme.len()].eq_ignore_ascii_case(scheme)
        })
        .map(|scheme| scheme.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_derive::Deserialize;

    /// Shared with the JS driver's `redactUrlCredentials` tests, so the two ports cannot drift.
    #[derive(Deserialize)]
    struct Cases {
        urls: Vec<(String, String)>,
        text: Vec<(String, String)>,
        unchanged: Vec<String>,
    }

    fn cases() -> Cases {
        serde_json::from_str(include_str!("redact_cases.json")).unwrap()
    }

    #[test]
    fn redacts_presigned_urls() {
        for (url, expected) in cases().urls {
            assert_eq!(redact_url_credentials(&url), expected);
            assert_eq!(
                redact_url_credentials(&expected),
                expected,
                "not idempotent"
            );
        }
    }

    #[test]
    fn redacts_each_url_in_text() {
        for (text, expected) in cases().text {
            assert_eq!(redact_url_credentials(&text), expected);
        }
        let debug = format!("{:?}", vec!["https://h.example.com/a.csv?sig=deadbeef"]);
        assert_eq!(
            redact_url_credentials(&debug),
            r#"["https://h.example.com/a.csv?<redacted>"]"#
        );
    }

    #[test]
    fn leaves_other_text_unchanged() {
        for text in cases().unchanged {
            assert!(
                matches!(redact_url_credentials(&text), Cow::Borrowed(t) if t == text),
                "{text}"
            );
        }
    }
}
