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

    #[test]
    fn redacts_presigned_query_strings() {
        for (url, expected) in [
            (
                "https://bucket.s3.eu-west-1.amazonaws.com/x/data.csv.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261029%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Date=20261029T000000Z&X-Amz-Expires=3600&X-Amz-Security-Token=tokenexample&X-Amz-Signature=deadbeef",
                "https://bucket.s3.eu-west-1.amazonaws.com/x/data.csv.gz?<redacted>",
            ),
            (
                "https://bucket.s3.amazonaws.com/x/data.csv?AWSAccessKeyId=AKIAEXAMPLE&Expires=1761696000&Signature=deadbeef",
                "https://bucket.s3.amazonaws.com/x/data.csv?<redacted>",
            ),
            (
                "https://storage.googleapis.com/bucket/x/data.csv.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=example%40project.iam.gserviceaccount.com%2F20261029%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20261029T000000Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=deadbeef",
                "https://storage.googleapis.com/bucket/x/data.csv.gz?<redacted>",
            ),
            (
                "https://account.blob.core.windows.net/container/x/data.csv.gz?sv=2022-11-02&ss=b&srt=co&sp=r&se=2026-10-30T00:00:00Z&skoid=00000000-0000-0000-0000-000000000000&sig=deadbeef%3D",
                "https://account.blob.core.windows.net/container/x/data.csv.gz?<redacted>",
            ),
            (
                "HTTPS://bucket.s3.amazonaws.com/x/data.csv?Signature=deadbeef",
                "HTTPS://bucket.s3.amazonaws.com/x/data.csv?<redacted>",
            ),
            (
                "https://user:secretexample@host.example.com/x/data.csv",
                "https://<redacted>@host.example.com/x/data.csv",
            ),
            (
                "http://user:secretexample@host.example.com:8080/x/data.csv?sig=deadbeef#part",
                "http://<redacted>@host.example.com:8080/x/data.csv?<redacted>",
            ),
            (
                "https://account.blob.core.windows.net/container/my file.csv?sig=deadbeef",
                "https://account.blob.core.windows.net/container/my file.csv?<redacted>",
            ),
        ] {
            assert_eq!(redact_url_credentials(url), expected);
            assert_eq!(redact_url_credentials(expected), expected, "not idempotent");
        }
    }

    #[test]
    fn redacts_every_url_in_text() {
        let sql = "CREATE TABLE s.t (a int) LOCATION 'https://h1.example.com/a.csv?X-Amz-Signature=deadbeef', 'https://h2.example.com/b.csv?sig=deadbeef', 'stream://kafka/topic'";
        assert_eq!(
            redact_url_credentials(sql),
            "CREATE TABLE s.t (a int) LOCATION 'https://h1.example.com/a.csv?<redacted>', 'https://h2.example.com/b.csv?<redacted>', 'stream://kafka/topic'"
        );

        let debug = format!("{:?}", vec!["https://h.example.com/a.csv?sig=deadbeef"]);
        assert_eq!(
            redact_url_credentials(&debug),
            r#"["https://h.example.com/a.csv?<redacted>"]"#
        );
    }

    #[test]
    fn redacts_each_url_on_its_own() {
        for (text, expected) in [
            (
                "https://a.example.com/x https://user:secretexample@b.example.com/y",
                "https://a.example.com/x https://<redacted>@b.example.com/y",
            ),
            (
                "https://a.example.com/x and https://b.example.com/y?sig=deadbeef done",
                "https://a.example.com/x and https://b.example.com/y?<redacted> done",
            ),
            (
                "https://a.example.com/x?redirect=https://user:secretexample@b.example.com/y&sig=deadbeef",
                "https://a.example.com/x?<redacted>",
            ),
            (
                "url (https://a.example.com/x?sig=deadbeef) after 3 retries",
                "url (https://a.example.com/x?<redacted> after 3 retries",
            ),
            (
                "url (https://a.example.com/x?<redacted>) after 3 retries",
                "url (https://a.example.com/x?<redacted> after 3 retries",
            ),
        ] {
            assert_eq!(redact_url_credentials(text), expected);
        }
    }

    #[test]
    fn leaves_other_text_unchanged() {
        for text in [
            "stream://kafka/topic/0",
            "temp://upload-1.csv",
            "s3://bucket/key.csv?versionId=1",
            "gs://bucket/key.csv",
            "/data/tmp/file.csv.gz",
            "https://host.example.com/x/data.csv",
            "http://localhost:8080/data.csv",
            "httpfoo?bar",
            "Internal: Unable to import from http location, status code: 403 Forbidden",
            "",
        ] {
            assert!(
                matches!(redact_url_credentials(text), Cow::Borrowed(t) if t == text),
                "{text}"
            );
        }
    }
}
