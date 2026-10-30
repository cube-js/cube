use std::borrow::Cow;

const REDACTED: &str = "<redacted>";

/// Replaces the userinfo and the query string of every http(s) URL in `text` with `<redacted>`:
/// presigned S3, GCS and Azure SAS URLs carry their credential and signature in the query.
/// Everything else, `s3://` and `stream://` locations included, is returned unchanged.
pub fn redact_url_credentials(text: &str) -> Cow<'_, str> {
    let mut redacted: Option<String> = None;
    let mut copied = 0;
    let mut pos = 0;
    while let Some((authority_start, end)) = next_http_url(text, pos) {
        pos = end;
        let url = &text[authority_start..end];
        let authority_len = url
            .find(|c: char| matches!(c, '/' | '?' | '#') || c.is_whitespace())
            .unwrap_or(url.len());
        let userinfo_end = url[..authority_len].rfind('@').map(|at| at + 1);
        let query_start = url.find('?');
        if userinfo_end.is_none() && query_start.is_none() {
            continue;
        }

        let out = redacted.get_or_insert_with(|| String::with_capacity(text.len()));
        out.push_str(&text[copied..authority_start]);
        if userinfo_end.is_some() {
            out.push_str(REDACTED);
            out.push('@');
        }
        let rest = &url[userinfo_end.unwrap_or(0)..];
        match rest.find('?') {
            Some(query_start) => {
                out.push_str(&rest[..=query_start]);
                out.push_str(REDACTED);
            }
            None => out.push_str(rest),
        }
        copied = end;
    }

    match redacted {
        None => Cow::Borrowed(text),
        Some(mut out) => {
            out.push_str(&text[copied..]);
            Cow::Owned(out)
        }
    }
}

/// Finds the next http(s) URL at or after `from`; returns where its authority starts and where it
/// ends. Only quotes, a backslash or a line break end it: a space in a path must not leave the
/// query after it unredacted, so over-redacting trailing text is the accepted cost.
fn next_http_url(text: &str, from: usize) -> Option<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut search = from;
    while let Some(offset) = bytes[search..]
        .iter()
        .position(|b| b.eq_ignore_ascii_case(&b'h'))
    {
        let start = search + offset;
        let scheme_len = if starts_with_ignore_ascii_case(&bytes[start..], b"https://") {
            8
        } else if starts_with_ignore_ascii_case(&bytes[start..], b"http://") {
            7
        } else {
            search = start + 1;
            continue;
        };
        let authority_start = start + scheme_len;
        let end = text[authority_start..]
            .find(|c: char| matches!(c, '"' | '\'' | '`' | '\\' | '\n' | '\r'))
            .map_or(text.len(), |len| authority_start + len);
        return Some((authority_start, end));
    }
    None
}

fn starts_with_ignore_ascii_case(bytes: &[u8], prefix: &[u8]) -> bool {
    bytes.len() >= prefix.len() && bytes[..prefix.len()].eq_ignore_ascii_case(prefix)
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
