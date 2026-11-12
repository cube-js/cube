use crate::CubeError;
use std::borrow::Cow;
use std::ffi::OsString;
use std::path::PathBuf;

pub fn path_to_string(p: PathBuf) -> Result<String, CubeError> {
    os_to_string(p.into_os_string())
}

pub fn os_to_string(s: OsString) -> Result<String, CubeError> {
    match s.into_string() {
        Ok(s) => Ok(s),
        Err(s) => Err(CubeError::internal(format!(
            "Cannot convert string to UTF8: {:?}",
            s
        ))),
    }
}

/// Drops the query string of a URL location, which may carry pre-signed credentials,
/// so the location can be logged. Locations without a scheme are returned as is.
pub fn redact_location(location: &str) -> Cow<'_, str> {
    match location.split_once('?') {
        Some((base, _)) if base.contains("://") => Cow::Owned(format!("{}?<redacted>", base)),
        _ => Cow::Borrowed(location),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_location_strips_query() {
        assert_eq!(
            redact_location(
                "https://b.s3.amazonaws.com/a/b.csv?X-Amz-Signature=s&X-Amz-Credential=c"
            ),
            "https://b.s3.amazonaws.com/a/b.csv?<redacted>"
        );
        assert_eq!(redact_location("https://host/a.csv"), "https://host/a.csv");
        assert_eq!(redact_location("temp://a.csv"), "temp://a.csv");
        assert_eq!(redact_location("/tmp/a?b.csv"), "/tmp/a?b.csv");
    }
}
