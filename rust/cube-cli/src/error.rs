/// An error that came from an API response — an unsuccessful status, or a
/// body the CLI could not make sense of — as opposed to a local failure or a
/// transport error (DNS, TLS, connection refused).
///
/// It is a distinct error type so `main` can recognize it in the error chain
/// and suggest an update: skew between an older CLI and a newer API is a
/// common cause of otherwise puzzling API errors.
#[derive(Debug)]
pub struct ApiError(String);

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ApiError {}

/// Build an [`ApiError`] as an `anyhow::Error`, ready to return or wrap in
/// further context.
pub fn api_error(message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(ApiError(message.into()))
}

/// `bail!` for API failures: returns an [`ApiError`] instead of a plain one.
macro_rules! api_bail {
    ($($arg:tt)*) => {
        return Err($crate::error::api_error(format!($($arg)*)))
    };
}

pub(crate) use api_bail;

/// True when `err`, or anything it wraps, came from an API response.
pub fn is_api_error(err: &anyhow::Error) -> bool {
    err.chain().any(|e| e.is::<ApiError>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::{anyhow, Context as _};

    #[test]
    fn recognizes_api_errors() {
        assert!(is_api_error(&api_error("GET /x failed with 400")));
    }

    #[test]
    fn recognizes_api_errors_wrapped_in_context() {
        let err = Err::<(), _>(api_error("GET /x failed with 400"))
            .context("while listing deployments")
            .unwrap_err();
        assert!(is_api_error(&err));
    }

    #[test]
    fn ignores_other_errors() {
        assert!(!is_api_error(&anyhow!("no config file found")));
        assert!(!is_api_error(
            &anyhow!("connection refused").context("request to https://x failed")
        ));
    }

    #[test]
    fn displays_the_message_it_was_built_with() {
        assert_eq!(
            api_error("GET /x failed with 400").to_string(),
            "GET /x failed with 400"
        );
    }
}
