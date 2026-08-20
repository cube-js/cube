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
///
/// Two types answer to this. [`ApiError`] carries a rendered message and
/// nothing else, which is all `oauth` and the unreadable-body checks need.
/// [`crate::client::ApiError`] is the richer one every unsuccessful status
/// becomes: it keeps the status, method and path so callers can branch on them
/// — `get_optional` recognizes a 404, `client::is_transient` decides whether a
/// wait loop may retry. Both mean "the API answered and the answer was a
/// failure", so both belong here; the hint in `main` is the only reader and it
/// does not care which of the two shapes carried it.
///
/// `client::TransportError` deliberately does NOT count: a request that never
/// got an answer says nothing about API skew, so an update hint under it would
/// be advice about the wrong thing.
pub fn is_api_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|e| e.is::<ApiError>() || e.is::<crate::client::ApiError>())
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
    fn recognizes_the_client_s_typed_api_error() {
        // The shape every unsuccessful status becomes. `client`'s own tests
        // cover it end to end from a response; this pins the recognition
        // itself, which is what `main` calls to decide on the update hint.
        let err = anyhow::Error::new(crate::client::ApiError {
            status: reqwest::StatusCode::BAD_REQUEST,
            method: reqwest::Method::GET,
            path: "/api/v1/deployments".to_string(),
            detail: "unknown field".to_string(),
        });
        assert!(is_api_error(&err));
        assert!(is_api_error(&err.context("while listing deployments")));
    }

    #[test]
    fn a_request_that_never_got_an_answer_is_not_an_api_error() {
        // Nothing was learned about the API, so `cube update` is not the advice.
        let err = anyhow::Error::new(crate::client::TransportError {
            url: "https://x.cubecloud.dev".to_string(),
            source: "dns error".to_string(),
        });
        assert!(!is_api_error(&err));
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
