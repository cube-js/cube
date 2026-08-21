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
/// The line is whether an HTTP answer arrived at all — not whose fault it was.
/// `client::TransportError` is out because nothing answered: DNS, TLS, a dropped
/// connection, so there is no API verdict to form a hypothesis about. A 502 or
/// 503 is IN, even though a wobbling proxy says as little about versions as a
/// dead socket does. Keeping it is a decision, not an oversight: the status
/// alone cannot separate a gateway's 502 from one the application itself
/// produced, nor a load-shedding 503 from a deploy in progress, and the CLI
/// knows nothing of the topology that could. What keeps the wide set from
/// becoming noise is that the hint is a hypothesis rather than a diagnosis: no
/// arm of `update::api_error_hint` claims an update will fix anything — two say
/// "may already fix it", the third only reports that a newer release exists and
/// invites a retry — and it says nothing at all once the check confirms this
/// binary is already current.
///
/// Deliberately NOT `client::is_transient`, which nearly coincides with the
/// narrower line one might draw here: that one answers "could retrying work",
/// and answers yes for a 500 — the status most worth hinting on of any, since a
/// 500 is often the server choking on a shape this CLI sent. Retryable and
/// version-suspect are different questions about the same status.
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
    fn an_answer_the_api_may_not_have_authored_is_still_an_answer() {
        // The gateway statuses are deliberately IN. Pinned because the reason is
        // an argument rather than an obvious truth — see `is_api_error` — and a
        // reader who thinks the line should be `client::is_transient` would
        // otherwise narrow it and lose the 500 along with them.
        for status in [
            reqwest::StatusCode::BAD_GATEWAY,
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            reqwest::StatusCode::GATEWAY_TIMEOUT,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            let err = anyhow::Error::new(crate::client::ApiError {
                status,
                method: reqwest::Method::GET,
                path: "/api/v1/deployments".to_string(),
                detail: String::new(),
            });
            assert!(is_api_error(&err), "{status} should be an API error");
        }
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
