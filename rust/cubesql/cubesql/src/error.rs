use cubeclient::apis::default_api::{LoadV1Error, MetaV1Error};
use datafusion::arrow;
use log::SetLoggerError;
use sqlparser::parser::ParserError;
use std::{
    any::Any,
    backtrace::Backtrace,
    collections::HashMap,
    fmt,
    fmt::{Debug, Formatter},
    num::ParseIntError,
};
use tokio::{sync::mpsc::error::SendError, time::error::Elapsed};

/// Canonical spelling of the queue's "not finished yet" signal. It is the wire
/// value the api-gateway sends and the event name query history reads, so it is
/// also what a `ContinueWait` error normalizes back to.
pub const CONTINUE_WAIT_MESSAGE: &str = "Continue wait";

#[derive(thiserror::Error, Debug)]
pub struct CubeError {
    pub message: String,
    pub cause: CubeErrorCauseType,
    pub backtrace: Option<Backtrace>,
}

#[derive(Debug, Clone)]
pub enum CubeErrorCauseType {
    // User Error is an uncategorized error caused by user input or action
    User(Option<HashMap<String, String>>),
    // Internal Error is an uncategorized error caused by internal failures
    Internal(Option<HashMap<String, String>>),
    // REST API Error is an error thrown by REST API when running in standalone mode
    RestApi(Option<HashMap<String, String>>),
    // SQL Parser Error is an error thrown when SQL cannot be parsed
    SqlParser(Option<HashMap<String, String>>),
    // Unsupported Error is an error thrown when a feature/option is not supported
    Unsupported(Option<HashMap<String, String>>),
    // Planning Error is an error thrown when the query plan is invalid
    Planning(Option<HashMap<String, String>>),
    // Post-Processing Error is an error thrown during execution of the query
    PostProcessing(Option<HashMap<String, String>>),
    // Rewrite Error is an error thrown during logical plan e-graph rewriting
    Rewrite(Option<HashMap<String, String>>),
    // Database Execution Error is an error thrown during execution of the query in the database
    DatabaseExecution(Option<HashMap<String, String>>),
    // Continue wait is an error used internally to indicate that the query is still
    // being processed and the client should send the request again
    ContinueWait,
}

impl CubeError {
    pub fn user(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::User(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn internal(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Internal(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn internal_with_bt(message: String, backtrace: Option<Backtrace>) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Internal(None),
            backtrace,
        }
    }

    pub fn rest_api(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::RestApi(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn sql_parser(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::SqlParser(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn unsupported(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Unsupported(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn planning(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Planning(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn post_processing(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::PostProcessing(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn rewrite(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Rewrite(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn database_execution(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::DatabaseExecution(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn continue_wait() -> Self {
        Self {
            message: CONTINUE_WAIT_MESSAGE.to_string(),
            cause: CubeErrorCauseType::ContinueWait,
            backtrace: None,
        }
    }

    pub fn panic(error: Box<dyn Any + Send>) -> Self {
        Self::panic_with_message(error, "Unexpected panic")
    }

    pub fn panic_with_message(error: Box<dyn Any + Send>, message: &str) -> Self {
        if let Some(reason) = error.downcast_ref::<&str>() {
            CubeError::internal(format!("{}. Reason: {}", message, reason))
        } else if let Some(reason) = error.downcast_ref::<String>() {
            CubeError::internal(format!("{}. Reason: {}", message, reason))
        } else {
            CubeError::internal(format!("{} without reason", message))
        }
    }
}

impl CubeError {
    /// Whether this error is the queue's `Continue wait` signal rather than a
    /// failure. Nothing user-visible may be reported for it - see
    /// `handle_sql_query` in `cubejs-backend-native`, which must not log a
    /// `Cube SQL Error` load event for one.
    ///
    /// The cause is the reliable half; the message check is the fallback for an
    /// error that lost its cause on the way here. That happens: DataFusion's
    /// `RepartitionExec` has to hand one error to every output partition and a
    /// boxed error is not `Clone`, so `wait_for_task` flattens it to its
    /// `Display` string and re-wraps it as `DataFusionError::Execution`. The
    /// typed `CubeError` is gone at that point and the message has grown a
    /// prefix (`Execution error: Continue wait`), which is why the message check
    /// is not an equality one - and why every prefix would otherwise compound
    /// through the next wrapping layer.
    pub fn is_continue_wait(&self) -> bool {
        matches!(self.cause, CubeErrorCauseType::ContinueWait)
            || Self::is_continue_wait_message(&self.message)
    }

    /// `is_continue_wait` for a bare message, when no cause is available - a
    /// message carried over the JS bridge, or one already flattened to a string.
    ///
    /// These messages have a structure, and the check follows it rather than
    /// scanning for the phrase anywhere in the text. Each wrapping layer prepends
    /// its own label and a colon (`Execution error: `, `Database Execution Error: `,
    /// and these compound), and a message that arrived over the JS bridge can have
    /// a stack appended, which puts the message on the first line and the frames
    /// after it (`errorString` in `js/index.ts` falls back to `err.stack`). So the
    /// phrase always lands as a whole `:`- or newline-delimited part, however many
    /// layers wrapped it - and splitting on both separators matches every one of
    /// those shapes without enumerating them.
    ///
    /// Matching parts rather than a substring is what keeps a real failure intact.
    /// A false positive is expensive here, more so than at the original site where
    /// it only meant one more retry. Two consumers are new to this predicate.
    /// `normalize_continue_wait` runs on every DataFusion and Arrow conversion and
    /// *replaces* the message, so the original text is gone before anything
    /// downstream sees it. And `load_data` (`scan.rs`) held an equality check
    /// until this change: a real database error misread here is minted with the
    /// `ContinueWait` cause locally, so - unlike a genuine continue wait, which
    /// the transport retries and never surfaces on the Postgres path - nothing
    /// re-classifies it, and `sql/postgres/error.rs` answers the client
    /// `SqlStatementNotYetComplete` (`03000`) instead of their failure. These
    /// messages interpolate user-controlled SQL, so the phrase turns up inside
    /// one as an identifier or a literal (`No field named 'continue wait'`,
    /// `status = Utf8("continue wait")`); as part of a larger part it is not the
    /// signal, and a substring test could not tell the two apart.
    ///
    /// The trade runs the other way for a wrapper that appends instead of
    /// prepending: `Continue wait.` or `Continue wait (retrying)` is one part and
    /// does not match. Exactly one such wrapper exists, and it is in this file -
    /// the `Rewrite` arm of `Display` renders
    /// `Rewrite Error: {}. Please check logs for additional information`, where
    /// every other arm is `<Label>: {}`. A continue wait never carries that
    /// cause: `CubeError::rewrite` is minted only inside the rewrite engine with
    /// its own messages, while a continue wait comes from `transport.load` at
    /// execution time, after rewriting. `every_cause_renders_a_matchable_continue_wait`
    /// pins both halves, so a new appending wrapper - or an existing arm taught to
    /// append - fails there rather than silently swallowing the signal.
    pub fn is_continue_wait_message(message: &str) -> bool {
        message
            .split(['\n', ':'])
            .any(|part| part.trim().eq_ignore_ascii_case(CONTINUE_WAIT_MESSAGE))
    }

    /// Restores the `ContinueWait` cause on an error that reached us as a
    /// stringified continue wait (see `is_continue_wait` for how the cause gets
    /// lost), and resets the message to its canonical spelling so the accumulated
    /// prefixes do not travel any further.
    ///
    /// Applied at the DataFusion and Arrow conversion boundaries, which is where
    /// the flattening happens, so consumers downstream of them can match on the
    /// cause.
    ///
    /// Both halves are normalized, not just the cause-less one: an error can
    /// arrive carrying the cause *and* a prefixed message - `CubeScanMemoryStream`
    /// sets the cause without rewriting the message, and the `CubeError` downcast
    /// arm hands that straight back here - and the message still has to be
    /// canonical, because JS matches it exactly in places (`gateway.ts`'s
    /// `err.message === 'Continue wait'`).
    fn normalize_continue_wait(mut self) -> Self {
        if self.is_continue_wait() {
            self.message = CONTINUE_WAIT_MESSAGE.to_string();
            self.cause = CubeErrorCauseType::ContinueWait;
        }

        self
    }

    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.backtrace.as_ref()
    }

    pub fn to_backtrace(self) -> Option<Backtrace> {
        self.backtrace
    }
}

impl CubeErrorCauseType {
    pub fn meta(&self) -> Option<&HashMap<String, String>> {
        match self {
            CubeErrorCauseType::User(meta)
            | CubeErrorCauseType::Internal(meta)
            | CubeErrorCauseType::RestApi(meta)
            | CubeErrorCauseType::SqlParser(meta)
            | CubeErrorCauseType::Unsupported(meta)
            | CubeErrorCauseType::Planning(meta)
            | CubeErrorCauseType::PostProcessing(meta)
            | CubeErrorCauseType::Rewrite(meta)
            | CubeErrorCauseType::DatabaseExecution(meta) => meta.as_ref(),
            CubeErrorCauseType::ContinueWait => None,
        }
    }
}

impl fmt::Display for CubeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.cause {
            CubeErrorCauseType::User(_) => {
                f.write_fmt(format_args!("User Error: {}", self.message))
            }
            CubeErrorCauseType::Internal(_) => {
                f.write_fmt(format_args!("Internal Error: {}", self.message))
            }
            CubeErrorCauseType::RestApi(_) => {
                f.write_fmt(format_args!("REST API Error: {}", self.message))
            }
            CubeErrorCauseType::SqlParser(_) => {
                f.write_fmt(format_args!("SQL Parser Error: {}", self.message))
            }
            CubeErrorCauseType::Unsupported(_) => {
                f.write_fmt(format_args!("Unsupported Error: {}", self.message))
            }
            CubeErrorCauseType::Planning(_) => {
                f.write_fmt(format_args!("Planning Error: {}", self.message))
            }
            CubeErrorCauseType::PostProcessing(_) => {
                f.write_fmt(format_args!("Post-Processing Error: {}", self.message))
            }
            CubeErrorCauseType::Rewrite(_) => f.write_fmt(format_args!(
                "Rewrite Error: {}. Please check logs for additional information",
                self.message
            )),
            CubeErrorCauseType::DatabaseExecution(_) => {
                f.write_fmt(format_args!("Database Execution Error: {}", self.message))
            }
            CubeErrorCauseType::ContinueWait => write!(f, "Continue wait"),
        }
    }
}

impl From<cubeclient::apis::Error<LoadV1Error>> for CubeError {
    fn from(v: cubeclient::apis::Error<LoadV1Error>) -> Self {
        let message: String = match v {
            cubeclient::apis::Error::ResponseError(e) => match e.entity {
                None => e.content,
                Some(LoadV1Error::UnknownValue(_)) => e.content,
                Some(LoadV1Error::Status4XX(unwrapped)) => unwrapped.error,
                Some(LoadV1Error::Status5XX(unwrapped)) => unwrapped.error,
            },
            _ => v.to_string(),
        };
        return CubeError::rest_api(message);
    }
}

impl From<cubeclient::apis::Error<MetaV1Error>> for CubeError {
    fn from(v: cubeclient::apis::Error<MetaV1Error>) -> Self {
        let message: String = match v {
            cubeclient::apis::Error::ResponseError(e) => match e.entity {
                None => e.content,
                Some(MetaV1Error::UnknownValue(_)) => e.content,
                Some(MetaV1Error::Status4XX(unwrapped)) => unwrapped.error,
                Some(MetaV1Error::Status5XX(unwrapped)) => unwrapped.error,
            },
            _ => v.to_string(),
        };
        return CubeError::rest_api(message);
    }
}

impl From<crate::compile::CompilationError> for CubeError {
    fn from(v: crate::compile::CompilationError) -> Self {
        let (message, cause) = match &v {
            crate::compile::CompilationError::Internal(message, _, meta)
            | crate::compile::CompilationError::Fatal(message, meta) => {
                (message.clone(), CubeErrorCauseType::Internal(meta.clone()))
            }
            crate::compile::CompilationError::User(message, meta) => {
                (message.clone(), CubeErrorCauseType::User(meta.clone()))
            }
            crate::compile::CompilationError::RestApi(message, meta) => {
                (message.clone(), CubeErrorCauseType::RestApi(meta.clone()))
            }
            crate::compile::CompilationError::SqlParser(message, meta) => {
                (message.clone(), CubeErrorCauseType::SqlParser(meta.clone()))
            }
            crate::compile::CompilationError::Unsupported(message, meta) => (
                message.clone(),
                CubeErrorCauseType::Unsupported(meta.clone()),
            ),
            crate::compile::CompilationError::Planning(message, meta) => {
                (message.clone(), CubeErrorCauseType::Planning(meta.clone()))
            }
            crate::compile::CompilationError::PostProcessing(message, meta) => (
                message.clone(),
                CubeErrorCauseType::PostProcessing(meta.clone()),
            ),
            crate::compile::CompilationError::Rewrite(message, meta) => {
                (message.clone(), CubeErrorCauseType::Rewrite(meta.clone()))
            }
            crate::compile::CompilationError::DatabaseExecution(message, meta) => (
                message.clone(),
                CubeErrorCauseType::DatabaseExecution(meta.clone()),
            ),
            crate::compile::CompilationError::ContinueWait => (
                "Continue wait".to_string(),
                CubeErrorCauseType::ContinueWait,
            ),
        };
        let mut err = CubeError::internal_with_bt(message, v.to_backtrace());
        err.cause = cause;

        err
    }
}

impl From<std::io::Error> for CubeError {
    fn from(v: std::io::Error) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<ParserError> for CubeError {
    fn from(v: ParserError) -> Self {
        match v {
            ParserError::ParserError(message) => CubeError::sql_parser(message),
            ParserError::TokenizerError(message) => CubeError::sql_parser(message),
            ParserError::RecursionLimitExceeded => {
                CubeError::sql_parser("recursion limit exceeded".to_string())
            }
        }
    }
}

impl From<rust_decimal::Error> for CubeError {
    fn from(v: rust_decimal::Error) -> Self {
        CubeError::internal(format!("Decimal Error: {}", v))
    }
}

impl From<tokio::task::JoinError> for CubeError {
    fn from(v: tokio::task::JoinError) -> Self {
        if v.is_panic() {
            CubeError::panic(v.into_panic())
        } else {
            // JoinError can return CanceledError
            CubeError::internal(v.to_string())
        }
    }
}

impl<T> From<SendError<T>> for CubeError
where
    T: Debug,
{
    fn from(v: SendError<T>) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<std::time::SystemTimeError> for CubeError {
    fn from(v: std::time::SystemTimeError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<Elapsed> for CubeError {
    fn from(v: Elapsed) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for CubeError {
    fn from(v: tokio::sync::broadcast::error::RecvError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for CubeError {
    fn from(v: datafusion::error::DataFusionError) -> Self {
        let converted = match v {
            datafusion::error::DataFusionError::ArrowError(e) => CubeError::from(e),
            datafusion::error::DataFusionError::SQL(e) => CubeError::from(e),
            datafusion::error::DataFusionError::NotImplemented(e) => CubeError::unsupported(e),
            datafusion::error::DataFusionError::Internal(e) => CubeError::internal(e),
            datafusion::error::DataFusionError::Plan(e) => CubeError::planning(e),
            datafusion::error::DataFusionError::Execution(e) => CubeError::post_processing(e),
            datafusion::error::DataFusionError::External(e) => match e.downcast::<CubeError>() {
                Ok(e) => *e,
                Err(e) => match e.downcast::<arrow::error::ArrowError>() {
                    Ok(e) => CubeError::from(*e),
                    // `From<ArrowError>` tries `DataFusionError` here as well,
                    // and a `DataFusionError` does end up boxed inside
                    // `External` - `From<DataFusionError> for ArrowError` in the
                    // fork boxes anything it cannot map. Without this arm such
                    // an error only ever reached us as its `Display` string.
                    Err(e) => match e.downcast::<datafusion::error::DataFusionError>() {
                        Ok(e) => CubeError::from(*e),
                        Err(e) => CubeError::internal(e.to_string()),
                    },
                },
            },
            _ => CubeError::internal(v.to_string()),
        };

        converted.normalize_continue_wait()
    }
}

impl From<arrow::error::ArrowError> for CubeError {
    fn from(v: arrow::error::ArrowError) -> Self {
        let converted = match v {
            arrow::error::ArrowError::NotYetImplemented(e) => CubeError::unsupported(e),
            arrow::error::ArrowError::ExternalError(e) => match e.downcast::<CubeError>() {
                Ok(e) => *e,
                Err(e) => match e.downcast::<datafusion::error::DataFusionError>() {
                    Ok(e) => CubeError::from(*e),
                    Err(e) => CubeError::internal(e.to_string()),
                },
            },
            v @ arrow::error::ArrowError::CastError(_)
            | v @ arrow::error::ArrowError::ParseError(_)
            | v @ arrow::error::ArrowError::ComputeError(_)
            | v @ arrow::error::ArrowError::DivideByZero => {
                CubeError::post_processing(v.to_string())
            }
            _ => CubeError::internal(v.to_string()),
        };

        converted.normalize_continue_wait()
    }
}

impl From<chrono::ParseError> for CubeError {
    fn from(v: chrono::ParseError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<std::string::FromUtf8Error> for CubeError {
    fn from(v: std::string::FromUtf8Error) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for CubeError {
    fn from(v: tokio::sync::oneshot::error::RecvError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for CubeError {
    fn from(v: Box<bincode::ErrorKind>) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl<T> From<tokio::sync::watch::error::SendError<T>> for CubeError {
    fn from(v: tokio::sync::watch::error::SendError<T>) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::watch::error::RecvError> for CubeError {
    fn from(v: tokio::sync::watch::error::RecvError) -> Self {
        CubeError::internal(v.to_string())
    }
}
impl From<ParseIntError> for CubeError {
    fn from(v: ParseIntError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<SetLoggerError> for CubeError {
    fn from(v: SetLoggerError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<serde_json::Error> for CubeError {
    fn from(v: serde_json::Error) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<std::num::ParseFloatError> for CubeError {
    fn from(v: std::num::ParseFloatError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<base64::DecodeError> for CubeError {
    fn from(v: base64::DecodeError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::AcquireError> for CubeError {
    fn from(v: tokio::sync::AcquireError) -> Self {
        CubeError::internal(v.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::error::DataFusionError;

    /// The shape a continue wait actually arrives in once DataFusion has
    /// parallelized the plan. `RepartitionExec::wait_for_task` has to deliver one
    /// error to every output partition, and a boxed error is not `Clone`, so it
    /// flattens ours to its `Display` string and re-wraps it as
    /// `DataFusionError::Execution` - then `From<DataFusionError> for ArrowError`
    /// boxes that into `ExternalError` on the way back into the record batch
    /// stream. `target_partitions` defaults to `num_cpus::get()` and
    /// `repartition_aggregations` / `repartition_windows` default to true, so any
    /// GROUP BY or window function over a `CubeScan` takes this path.
    fn repartitioned(message: &str) -> arrow::error::ArrowError {
        arrow::error::ArrowError::ExternalError(Box::new(DataFusionError::Execution(
            message.to_string(),
        )))
    }

    #[test]
    fn continue_wait_survives_repartition_flattening() {
        let err = CubeError::from(repartitioned(CONTINUE_WAIT_MESSAGE));

        assert!(err.is_continue_wait());
        assert!(matches!(err.cause, CubeErrorCauseType::ContinueWait));
        // Normalized, so the prefix cannot compound if it is wrapped again.
        assert_eq!(err.message, CONTINUE_WAIT_MESSAGE);
        assert_eq!(err.to_string(), CONTINUE_WAIT_MESSAGE);
    }

    /// The regression this guards: the flattened error's `Display` already
    /// carries DataFusion's `Execution error: ` prefix, so a second wrapping
    /// layer produces a doubly-prefixed message. An equality check against
    /// `"continue wait"` matches neither, which is how the queue signal reached
    /// query history as a failed request.
    #[test]
    fn continue_wait_survives_a_prefixed_message() {
        for message in [
            "Execution error: Continue wait",
            "Execution error: Execution error: Continue wait",
            "Database Execution Error: Continue wait",
            "CONTINUE WAIT",
        ] {
            let err = CubeError::from(repartitioned(message));

            assert!(err.is_continue_wait(), "not detected: {}", message);
            assert!(matches!(err.cause, CubeErrorCauseType::ContinueWait));
            assert_eq!(err.message, CONTINUE_WAIT_MESSAGE);
        }
    }

    #[test]
    fn continue_wait_survives_as_a_typed_external_error() {
        let err = CubeError::from(arrow::error::ArrowError::ExternalError(Box::new(
            CubeError::continue_wait(),
        )));

        assert!(err.is_continue_wait());
        assert!(matches!(err.cause, CubeErrorCauseType::ContinueWait));
    }

    /// `DataFusionError::External` can hold a `DataFusionError` - the fork's
    /// `From<DataFusionError> for ArrowError` boxes anything it cannot map, and
    /// the round trip back lands here. Only `From<ArrowError>` used to try that
    /// downcast, so this direction reported the inner error as its `Display`
    /// string and lost the cause with it.
    #[test]
    fn nested_datafusion_error_keeps_its_cause() {
        let err = CubeError::from(DataFusionError::External(Box::new(
            DataFusionError::Execution(CONTINUE_WAIT_MESSAGE.to_string()),
        )));

        assert!(err.is_continue_wait());
        assert!(matches!(err.cause, CubeErrorCauseType::ContinueWait));
    }

    /// An error can arrive with the cause already set *and* a prefixed message:
    /// `CubeScanMemoryStream` sets the cause without rewriting the message, and
    /// the `CubeError` downcast arm returns it unchanged. The message still has to
    /// end up canonical, because JS compares it exactly (`gateway.ts`'s
    /// `err.message === 'Continue wait'`).
    #[test]
    fn a_typed_continue_wait_still_gets_a_canonical_message() {
        let mut err = CubeError::continue_wait();
        err.message = "Database Execution Error: Continue wait".to_string();

        let err = CubeError::from(arrow::error::ArrowError::ExternalError(Box::new(err)));

        assert!(err.is_continue_wait());
        assert!(matches!(err.cause, CubeErrorCauseType::ContinueWait));
        assert_eq!(err.message, CONTINUE_WAIT_MESSAGE);
    }

    /// A message carried over the JS bridge can have a stack appended:
    /// `errorString` falls back to `err.stack`, which renders the message on the
    /// first line and the frames after it.
    #[test]
    fn continue_wait_survives_an_appended_stack() {
        let err = CubeError::from(repartitioned(
            "Error: Continue wait\n    at load (/app/js/index.js:12:34)",
        ));

        assert!(err.is_continue_wait());
        assert!(matches!(err.cause, CubeErrorCauseType::ContinueWait));
        assert_eq!(err.message, CONTINUE_WAIT_MESSAGE);
    }

    /// A message that does not carry the phrase at all, including shapes that
    /// have tripped up earlier implementations: empty, shorter than the phrase,
    /// and non-ASCII.
    #[test]
    fn a_message_without_the_phrase_is_not_a_continue_wait() {
        for message in ["", "wait", "Ошибка: продолжить ожидание"] {
            assert!(
                !CubeError::is_continue_wait_message(message),
                "wrongly detected: {}",
                message
            );
        }
    }

    /// The phrase is matched as a whole part, so it still resolves however many
    /// layers wrapped it and wherever the split lands it - including a part that
    /// a stack frame's own colons cut out of the first line.
    #[test]
    fn the_phrase_is_matched_as_a_whole_part() {
        for message in [
            CONTINUE_WAIT_MESSAGE,
            "Execution error: Continue wait",
            "Post-Processing Error: Execution error: Continue wait",
            "Error: Continue wait\n    at load (/app/js/index.js:12:34)",
        ] {
            assert!(
                CubeError::is_continue_wait_message(message),
                "not detected: {}",
                message
            );
        }
    }

    /// `RepartitionExec` flattens through `Display`, so every cause's rendering
    /// of a continue wait has to stay matchable by the predicate.
    ///
    /// The `Rewrite` arm is the one that also *appends*
    /// (`. Please check logs for additional information`), so it is the one that
    /// does not survive the round trip - and it is unreachable for a continue
    /// wait, because `CubeError::rewrite` is minted only inside the rewrite engine
    /// with its own messages, while a continue wait comes from `transport.load`
    /// at execution time, after rewriting.
    ///
    /// The inner match is exhaustive on purpose: a new cause variant, or an
    /// existing one taught to append, stops compiling here rather than silently
    /// swallowing the queue's signal.
    #[test]
    fn every_cause_renders_a_matchable_continue_wait() {
        use CubeErrorCauseType::*;

        for cause in [
            User(None),
            Internal(None),
            RestApi(None),
            SqlParser(None),
            Unsupported(None),
            Planning(None),
            PostProcessing(None),
            Rewrite(None),
            DatabaseExecution(None),
            ContinueWait,
        ] {
            let appends = match &cause {
                Rewrite(_) => true,
                User(_) | Internal(_) | RestApi(_) | SqlParser(_) | Unsupported(_)
                | Planning(_) | PostProcessing(_) | DatabaseExecution(_) | ContinueWait => false,
            };

            let rendered = CubeError {
                message: CONTINUE_WAIT_MESSAGE.to_string(),
                cause,
                backtrace: None,
            }
            .to_string();

            assert_eq!(
                CubeError::is_continue_wait_message(&rendered),
                !appends,
                "unexpected verdict for: {}",
                rendered
            );
        }
    }

    #[test]
    fn a_real_failure_is_left_alone() {
        for message in [
            "Table 'orders' not found",
            "Database Execution Error: syntax error at or near \"SELCT\"",
            // The phrase is the user's identifier or literal, carried inside a
            // larger part rather than being one - and its text has to survive,
            // because `normalize_continue_wait` would otherwise replace it.
            "No field named 'continue wait' in table 'orders'",
            "Unsupported filter: status = Utf8(\"Continue wait\")",
            "Can't find column `continue wait` in projection",
            "Execution error: No field named 'continue wait'",
            "Execution error: continue wait is not a column\n    at plan (/app/js/index.js:1:2)",
        ] {
            let err = CubeError::from(repartitioned(message));

            assert!(!err.is_continue_wait(), "wrongly swallowed: {}", message);
            assert!(matches!(err.cause, CubeErrorCauseType::PostProcessing(_)));
            assert_eq!(err.message, message);
        }
    }
}
