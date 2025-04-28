use axum::http::header::ToStrError;
use axum::response::{IntoResponse, Response};
use axum::Json;
use cubesql::{CubeError, CubeErrorCauseType};
use serde::Serialize;

// Re-export axum::http::StatusCode as public API
pub type HttpStatusCode = axum::http::StatusCode;

pub enum HttpErrorCode {
    StatusCode(HttpStatusCode),
}

pub struct HttpError {
    code: HttpErrorCode,
    message: String,
}

impl HttpError {
    pub fn forbidden(message: String) -> HttpError {
        Self {
            code: HttpErrorCode::StatusCode(HttpStatusCode::FORBIDDEN),
            message,
        }
    }

    pub fn unauthorized(message: String) -> HttpError {
        Self {
            code: HttpErrorCode::StatusCode(HttpStatusCode::UNAUTHORIZED),
            message,
        }
    }

    pub fn status_code(&self) -> HttpStatusCode {
        match self.code {
            HttpErrorCode::StatusCode(code) => code,
        }
    }

    /// CubeError may contain unsafe error message, when it's internal error
    /// We cannot map this error to HTTP status code, that's why we pass it as argument
    pub fn from_user_with_status_code(error: CubeError, code: HttpErrorCode) -> Self {
        Self {
            code: match error.cause {
                CubeErrorCauseType::User(_) => code,
                CubeErrorCauseType::Internal(_) => {
                    HttpErrorCode::StatusCode(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                }
            },
            message: match error.cause {
                CubeErrorCauseType::User(_) => error.message,
                CubeErrorCauseType::Internal(_) => "Internal Server Error".to_string(),
            },
        }
    }
}

#[derive(Serialize)]
pub struct HttpErrorResponse {
    message: String,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status_code = self.status_code();

        (
            status_code,
            Json(HttpErrorResponse {
                message: self.message,
            }),
        )
            .into_response()
    }
}

impl From<ToStrError> for HttpError {
    fn from(value: ToStrError) -> Self {
        HttpError {
            code: HttpErrorCode::StatusCode(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            message: value.to_string(),
        }
    }
}
