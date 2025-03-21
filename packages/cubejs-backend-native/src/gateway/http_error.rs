use axum::http::header::ToStrError;
use axum::response::{IntoResponse, Response};

pub enum HttpErrorCode {
    NotFound,
    Unauthorized,
    InternalServerError,
}

pub struct HttpError {
    code: HttpErrorCode,
    message: String,
}

impl HttpError {
    pub fn unauthorized(message: String) -> HttpError {
        Self {
            code: HttpErrorCode::Unauthorized,
            message,
        }
    }

    pub fn status_code(&self) -> axum::http::StatusCode {
        match self.code {
            HttpErrorCode::NotFound => axum::http::StatusCode::NOT_FOUND,
            HttpErrorCode::InternalServerError => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            HttpErrorCode::Unauthorized => axum::http::StatusCode::UNAUTHORIZED,
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status_code = self.status_code();

        (status_code, status_code.canonical_reason().unwrap_or_default())
            .into_response()
    }
}

impl From<ToStrError> for HttpError {
    fn from(value: ToStrError) -> Self {
        HttpError {
            code: HttpErrorCode::InternalServerError,
            message: value.to_string(),
        }
    }
}
