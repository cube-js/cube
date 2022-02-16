use crate::CubeError;
use futures::future::FutureExt;
use std::future::Future;
use std::panic::AssertUnwindSafe;

pub async fn async_try_with_catch_unwind<F, R>(future: F) -> Result<R, CubeError>
where
    F: Future<Output = Result<R, CubeError>>,
{
    let result = AssertUnwindSafe(future).catch_unwind().await;
    return match result {
        Ok(x) => x,
        Err(e) => Err(match e.downcast::<String>() {
            Ok(s) => CubeError::panic(*s),
            Err(e) => match e.downcast::<&str>() {
                Ok(m1) => CubeError::panic(m1.to_string()),
                Err(_) => CubeError::panic("unknown cause".to_string()),
            },
        }),
    };
}

#[cfg(test)]
mod tests {
    extern crate test;

    use super::*;
    use std::panic;
    use std::pin::Pin;

    #[tokio::test]
    async fn test_async_try_with_catch_unwind() {
        let f0: Pin<Box<dyn Future<Output = Result<String, CubeError>>>> =
            Box::pin(async { Ok("ok".to_string()) });
        let x0 = async_try_with_catch_unwind(f0).await;
        assert_eq!(x0, Ok("ok".to_string()));
        let f1: Pin<Box<dyn Future<Output = Result<String, CubeError>>>> =
            Box::pin(async { Err(CubeError::internal("err".to_string())) });
        let x1 = async_try_with_catch_unwind(f1).await;
        assert_eq!(x1, Err(CubeError::internal("err".to_string())));
        let f2: Pin<Box<dyn Future<Output = Result<String, CubeError>>>> =
            Box::pin(async { panic!("oops") });
        let x2 = async_try_with_catch_unwind(f2).await;
        assert_eq!(x2, Err(CubeError::panic("oops".to_string())));
        let f3: Pin<Box<dyn Future<Output = Result<String, CubeError>>>> =
            Box::pin(async { panic!("oops{}", "ie") });
        let x3 = async_try_with_catch_unwind(f3).await;
        assert_eq!(x3, Err(CubeError::panic("oopsie".to_string())));
    }
}
