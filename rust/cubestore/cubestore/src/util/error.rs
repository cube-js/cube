use std::any::Any;

/// Use a macro to keep the call site information (file, line number) in the log message.
#[macro_export]
macro_rules! ack_error {
    ($x:expr) => {
        if let std::result::Result::Err(e) = $x {
            log::error!("Error: {:?}", e);
        }
    };
}

/// Converts the error of catch_unwind to String.
/// See https://stackoverflow.com/a/68558313
fn catch_unwind_error_to_string<R>(r: Result<R, Box<dyn Any + Send>>) -> Result<R, String> {
    return match r {
        Ok(x) => Ok(x),
        Err(e) => Err(
            match e.downcast::<String>() {
                Ok(s) => *s,
                Err(e) => match e.downcast::<&str>() {
                    Ok(m1) => m1.to_string(),
                    Err(_) => "unknown panic cause".to_string(),
                }
            }
        )
    }
}

fn with_catch_unwind

#[cfg(test)]
mod tests {
    extern crate test;

    use std::panic;
    use super::*;

    #[test]
    fn test_catch_unwind_error_to_string() {
        let r0 = panic::catch_unwind(|| {
            return "result";
        });
        assert_eq!(catch_unwind_error_to_string(r0), Ok("result"));
        let r1 = panic::catch_unwind(|| {
            panic!("panic static string");
        });
        assert_eq!(catch_unwind_error_to_string(r1), Err("panic static string".to_string()));
        let r2 = panic::catch_unwind(|| {
            panic!("panic format '{}'", "string");
        });
        assert_eq!(catch_unwind_error_to_string(r2), Err("panic format 'string'".to_string()));
    }
}

