/// Use a macro to keep the call site information (file, line number) in the log message.
#[macro_export]
macro_rules! ack_error {
    ($x:expr) => {
        if let std::result::Result::Err(e) = $x {
            log::error!("Error: {:?}", e);
        }
    };
}
