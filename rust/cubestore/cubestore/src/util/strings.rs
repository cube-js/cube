use crate::CubeError;
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
