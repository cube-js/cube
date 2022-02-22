use cubestore::CubeError;
use std::io::Write;
use tempfile::NamedTempFile;

pub fn write_tmp_file(text: &str) -> Result<NamedTempFile, CubeError> {
    let mut file = NamedTempFile::new()?;
    file.write_all(text.as_bytes())?;
    return Ok(file);
}
