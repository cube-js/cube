use cubestore::CubeError;
use flate2::read::GzDecoder;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;
use tar::Archive;
use tempfile::NamedTempFile;

pub fn write_tmp_file(text: &str) -> Result<NamedTempFile, CubeError> {
    let mut file = NamedTempFile::new()?;
    file.write_all(text.as_bytes())?;
    return Ok(file);
}

pub async fn download_and_unzip(url: &str, dataset: &str) -> Result<Box<Path>, CubeError> {
    let root = std::env::current_dir()?.join("data");
    let dataset_path = root.join(dataset);
    if !dataset_path.exists() {
        println!("Downloading {}", dataset);
        let response = reqwest::get(url).await?;
        let content = Cursor::new(response.bytes().await?);
        let tarfile = GzDecoder::new(content);
        let mut archive = Archive::new(tarfile);
        archive.unpack(root)?;
    }
    assert!(dataset_path.exists());
    Ok(dataset_path.into_boxed_path())
}

/// Recursively copies files and directories from `from` to `to`, which must not exist yet.  Errors
/// if anything other than a file or directory is found.
///
/// We don't use a lib because the first that was tried was broken.
pub fn recursive_copy_directory(from: &Path, to: &Path) -> Result<(), CubeError> {
    let mut dir = std::fs::read_dir(from)?;

    // This errors if the destination already exists, and that's what we want.
    std::fs::create_dir(to)?;

    while let Some(entry) = dir.next() {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            recursive_copy_directory(&entry.path(), &to.join(entry.file_name()))?;
        } else if file_type.is_file() {
            let _file_size = std::fs::copy(entry.path(), to.join(entry.file_name()))?;
        } else {
            return Err(CubeError::corrupt_data(format!(
                "cannot copy file of type {:?} at location {:?}",
                file_type,
                entry.path()
            )));
        }
    }

    Ok(())
}
