use cubestore::CubeError;
use flate2::read::GzDecoder;
use std::io::Cursor;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tar::Archive;
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub fn write_tmp_file(text: &str) -> Result<NamedTempFile, CubeError> {
    let mut file = NamedTempFile::new()?;
    file.write_all(text.as_bytes())?;
    Ok(file)
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
    let dir = std::fs::read_dir(from)?;

    // This errors if the destination already exists, and that's what we want.
    std::fs::create_dir(to)?;

    for entry in dir {
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

/// Serves one in-memory file over HTTP, for as long as this value is alive.
pub struct TestFileServer {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl TestFileServer {
    pub fn url(&self, name: &str) -> String {
        format!("http://{}/{}", self.addr, name)
    }
}

impl Drop for TestFileServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `download_delay` holds back the body, so a caller can observe the state a
/// location is in while its download is still running. Head requests are
/// answered immediately.
pub async fn serve_file(
    body: String,
    download_delay: Duration,
) -> Result<TestFileServer, CubeError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_one_request(socket, &body, download_delay).await {
                    log::error!("Test file server: {}", e);
                }
            });
        }
    });
    Ok(TestFileServer { addr, task })
}

async fn serve_one_request(
    mut socket: TcpStream,
    body: &str,
    download_delay: Duration,
) -> Result<(), CubeError> {
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        let read = socket.read(&mut buf).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buf[..read]);
    }

    let mut response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/csv\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    if !request.starts_with(b"HEAD ") {
        tokio::time::sleep(download_delay).await;
        response += body;
    }

    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await?;
    Ok(())
}

/// Serves `body` for every path, like `serve_file`, but resets any connection
/// made while `max_connections` are already open, as a NAT or a storage
/// endpoint with a connection limit does. Head requests are held for
/// `head_delay` so that concurrent ones overlap. Returns the server and the peak
/// number of connections it saw open at once.
pub async fn serve_file_with_connection_limit(
    body: String,
    max_connections: usize,
    head_delay: Duration,
) -> Result<(TestFileServer, Arc<AtomicUsize>), CubeError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let open = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let peak_to_return = peak.clone();
    let task = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let now_open = open.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now_open, Ordering::SeqCst);
            if now_open > max_connections {
                open.fetch_sub(1, Ordering::SeqCst);
                // Linger 0 makes the close send a reset.
                let _ = socket.set_linger(Some(Duration::from_secs(0)));
                drop(socket);
                continue;
            }
            let body = body.clone();
            let open = open.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_one_request_with_head_delay(socket, &body, head_delay).await
                {
                    log::error!("Test file server: {}", e);
                }
                open.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    Ok((TestFileServer { addr, task }, peak_to_return))
}

async fn serve_one_request_with_head_delay(
    mut socket: TcpStream,
    body: &str,
    head_delay: Duration,
) -> Result<(), CubeError> {
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        let read = socket.read(&mut buf).await?;
        if read == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buf[..read]);
    }

    let mut response = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/csv\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        body.len()
    );
    if request.starts_with(b"HEAD ") {
        tokio::time::sleep(head_delay).await;
    } else {
        response += body;
    }

    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await?;
    Ok(())
}
