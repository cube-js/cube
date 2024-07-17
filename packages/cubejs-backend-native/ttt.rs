pub trait WarpT {
    fn start();
    fn stop();
}

struct OSWarp {

}

impl OSWarp {
    pub fn makerouting(&self) -> Or<impl> {

    }
}

struct EntWarp {
    os: Arc<OSWarp>
}

impl OSWarp {
    pub fn makerouting(&self) -> Or<impl> {
        self.os.makerouting().or()
    }
}
