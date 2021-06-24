#[cfg(feature = "process-cleanup")]
pub fn die_with_parent(parent_pid: u64) {
    unsafe {
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGHUP) != 0 {
            log::error!("Failed to call PR_SET_DEATHSIG");
            return;
        }
        // Handle parent process exit to finish as early as possible.
        if libc::getppid() as u64 != parent_pid {
            log::error!("parent process died, exiting");
            std::process::exit(1);
        }
    }
}

#[cfg(not(feature = "process-cleanup"))]
pub fn die_with_parent(_parent_pid: u64) {
    // nop
}
