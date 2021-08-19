#[cfg(feature = "process-cleanup")]
pub fn avoid_child_zombies() {
    unsafe {
        let mut a = libc::sigaction {
            sa_sigaction: libc::SIG_DFL,
            sa_mask: std::mem::zeroed(),
            sa_flags: libc::SA_NOCLDWAIT,
            sa_restorer: None,
        };
        if libc::sigemptyset(&mut a.sa_mask) != 0 {
            log::error!("failed to fill empty signal mask, cannot set SA_NOCLDWAIT");
            return;
        }
        if libc::sigaction(libc::SIGCHLD, &a, std::ptr::null_mut()) != 0 {
            log::error!("Failed to set SA_NOCLDWAIT flag");
        }
    }
}

#[cfg(not(feature = "process-cleanup"))]
pub fn avoid_child_zombies() {
    // nop
}

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
