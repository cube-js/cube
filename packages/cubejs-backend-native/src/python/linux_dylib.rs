use log::error;
use std::os::unix::prelude::OsStrExt;

// (∩ ͡° ͜ʖ ͡°)⊃━☆ﾟ. *
// It's required to export public symbols from libpython.so in order to support loading external modules for Python
// todo: Find a way how to do it with build.rs (right now, this approach doesn't work)
pub(crate) fn load_python_symbols() {
    use findshlibs::{SharedLibrary, TargetSharedLibrary};

    let mut libpython_path = None;

    TargetSharedLibrary::each(|shlib| {
        if shlib
            .name()
            .to_string_lossy()
            .to_string()
            .contains("libpython")
        {
            let path = shlib.name().to_os_string();
            libpython_path = Some(path);
        }
    });

    if let Some(libpython_path) = libpython_path {
        let mut os_str_bytes = libpython_path.as_bytes().to_vec();
        os_str_bytes.push(b'\0');

        let library_path = std::ffi::CStr::from_bytes_with_nul(&os_str_bytes).unwrap();

        unsafe {
            let handle = libc::dlopen(
                library_path.as_ptr() as *const libc::c_char,
                libc::RTLD_GLOBAL | libc::RTLD_LAZY,
            );
            assert!(!handle.is_null());
        }
    } else {
        error!("Unable to load all python symbols, error: libpython was not found")
    }
}
