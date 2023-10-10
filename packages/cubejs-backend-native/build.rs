fn main() {
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS not defined");

    // We need to export public symbols in order to support loading external modules for Python
    // todo: It doesnt work, probably our rust nightly has an issue with linker
    match target_os.as_str() {
        "linux" => {
            println!("cargo:rustc-link-arg=-Wl,-export-dynamic");
        }
        "macos" => {
            println!("cargo:rustc-link-arg=-rdynamic");
        }
        _ => {}
    };
}
