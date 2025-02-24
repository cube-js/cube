pub fn escape_snapshot_name(name: String) -> String {
    let mut name = name
        .to_lowercase()
        // @todo Real escape?
        .replace("\r", "")
        .replace("\n", "")
        .replace("\t", "")
        .replace(">", "")
        .replace("<", "")
        .replace("'", "")
        .replace("::", "_")
        .replace(":", "")
        .replace(" ", "_")
        .replace("*", "asterisk")
        // shorter variant
        .replace(",_", "_");

    for _ in 0..32 {
        name = name.replace("__", "_");
    }

    // Windows limit
    if name.len() > 200 {
        name.chars().take(200).collect()
    } else {
        name
    }
}
