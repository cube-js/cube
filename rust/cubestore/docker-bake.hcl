# EOL LTS 2030-06-30
target "rust-builder-trixie" {
  context = "."
  dockerfile = "builder.Dockerfile"
  args = {
    RUST_TAG = "1-slim-trixie"
    OS_NAME = "trixie"
    LLVM_VERSION = "22"
  }
  tags = ["cubejs/rust-builder:trixie-llvm-22"]
  platforms = ["linux/amd64", "linux/arm64"]
}

# EOL LTS 2028-06-30
target "rust-builder-bookworm" {
  context = "."
  dockerfile = "builder.Dockerfile"
  args = {
    RUST_TAG = "1-slim-bookworm"
    OS_NAME = "bookworm"
    LLVM_VERSION = "18"
  }
  tags = ["cubejs/rust-builder:bookworm-llvm-18"]
  platforms = ["linux/amd64", "linux/arm64"]
}
