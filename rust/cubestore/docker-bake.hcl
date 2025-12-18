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

# EOL LTS 2026-08-31
target "rust-builder-bullseye" {
  context = "."
  dockerfile = "builder.Dockerfile"
  args = {
    RUST_TAG = "1-slim-bullseye"
    OS_NAME = "bullseye"
    LLVM_VERSION = "18"
  }
  tags = ["cubejs/rust-builder:bullseye-llvm-18"]
  platforms = ["linux/amd64", "linux/arm64"]
}
