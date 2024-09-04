target "rust-builder-bookworm" {
  context = "."
  dockerfile = "builder.Dockerfile"
  args = {
    OS_NAME = "1-slim-bookworm"
    LLVM_VERSION = "18"
  }
  tags = ["cubejs/rust-builder:bookworm-llvm-18"]
  platforms = ["linux/amd64", "linux/arm64"]
}
