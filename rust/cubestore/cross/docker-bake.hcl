variable "CROSS_VERSION" {
  default = "15082024"
}

variable "LLVM_VERSION" {
  default = "18"
}

target "aarch64-unknown-linux-gnu" {
  context = "."
  dockerfile = "aarch64-unknown-linux-gnu.Dockerfile"
  args = {
    LLVM_VERSION = LLVM_VERSION
  }
  tags = ["cubejs/rust-cross:aarch64-unknown-linux-gnu-${CROSS_VERSION}"]
  platforms = ["linux/amd64"]
}

target "aarch64-unknown-linux-gnu-python" {
  inherits = ["aarch64-unknown-linux-gnu"]
  contexts = {
    base = "target:aarch64-unknown-linux-gnu"
  }
  dockerfile = "aarch64-unknown-linux-gnu-python.Dockerfile"
  name = "aarch64-unknown-linux-gnu-python-${replace(item.python_release, ".", "-")}"
  matrix = {
    item = [
      {
        python_version = "3.12.4"
        python_release = "3.12"
      },
      {
        python_version = "3.11.3"
        python_release = "3.11"
      },
      {
        python_version = "3.10.11"
        python_release = "3.10"
      },
      {
        python_version = "3.9.18"
        python_release = "3.9"
      }
    ]
  }
  args = {
    CROSS_VERSION = CROSS_VERSION
    PYTHON_VERSION = item.python_version
    PYTHON_RELEASE = item.python_release
  }
  tags = ["cubejs/rust-cross:aarch64-unknown-linux-gnu-${CROSS_VERSION}-python-${item.python_release}"]
  platforms = ["linux/amd64"]
}

# We use -python prefix for ci
target "x86_64-unknown-linux-musl-python" {
  context = "."
  dockerfile = "x86_64-unknown-linux-musl.Dockerfile"
  args = {
    LLVM_VERSION = LLVM_VERSION
  }
  tags = ["cubejs/rust-cross:x86_64-unknown-linux-musl-${CROSS_VERSION}"]
  platforms = ["linux/amd64"]
}

target "x86_64-unknown-linux-gnu" {
  context = "."
  dockerfile = "x86_64-unknown-linux-gnu.Dockerfile"
  args = {
    LLVM_VERSION = LLVM_VERSION
  }
  tags = ["cubejs/rust-cross:x86_64-unknown-linux-gnu-${CROSS_VERSION}"]
  platforms = ["linux/amd64"]
}

target "x86_64-unknown-linux-gnu-python" {
  inherits = ["x86_64-unknown-linux-gnu"]
  contexts = {
    base = "target:x86_64-unknown-linux-gnu"
  }
  dockerfile = "x86_64-unknown-linux-gnu-python.Dockerfile"
  name = "x86_64-unknown-linux-gnu-python-${replace(item.python_release, ".", "-")}"
  matrix = {
    item = [
      {
        python_version = "3.12.4"
        python_release = "3.12"
      },
      {
        python_version = "3.11.3"
        python_release = "3.11"
      },
      {
        python_version = "3.10.11"
        python_release = "3.10"
      },
      {
        python_version = "3.9.18"
        python_release = "3.9"
      }
    ]
  }
  args = {
    CROSS_VERSION = CROSS_VERSION
    PYTHON_VERSION = item.python_version
    PYTHON_RELEASE = item.python_release
  }
  tags = ["cubejs/rust-cross:x86_64-unknown-linux-gnu-${CROSS_VERSION}-python-${item.python_release}"]
  platforms = ["linux/amd64"]
}
