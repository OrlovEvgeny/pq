#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://pq.eorlov.org"
BINARY="pq"

usage() {
  cat <<EOF
Install ${BINARY} from pq.eorlov.org.

Usage:
  install.sh [options]

Options:
  --version <version>  Install specific version (e.g. v1.0.0). Default: latest.
  --to <dir>           Install to directory. Default: /usr/local/bin or ~/.local/bin.
  -h, --help           Show this help.
EOF
}

get_latest_version() {
  curl -sSf "${BASE_URL}/dist/latest.json" \
    | grep '"version"' \
    | sed -E 's/.*"version"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/'
}

detect_target() {
  local os arch

  case "$(uname -s)" in
    Linux)  os="unknown-linux-gnu" ;;
    Darwin) os="apple-darwin" ;;
    *)
      echo "Error: unsupported OS '$(uname -s)'" >&2
      exit 1
      ;;
  esac

  case "$(uname -m)" in
    x86_64 | amd64)  arch="x86_64" ;;
    aarch64 | arm64)  arch="aarch64" ;;
    *)
      echo "Error: unsupported architecture '$(uname -m)'" >&2
      exit 1
      ;;
  esac

  echo "${arch}-${os}"
}

default_install_dir() {
  if [ -d /usr/local/bin ] && [ -w /usr/local/bin ]; then
    echo "/usr/local/bin"
  elif [ -d "${HOME}/.local/bin" ]; then
    echo "${HOME}/.local/bin"
  else
    mkdir -p "${HOME}/.local/bin"
    echo "${HOME}/.local/bin"
  fi
}

verify_checksum() {
  local file="$1"
  local expected="$2"

  local actual
  if command -v sha256sum > /dev/null 2>&1; then
    actual=$(sha256sum "$file" | awk '{print $1}')
  elif command -v shasum > /dev/null 2>&1; then
    actual=$(shasum -a 256 "$file" | awk '{print $1}')
  else
    echo "Warning: no sha256sum or shasum found, skipping checksum verification" >&2
    return 0
  fi

  if [ "$actual" != "$expected" ]; then
    echo "Error: checksum mismatch" >&2
    echo "  expected: ${expected}" >&2
    echo "  actual:   ${actual}" >&2
    exit 1
  fi
}

main() {
  local version=""
  local install_dir=""

  while [ $# -gt 0 ]; do
    case "$1" in
      --version)  version="$2"; shift 2 ;;
      --to)       install_dir="$2"; shift 2 ;;
      -h|--help)  usage; exit 0 ;;
      *)          echo "Unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done

  if [ -z "$version" ]; then
    echo "Fetching latest version..."
    version=$(get_latest_version)
  fi

  if [ -z "$install_dir" ]; then
    install_dir=$(default_install_dir)
  fi

  local target
  target=$(detect_target)

  local url="${BASE_URL}/dist/${version}/${BINARY}-${version}-${target}.tar.gz"
  local sha_url="${BASE_URL}/dist/${version}/${BINARY}-${version}-${target}.tar.gz.sha256"

  echo "Installing ${BINARY} ${version} (${target})..."
  echo "  from: ${url}"
  echo "  to:   ${install_dir}/${BINARY}"

  tmpdir=$(mktemp -d)
  trap 'rm -rf "$tmpdir"' EXIT

  if ! curl -sSfL "$url" -o "${tmpdir}/${BINARY}.tar.gz"; then
    echo "Error: failed to download ${url}" >&2
    echo "Check that the version '${version}' exists and has a binary for '${target}'." >&2
    exit 1
  fi

  # Verify SHA256 checksum
  local expected_sha
  if expected_sha=$(curl -sSf "$sha_url" | awk '{print $1}'); then
    verify_checksum "${tmpdir}/${BINARY}.tar.gz" "$expected_sha"
    echo "  checksum verified"
  fi

  tar xzf "${tmpdir}/${BINARY}.tar.gz" -C "$tmpdir"

  mkdir -p "$install_dir"
  install -m 755 "${tmpdir}/${BINARY}" "${install_dir}/${BINARY}"

  echo ""
  echo "${BINARY} ${version} installed successfully!"

  if ! echo "$PATH" | tr ':' '\n' | grep -qx "$install_dir"; then
    echo ""
    echo "WARNING: ${install_dir} is not in your \$PATH."
    echo "Add it with:"
    echo "  export PATH=\"${install_dir}:\$PATH\""
  fi
}

main "$@"
