#!/usr/bin/env bash
set -euo pipefail

# Usage: update-submodule.sh <repo-url>
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <repo-url>" >&2
  exit 1
fi

REPO_URL="$1"
WORK_DIR=$(mktemp -d)

cleanup() {
  if [[ -n "${WORK_DIR:-}" && "$WORK_DIR" == /tmp/* && -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

git clone --recurse-submodules "$REPO_URL" "$WORK_DIR"
cd "$WORK_DIR"

git submodule update --remote

if git diff --quiet; then
  echo "No submodule updates."
  exit 0
fi

git add -A
git commit -m "Update data as of $(date +%Y-%m-%d)"
git push

echo "Submodule update pushed."

