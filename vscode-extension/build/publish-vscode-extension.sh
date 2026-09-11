#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_SCRIPT="$SCRIPT_DIR/build.sh"
OUT_DIR="$SCRIPT_DIR/../out"

if [[ ! -f "$BUILD_SCRIPT" ]]; then
  echo "Build script not found: $BUILD_SCRIPT" >&2
  exit 1
fi

if [[ -z "${VSCE_PAT:-}" ]]; then
  echo "VSCE_PAT is required" >&2
  exit 1
fi

export VSCE_STORE="${VSCE_STORE:-file}"

"$BUILD_SCRIPT"

VSIX_PATH=""
if compgen -G "$OUT_DIR/*.vsix" >/dev/null; then
  VSIX_PATH="$(ls -t "$OUT_DIR"/*.vsix | head -n 1)"
fi

if [[ -z "$VSIX_PATH" || ! -f "$VSIX_PATH" ]]; then
  echo "No .vsix found in $OUT_DIR" >&2
  exit 1
fi

set +e
PUBLISH_OUTPUT="$(npx --yes @vscode/vsce publish --packagePath "$VSIX_PATH" 2>&1)"
PUBLISH_EXIT_CODE="$?"
set -e

if [[ "$PUBLISH_EXIT_CODE" -ne 0 ]]; then
  if echo "$PUBLISH_OUTPUT" | grep -qi "already exists"; then
    exit 0
  fi

  echo "$PUBLISH_OUTPUT" >&2
  exit "$PUBLISH_EXIT_CODE"
fi
