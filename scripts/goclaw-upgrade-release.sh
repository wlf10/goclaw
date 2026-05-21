#!/usr/bin/env bash
set -Eeuo pipefail

REPO="digitopvn/goclaw"
BASE_DIR="/opt/goclaw"
RELEASES_DIR="${BASE_DIR}/releases"
DEPLOY_BIN="/usr/local/bin/goclaw-deploy"
STATUS_DIR="/var/lib/goclaw/update-jobs"
STATUS_FILE="${STATUS_DIR}/current.json"
STATUS_OWNER="${GOCLAW_STATUS_OWNER:-goclaw:goclaw}"
DRY_RUN=0

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
json_escape() { python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"; }

write_status() {
  local state="$1" requested="$2" resolved="$3" error_msg="${4:-}"
  mkdir -p "$STATUS_DIR"
  local now job target before tmp
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  job="$(date -u +%Y%m%dT%H%M%SZ)-${resolved:-$requested}"
  target="${RELEASES_DIR}/${resolved:-$requested}"
  before=""
  if [ -L "${BASE_DIR}/current" ]; then
    before="$(readlink -f "${BASE_DIR}/current" || true)"
  fi
  local finished_json
  if [ "$state" = "running" ]; then
    finished_json="null"
  else
    finished_json="$(json_escape "$now")"
  fi
  tmp="$(mktemp "${STATUS_DIR}/current.XXXXXX")"
  cat > "$tmp" <<JSON
{
  "jobId": $(json_escape "$job"),
  "state": $(json_escape "$state"),
  "requestedTag": $(json_escape "$requested"),
  "resolvedTag": $(json_escape "$resolved"),
  "startedAt": $(json_escape "$now"),
  "finishedAt": ${finished_json},
  "currentReleaseBefore": $(json_escape "$before"),
  "targetRelease": $(json_escape "$target"),
  "error": $(json_escape "$error_msg")
}
JSON
  chown "$STATUS_OWNER" "$tmp" 2>/dev/null || true
  chmod 0640 "$tmp"
  mv "$tmp" "$STATUS_FILE"
}

fail() {
  local msg="$1"
  log "ERROR: $msg"
  if [ "$DRY_RUN" != "1" ]; then
    write_status "failed" "${REQUESTED_TAG:-}" "${RESOLVED_TAG:-}" "$msg" || true
  fi
  exit 1
}

usage() {
  cat <<'EOF'
usage: goclaw-upgrade-release [--dry-run] <latest|vMAJOR.MINOR.PATCH[-beta.N|-rc.N]>
EOF
}

if [ "${1:-}" = "--dry-run" ]; then
  DRY_RUN=1
  shift
fi

REQUESTED_TAG="${1:-}"
if [ -z "$REQUESTED_TAG" ] || [ "${2:-}" != "" ]; then
  usage >&2
  exit 2
fi

if [ "$REQUESTED_TAG" != "latest" ] && ! [[ "$REQUESTED_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-(beta|rc)\.[0-9]+)?$ ]]; then
  fail "invalid tag"
fi

require_bin() { command -v "$1" >/dev/null 2>&1 || fail "missing dependency: $1"; }
require_bin curl
require_bin flock
require_bin tar
require_bin sha256sum
require_bin python3

if [ "$DRY_RUN" != "1" ]; then
  mkdir -p "$STATUS_DIR"
  exec 9>"${STATUS_DIR}/upgrade.lock"
  flock -n 9 || fail "gateway upgrade already running"
fi

RESOLVED_TAG="$REQUESTED_TAG"
if [ "$REQUESTED_TAG" = "latest" ]; then
  log "resolving latest stable server release"
  RESOLVED_TAG="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases?per_page=50" | python3 -c '
import json, re, sys
for rel in json.load(sys.stdin):
    tag = rel.get("tag_name", "")
    if rel.get("draft") or rel.get("prerelease"):
        continue
    if re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
        print(tag)
        raise SystemExit(0)
raise SystemExit("no stable server release found")
')"
fi

if ! [[ "$RESOLVED_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-(beta|rc)\.[0-9]+)?$ ]]; then
  fail "resolved invalid tag: $RESOLVED_TAG"
fi

VERSION="${RESOLVED_TAG#v}"
ASSET="goclaw-${VERSION}-linux-amd64.tar.gz"
ASSET_URL="https://github.com/${REPO}/releases/download/${RESOLVED_TAG}/${ASSET}"
CHECKSUM_URL="https://github.com/${REPO}/releases/download/${RESOLVED_TAG}/CHECKSUMS.sha256"
TARGET_DIR="${RELEASES_DIR}/${RESOLVED_TAG}"

log "requested=${REQUESTED_TAG} resolved=${RESOLVED_TAG} asset=${ASSET}"

if [ "$DRY_RUN" = "1" ]; then
  TMP_DIR="$(mktemp -d)"
  cleanup() { rm -rf "$TMP_DIR"; }
  trap cleanup EXIT
  cd "$TMP_DIR"
  curl -fsSLO "$ASSET_URL"
  curl -fsSLO "$CHECKSUM_URL"
  grep " ${ASSET}$\|${ASSET}$" CHECKSUMS.sha256 | sha256sum -c -
  log "dry-run ok"
  exit 0
fi

write_status "running" "$REQUESTED_TAG" "$RESOLVED_TAG" ""

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

cd "$TMP_DIR"
log "downloading release asset"
curl -fsSLO "$ASSET_URL"
curl -fsSLO "$CHECKSUM_URL"

grep " ${ASSET}$\|${ASSET}$" CHECKSUMS.sha256 | sha256sum -c - || fail "checksum verification failed"

if [ -e "$TARGET_DIR" ]; then
  fail "target release already exists: $TARGET_DIR"
fi
mkdir -p "$TARGET_DIR"
tar -xzf "$ASSET" -C "$TARGET_DIR"
chmod +x "$TARGET_DIR/goclaw"

if [ ! -x "$TARGET_DIR/goclaw" ] || [ ! -d "$TARGET_DIR/migrations" ]; then
  fail "release archive missing goclaw binary or migrations directory"
fi

log "deploying ${RESOLVED_TAG}"
"$DEPLOY_BIN" "$TARGET_DIR" || fail "deploy failed"
write_status "succeeded" "$REQUESTED_TAG" "$RESOLVED_TAG" ""
log "upgrade complete: ${RESOLVED_TAG}"
