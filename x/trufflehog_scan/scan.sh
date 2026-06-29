#!/usr/bin/env bash
#
# Pre-push hook: scan only the lines ADDED in the pushed range for verified
# secrets, via the TruffleHog Docker image. Avoids `trufflehog git file://.`,
# which clones the whole repo (slow) and trips a //.gitconfig lock error under
# pre-commit's docker_image language. Wired from .pre-commit-config.yaml.
set -uo pipefail

IMAGE="trufflesecurity/trufflehog:3.95.3"

if ! command -v docker >/dev/null 2>&1; then
  echo "trufflehog pre-push scan: docker not found on PATH; cannot scan for secrets." >&2
  exit 1
fi

# pre-commit exports these for pre-push hooks (FROM = remote tip, TO = local tip).
from_ref="${PRE_COMMIT_FROM_REF:-${PRE_COMMIT_ORIGIN:-}}"
to_ref="${PRE_COMMIT_TO_REF:-${PRE_COMMIT_SOURCE:-HEAD}}"
[ -n "$to_ref" ] || to_ref="HEAD"

# New branch: remote tip is all-zeros. Fall back to merge-base with the default
# branch so we scan only this branch's new commits.
if [ -z "$from_ref" ] || ! git rev-parse --verify --quiet "${from_ref}^{commit}" >/dev/null 2>&1; then
  base_branch="$(git symbolic-ref --quiet refs/remotes/origin/HEAD 2>/dev/null | sed 's@^refs/remotes/@@')"
  [ -n "$base_branch" ] || base_branch="origin/master"
  from_ref="$(git merge-base "$to_ref" "$base_branch" 2>/dev/null || true)"
fi

[ -n "$from_ref" ] && range="${from_ref}..${to_ref}" || range="$to_ref"

# Collect added lines per file (keeps file paths in findings).
workdir="$(mktemp -d /tmp/trufflehog-scan.XXXXXX)"
trap 'rm -rf "$workdir"' EXIT

found_any=0
while IFS= read -r -d '' file; do
  # -U0 = changed lines only; keep added lines (drop `+++` header), strip the `+`.
  added="$(git diff --no-color -U0 "$range" -- "$file" \
             | grep -E '^\+' | grep -vE '^\+\+\+' | sed 's/^+//')"
  [ -n "$added" ] || continue
  mkdir -p "$workdir/$(dirname "$file")"
  printf '%s\n' "$added" > "$workdir/$file"
  found_any=1
done < <(git diff --name-only -z "$range")

[ "$found_any" -eq 0 ] && exit 0

# Run as root with a writable HOME so TruffleHog's git-config write doesn't error.
docker run --rm -e HOME=/tmp -v "$workdir:/scan:ro" \
  "$IMAGE" --no-update filesystem --only-verified --fail /scan
