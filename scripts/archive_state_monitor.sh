#!/usr/bin/env bash
# Re-run the tier-one corpus and report what the archives changed since last time.
#
# The longitudinal record nobody else is keeping. Re-created BioProjects,
# withdrawn series, retro-added SRA relations: none of these is announced
# anywhere, and each one silently changes what a pipeline resolves. Running this
# on a schedule turns that into a dated diff.
#
# It also runs the inverted pathology suite. A failure there is good news: the
# archive fixed something, and the knowledge-base page needs updating.
#
# Usage:  scripts/archive_state_monitor.sh [snapshot-dir]
set -euo pipefail

SNAPSHOTS="${1:-data/snapshots}"
CACHE=".cache/fetch-series.sqlite"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$SNAPSHOTS"

PREVIOUS="$(ls -1 "$SNAPSHOTS"/*.sqlite 2>/dev/null | tail -1 || true)"

# Every route that has an implementation, over the 46-accession tier-one corpus.
# --retry-failed matters: a route that failed last time is exactly the one whose
# recovery is worth noticing.
while read -r ROUTE; do
  uv run fetch survey run --route "$ROUTE" --corpus hard-cases \
    --rps 4 --concurrency 4 --no-resume --retry-failed >/dev/null || true
done < <(uv run fetch routes list --implemented --ids-only)

if [[ -n "$PREVIOUS" ]]; then
  echo "diffing against $PREVIOUS"
  uv run fetch cache diff "$PREVIOUS" --corpus hard-cases || CHANGED=1
fi

cp "$CACHE" "$SNAPSHOTS/$STAMP.sqlite"
echo "snapshot written to $SNAPSHOTS/$STAMP.sqlite"

echo "--- pathology suite (a FAILURE here means an archive fixed something)"
uv run pytest -m pathology || true

exit "${CHANGED:-0}"
