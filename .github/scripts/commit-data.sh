#!/usr/bin/env bash
# Commit and push freshly scraped data — but only when the data itself changed.
# Usage: commit-data.sh <path> <commit message>
#
# Every run rewrites "scrapedAt" / "updatedAt" in every file. When nothing else changed,
# the published data is the same, so the run commits nothing: weekly runs would otherwise
# add a commit touching every file each time.
set -eu

path="$1"
message="$2"

git config user.name "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"
git add "$path"

real_changes=$(
  git diff --staged -U0 \
    | grep -E '^[+-]' \
    | grep -vE '^(\+\+\+|---) ' \
    | grep -vE '^[+-][[:space:]]*"(scrapedAt|updatedAt)":' \
    | wc -l
)

if [ "$real_changes" -eq 0 ]; then
  echo "No data changes (timestamps only) — skipping commit"
  git reset -q
  exit 0
fi

echo "Data changed ($((real_changes)) lines) — committing"
git commit -q -m "$message"
# The other scraper may have pushed in the meantime; the two never touch the same files.
git pull -q --rebase origin main
git push -q
