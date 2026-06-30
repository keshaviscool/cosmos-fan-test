#!/usr/bin/env bash
# Stands in for the real `dwh-transforms` GitHub repo: turns the dbt project
# checked into ./dbt_source_repo into a local bare git repo that the DAG's
# clone_repo task can clone via a file:// URL, with no real git host involved.
set -euo pipefail

SRC_DIR="${1:-/seed-src}"
BARE_DIR="${2:-/git-store/dwh-transforms.git}"

WORK_DIR=$(mktemp -d)
cp -r "$SRC_DIR"/. "$WORK_DIR"/

cd "$WORK_DIR"
git init -q
git config user.email "ci@example.com"
git config user.name "ci"
git checkout -q -b main
git add -A
git commit -q -m "seed dwh-transforms dbt project"

rm -rf "$BARE_DIR"
git clone -q --bare "$WORK_DIR" "$BARE_DIR"

echo "Bare repo created at $BARE_DIR (branch: main)"