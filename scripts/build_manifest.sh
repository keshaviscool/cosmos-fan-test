#!/usr/bin/env bash
# Stands in for: "dwh-transforms pushes manifest.json to S3 on commit."
# Clones the project fresh (like a CI job would) and writes target/manifest.json
# into the local manifest store that the DAG's ProjectConfig.manifest_path reads.
set -euo pipefail

REPO_URL="${DBT_REPO:-file:///opt/airflow/local_git/dwh-transforms.git}"
MANIFEST_STORE="${MANIFEST_STORE_DIR:-/opt/airflow/manifest_store}"
PROFILES_DIR="${PROFILES_DIR:-/opt/airflow/dbt_profiles}"
DBT_BINARY="${DBT_BINARY:-/opt/airflow/dbt_env/bin/dbt}"

BUILD_DIR=$(mktemp -d)
echo "Simulating CI: cloning $REPO_URL into $BUILD_DIR"
git config --global --add safe.directory /opt/airflow/local_git/dwh-transforms.git
git clone --depth 1 "$REPO_URL" "$BUILD_DIR"

cd "$BUILD_DIR"
unset PYTHONPATH
"$DBT_BINARY" deps || true
"$DBT_BINARY" parse --profiles-dir "$PROFILES_DIR"

mkdir -p "$MANIFEST_STORE"
cp target/manifest.json "$MANIFEST_STORE/manifest.json"
echo "manifest.json written to $MANIFEST_STORE/manifest.json"