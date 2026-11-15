#!/bin/sh
set -eu

for _ in $(seq 1 90); do
  if body=$(curl -fsS http://localhost:8080/metrics 2>/dev/null); then
    if printf '%s\n' "$body" | grep -q 'iceberg_catalog_up{catalog="local",type="nessie"} 1' \
      && printf '%s\n' "$body" | grep -Eq 'iceberg_tables\{[^}]*\} [1-9]'; then
      echo "catalog is up and tables are visible"
      exit 0
    fi
  fi
  sleep 5
done

echo "metrics were not ready in time" >&2
exit 1
