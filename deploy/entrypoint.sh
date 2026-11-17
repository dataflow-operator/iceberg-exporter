#!/bin/sh
# Start iceberg-exporter and leave it running.
# The process refreshes the Keycloak bearer from KEYCLOAK_TOKEN_URL and
# OAUTH_CREDENTIALS. Killing it to rotate NESSIE_TOKEN serves 503 until the
# next catalog walk finishes, and a restart drops every scraped series.
set -eu
exec iceberg-exporter
