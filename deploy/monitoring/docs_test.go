package monitoring

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestDocsMatchExporter(t *testing.T) {
	root := repoRoot(t)
	readme := mustRead(t, filepath.Join(root, "README.md"))
	metrics := mustRead(t, filepath.Join(root, "docs", "metrics.md"))
	alerts := mustRead(t, filepath.Join(root, "docs", "alerts.md"))

	for _, link := range []string{
		"docs/metrics.md",
		"docs/alerts.md",
		"deploy/grafana/dashboards/",
		"deploy/prometheus/iceberg.rules.yaml",
		"internal/catalog",
		"internal/nessie",
		"internal/metadata",
		"internal/exporter",
		"/metrics",
		"go test ./...",
	} {
		if !strings.Contains(readme, link) {
			t.Errorf("README.md does not mention %s", link)
		}
	}
	for _, field := range []string{
		"NESSIE_TOKEN",
		"BaseRef",
		"HistoryMaxRecords",
		"ListInterval",
		"ReconcileInterval",
		"RefreshInterval",
		"SmallFileBytes",
		"DeleteManifests",
		"60",
		"32 MiB",
	} {
		if !strings.Contains(readme, field) {
			t.Errorf("README.md does not mention %s", field)
		}
	}

	descName := regexp.MustCompile(`prometheus\.NewDesc\(\s*\n\s*"([^"]+)"`)
	var names []string
	for _, rel := range []string{
		"internal/exporter/metrics.go",
		"internal/metadata/collector.go",
	} {
		for _, match := range descName.FindAllStringSubmatch(mustRead(t, filepath.Join(root, rel)), -1) {
			names = append(names, match[1])
		}
	}
	if len(names) < 30 {
		t.Fatalf("found %d metric descriptors, source layout changed", len(names))
	}
	for _, name := range names {
		if !strings.Contains(metrics, name) {
			t.Errorf("docs/metrics.md missing %s", name)
		}
	}
	for _, piece := range []string{
		"sum_over_time",
		"stage",
		"manifest_entries",
		"iceberg_table_manifest_cache_age_seconds",
		"8 MiB",
		"histogram_quantile",
	} {
		if !strings.Contains(metrics, piece) {
			t.Errorf("docs/metrics.md missing %s", piece)
		}
	}

	rules := mustRead(t, filepath.Join(root, "deploy", "prometheus", "iceberg.rules.yaml"))
	alertName := regexp.MustCompile(`(?m)^      - alert: (\w+)`)
	recordName := regexp.MustCompile(`(?m)^      - record: (\S+)`)
	alertsFound := alertName.FindAllStringSubmatch(rules, -1)
	recordsFound := recordName.FindAllStringSubmatch(rules, -1)
	if len(alertsFound) == 0 || len(recordsFound) == 0 {
		t.Fatal("rules file has no alerts or recording rules")
	}
	for _, match := range alertsFound {
		if !strings.Contains(alerts, match[1]) {
			t.Errorf("docs/alerts.md missing alert %s", match[1])
		}
	}
	for _, match := range recordsFound {
		if !strings.Contains(alerts, match[1]) {
			t.Errorf("docs/alerts.md missing recording rule %s", match[1])
		}
	}
	for _, piece := range []string{
		"sum_over_time",
		"histogram_quantile",
		"iceberg-catalog.json",
		"iceberg-table-health.json",
		"iceberg-table.json",
		"iceberg-namespace.json",
		"iceberg.rules.yaml",
		"ScanReport",
	} {
		if !strings.Contains(alerts, piece) {
			t.Errorf("docs/alerts.md missing %s", piece)
		}
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
