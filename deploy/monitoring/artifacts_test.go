package monitoring

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPrometheusRules(t *testing.T) {
	path := filepath.Join(repoRoot(t), "deploy", "prometheus", "iceberg.rules.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var file ruleFile
	if err := yaml.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Groups) != 1 || file.Groups[0].Name != "iceberg" {
		t.Fatalf("groups = %+v", file.Groups)
	}
	rules := map[string]rule{}
	var alerts []rule
	for _, r := range file.Groups[0].Rules {
		switch {
		case r.Record != "":
			rules[r.Record] = r
		case r.Alert != "":
			rules[r.Alert] = r
			alerts = append(alerts, r)
		default:
			t.Fatalf("rule without record or alert: %+v", r)
		}
	}

	if got := rules["iceberg_rules:catalog_list_interval_seconds"].Expr; strings.TrimSpace(got) != "vector(60)" {
		t.Fatalf("list interval = %q", got)
	}
	if got := strings.TrimSpace(rules["iceberg_rules:catalog_list_max_age_seconds"].Expr); got != "3 * iceberg_rules:catalog_list_interval_seconds" {
		t.Fatalf("list max age = %q", got)
	}
	if got := strings.TrimSpace(rules["iceberg_rules:manifest_refresh_interval_seconds"].Expr); got != "vector(3600)" {
		t.Fatalf("manifest refresh = %q, want the exporter default of 1h", got)
	}
	if got := strings.TrimSpace(rules["iceberg_rules:manifest_cache_fresh_seconds"].Expr); got != "2 * iceberg_rules:manifest_refresh_interval_seconds" {
		t.Fatalf("cache fresh = %q", got)
	}

	page := map[string]string{
		"IcebergCatalogDown":         "5m",
		"IcebergExporterDown":        "5m",
		"IcebergCatalogScrapeErrors": "15m",
	}
	for name, dur := range page {
		assertAlert(t, rules, name, "page", dur)
	}
	if !strings.Contains(rules["IcebergCatalogDown"].Expr, "iceberg_catalog_up == 0") {
		t.Fatalf("catalog down expr: %s", rules["IcebergCatalogDown"].Expr)
	}
	if !strings.Contains(rules["IcebergExporterDown"].Expr, `up{job="iceberg-exporter"} == 0`) {
		t.Fatalf("exporter down expr: %s", rules["IcebergExporterDown"].Expr)
	}
	assertAlert(t, rules, "IcebergCatalogListStale", "page", "")
	if !strings.Contains(rules["IcebergCatalogListStale"].Expr, "iceberg_catalog_list_age_seconds") ||
		!strings.Contains(rules["IcebergCatalogListStale"].Expr, "iceberg_rules:catalog_list_max_age_seconds") {
		t.Fatalf("list stale expr: %s", rules["IcebergCatalogListStale"].Expr)
	}
	if !strings.Contains(rules["IcebergCatalogScrapeErrors"].Expr, "0.05") ||
		!strings.Contains(rules["IcebergCatalogScrapeErrors"].Expr, "iceberg_tables") {
		t.Fatalf("scrape error expr: %s", rules["IcebergCatalogScrapeErrors"].Expr)
	}

	assertAlert(t, rules, "IcebergManifestQueueGrowing", "warning", "30m")
	if !strings.Contains(rules["IcebergManifestQueueGrowing"].Expr, "deriv(iceberg_exporter_manifest_queue[15m]) > 0") {
		t.Fatalf("queue expr: %s", rules["IcebergManifestQueueGrowing"].Expr)
	}

	assertAlert(t, rules, "IcebergCatalogRefHeadStale", "warning", "15m")
	head := rules["IcebergCatalogRefHeadStale"].Expr
	if !strings.Contains(head, "iceberg_rules:ref_head_max_age_seconds") || !strings.Contains(head, "on (catalog, ref)") {
		t.Fatalf("head age alert must be opt-in per ref, got %s", head)
	}
	if strings.Contains(head, ">") && strings.Contains(head, "3600") {
		t.Fatalf("head age alert has a global threshold: %s", head)
	}

	assertAlert(t, rules, "IcebergTableMetadataRetention", "warning", "1h")
	retention := rules["IcebergTableMetadataRetention"].Expr
	if !strings.Contains(retention, "iceberg_table_snapshots > 1000") ||
		!strings.Contains(retention, "iceberg_table_metadata_log_entries > 100") ||
		!strings.Contains(retention, "unless on (catalog, ref, namespace, table)") ||
		!strings.Contains(retention, "iceberg_table_has_snapshot == 0") {
		t.Fatalf("retention expr: %s", retention)
	}

	assertAlert(t, rules, "IcebergTablePositionDeletes", "warning", "24h")
	pos := rules["IcebergTablePositionDeletes"].Expr
	if !strings.Contains(pos, "> 0.2") || !strings.Contains(pos, "iceberg_table_records > 1000000") {
		t.Fatalf("position delete expr: %s", pos)
	}
	if strings.Contains(pos, "manifest_cache_age") {
		t.Fatalf("position deletes come from snapshot summary, not the manifest cache: %s", pos)
	}

	for _, name := range []string{"IcebergTableSmallFiles", "IcebergTablePartitionSkew"} {
		expr := rules[name].Expr
		if !strings.Contains(expr, "iceberg_table_manifest_cache_age_seconds") ||
			!strings.Contains(expr, "< on() group_left()") ||
			!strings.Contains(expr, "iceberg_rules:manifest_cache_fresh_seconds") {
			t.Fatalf("%s must be gated on per-table cache age: %s", name, expr)
		}
		if strings.Contains(expr, "manifest_cache_age_seconds\n            >") || strings.Contains(expr, "manifest_cache_age_seconds >") {
			t.Fatalf("%s treats a stale cache as the table fault: %s", name, expr)
		}
	}
	assertAlert(t, rules, "IcebergTableSmallFiles", "warning", "6h")
	small := rules["IcebergTableSmallFiles"].Expr
	if !strings.Contains(small, "iceberg_table_small_files_ratio > 0.5") || !strings.Contains(small, "iceberg_table_data_files > 100") {
		t.Fatalf("small files expr: %s", small)
	}
	assertAlert(t, rules, "IcebergTablePartitionSkew", "warning", "1h")
	skew := rules["IcebergTablePartitionSkew"].Expr
	if !strings.Contains(skew, "iceberg_table_partition_skew_ratio > 10") || !strings.Contains(skew, "iceberg_table_partitions >= 10") {
		t.Fatalf("skew expr: %s", skew)
	}

	assertAlert(t, rules, "IcebergTableManifestCacheStale", "warning", "30m")
	stale := rules["IcebergTableManifestCacheStale"].Expr
	if !strings.Contains(stale, "iceberg_table_manifest_cache_age_seconds") ||
		!strings.Contains(stale, "> on() group_left()") ||
		!strings.Contains(stale, "iceberg_rules:manifest_cache_fresh_seconds") {
		t.Fatalf("stale cache expr: %s", stale)
	}

	for _, alert := range alerts {
		if strings.Contains(alert.Expr, "format_version") {
			t.Fatalf("format version is a dashboard list, not an alert: %s", alert.Alert)
		}
		if strings.Contains(alert.Expr, "iceberg_ref_tables_diverged") {
			t.Fatalf("branch divergence is a panel, not a default alert: %s", alert.Alert)
		}
		if strings.Contains(alert.Expr, "rate(") {
			t.Fatalf("alert %s uses rate()", alert.Alert)
		}
		if alert.Annotations["summary"] == "" || alert.Annotations["description"] == "" {
			t.Fatalf("alert %s is missing annotations", alert.Alert)
		}
	}

	if _, err := exec.LookPath("promtool"); err == nil {
		out, err := exec.Command("promtool", "check", "rules", path).CombinedOutput()
		if err != nil {
			t.Fatalf("promtool: %v\n%s", err, out)
		}
	}
}

func TestGrafanaDashboards(t *testing.T) {
	dir := filepath.Join(repoRoot(t), "deploy", "grafana", "dashboards")
	catalog := loadDashboard(t, filepath.Join(dir, "iceberg-catalog.json"))
	health := loadDashboard(t, filepath.Join(dir, "iceberg-table-health.json"))
	table := loadDashboard(t, filepath.Join(dir, "iceberg-table.json"))
	namespace := loadDashboard(t, filepath.Join(dir, "iceberg-namespace.json"))

	if catalog.UID != "iceberg-catalog" || health.UID != "iceberg-table-health" || table.UID != "iceberg-table" || namespace.UID != "iceberg-namespace" {
		t.Fatalf("uids: %s %s %s %s", catalog.UID, health.UID, table.UID, namespace.UID)
	}
	for _, dash := range []dashboard{catalog, health, table, namespace} {
		ref := variable(t, dash, "ref")
		if ref.Current.Value != "main" {
			t.Fatalf("%s ref default = %#v", dash.UID, ref.Current)
		}
		variable(t, dash, "exported_namespace")
		for _, expr := range exprs(dash) {
			if !strings.Contains(expr, "exported_namespace=") {
				t.Fatalf("%s expr missing exported_namespace: %s", dash.UID, expr)
			}
		}
	}
	if variable(t, table, "ref").Multi {
		t.Fatal("single-table ref variable selects many refs")
	}
	if variable(t, table, "table").Name != "table" || variable(t, table, "namespace").Name != "namespace" {
		t.Fatal("single-table dashboard is missing namespace or table")
	}

	catalogExpr := strings.Join(exprs(catalog), "\n")
	if !strings.Contains(catalogExpr, "sum_over_time(iceberg_catalog_commits_since_scrape") {
		t.Fatal("catalog dashboard does not sum commits over 1h")
	}
	for _, expr := range exprs(catalog) {
		if !strings.Contains(expr, "max by (") {
			t.Fatalf("catalog series are not collapsed: %s", expr)
		}
	}
	if strings.Contains(catalogExpr, "rate(") {
		t.Fatal("catalog dashboard uses rate()")
	}
	for _, metric := range []string{
		"iceberg_catalog_up",
		"iceberg_catalog_ref_head_age_seconds",
		"iceberg_catalog_list_age_seconds",
		"iceberg_tables",
		"iceberg_views",
		"iceberg_namespaces",
		"iceberg_ref_tables_diverged",
		"iceberg_exporter_manifest_queue",
		"iceberg_table_scrape_error",
	} {
		if !strings.Contains(catalogExpr, metric) {
			t.Fatalf("catalog dashboard missing %s", metric)
		}
	}

	healthExpr := strings.Join(exprs(health), "\n")
	if strings.Contains(healthExpr, "histogram_quantile") {
		t.Fatal("table health dashboard quantiles every table")
	}
	if !strings.Contains(healthExpr, "topk(20, max by (exported_namespace, catalog, ref, namespace, table) (iceberg_table_snapshot_age_seconds") {
		t.Fatal("snapshot age is not topk")
	}
	for _, expr := range exprs(health) {
		switch {
		case strings.Contains(expr, "sum by (content)"):
			if !strings.Contains(expr, "max by (exported_namespace, catalog, ref, namespace, table, content)") {
				t.Fatalf("manifest content sum counts replicas: %s", expr)
			}
		case strings.Contains(expr, "iceberg_ref_tables_diverged"):
			if !strings.Contains(expr, "max by (exported_namespace, catalog, ref, base_ref)") {
				t.Fatalf("diverged series are not collapsed: %s", expr)
			}
		case strings.Contains(expr, "count by (operation)"):
			if !strings.Contains(expr, "max by (exported_namespace, catalog, ref, namespace, table, operation)") {
				t.Fatalf("operation count is not collapsed: %s", expr)
			}
		case strings.Contains(expr, "sum by (exported_namespace, catalog, ref, namespace, table)"):
		default:
			if !strings.Contains(expr, "max by (exported_namespace, catalog, ref, namespace, table)") {
				t.Fatalf("health series are not collapsed: %s", expr)
			}
		}
	}
	for _, piece := range []string{
		"iceberg_table_files_size_bytes",
		"clamp_min(iceberg_table_data_files",
		"delta(iceberg_table_records",
		"iceberg_table_position_deletes",
		"iceberg_table_equality_deletes",
		"iceberg_table_snapshots",
		"iceberg_table_metadata_log_entries",
		"iceberg_table_manifests",
		"iceberg_table_format_version",
		"iceberg_table_last_snapshot_info",
		"iceberg_table_has_snapshot",
		"iceberg_ref_tables_diverged",
	} {
		if !strings.Contains(healthExpr, piece) {
			t.Fatalf("health dashboard missing %s", piece)
		}
	}
	if !strings.Contains(healthExpr, "== 0") {
		t.Fatal("health dashboard has no empty-table filter")
	}
	empty := health.panelByTitle(t, "Пустые таблицы")
	if len(empty.Targets) != 1 || empty.Targets[0].Format != "table" {
		t.Fatalf("empty tables panel format = %+v", empty.Targets)
	}

	tableExpr := strings.Join(exprs(table), "\n")
	if strings.Count(tableExpr, "histogram_quantile(") != 2 {
		t.Fatalf("want p50 and p95 only, quantile count = %d", strings.Count(tableExpr, "histogram_quantile("))
	}
	for _, expr := range exprs(table) {
		if strings.Contains(expr, "histogram_quantile(") && !strings.Contains(expr, `table="$table"`) {
			t.Fatalf("quantile is not restricted to the selected table: %s", expr)
		}
		if !strings.Contains(expr, "max by (") && !strings.Contains(expr, "max(") {
			t.Fatalf("table series are not collapsed: %s", expr)
		}
	}
	for _, piece := range []string{
		"iceberg_table_records",
		"iceberg_table_data_files",
		"iceberg_table_files_size_bytes",
		"iceberg_table_snapshot_age_seconds",
		"iceberg_table_partitions",
		"iceberg_table_manifests",
		"iceberg_table_last_snapshot_info",
		"iceberg_table_position_deletes",
		"iceberg_table_equality_deletes",
		"iceberg_table_small_files_ratio",
		"iceberg_table_partition_skew_ratio",
		"iceberg_table_partition_bytes_max",
		"iceberg_table_manifest_cache_age_seconds",
		`le="8388608"`,
		`le="33554432"`,
		`le="134217728"`,
		`le="536870912"`,
		`le="1073741824"`,
		`le="+Inf"`,
	} {
		if !strings.Contains(tableExpr, piece) {
			t.Fatalf("table dashboard missing %s", piece)
		}
	}
	if !strings.Contains(table.panelByTitle(t, "Доля мелких файлов").Description, "кэш") {
		t.Fatal("small-file panel does not say the series is cached")
	}
	if table.panelByTitle(t, "Возраст кэша manifest entries").GridPos.Y <= table.panelByTitle(t, "Доля мелких файлов").GridPos.Y {
		t.Fatal("cache age panel is not next to the file-level panels")
	}

	if namespace.Title != "Iceberg: настройки таблиц" {
		t.Fatalf("namespace title %q", namespace.Title)
	}
	if !variable(t, namespace, "namespace").Multi || !variable(t, namespace, "exported_namespace").Multi {
		t.Fatal("namespace dashboard filters are not multi")
	}
	nsExpr := strings.Join(exprs(namespace), "\n")
	if strings.Contains(nsExpr, "rate(") {
		t.Fatal("namespace dashboard uses rate()")
	}
	for _, piece := range []string{
		"iceberg_table_write_config",
		"iceberg_table_write_target_file_size_bytes",
		"iceberg_table_parquet_row_group_size_bytes",
		"iceberg_table_parquet_page_size_bytes",
		"iceberg_table_manifest_target_size_bytes",
		"iceberg_table_partition_fields",
		"iceberg_table_partition_spec",
		"clamp_min(iceberg_table_data_files",
		"iceberg_table_small_files_ratio",
		"iceberg_table_partition_skew_ratio",
		"iceberg_table_manifest_cache_age_seconds",
		`delete_mode="merge-on-read"`,
		"67108864",
		"1073741824",
		"8388608",
		"536870912",
		"107374182400",
		"> bool 10000",
		`namespace=~"$namespace"`,
	} {
		if !strings.Contains(nsExpr, piece) {
			t.Fatalf("namespace dashboard missing %s", piece)
		}
	}
	list := namespace.panelByTitle(t, "Таблицы")
	if list.Type != "table" {
		t.Fatalf("tables panel type %s", list.Type)
	}
	for _, target := range list.Targets {
		if target.Format != "" {
			t.Fatalf("tables panel query format %q duplicates label columns", target.Format)
		}
	}
	var ids []string
	var filter, join string
	for _, tr := range list.Transformations {
		ids = append(ids, tr.ID)
		switch tr.ID {
		case "filterFieldsByName":
			filter = string(tr.Options)
		case "joinByField":
			join = string(tr.Options)
		}
	}
	if strings.Join(ids, ",") != "timeSeriesTable,filterFieldsByName,joinByField,organize" {
		t.Fatalf("tables panel transformations = %v", ids)
	}
	for _, label := range []string{"catalog", "ref", "namespace", "table", "row_id", "delete_mode", "transforms"} {
		if !strings.Contains(filter, label) {
			t.Fatalf("tables panel keeps no %s column", label)
		}
	}
	for _, label := range []string{"pod", "job", "instance", "__name__"} {
		if strings.Contains(filter, label) {
			t.Fatalf("tables panel keeps infra label %s", label)
		}
	}
	if !strings.Contains(join, `"byField"`) || !strings.Contains(join, `row_id`) {
		t.Fatalf("tables panel joins on %s", join)
	}
	keptLabels := 0
	for _, target := range list.Targets {
		if !strings.Contains(target.Expr, `label_join`) || !strings.Contains(target.Expr, `"row_id"`) || !strings.Contains(target.Expr, "max by (row_id") {
			t.Fatalf("tables panel query does not collapse labels: %s", target.RefID)
		}
		if strings.Contains(target.Expr, "max by (row_id, catalog, ref, namespace, table)") {
			keptLabels++
		}
	}
	if keptLabels != 1 {
		t.Fatalf("want one query to keep table labels, got %d", keptLabels)
	}
	for _, title := range []string{"Число таблиц", "С замечаниями"} {
		expr := namespace.panelByTitle(t, title).Targets[0].Expr
		if !strings.Contains(expr, "count(max by (exported_namespace, catalog, ref, namespace, table)") {
			t.Fatalf("%s counts replicas: %s", title, expr)
		}
	}
}

func assertAlert(t *testing.T, rules map[string]rule, name, severity, forDur string) {
	t.Helper()
	alert, ok := rules[name]
	if !ok {
		t.Fatalf("missing alert %s", name)
	}
	if alert.Labels["severity"] != severity {
		t.Fatalf("%s severity = %q", name, alert.Labels["severity"])
	}
	if alert.For != forDur {
		t.Fatalf("%s for = %q, want %q", name, alert.For, forDur)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

type ruleFile struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []rule `yaml:"rules"`
	} `yaml:"groups"`
}

type rule struct {
	Record      string            `yaml:"record"`
	Alert       string            `yaml:"alert"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for"`
	Labels      map[string]string `yaml:"labels"`
	Annotations map[string]string `yaml:"annotations"`
}

type dashboard struct {
	UID        string `json:"uid"`
	Title      string `json:"title"`
	Panels     []dashPanel
	Templating struct {
		List []variableDef `json:"list"`
	} `json:"templating"`
}

type dashPanel struct {
	Title       string `json:"title"`
	Type        string `json:"type"`
	Description string `json:"description"`
	GridPos     struct {
		Y int `json:"y"`
	} `json:"gridPos"`
	Targets []struct {
		Expr   string `json:"expr"`
		Format string `json:"format"`
		RefID  string `json:"refId"`
	} `json:"targets"`
	Transformations []struct {
		ID      string          `json:"id"`
		Options json.RawMessage `json:"options"`
	} `json:"transformations"`
	Panels []dashPanel `json:"panels"`
}

type variableDef struct {
	Name    string `json:"name"`
	Multi   bool   `json:"multi"`
	Current struct {
		Value any `json:"value"`
	} `json:"current"`
}

func loadDashboard(t *testing.T, path string) dashboard {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var dash dashboard
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatal(err)
	}
	return dash
}

func variable(t *testing.T, dash dashboard, name string) variableDef {
	t.Helper()
	for _, v := range dash.Templating.List {
		if v.Name == name {
			return v
		}
	}
	t.Fatalf("dashboard %s has no variable %s", dash.UID, name)
	return variableDef{}
}

func exprs(dash dashboard) []string {
	var out []string
	var walk func([]dashPanel)
	walk = func(panels []dashPanel) {
		for _, p := range panels {
			for _, target := range p.Targets {
				out = append(out, target.Expr)
			}
			walk(p.Panels)
		}
	}
	walk(dash.Panels)
	return out
}

func (d dashboard) panelByTitle(t *testing.T, title string) dashPanel {
	t.Helper()
	var walk func([]dashPanel) (dashPanel, bool)
	walk = func(panels []dashPanel) (dashPanel, bool) {
		for _, p := range panels {
			if p.Title == title {
				return p, true
			}
			if nested, ok := walk(p.Panels); ok {
				return nested, true
			}
		}
		return dashPanel{}, false
	}
	p, ok := walk(d.Panels)
	if !ok {
		t.Fatalf("dashboard %s has no panel %q", d.UID, title)
	}
	return p
}
