package exporter

import "github.com/prometheus/client_golang/prometheus"

type catalogDescs struct {
	up         *prometheus.Desc
	tables     *prometheus.Desc
	views      *prometheus.Desc
	namespaces *prometheus.Desc
	listAge    *prometheus.Desc
	headAge    *prometheus.Desc
	commits    *prometheus.Desc
	diverged   *prometheus.Desc
}

func newCatalogDescs() catalogDescs {
	refLabels := []string{"catalog", "ref"}
	return catalogDescs{
		up: prometheus.NewDesc(
			"iceberg_catalog_up",
			"1 when the latest catalog probe succeeded. Table series keep the last successful cache when this is 0.",
			[]string{"catalog", "type"}, nil,
		),
		tables: prometheus.NewDesc(
			"iceberg_tables",
			"Iceberg tables on the ref. Views are not included.",
			refLabels, nil,
		),
		views: prometheus.NewDesc(
			"iceberg_views",
			"Iceberg views on the ref.",
			refLabels, nil,
		),
		namespaces: prometheus.NewDesc(
			"iceberg_namespaces",
			"Distinct namespaces among tables and views on the ref.",
			refLabels, nil,
		),
		listAge: prometheus.NewDesc(
			"iceberg_catalog_list_age_seconds",
			"Age of the cached table list. For a revisioned catalog this is the age of the last successful hash check. For every other catalog it is the age of the last full list. Served from memory.",
			refLabels, nil,
		),
		headAge: prometheus.NewDesc(
			"iceberg_catalog_ref_head_age_seconds",
			"Age of the ref HEAD commit. Absent when the catalog has no revision token or the commit time is not known.",
			refLabels, nil,
		),
		commits: prometheus.NewDesc(
			"iceberg_catalog_commits_since_scrape",
			"New revision tokens since this series was last scraped. The scrape reads the gauge and resets it, so sum_over_time counts commits. Absent when the catalog does not expose a commit log.",
			refLabels, nil,
		),
		diverged: prometheus.NewDesc(
			"iceberg_ref_tables_diverged",
			"Tables on ref whose snapshot id differs from base_ref, including tables that exist only on ref. Absent when the catalog does not compare refs.",
			[]string{"catalog", "ref", "base_ref"}, nil,
		),
	}
}

func (d catalogDescs) describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		d.up,
		d.tables,
		d.views,
		d.namespaces,
		d.listAge,
		d.headAge,
		d.commits,
		d.diverged,
	} {
		ch <- desc
	}
}
