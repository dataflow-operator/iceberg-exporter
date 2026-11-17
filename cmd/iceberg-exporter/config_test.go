package main

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig([]string{"NESSIE_URL=http://nessie:19120"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CatalogName != "local" || cfg.BaseRef != "main" || cfg.Listen != ":8080" {
		t.Fatalf("defaults: %+v", cfg)
	}
	if cfg.Interval != 60*time.Second || cfg.Refresh != time.Hour || !cfg.Deletes {
		t.Fatalf("durations: %+v", cfg)
	}
	if len(cfg.Refs) != 2 || cfg.Refs[0].Name != "main" || cfg.Refs[1].Name != "dev" {
		t.Fatalf("refs: %+v", cfg.Refs)
	}
	if !cfg.Storage.PathStyle || cfg.Storage.Region != "us-east-1" {
		t.Fatalf("storage: %+v", cfg.Storage)
	}
	if cfg.PageSize != 0 || cfg.EntryConcurrency != 0 || cfg.EntryMaxQueue != 0 || cfg.EntryMaxBytes != 0 {
		t.Fatalf("entry defaults: %+v", cfg)
	}
	if len(cfg.Namespaces.Include) != 0 || len(cfg.Namespaces.Exclude) != 0 {
		t.Fatalf("namespaces: %+v", cfg.Namespaces)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig([]string{
		"NESSIE_URL=http://nessie:19120",
		"NESSIE_TOKEN=supersecret",
		"CATALOG_NAME=prod",
		"REFS=main, feature",
		"BASE_REF=main",
		"LISTEN=:9099",
		"INTERVAL=5s",
		"MANIFEST_REFRESH=30s",
		"DELETE_MANIFESTS=false",
		"NAMESPACES=analytics, ops, !analytics.tmp",
		"PAGE_SIZE=500",
		"ENTRY_CONCURRENCY=8",
		"ENTRY_MAX_QUEUE=1024",
		"ENTRY_MAX_BYTES=1048576",
		"S3_ENDPOINT=http://minio:9000",
		"S3_ACCESS_KEY=minioadmin",
		"S3_SECRET_KEY=supersecret",
		"S3_PATH_STYLE=false",
		"MAX_OBJECT_BYTES=128",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NessieToken != "supersecret" || cfg.Storage.SecretKey != "supersecret" {
		t.Fatal("credentials were not kept")
	}
	if cfg.CatalogName != "prod" || cfg.Listen != ":9099" || cfg.Deletes || cfg.Storage.PathStyle {
		t.Fatalf("overrides: %+v", cfg)
	}
	if cfg.Interval != 5*time.Second || cfg.Refresh != 30*time.Second || cfg.Storage.MaxBytes != 128 {
		t.Fatalf("numbers: %+v refresh %s", cfg.Storage, cfg.Refresh)
	}
	if len(cfg.Refs) != 2 || cfg.Refs[1].Name != "feature" {
		t.Fatalf("refs: %+v", cfg.Refs)
	}
	if cfg.PageSize != 500 || cfg.EntryConcurrency != 8 || cfg.EntryMaxQueue != 1024 || cfg.EntryMaxBytes != 1048576 {
		t.Fatalf("entry overrides: %+v", cfg)
	}
	if len(cfg.Namespaces.Include) != 2 || cfg.Namespaces.Include[0] != "analytics" || cfg.Namespaces.Include[1] != "ops" {
		t.Fatalf("include: %+v", cfg.Namespaces.Include)
	}
	if len(cfg.Namespaces.Exclude) != 1 || cfg.Namespaces.Exclude[0] != "analytics.tmp" {
		t.Fatalf("exclude: %+v", cfg.Namespaces.Exclude)
	}
}

func TestLoadConfigZeroEntryLimits(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig([]string{
		"NESSIE_URL=http://nessie:19120",
		"NAMESPACES= , ",
		"PAGE_SIZE=0",
		"ENTRY_CONCURRENCY=0",
		"ENTRY_MAX_QUEUE=0",
		"ENTRY_MAX_BYTES=0",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PageSize != 0 || cfg.EntryConcurrency != 0 || cfg.EntryMaxQueue != 0 || cfg.EntryMaxBytes != 0 {
		t.Fatalf("zeros: %+v", cfg)
	}
	if len(cfg.Namespaces.Include) != 0 || len(cfg.Namespaces.Exclude) != 0 {
		t.Fatalf("namespaces: %+v", cfg.Namespaces)
	}
}

func TestLoadConfigRejects(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		nil,
		{"NESSIE_URL=http://nessie:19120", "REFS=,"},
		{"NESSIE_URL=http://nessie:19120", "INTERVAL=soon"},
		{"NESSIE_URL=http://nessie:19120", "DELETE_MANIFESTS=maybe"},
		{"NESSIE_URL=http://nessie:19120", "REFS=main,main"},
		{"NESSIE_URL=http://nessie:19120", "PAGE_SIZE=-1"},
		{"NESSIE_URL=http://nessie:19120", "PAGE_SIZE=lots"},
		{"NESSIE_URL=http://nessie:19120", "ENTRY_CONCURRENCY=-2"},
		{"NESSIE_URL=http://nessie:19120", "ENTRY_MAX_QUEUE=-1"},
		{"NESSIE_URL=http://nessie:19120", "ENTRY_MAX_BYTES=-1"},
		{"NESSIE_URL=http://nessie:19120", "NAMESPACES=!"},
		{"NESSIE_URL=http://nessie:19120", "NAMESPACES=analytics,analytics"},
		{"NESSIE_URL=http://nessie:19120", "NAMESPACES=analytics,!analytics"},
	}
	for _, env := range cases {
		t.Run(strings.Join(env, ","), func(t *testing.T) {
			_, err := loadConfig(env)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
