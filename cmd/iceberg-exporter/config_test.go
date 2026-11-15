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
	if cfg.Interval != 15*time.Second || cfg.Refresh != time.Minute || !cfg.Deletes {
		t.Fatalf("durations: %+v", cfg)
	}
	if len(cfg.Refs) != 2 || cfg.Refs[0].Name != "main" || cfg.Refs[1].Name != "dev" {
		t.Fatalf("refs: %+v", cfg.Refs)
	}
	if !cfg.Storage.PathStyle || cfg.Storage.Region != "us-east-1" {
		t.Fatalf("storage: %+v", cfg.Storage)
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
}

func TestLoadConfigRejects(t *testing.T) {
	t.Parallel()
	cases := [][]string{
		nil,
		{"NESSIE_URL=http://nessie:19120", "REFS=,"},
		{"NESSIE_URL=http://nessie:19120", "INTERVAL=soon"},
		{"NESSIE_URL=http://nessie:19120", "DELETE_MANIFESTS=maybe"},
		{"NESSIE_URL=http://nessie:19120", "REFS=main,main"},
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
