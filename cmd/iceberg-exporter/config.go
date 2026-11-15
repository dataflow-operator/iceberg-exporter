package main

import (
	"fmt"
	"strings"
	"time"

	"iceberg-exporter/internal/catalog"
	"iceberg-exporter/internal/storage"
)

type config struct {
	CatalogName string
	NessieURL   string
	NessieToken string
	Refs        []catalog.Ref
	BaseRef     string
	Listen      string
	Interval    time.Duration
	Refresh     time.Duration
	Deletes     bool
	Storage     storage.Config
}

func loadConfig(environ []string) (config, error) {
	env := make(map[string]string, len(environ))
	for _, item := range environ {
		key, value, ok := strings.Cut(item, "=")
		if !ok || key == "" {
			continue
		}
		env[key] = value
	}
	cfg := config{
		CatalogName: "local",
		Refs:        []catalog.Ref{{Name: "main", Kind: catalog.RefBranch}, {Name: "dev", Kind: catalog.RefBranch}},
		BaseRef:     "main",
		Listen:      ":8080",
		Interval:    15 * time.Second,
		Refresh:     time.Minute,
		Deletes:     true,
		Storage: storage.Config{
			Region:    "us-east-1",
			PathStyle: true,
		},
	}
	if v := strings.TrimSpace(env["CATALOG_NAME"]); v != "" {
		cfg.CatalogName = v
	}
	cfg.NessieURL = strings.TrimSpace(env["NESSIE_URL"])
	if cfg.NessieURL == "" {
		return config{}, fmt.Errorf("NESSIE_URL is required")
	}
	cfg.NessieToken = strings.TrimSpace(env["NESSIE_TOKEN"])
	if raw, ok := env["REFS"]; ok {
		refs, err := parseRefs(raw)
		if err != nil {
			return config{}, err
		}
		cfg.Refs = refs
	}
	if v, ok := env["BASE_REF"]; ok {
		cfg.BaseRef = strings.TrimSpace(v)
	}
	if v := strings.TrimSpace(env["LISTEN"]); v != "" {
		cfg.Listen = v
	}
	var err error
	if v := strings.TrimSpace(env["INTERVAL"]); v != "" {
		cfg.Interval, err = time.ParseDuration(v)
		if err != nil {
			return config{}, fmt.Errorf("INTERVAL: %w", err)
		}
	}
	if v := strings.TrimSpace(env["MANIFEST_REFRESH"]); v != "" {
		cfg.Refresh, err = time.ParseDuration(v)
		if err != nil {
			return config{}, fmt.Errorf("MANIFEST_REFRESH: %w", err)
		}
	}
	if v, ok := env["DELETE_MANIFESTS"]; ok {
		cfg.Deletes, err = parseBool(v)
		if err != nil {
			return config{}, fmt.Errorf("DELETE_MANIFESTS: %w", err)
		}
	}
	cfg.Storage.Endpoint = strings.TrimSpace(env["S3_ENDPOINT"])
	if v := strings.TrimSpace(env["S3_REGION"]); v != "" {
		cfg.Storage.Region = v
	}
	cfg.Storage.AccessKey = env["S3_ACCESS_KEY"]
	cfg.Storage.SecretKey = env["S3_SECRET_KEY"]
	if v, ok := env["S3_PATH_STYLE"]; ok && strings.TrimSpace(v) != "" {
		cfg.Storage.PathStyle, err = parseBool(v)
		if err != nil {
			return config{}, fmt.Errorf("S3_PATH_STYLE: %w", err)
		}
	}
	if v := strings.TrimSpace(env["MAX_OBJECT_BYTES"]); v != "" {
		var n int64
		_, err = fmt.Sscan(v, &n)
		if err != nil {
			return config{}, fmt.Errorf("MAX_OBJECT_BYTES: %w", err)
		}
		cfg.Storage.MaxBytes = n
	}
	return cfg, nil
}

func parseRefs(raw string) ([]catalog.Ref, error) {
	parts := strings.Split(raw, ",")
	refs := make([]catalog.Ref, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("REFS: duplicate %q", name)
		}
		seen[name] = struct{}{}
		refs = append(refs, catalog.Ref{Name: name, Kind: catalog.RefBranch})
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("REFS: empty")
	}
	return refs, nil
}

func parseBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes":
		return true, nil
	case "0", "false", "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid bool %q", raw)
	}
}
