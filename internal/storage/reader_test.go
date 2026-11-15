package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")
	if err := os.WriteFile(path, []byte("{\"ok\":true}"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := New(Config{MaxBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	got, err := r.Read(context.Background(), "file://"+path+"?token=supersecret")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "{\"ok\":true}" {
		t.Fatalf("body %q", got)
	}
}

func TestReadFileOmitsQuery(t *testing.T) {
	t.Parallel()
	r, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Read(context.Background(), "file:///tmp/iceberg-exporter-missing?token=supersecret")
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "supersecret") {
		t.Fatalf("query leaked: %v", err)
	}
}

func TestReadRejectsSecretsAndSchemes(t *testing.T) {
	t.Parallel()
	r, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	cases := []string{
		"http://example.test/obj?token=supersecret",
		"s3://warehouse/path/meta.json?X-Amz-Signature=supersecret",
		"s3a://warehouse/path/meta.json?X-Amz-Signature=supersecret",
		"s3://warehouse?token=supersecret",
		"://not-a-url",
	}
	for _, location := range cases {
		t.Run(location, func(t *testing.T) {
			_, err := r.Read(context.Background(), location)
			if err == nil {
				t.Fatal("expected error")
			}
			if strings.Contains(err.Error(), "supersecret") {
				t.Fatalf("secret leaked: %v", err)
			}
		})
	}
}

func TestReadFileTooLarge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "big")
	if err := os.WriteFile(path, []byte("abcdef"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := New(Config{MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.Read(context.Background(), "file://"+path)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("got %v", err)
	}
}

func TestNewRequiresS3Credentials(t *testing.T) {
	t.Parallel()
	_, err := New(Config{Endpoint: "http://minio:9000", Region: "us-east-1"})
	if err == nil {
		t.Fatal("expected error")
	}
	_, err = New(Config{MaxBytes: -1})
	if err == nil {
		t.Fatal("expected error")
	}
}
