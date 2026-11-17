package deploy_test

import (
	"os"
	"strings"
	"testing"
)

func TestEntrypointDoesNotKillExporter(t *testing.T) {
	raw, err := os.ReadFile("entrypoint.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.Contains(text, "exec iceberg-exporter") {
		t.Fatal("entrypoint does not exec iceberg-exporter")
	}
	if strings.Contains(text, "kill") || strings.Contains(text, "TOKEN_RESTART_INTERVAL") {
		t.Fatal("entrypoint restarts the process to rotate the token")
	}
}
