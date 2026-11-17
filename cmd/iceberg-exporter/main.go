package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"iceberg-exporter/internal/exporter"
	"iceberg-exporter/internal/metadata"
	"iceberg-exporter/internal/nessie"
	"iceberg-exporter/internal/storage"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := run(os.Environ()); err != nil {
		slog.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(environ []string) error {
	cfg, err := loadConfig(environ)
	if err != nil {
		return err
	}
	reader, err := storage.New(cfg.Storage)
	if err != nil {
		return err
	}
	client, err := nessie.New(nessie.Config{
		Name:     cfg.CatalogName,
		BaseURL:  cfg.NessieURL,
		Token:    cfg.NessieToken,
		Refs:     cfg.Refs,
		BaseRef:  cfg.BaseRef,
		PageSize: cfg.PageSize,
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := refreshNessieToken(ctx, cfg, client); err != nil {
		return err
	}
	meta := metadata.NewCollector(reader, metadata.WithEntries(metadata.EntriesConfig{
		RefreshInterval: cfg.Refresh,
		DeleteManifests: cfg.Deletes,
		Concurrency:     cfg.EntryConcurrency,
		MaxQueue:        cfg.EntryMaxQueue,
		MaxBytes:        cfg.EntryMaxBytes,
	}))
	defer meta.Close()
	exp, err := exporter.New(exporter.Config{
		Interval:   cfg.Interval,
		Namespaces: cfg.Namespaces,
		PageSize:   cfg.PageSize,
	}, meta, client)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		err := exp.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	mux := http.NewServeMux()
	mux.Handle("/metrics", exp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("listening", "addr", cfg.Listen, "catalog", cfg.CatalogName, "nessie", cfg.NessieURL)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		stop()
		shutdown(srv)
		if err != nil {
			return err
		}
	}
	shutdown(srv)
	err = <-errCh
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func shutdown(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("http shutdown", "err", err)
	}
}
