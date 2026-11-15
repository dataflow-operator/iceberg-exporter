// Package storage reads warehouse objects for the metadata collector.
// Locations may carry credentials in the query string. Errors omit userinfo
// and the query.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const defaultMaxBytes int64 = 64 << 20

// Config is the warehouse reader. An empty Endpoint accepts only file://.
type Config struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	PathStyle bool
	// MaxBytes is the largest object Read returns. Zero uses 64 MiB.
	MaxBytes int64
}

// Reader loads s3:// and file:// objects.
type Reader struct {
	s3       *s3.Client
	maxBytes int64
}

// New builds a reader. S3 credentials stay in the client and are not logged.
func New(cfg Config) (*Reader, error) {
	maxBytes := cfg.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxBytes
	}
	if maxBytes < 0 {
		return nil, errors.New("storage: max bytes must not be negative")
	}
	r := &Reader{maxBytes: maxBytes}
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return r, nil
	}
	if strings.TrimSpace(cfg.Region) == "" {
		return nil, errors.New("storage: s3 region is required")
	}
	if strings.TrimSpace(cfg.AccessKey) == "" || strings.TrimSpace(cfg.SecretKey) == "" {
		return nil, errors.New("storage: s3 credentials are required")
	}
	r.s3 = s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: cfg.PathStyle,
	})
	return r, nil
}

// Read returns the object at location.
// file:// ignores the query. s3:// uses the bucket host and the decoded path.
func (r *Reader) Read(ctx context.Context, location string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	u, err := url.Parse(location)
	if err != nil || u.Scheme == "" {
		return nil, errors.New("storage: invalid location")
	}
	switch u.Scheme {
	case "file":
		path, err := filePath(u)
		if err != nil {
			return nil, err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("storage: read %s: %w", redact(u), err)
		}
		defer f.Close()
		return readLimited(f, r.maxBytes)
	case "s3", "s3a":
		if r.s3 == nil {
			return nil, fmt.Errorf("storage: read %s: s3 endpoint is not configured", redact(u))
		}
		bucket, key, err := s3Object(u)
		if err != nil {
			return nil, err
		}
		out, err := r.s3.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, fmt.Errorf("storage: read %s: %w", redact(u), err)
		}
		defer out.Body.Close()
		body, err := readLimited(out.Body, r.maxBytes)
		if err != nil {
			return nil, fmt.Errorf("storage: read %s: %w", redact(u), err)
		}
		return body, nil
	default:
		return nil, fmt.Errorf("storage: unsupported scheme %s", redact(u))
	}
}

func filePath(u *url.URL) (string, error) {
	if u.Host != "" && !strings.EqualFold(u.Host, "localhost") {
		return "", fmt.Errorf("storage: read %s: file host is not local", redact(u))
	}
	if u.Path == "" {
		return "", fmt.Errorf("storage: read %s: empty file path", redact(u))
	}
	return u.Path, nil
}

func s3Object(u *url.URL) (string, string, error) {
	bucket := u.Host
	key := strings.TrimPrefix(u.Path, "/")
	if bucket == "" || key == "" {
		return "", "", fmt.Errorf("storage: read %s: bucket and key are required", redact(u))
	}
	return bucket, key, nil
}

func redact(u *url.URL) string {
	clone := *u
	clone.User = nil
	clone.RawQuery = ""
	clone.Fragment = ""
	return clone.String()
}

func readLimited(r io.Reader, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("object exceeds %d bytes", maxBytes)
	}
	return data, nil
}
