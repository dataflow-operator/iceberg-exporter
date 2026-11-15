package catalog

import (
	"context"
	"fmt"
)

const maxListPages = 100_000

// ListAllTables pages through Catalog.ListTables and applies opts.Namespaces
// even when the adapter ignored the selector.
func ListAllTables(ctx context.Context, cat Catalog, ref Ref, opts ListOptions) ([]TablePointer, error) {
	if cat == nil {
		return nil, fmt.Errorf("catalog: nil catalog")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	var all []TablePointer
	token := opts.PageToken
	for pageN := 0; pageN < maxListPages; pageN++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := cat.ListTables(ctx, ref, ListOptions{
			PageToken:  token,
			PageSize:   opts.PageSize,
			Namespaces: opts.Namespaces,
		})
		if err != nil {
			return nil, fmt.Errorf("catalog: list tables ref %q: %w", ref.Name, err)
		}
		for _, p := range filterPointers(page.Tables, opts.Namespaces) {
			all = append(all, clonePointer(p))
		}
		next := page.NextPageToken
		if next == "" {
			return all, nil
		}
		if _, ok := seen[next]; ok || next == token {
			return nil, fmt.Errorf("catalog: list tables ref %q: repeated page token %q", ref.Name, next)
		}
		seen[next] = struct{}{}
		token = next
	}
	return nil, fmt.Errorf("catalog: list tables ref %q: page limit exceeded", ref.Name)
}
