package nessie

import (
	"strings"

	"iceberg-exporter/internal/catalog"
)

// entriesFilter selects Iceberg tables and views. Namespace include and exclude
// patterns are pushed into the same CEL expression. Nessie matches a namespace
// and its children the same way catalog.NamespaceSelector does: the pattern
// itself, or that pattern followed by a dot.
func entriesFilter(sel catalog.NamespaceSelector) string {
	base := "entry.contentType=='ICEBERG_TABLE' || entry.contentType=='ICEBERG_VIEW'"
	namespaces := namespaceFilter(sel)
	if namespaces == "" {
		return base
	}
	return "(" + base + ") && (" + namespaces + ")"
}

func namespaceFilter(sel catalog.NamespaceSelector) string {
	if len(sel.Include) == 0 && len(sel.Exclude) == 0 {
		return ""
	}
	var parts []string
	if len(sel.Include) > 0 {
		included := make([]string, len(sel.Include))
		for i, pattern := range sel.Include {
			included[i] = namespaceMatch(pattern)
		}
		parts = append(parts, "("+strings.Join(included, " || ")+")")
	}
	for _, pattern := range sel.Exclude {
		parts = append(parts, "!("+namespaceMatch(pattern)+")")
	}
	return strings.Join(parts, " && ")
}

func namespaceMatch(pattern string) string {
	equal := "entry.namespace == " + celString(pattern)
	if pattern == "" {
		return equal
	}
	return "(" + equal + " || entry.namespace.startsWith(" + celString(pattern+".") + "))"
}

func celString(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('\'')
	for _, r := range value {
		switch r {
		case '\\', '\'':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String()
}
