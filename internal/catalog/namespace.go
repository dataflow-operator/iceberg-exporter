package catalog

import "strings"

// NamespaceSelector is the include and exclude list for namespaces.
// An empty Include allows every namespace that Exclude does not reject.
// Exclude wins when both match. A pattern matches that namespace and any
// child namespace (analytics matches analytics.daily).
type NamespaceSelector struct {
	Include []string
	Exclude []string
}

// Allows reports whether namespace survives the selector.
func (s NamespaceSelector) Allows(namespace string) bool {
	if len(s.Include) > 0 && !matchesAny(s.Include, namespace) {
		return false
	}
	if matchesAny(s.Exclude, namespace) {
		return false
	}
	return true
}

func matchesAny(patterns []string, namespace string) bool {
	for _, pattern := range patterns {
		if namespace == pattern || strings.HasPrefix(namespace, pattern+".") {
			return true
		}
	}
	return false
}

func filterPointers(in []TablePointer, sel NamespaceSelector) []TablePointer {
	if len(sel.Include) == 0 && len(sel.Exclude) == 0 {
		return in
	}
	out := make([]TablePointer, 0, len(in))
	for _, p := range in {
		if sel.Allows(p.Namespace) {
			out = append(out, p)
		}
	}
	return out
}
