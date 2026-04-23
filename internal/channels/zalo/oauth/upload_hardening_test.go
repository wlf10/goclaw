package zalooauth

import (
	"context"
	"strings"
	"testing"
)

func TestSanitizeFilename(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want func(string) bool // matcher
	}{
		{"plain", "report.pdf", func(s string) bool { return s == "report.pdf" }},
		{"strip path", "/etc/passwd", func(s string) bool { return s == "passwd" }},
		{"trim spaces", "  doc.txt  ", func(s string) bool { return s == "doc.txt" }},
		{"dot only", ".", func(s string) bool { return strings.HasPrefix(s, "file-") && strings.HasSuffix(s, ".bin") }},
		{"double dot", "..", func(s string) bool { return strings.HasPrefix(s, "file-") && strings.HasSuffix(s, ".bin") }},
		{"empty", "", func(s string) bool { return strings.HasPrefix(s, "file-") && strings.HasSuffix(s, ".bin") }},
		{"path traversal", "../../etc/passwd", func(s string) bool { return s == "passwd" }},
		{"long name capped", strings.Repeat("a", 300) + ".pdf", func(s string) bool { return len(s) <= 200 }},
		{"unicode preserved", "báo cáo.pdf", func(s string) bool { return s == "báo cáo.pdf" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeFilename(tc.in)
			if !tc.want(got) {
				t.Errorf("sanitizeFilename(%q) = %q, predicate failed", tc.in, got)
			}
		})
	}
}

func TestSendFile_RejectsZeroBytes(t *testing.T) {
	t.Parallel()
	api, captured, _ := newAPIServer(t, apiServerOpts{})
	refresh, _ := newRefreshServer(t, "")
	c := newSendChannel(t, api, refresh, &fakeStore{})

	_, err := c.SendFile(context.Background(), "u1", []byte{}, "empty.txt")
	if err == nil {
		t.Fatal("expected error for zero-byte file")
	}
	if !strings.Contains(err.Error(), "empty") && !strings.Contains(err.Error(), "zero") {
		t.Errorf("err = %v, want 'empty/zero' message", err)
	}
	if len(*captured) != 0 {
		t.Errorf("captured %d HTTP calls; expected 0 (rejected before upload)", len(*captured))
	}
}

