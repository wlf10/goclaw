package oa

import (
	"bytes"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png" // register PNG decoder
	"log/slog"

	"github.com/disintegration/imaging"
	_ "golang.org/x/image/webp" // register WebP decoder
)

// Zalo OA's /v2.0/oa/upload/image endpoint hard-rejects payloads over
// 1MB (error -210). AI-generated PNGs routinely exceed that, so on the
// outbound path we attempt a resize + JPEG re-encode before giving up.
//
// Strategy: scale the longest side down progressively, then loop JPEG
// quality 85→35 at each size. Returns the first encoding that fits.

var (
	jpegQualityLadder = []int{85, 75, 65, 55, 45, 35}
	maxSideLadder     = []int{1600, 1200, 900, 600}
)

// compressForZaloImage takes raw image bytes of any format and tries to
// produce a JPEG under maxBytes. Returns the compressed bytes and the
// resulting MIME type on success; returns the original bytes + MIME
// unchanged when they already fit. Never silently upscales or discards
// the original.
func compressForZaloImage(data []byte, originalMIME string, maxBytes int) ([]byte, string, error) {
	if len(data) <= maxBytes {
		return data, originalMIME, nil
	}

	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", fmt.Errorf("zalo_oa: decode image for compression: %w", err)
	}
	bounds := img.Bounds()
	origW, origH := bounds.Dx(), bounds.Dy()

	for _, side := range maxSideLadder {
		scaled := img
		if origW > side || origH > side {
			scaled = imaging.Fit(img, side, side, imaging.Lanczos)
		}
		for _, q := range jpegQualityLadder {
			var buf bytes.Buffer
			if err := jpeg.Encode(&buf, scaled, &jpeg.Options{Quality: q}); err != nil {
				return nil, "", fmt.Errorf("zalo_oa: jpeg encode (side=%d q=%d): %w", side, q, err)
			}
			if buf.Len() <= maxBytes {
				slog.Info("zalo_oa.image.compressed",
					"orig_bytes", len(data), "orig_mime", originalMIME,
					"new_bytes", buf.Len(), "side", side, "quality", q)
				return buf.Bytes(), "image/jpeg", nil
			}
		}
		// If even lowest quality at this side is still too big, shrink further.
	}
	return nil, "", fmt.Errorf("zalo_oa: image cannot fit under %d bytes (%dx%d original %d bytes)",
		maxBytes, origW, origH, len(data))
}
