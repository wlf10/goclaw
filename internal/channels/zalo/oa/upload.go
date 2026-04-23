package oa

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const maxFilenameLen = 200 // Zalo's observed cap

// uploadImage uploads raw image bytes to Zalo and returns the upload `token`
// that subsequent send-attachment calls reference. Filename carries a real
// extension because Zalo's endpoint uses it to validate the payload type
// (live observation: filename without extension yields a 0-error but
// empty-data response).
func (c *Channel) uploadImage(ctx context.Context, data []byte, mime string) (string, error) {
	tok, err := c.tokens.Access(ctx)
	if err != nil {
		return "", err
	}
	filename := "image.jpg"
	if mime == "image/png" {
		filename = "image.png"
	}
	raw, err := c.client.apiPostMultipart(ctx, pathUploadImage, "file", filename, data, nil, tok)
	if err != nil {
		return "", err
	}
	return parseUploadAttachmentID(raw)
}

// uploadGIF uploads animated-GIF bytes to Zalo's dedicated gif endpoint
// (cap 5MB) and returns the upload token for the subsequent send call.
func (c *Channel) uploadGIF(ctx context.Context, data []byte) (string, error) {
	tok, err := c.tokens.Access(ctx)
	if err != nil {
		return "", err
	}
	raw, err := c.client.apiPostMultipart(ctx, pathUploadGIF, "file", "image.gif", data, nil, tok)
	if err != nil {
		return "", err
	}
	return parseUploadAttachmentID(raw)
}

// uploadFile uploads a file with its original filename and returns the
// upload token. filename is sent in the multipart "filename" field so Zalo
// preserves it for the recipient. Filename is sanitized — pathological
// inputs (path traversal, dot-only, empty, oversized) get a safe fallback.
func (c *Channel) uploadFile(ctx context.Context, data []byte, filename string) (string, error) {
	tok, err := c.tokens.Access(ctx)
	if err != nil {
		return "", err
	}
	safe := sanitizeFilename(filename)
	raw, err := c.client.apiPostMultipart(ctx, pathUploadFile, "file", safe,
		data, map[string]string{"filename": safe}, tok)
	if err != nil {
		return "", err
	}
	return parseUploadAttachmentID(raw)
}

// sanitizeFilename strips any path component, trims whitespace, replaces
// dot-only / empty names with a unique fallback, and caps length at 200.
// Unicode is preserved (Zalo accepts UTF-8 filenames).
func sanitizeFilename(raw string) string {
	name := filepath.Base(strings.TrimSpace(raw))
	switch name {
	case "", ".", "..", string(filepath.Separator):
		return fmt.Sprintf("file-%d.bin", time.Now().Unix())
	}
	if len(name) > maxFilenameLen {
		name = name[:maxFilenameLen]
	}
	return name
}

// parseUploadAttachmentID extracts the attachment ID from the upload
// response. Live Zalo returns:
//
//	{"data":{"attachment_id":"1I5sCR-..."}, "error":0, "message":"Success"}
//
// Older community wrappers + our plan-03 called this field "token" but
// the wire name is `attachment_id`. We accept both for defensive forward-
// compat: if Zalo ever adds a `token` alias (or if a different endpoint
// uses it), we still parse.
func parseUploadAttachmentID(raw json.RawMessage) (string, error) {
	var env struct {
		Data struct {
			AttachmentID string `json:"attachment_id"`
			Token        string `json:"token"` // legacy fallback
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", fmt.Errorf("zalo_oa: decode upload response: %w", err)
	}
	id := env.Data.AttachmentID
	if id == "" {
		id = env.Data.Token
	}
	if id == "" {
		preview := string(raw)
		if len(preview) > 500 {
			preview = preview[:500] + "…(truncated)"
		}
		return "", fmt.Errorf("zalo_oa: upload response missing data.attachment_id (raw=%s)", preview)
	}
	return id, nil
}
