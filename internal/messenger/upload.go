package messenger

import (
	"bytes"
	"context"
	"encoding/binary"
	"image"
	_ "image/jpeg" // photo sizes, for an encrypted photo's message
	_ "image/png"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"

	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"google.golang.org/protobuf/proto"
)

// fileKind is how a file goes: a JPEG or PNG photo or an MP4 or QuickTime
// video up to 16 MB, else a file (a document) up to 25 MB: WhatsApp's
// rule (step 2), with Messenger's attachment limit.
func fileKind(path, name string) (kind, mimetype string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", errNotFound
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.Mode().IsRegular() {
		return "", "", errBadRequest
	}
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	sniffed := http.DetectContentType(head[:n])
	if n >= 12 && string(head[4:8]) == "ftyp" && string(head[8:12]) == "qt  " {
		sniffed = "video/quicktime"
	}
	size := fi.Size()
	switch {
	case (sniffed == "image/jpeg" || sniffed == "image/png") && size <= photoLimit:
		return "photo", sniffed, nil
	case (sniffed == "video/mp4" || sniffed == "video/quicktime") && size <= videoLimit:
		return "video", sniffed, nil
	case size <= fileLimit:
		return "file", documentType(name, path, sniffed), nil
	}
	return "", "", errTooLarge
}

// documentType is a document's MIME type: its name's, else its content's
// (WhatsApp's, step 2).
func documentType(name, path, sniffed string) string {
	for _, ext := range []string{filepath.Ext(name), filepath.Ext(path)} {
		if ext == "" {
			continue
		}
		if t, _, err := mime.ParseMediaType(mime.TypeByExtension(ext)); err == nil && t != "" {
			return t
		}
	}
	if t, _, err := mime.ParseMediaType(sniffed); err == nil && t != "" {
		return t
	}
	return "application/octet-stream"
}

// readSendFile reads a send's file again, as it is now.
func readSendFile(req sendRequest) ([]byte, error) {
	data, err := os.ReadFile(req.File)
	if err != nil {
		return nil, errNotFound
	}
	limit := int64(fileLimit)
	switch req.Kind {
	case "photo":
		limit = photoLimit
	case "video":
		limit = videoLimit
	}
	if int64(len(data)) > limit {
		return nil, errTooLarge
	}
	return data, nil
}

// upload uploads the send's file once, with a budget of its own, and keeps
// what Meta gave for it in the row before any message goes: the
// attachment ID on an unencrypted thread (meta.Upload), else the encrypted
// file's transport (UploadFB), built as the connector's
// reuploadMediaToWhatsApp builds it, with a thumbnail size Messenger's
// apps need: a photo's or video's own (clamped to 400), else 400×400.
func (s *sender) upload(data []byte) error {
	ctx, cancel := s.m.writeCtx()
	defer cancel()
	if s.encrypted() {
		var up whatsmeow.UploadResponse
		if _, _, err := s.m.ask(ctx, s.gen, func(ctx context.Context) (err error) {
			up, err = s.w.UploadFB(ctx, data, mediaTypeOf(s.req.Kind))
			return err
		}); err != nil {
			return err
		}
		if up.DirectPath == "" {
			return errRefused
		}
		width, height := mediaSize(s.req.Kind, data)
		tw, th := clampTo400(width, height)
		if tw <= 0 || th <= 0 {
			tw, th = 400, 400
		}
		if width <= 0 || height <= 0 {
			width, height = tw, th // never 0×0
		}
		mimetype := s.req.Mimetype
		b, err := proto.Marshal(&waMediaTransport.WAMediaTransport{
			Integral: &waMediaTransport.WAMediaTransport_Integral{
				FileSHA256: up.FileSHA256, MediaKey: up.MediaKey, FileEncSHA256: up.FileEncSHA256,
				DirectPath: proto.String(up.DirectPath), MediaKeyTimestamp: proto.Int64(s.m.clock.Now().Unix()),
			},
			Ancillary: &waMediaTransport.WAMediaTransport_Ancillary{
				FileLength: proto.Uint64(uint64(len(data))), Mimetype: &mimetype,
				// Messenger's apps show a file only with a thumbnail size (the connector's note).
				Thumbnail: &waMediaTransport.WAMediaTransport_Ancillary_Thumbnail{
					ThumbnailWidth: proto.Uint32(uint32(tw)), ThumbnailHeight: proto.Uint32(uint32(th))},
				ObjectID: proto.String(up.ObjectID),
			},
		})
		if err != nil {
			return errInternal
		}
		s.p.Upload, s.p.Width, s.p.Height = b, width, height
	} else {
		var resp *types.MercuryUploadResponse
		if _, _, err := s.m.ask(ctx, s.gen, func(ctx context.Context) (err error) {
			resp, err = s.w.Upload(ctx, s.thread, &httpclient.MercuryUploadMedia{Filename: s.req.FileName, MimeType: s.req.Mimetype, MediaData: data})
			return err
		}); err != nil {
			return err
		}
		var fbid int64
		if resp != nil && resp.Payload.RealMetadata != nil {
			fbid = resp.Payload.RealMetadata.GetFbId()
		}
		if fbid == 0 {
			return errRefused
		}
		s.p.Attachment = fbid
	}
	return s.m.keepProgress(s.o.ID, s.p)
}

// mediaSize is a photo's or video's size, read from the file, or 0, 0.
func mediaSize(kind string, data []byte) (int, int) {
	switch kind {
	case "photo":
		if cfg, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil {
			return cfg.Width, cfg.Height
		}
	case "video":
		return mp4Size(data)
	}
	return 0, 0
}

// mp4Size is an MP4 or QuickTime video's size: the first track header
// (moov/trak/tkhd) with one, or 0, 0. A track header's width and height
// are 16.16 fixed-point numbers at its end (ISO/IEC 14496-12 §8.3.2).
func mp4Size(data []byte) (int, int) {
	var walk func(b []byte) (int, int)
	walk = func(b []byte) (int, int) {
		for len(b) >= 8 {
			size, head := uint64(binary.BigEndian.Uint32(b)), uint64(8)
			typ := string(b[4:8])
			switch size {
			case 0:
				size = uint64(len(b))
			case 1:
				if len(b) < 16 {
					return 0, 0
				}
				size, head = binary.BigEndian.Uint64(b[8:16]), 16
			}
			if size < head || size > uint64(len(b)) {
				return 0, 0
			}
			body := b[head:size]
			switch typ {
			case "moov", "trak":
				if w, h := walk(body); w > 0 && h > 0 {
					return w, h
				}
			case "tkhd":
				at := 4 + 72 // version 0: after version and flags, 72 bytes to the width
				if len(body) > 0 && body[0] == 1 {
					at = 4 + 84
				}
				if len(body) >= at+8 {
					w, h := int(binary.BigEndian.Uint32(body[at:])>>16), int(binary.BigEndian.Uint32(body[at+4:])>>16)
					if w > 0 && h > 0 {
						return w, h
					}
				}
			}
			b = b[size:]
		}
		return 0, 0
	}
	return walk(data)
}

// mediaTypeOf is the media type an encrypted file is uploaded as.
func mediaTypeOf(kind string) whatsmeow.MediaType {
	switch kind {
	case "photo":
		return whatsmeow.MediaImage
	case "video":
		return whatsmeow.MediaVideo
	}
	return whatsmeow.MediaDocument
}

// clampTo400 is a thumbnail's size: the connector's clampTo400
// (msgconv/to-whatsapp.go), copied.
func clampTo400(w, h int) (int, int) {
	if w > 400 {
		h = h * 400 / w
		w = 400
	}
	if h > 400 {
		w = w * 400 / h
		h = 400
	}
	return w, h
}
