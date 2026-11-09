// Package quiet switches every logger off. The helper handles message text,
// names and numbers, so nothing it or its libraries might log may reach a
// file, a terminal or Pllum. Init runs first thing in main.
package quiet

import (
	"io"
	"log"
	"log/slog"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
)

// Init switches off zerolog (globally, its context default and its global
// logger), the standard log package and slog's default. whatsmeow's own
// loggers are waLog.Noop, passed where whatsmeow is made.
func Init() {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	zerolog.DefaultContextLogger = nil
	zlog.Logger = zerolog.Nop()
	// slog.SetDefault points the log package at the new handler, so it goes
	// first and the log package is sent to io.Discard after it.
	slog.SetDefault(slog.New(slog.DiscardHandler))
	log.SetOutput(io.Discard)
	log.SetFlags(0)
}
