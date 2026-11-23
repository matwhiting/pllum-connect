// Package quiet switches every logger off. The helper handles message text,
// names and numbers, so nothing it or its libraries might log may reach a
// file, a terminal or Pllum. Init runs first thing in main.
package quiet

import (
	"io"
	"log"
	"log/slog"
	"sync/atomic"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	signallog "go.mau.fi/libsignal/logger"
)

// Init switches off zerolog (globally, its context default and its global
// logger), the standard log package and slog's default, and replaces
// libsignal's logger, which would print to stdout, the pipe to Pllum (see
// signalLog). whatsmeow's own loggers are waLog.Noop, or a network's
// diagnostics, passed where whatsmeow is made.
func Init() {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	zerolog.DefaultContextLogger = nil
	zlog.Logger = zerolog.Nop()
	// slog.SetDefault points the log package at the new handler, so it goes
	// first and the log package is sent to io.Discard after it.
	slog.SetDefault(slog.New(slog.DiscardHandler))
	log.SetOutput(io.Discard)
	log.SetFlags(0)
	var l signallog.Loggable = signalLog{}
	signallog.Setup(&l)
}

// signalLog is libsignal's logger: its warnings and errors, which say why a
// message failed to decrypt (whatsmeow reports only "no valid sessions"), go
// to the sink SignalTo set, if any; its debug lines, which print whole
// session records with their keys, and its info lines are dropped. libsignal
// has one logger for the process, so the sink hears every network's.
type signalLog struct{}

var signalSink atomic.Pointer[func(string)]

// SignalTo sets the sink for libsignal's warnings and errors; nil drops
// them. The sink must keep only what is safe to keep: the lines carry
// addresses (account and device numbers) and counters.
func SignalTo(sink func(string)) {
	if sink == nil {
		signalSink.Store(nil)
		return
	}
	signalSink.Store(&sink)
}

func (signalLog) Debug(string, string)  {}
func (signalLog) Info(string, string)   {}
func (signalLog) Warning(_, msg string) { signalTell(msg) }
func (signalLog) Error(_, msg string)   { signalTell(msg) }
func (signalLog) Configure(string)      {}

func signalTell(msg string) {
	if sink := signalSink.Load(); sink != nil {
		(*sink)(msg)
	}
}
