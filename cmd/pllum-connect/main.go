// Command pllum-connect is Pllum's network helper. Pllum starts it, writes
// requests to its standard input and reads responses and events from its
// standard output, one JSON object per line (PROTOCOL.md). It writes nothing
// else anywhere: every logger is off before anything runs.
//
// Exit codes: 0 at end of input or after shutdown; 2 for bad arguments;
// 3 when another copy already holds the data folder; 1 otherwise.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/matwhiting/pllum-connect/internal/network"
	"github.com/matwhiting/pllum-connect/internal/quiet"
	"github.com/matwhiting/pllum-connect/internal/server"
	"github.com/matwhiting/pllum-connect/internal/store"
)

const (
	exitOK     = 0
	exitFailed = 1
	exitUsage  = 2
	exitLocked = 3
)

func main() {
	quiet.Init()
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	flags := flag.NewFlagSet("pllum-connect", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	data := flags.String("data", "", "the data folder (made 0700)")
	if err := flags.Parse(args); err != nil || *data == "" || flags.NArg() != 0 {
		return exitUsage
	}
	dir, err := filepath.Abs(*data)
	if err != nil {
		return exitUsage
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return exitFailed
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return exitFailed
	}

	lock, err := lockFolder(dir)
	if errors.Is(err, errLocked) {
		return exitLocked
	}
	if err != nil {
		return exitFailed
	}
	defer lock.Close()

	// With SIGPIPE ignored, a write to a closed stdout fails instead of
	// killing the helper mid-write; end of input then stops it cleanly.
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	deps := server.Deps{
		Dir: dir,
		Open: func(key [32]byte) (*store.Store, error) {
			return &store.Store{}, nil
		},
		Networks: func(st *store.Store, dir string, key [32]byte, emit network.Emit) []network.Network {
			return nil
		},
	}
	if err := server.Serve(ctx, os.Stdin, os.Stdout, deps); err != nil {
		return exitFailed
	}
	return exitOK
}

var errLocked = errors.New("locked")

// lockFolder takes an exclusive flock on <dir>/connect.lock, so only one
// helper runs per data folder. The lock lasts until the file is closed or the
// process ends.
func lockFolder(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "connect.lock"), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errLocked
		}
		return nil, err
	}
	return f, nil
}
