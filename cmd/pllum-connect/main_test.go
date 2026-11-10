package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testKeyHex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"

func buildHelper(t *testing.T) string {
	t.Helper()
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("the go tool is not on PATH: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "pllum-connect")
	cmd := exec.Command(goTool, "build", "-o", bin, ".")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building the helper: %v\n%s", err, out)
	}
	return bin
}

func TestASecondCopyOnTheSameFolderExitsLocked(t *testing.T) {
	bin := buildHelper(t)
	data := filepath.Join(t.TempDir(), "connect")

	first := exec.Command(bin, "--data", data)
	firstIn, err := first.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	firstOut, err := first.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var firstErr bytes.Buffer
	first.Stderr = &firstErr
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	defer first.Process.Kill()

	// Once the first copy answers hello it holds the lock.
	io.WriteString(firstIn, `{"id":"h","method":"hello","params":{"protocol":1,"storeKey":"`+testKeyHex+`","app":"0.0.1-test"}}`+"\n")
	answered := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(firstOut).ReadString('\n')
		answered <- line
	}()
	select {
	case line := <-answered:
		if !strings.Contains(line, `"result"`) {
			t.Fatalf("the first copy did not answer hello: %s", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the first copy did not answer hello within 10 s")
	}

	info, err := os.Stat(data)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o700 {
		t.Errorf("the data folder is %o, want 700", perm)
	}
	// hello opened the store: every file in the folder is his alone.
	entries, err := os.ReadDir(data)
	if err != nil {
		t.Fatal(err)
	}
	files := 0
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if !fi.IsDir() {
			files++
			if perm := fi.Mode().Perm(); perm != 0o600 {
				t.Errorf("%s is %o, want 600", e.Name(), perm)
			}
		}
	}
	if files < 2 {
		t.Errorf("the folder holds %d files, want the lock and the store", files)
	}

	second := exec.Command(bin, "--data", data)
	var secondOut, secondErr bytes.Buffer
	second.Stdout = &secondOut
	second.Stderr = &secondErr
	second.Stdin = strings.NewReader(`{"id":"h","method":"hello","params":{"protocol":1,"storeKey":"` + testKeyHex + `","app":"0.0.1-test"}}` + "\n")
	err = second.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 3 {
		t.Fatalf("the second copy ended with %v, want exit code 3", err)
	}
	if secondOut.Len() != 0 || secondErr.Len() != 0 {
		t.Errorf("the second copy wrote %d bytes to stdout and %d to stderr, want none", secondOut.Len(), secondErr.Len())
	}

	// End of input: the first copy exits 0, quietly.
	firstIn.Close()
	waited := make(chan error, 1)
	go func() { waited <- first.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("the first copy ended with %v at end of input, want exit 0", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the first copy did not exit within 5 s of end of input")
	}
	if firstErr.Len() != 0 {
		t.Errorf("the first copy wrote %d bytes to stderr, want none", firstErr.Len())
	}
}

func TestNoDataFolderIsAUsageError(t *testing.T) {
	bin := buildHelper(t)
	cmd := exec.Command(bin)
	var errOut bytes.Buffer
	cmd.Stderr = &errOut
	err := cmd.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 {
		t.Fatalf("no --data ended with %v, want exit code 2", err)
	}
}
