package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"doubletake/internal/airplay"
)

func TestDaemonStreamsToTwoPasswordReceivers(t *testing.T) {
	if _, err := exec.LookPath("gst-launch-1.0"); err != nil {
		t.Skip("gst-launch-1.0 is required for the headless capture integration test")
	}
	if err := exec.Command("gst-inspect-1.0", "openh264enc").Run(); err != nil {
		t.Skip("GStreamer openh264enc is required for the headless capture integration test")
	}

	receiverCtx, cancelReceivers := context.WithCancel(context.Background())
	defer cancelReceivers()
	type runningReceiver struct {
		server *airplay.ReceiverServer
		done   chan error
		code   string
	}
	receivers := make([]runningReceiver, 0, 2)
	defer func() {
		cancelReceivers()
		for i, receiver := range receivers {
			_ = receiver.server.Close()
			select {
			case err := <-receiver.done:
				if err != nil {
					t.Errorf("receiver %d stopped with error: %v", i+1, err)
				}
			case <-time.After(3 * time.Second):
				t.Errorf("receiver %d did not stop", i+1)
			}
		}
	}()
	for i, cfg := range []struct {
		address string
		name    string
		code    string
	}{
		{address: "127.0.0.1:0", name: "First TV", code: "first password"},
		{address: "127.0.0.2:0", name: "Second TV", code: "second password"},
	} {
		server, err := airplay.NewReceiverServer(airplay.ReceiverConfig{
			ListenAddress: cfg.address,
			Profile:       airplay.ReceiverProfileRoku,
			Auth:          airplay.ReceiverAuthCombined,
			Code:          cfg.code,
			Name:          cfg.name,
			Logger:        log.New(io.Discard, "", 0),
		})
		if err != nil {
			t.Fatalf("start receiver %d: %v", i+1, err)
		}
		done := make(chan error, 1)
		go func() { done <- server.Serve(receiverCtx) }()
		receivers = append(receivers, runningReceiver{server: server, done: done, code: cfg.code})
	}

	socketPath := filepath.Join(t.TempDir(), "doubletake.sock")
	d, err := New(Config{
		SocketPath:  socketPath,
		CredBackend: "file",
		CredFile:    filepath.Join(t.TempDir(), "credentials.json"),
		FPS:         10,
		Bitrate:     500,
		HWAccel:     "openh264",
		TestMode:    true,
		NoAudio:     true,
	})
	if err != nil {
		t.Fatalf("create daemon: %v", err)
	}
	daemonCtx, cancelDaemon := context.WithCancel(context.Background())
	daemonDone := make(chan error, 1)
	go func() { daemonDone <- d.Run(daemonCtx) }()
	defer func() {
		cancelDaemon()
		d.Shutdown()
		select {
		case err := <-daemonDone:
			if err != nil {
				t.Errorf("daemon stopped with error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()
	waitForDaemonSocket(t, socketPath)

	for i, receiver := range receivers {
		addr := receiver.server.Addr().(*net.TCPAddr)
		resp := sendDaemonSocketRequest(t, socketPath, Request{
			Cmd:    "connect",
			Target: addr.IP.String(),
			Port:   addr.Port,
		})
		if !resp.OK {
			t.Fatalf("queue receiver %d: %+v", i+1, resp)
		}
	}

	status := waitForDaemonSocketStatus(t, socketPath, 10*time.Second, func(resp Response) bool {
		return len(resp.Streams) == 2 &&
			resp.Streams[0].State == StatePINRequired &&
			resp.Streams[1].State == StatePINRequired
	})
	for i, stream := range status.Streams {
		if stream.CredentialKind != CredentialKindPassword {
			t.Fatalf("stream %d credential kind = %q, want password", i+1, stream.CredentialKind)
		}
	}

	// Submit each password to its own still-live pairing connection. The first
	// target may advance while the second remains independently promptable.
	firstAddr := receivers[0].server.Addr().(*net.TCPAddr)
	resp := sendDaemonSocketRequest(t, socketPath, Request{
		Cmd:    "connect",
		Target: firstAddr.IP.String(),
		Pin:    receivers[0].code,
	})
	if !resp.OK {
		t.Fatalf("submit first password: %+v", resp)
	}
	status = waitForDaemonSocketStatus(t, socketPath, 3*time.Second, func(resp Response) bool {
		for _, stream := range resp.Streams {
			if stream.DeviceIP == receivers[1].server.Addr().(*net.TCPAddr).IP.String() {
				return stream.State == StatePINRequired && stream.CredentialKind == CredentialKindPassword
			}
		}
		return false
	})
	if !status.NeedsCredential {
		t.Fatalf("second prompt disappeared after first submission: %+v", status)
	}

	secondAddr := receivers[1].server.Addr().(*net.TCPAddr)
	resp = sendDaemonSocketRequest(t, socketPath, Request{
		Cmd:    "connect",
		Target: secondAddr.IP.String(),
		Pin:    receivers[1].code,
	})
	if !resp.OK {
		t.Fatalf("submit second password: %+v", resp)
	}

	waitForDaemonSocketStatus(t, socketPath, 15*time.Second, func(resp Response) bool {
		if len(resp.Streams) != 2 {
			return false
		}
		for _, stream := range resp.Streams {
			if stream.State != StateStreaming {
				return false
			}
		}
		return true
	})

	deadline := time.Now().Add(8 * time.Second)
	for {
		firstStats := receivers[0].server.Stats()
		secondStats := receivers[1].server.Stats()
		if firstStats.VideoPackets >= 3 && secondStats.VideoPackets >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("both receivers did not get sustained video: first=%+v second=%+v", firstStats, secondStats)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDisconnectDuringSharedCaptureStartupLeavesNoCapture(t *testing.T) {
	if _, err := exec.LookPath("gst-launch-1.0"); err != nil {
		t.Skip("gst-launch-1.0 is required for the headless capture integration test")
	}
	if err := exec.Command("gst-inspect-1.0", "openh264enc").Run(); err != nil {
		t.Skip("GStreamer openh264enc is required for the headless capture integration test")
	}

	d, err := New(Config{
		CredBackend: "file",
		CredFile:    filepath.Join(t.TempDir(), "credentials.json"),
		FPS:         10,
		Bitrate:     500,
		HWAccel:     "openh264",
		TestMode:    true,
		NoAudio:     true,
	})
	if err != nil {
		t.Fatalf("create daemon: %v", err)
	}
	t.Cleanup(func() { d.handleDisconnect(Request{}) })
	streamCtx, cancelStream := context.WithCancel(context.Background())
	const target = "192.0.2.10"
	d.streams[target] = &activeStream{
		deviceIP: target,
		state:    StateConnecting,
		cancelFn: cancelStream,
	}

	type captureResult struct {
		sink *airplay.BroadcastSink
		err  error
	}
	resultCh := make(chan captureResult, 1)
	go func() {
		sink, err := d.getOrStartBroadcastLocked("", "", 640, 480)
		resultCh <- captureResult{sink: sink, err: err}
	}()

	// Wait until startup has published its cancellation hook (or completed), then
	// remove the only consumer exactly as a targeted disconnect would.
	deadline := time.Now().Add(3 * time.Second)
	for {
		d.mu.Lock()
		startupVisible := d.captureCancel != nil
		d.mu.Unlock()
		if startupVisible {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("capture startup did not publish its cancellation hook")
		}
		time.Sleep(time.Millisecond)
	}
	d.mu.Lock()
	d.removeStreamLocked(target)
	d.mu.Unlock()

	var result captureResult
	select {
	case result = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("capture startup did not stop after its final stream was removed")
	}
	if result.sink != nil {
		result.sink.Close()
	}
	if result.err != nil && !errors.Is(result.err, context.Canceled) {
		t.Fatalf("capture startup returned unexpected error: %v", result.err)
	}
	select {
	case <-streamCtx.Done():
	default:
		t.Fatal("removing the stream did not cancel its context")
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.capture != nil || d.broadcast != nil || d.captureCancel != nil {
		t.Fatalf("disconnect left an orphan capture: capture=%p broadcast=%p cancel-set=%t",
			d.capture, d.broadcast, d.captureCancel != nil)
	}
}

func waitForDaemonSocket(t *testing.T, socketPath string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.DialTimeout("unix", socketPath, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon socket did not become ready: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

func sendDaemonSocketRequest(t *testing.T, socketPath string, req Request) Response {
	t.Helper()
	conn, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("connect to daemon socket: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set daemon socket deadline: %v", err)
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		t.Fatalf("send daemon request: %v", err)
	}
	var resp Response
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		t.Fatalf("read daemon response: %v", err)
	}
	return resp
}

func waitForDaemonSocketStatus(t *testing.T, socketPath string, timeout time.Duration, ready func(Response) bool) Response {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last Response
	for {
		last = sendDaemonSocketRequest(t, socketPath, Request{Cmd: "status"})
		if ready(last) {
			return last
		}
		if last.Error != "" {
			t.Fatalf("daemon failed while waiting for status: %+v", last)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for daemon status; last response: %+v", last)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
