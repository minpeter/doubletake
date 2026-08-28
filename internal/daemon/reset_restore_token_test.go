package daemon

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResetRestoreTokenRejectsMissingUnknownAndNonStreamingTargetsWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name   string
		target string
		state  State
	}{
		{name: "missing target"},
		{name: "unknown target", target: "192.0.2.99"},
		{name: "connecting target", target: "192.0.2.10", state: StateConnecting},
		{name: "credential-waiting target", target: "192.0.2.10", state: StatePINRequired},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, err := New(Config{CredFile: filepath.Join(t.TempDir(), "credentials.json")})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if err := d.credStore.SaveRestoreToken("device-1", "restore-1"); err != nil {
				t.Fatalf("SaveRestoreToken: %v", err)
			}
			entry := &activeStream{deviceIP: "192.0.2.10", deviceID: "device-1", state: test.state}
			if test.state != "" {
				d.streams[entry.deviceIP] = entry
			}

			response := d.handleRequest(Request{Cmd: "reset-restore-token", Target: test.target})

			if response.OK {
				t.Fatalf("reset unexpectedly succeeded: %+v", response)
			}
			if d.streams[entry.deviceIP] != entry && test.state != "" {
				t.Fatal("rejected reset detached the target")
			}
			creds := d.credStore.Lookup("device-1")
			if creds == nil || creds.RestoreToken != "restore-1" {
				t.Fatalf("rejected reset mutated credentials: %+v", creds)
			}
		})
	}
}

func TestResetRestoreTokenRejectsSharedCaptureGroupWithoutMutation(t *testing.T) {
	d, err := New(Config{CredFile: filepath.Join(t.TempDir(), "credentials.json")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := d.credStore.SaveRestoreToken("device-1", "restore-1"); err != nil {
		t.Fatalf("SaveRestoreToken: %v", err)
	}
	group := &videoCaptureGroup{key: normalizedVideoCaptureKey(1920, 1080)}
	target := &activeStream{deviceIP: "192.0.2.10", deviceID: "device-1", state: StateStreaming, captureGroup: group}
	peer := &activeStream{deviceIP: "192.0.2.11", deviceID: "device-2", state: StateStreaming, captureGroup: group}
	d.streams[target.deviceIP] = target
	d.streams[peer.deviceIP] = peer
	d.captureGroups[group.key] = group

	response := d.handleRequest(Request{Cmd: "reset-restore-token", Target: target.deviceIP})

	if response.OK || !strings.Contains(response.Error, "shared capture group") {
		t.Fatalf("shared-group reset response = %+v", response)
	}
	if d.streams[target.deviceIP] != target || d.streams[peer.deviceIP] != peer || d.captureGroups[group.key] != group {
		t.Fatal("shared-group rejection changed active stream state")
	}
	creds := d.credStore.Lookup("device-1")
	if creds == nil || creds.RestoreToken != "restore-1" {
		t.Fatalf("shared-group rejection mutated credentials: %+v", creds)
	}
}

func TestResetRestoreTokenClearsExclusiveTargetAndReconnectsActualPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()

	d, err := New(Config{CredFile: filepath.Join(t.TempDir(), "credentials.json")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.Shutdown()
	if err := d.credStore.SaveRestoreToken("device-1", "restore-1"); err != nil {
		t.Fatalf("SaveRestoreToken: %v", err)
	}
	address := listener.Addr().(*net.TCPAddr)
	group := &videoCaptureGroup{key: normalizedVideoCaptureKey(1920, 1080)}
	oldContext, cancelOld := context.WithCancel(context.Background())
	old := &activeStream{
		deviceIP:     address.IP.String(),
		deviceID:     "device-1",
		state:        StateStreaming,
		port:         address.Port,
		captureGroup: group,
		cancelFn:     cancelOld,
	}
	independentGroup := &videoCaptureGroup{key: normalizedVideoCaptureKey(1280, 720)}
	independent := &activeStream{
		deviceIP:     "192.0.2.20",
		deviceID:     "device-2",
		state:        StateStreaming,
		captureGroup: independentGroup,
	}
	d.streams[old.deviceIP] = old
	d.streams[independent.deviceIP] = independent
	d.captureGroups[group.key] = group
	d.captureGroups[independentGroup.key] = independentGroup

	response := d.handleRequest(Request{Cmd: "reset-restore-token", Target: old.deviceIP})

	if !response.OK {
		t.Fatalf("reset response = %+v", response)
	}
	creds := d.credStore.Lookup("device-1")
	if creds == nil || creds.RestoreToken != "" {
		t.Fatalf("restore token was not cleared: %+v", creds)
	}
	select {
	case <-oldContext.Done():
	default:
		t.Fatal("old stream cleanup did not finish before reset returned")
	}
	d.mu.Lock()
	replacement := d.streams[old.deviceIP]
	groupStillPresent := d.captureGroups[group.key] != nil
	independentPreserved := d.streams[independent.deviceIP] == independent &&
		d.captureGroups[independentGroup.key] == independentGroup
	d.mu.Unlock()
	if replacement == nil || replacement == old || replacement.port != address.Port {
		t.Fatalf("replacement stream = %+v, want new entry on port %d", replacement, address.Port)
	}
	if groupStillPresent {
		t.Fatal("exclusive old capture group remained active")
	}
	if !independentPreserved {
		t.Fatal("reset changed an independent stream or capture group")
	}
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("reset did not reconnect to the target's actual port")
	}
}
