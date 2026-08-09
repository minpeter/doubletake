package daemon

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"doubletake/internal/airplay"
)

func TestHandleConnectSubmitsPINToPendingStream(t *testing.T) {
	const (
		target = "192.0.2.10"
		pin    = "1234"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := airplay.NewAirPlayClient(target, 7000)
	credentialCh := make(chan string, 1)
	entry := &activeStream{
		deviceIP:     target,
		state:        StatePINRequired,
		client:       client,
		cancelFn:     cancel,
		credentialCh: credentialCh,
	}
	d := &Daemon{
		streams:       map[string]*activeStream{target: entry},
		pendingTarget: target,
	}

	pairingID := client.PairingID
	resp := d.handleConnect(Request{Cmd: "connect", Pin: pin})

	if !resp.OK {
		t.Fatalf("handleConnect() failed: %s", resp.Error)
	}
	if resp.State != StateConnecting {
		t.Fatalf("response state = %q, want %q", resp.State, StateConnecting)
	}
	if resp.Device != target {
		t.Fatalf("response device = %q, want %q", resp.Device, target)
	}
	if d.pendingTarget != "" {
		t.Fatalf("pending target = %q, want empty after claim", d.pendingTarget)
	}
	if got := d.streams[target]; got != entry {
		t.Fatalf("pending stream was replaced: got %p, want %p", got, entry)
	}
	if entry.client != client {
		t.Fatalf("pending client was replaced: got %p, want %p", entry.client, client)
	}
	if entry.client.PairingID != pairingID {
		t.Fatalf("pairing ID changed from %q to %q", pairingID, entry.client.PairingID)
	}
	if entry.state != StateConnecting {
		t.Fatalf("entry state = %q, want %q", entry.state, StateConnecting)
	}
	if entry.credentialCh != credentialCh {
		t.Fatal("pending credential channel was replaced")
	}

	select {
	case got := <-credentialCh:
		if got != pin {
			t.Fatalf("submitted PIN = %q, want %q", got, pin)
		}
	default:
		t.Fatal("PIN was not submitted to the pending stream")
	}

	select {
	case <-ctx.Done():
		t.Fatal("claiming a pending credential session cancelled its connection context")
	default:
	}
}

func TestHandleConnectRejectsPINForDifferentPendingTarget(t *testing.T) {
	const (
		pendingTarget = "192.0.2.10"
		requestTarget = "192.0.2.20"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	credentialCh := make(chan string, 1)
	entry := &activeStream{
		deviceIP:     pendingTarget,
		state:        StatePINRequired,
		client:       airplay.NewAirPlayClient(pendingTarget, 7000),
		cancelFn:     cancel,
		credentialCh: credentialCh,
	}
	d := &Daemon{
		streams:       map[string]*activeStream{pendingTarget: entry},
		pendingTarget: pendingTarget,
	}

	resp := d.handleConnect(Request{Cmd: "connect", Target: requestTarget, Pin: "1234"})

	if resp.OK {
		t.Fatal("handleConnect() accepted a PIN for a different pending target")
	}
	if resp.State != StatePINRequired {
		t.Fatalf("response state = %q, want %q", resp.State, StatePINRequired)
	}
	if !strings.Contains(resp.Error, "different device") {
		t.Fatalf("response error = %q, want a target-mismatch error", resp.Error)
	}
	if d.pendingTarget != pendingTarget {
		t.Fatalf("pending target = %q, want %q", d.pendingTarget, pendingTarget)
	}
	if got := d.streams[pendingTarget]; got != entry {
		t.Fatalf("pending stream changed: got %p, want %p", got, entry)
	}
	if entry.state != StatePINRequired {
		t.Fatalf("entry state = %q, want %q", entry.state, StatePINRequired)
	}
	select {
	case got := <-credentialCh:
		t.Fatalf("mismatched PIN %q was delivered to pending stream", got)
	default:
	}
	select {
	case <-ctx.Done():
		t.Fatal("target mismatch cancelled the pending connection")
	default:
	}
}

func TestHandleConnectRejectsTargetlessPINWithoutPendingSession(t *testing.T) {
	const streamingTarget = "192.0.2.30"
	d := &Daemon{
		streams: map[string]*activeStream{
			streamingTarget: {
				deviceIP: streamingTarget,
				state:    StateStreaming,
			},
		},
	}

	resp := d.handleConnect(Request{Cmd: "connect", Pin: "1234"})

	if resp.OK {
		t.Fatal("handleConnect() accepted a targetless PIN with no pending session")
	}
	if resp.State != StateStreaming {
		t.Fatalf("response state = %q, want existing state %q", resp.State, StateStreaming)
	}
	if !strings.Contains(resp.Error, "no device is waiting for a credential") {
		t.Fatalf("response error = %q, want no-pending-session error", resp.Error)
	}
	if len(d.streams) != 1 || d.streams[streamingTarget] == nil {
		t.Fatalf("streams changed after rejected PIN: %+v", d.streams)
	}
}

func TestTargetedDisconnectClearsPendingPINSession(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- conn
	}()

	addr := listener.Addr().(*net.TCPAddr)
	client := airplay.NewAirPlayClient(addr.IP.String(), addr.Port)
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer connectCancel()
	if err := client.Connect(connectCtx); err != nil {
		t.Fatalf("connect AirPlay client: %v", err)
	}
	defer client.Close()

	var peer net.Conn
	select {
	case peer = <-accepted:
		defer peer.Close()
	case err := <-acceptErr:
		t.Fatalf("accept: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out accepting AirPlay client connection")
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	entry := &activeStream{
		deviceIP:     addr.IP.String(),
		state:        StatePINRequired,
		client:       client,
		cancelFn:     streamCancel,
		credentialCh: make(chan string, 1),
	}
	d := &Daemon{
		streams:       map[string]*activeStream{entry.deviceIP: entry},
		pendingTarget: entry.deviceIP,
	}

	resp := d.handleDisconnect(Request{Cmd: "disconnect", Target: entry.deviceIP})

	if !resp.OK {
		t.Fatalf("handleDisconnect() failed: %s", resp.Error)
	}
	if resp.State != StateIdle {
		t.Fatalf("response state = %q, want %q", resp.State, StateIdle)
	}
	if d.pendingTarget != "" {
		t.Fatalf("pending target = %q, want empty", d.pendingTarget)
	}
	if _, ok := d.streams[entry.deviceIP]; ok {
		t.Fatalf("pending stream %q remains after disconnect", entry.deviceIP)
	}

	select {
	case <-streamCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("targeted disconnect did not cancel the pending stream context")
	}

	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set peer deadline: %v", err)
	}
	buf := make([]byte, 1)
	if n, err := peer.Read(buf); err == nil {
		t.Fatalf("pending AirPlay connection remained open after disconnect (read %d bytes)", n)
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("pending AirPlay connection remained open until the read deadline")
	}
}
