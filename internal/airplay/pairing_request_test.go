package airplay

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestPairHeadersAdvertiseIdentityAndPairingType(t *testing.T) {
	const pairingID = "12345678-1234-4234-8234-123456789abc"
	wantClientName := pairingClientName()

	for _, tt := range []struct {
		name     string
		pairType int
		wantHKP  string
	}{
		{name: "zero value defaults to screen capture", wantHKP: "5"},
		{name: "PIN screen capture", pairType: pairingTypeScreenCapture, wantHKP: "5"},
		{name: "transient", pairType: pairingTypeTransient, wantHKP: "4"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &AirPlayClient{
				PairingID: pairingID,
				pairType:  tt.pairType,
			}

			headers := client.pairHeaders()
			if got := headers["X-Apple-HKP"]; got != tt.wantHKP {
				t.Fatalf("X-Apple-HKP = %q, want %q", got, tt.wantHKP)
			}
			if got := headers["X-Apple-Client-ID"]; got != pairingID {
				t.Fatalf("X-Apple-Client-ID = %q, want %q", got, pairingID)
			}
			if got := headers["X-Apple-Client-Name"]; got != wantClientName {
				t.Fatalf("X-Apple-Client-Name = %q, want %q", got, wantClientName)
			}
		})
	}
}

func TestPairVerifyHeadersAdvertisePairedDevice(t *testing.T) {
	client := &AirPlayClient{PairingID: "12345678-1234-4234-8234-123456789abc"}
	headers := client.pairVerifyHeaders()

	if got := headers["X-Apple-PD"]; got != "1" {
		t.Fatalf("X-Apple-PD = %q, want %q", got, "1")
	}
	if got := headers["X-Apple-HKP"]; got != "5" {
		t.Fatalf("X-Apple-HKP = %q, want %q", got, "5")
	}
}

func TestSanitizePairingClientName(t *testing.T) {
	for _, tt := range []struct {
		name     string
		hostname string
		want     string
	}{
		{name: "hostname", hostname: "living-room-mac", want: "living-room-mac"},
		{name: "trim surrounding spaces", hostname: "  living-room-mac  ", want: "living-room-mac"},
		{name: "empty", want: defaultPairingClientName},
		{name: "whitespace", hostname: "   ", want: defaultPairingClientName},
		{name: "header injection", hostname: "living-room\r\nX-Forged: value", want: defaultPairingClientName},
		{name: "control character", hostname: "living-room\x00", want: defaultPairingClientName},
		{name: "invalid UTF-8", hostname: string([]byte{0xff}), want: defaultPairingClientName},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizePairingClientName(tt.hostname); got != tt.want {
				t.Fatalf("sanitizePairingClientName(%q) = %q, want %q", tt.hostname, got, tt.want)
			}
		})
	}
}

func TestStartPINDisplayAccepts453AndDrainsBody(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	const pairingID = "12345678-1234-4234-8234-123456789abc"
	wantClientName := pairingClientName()
	client := &AirPlayClient{
		conn:      clientConn,
		PairingID: pairingID,
		pairType:  pairingTypeTransient,
	}

	serverDone := make(chan error, 1)
	go func() {
		defer serverConn.Close()
		reader := bufio.NewReader(serverConn)
		request, err := readRTSPTestRequest(reader)
		if err != nil {
			serverDone <- fmt.Errorf("read PIN-start request: %w", err)
			return
		}
		if request.method != "POST" || request.uri != "/pair-pin-start" {
			serverDone <- fmt.Errorf("request = %s %s, want POST /pair-pin-start", request.method, request.uri)
			return
		}
		if len(request.body) != 0 {
			serverDone <- fmt.Errorf("PIN-start body length = %d, want 0", len(request.body))
			return
		}
		for header, want := range map[string]string{
			"x-apple-hkp":                 "5",
			"x-apple-client-id":           pairingID,
			"x-apple-client-name":         wantClientName,
			"x-apple-supportedpinlengths": "4",
		} {
			if got := request.headers[header]; got != want {
				serverDone <- fmt.Errorf("%s = %q, want %q", header, got, want)
				return
			}
		}

		// A non-empty body proves that accepting 453 does not leave response bytes
		// queued ahead of the next RTSP response on the retained connection.
		if err := writeRTSPTestResponse(serverConn, 453, nil, []byte("pending")); err != nil {
			serverDone <- fmt.Errorf("write 453 response: %w", err)
			return
		}

		request, err = readRTSPTestRequest(reader)
		if err != nil {
			serverDone <- fmt.Errorf("read request after 453: %w", err)
			return
		}
		if request.method != "GET" || request.uri != "/info" {
			serverDone <- fmt.Errorf("request after 453 = %s %s, want GET /info", request.method, request.uri)
			return
		}
		if err := writeRTSPTestResponse(serverConn, 200, nil, nil); err != nil {
			serverDone <- fmt.Errorf("write response after 453: %w", err)
			return
		}
		serverDone <- nil
	}()

	if err := client.StartPINDisplay(); err != nil {
		t.Fatalf("StartPINDisplay returned error for HTTP 453: %v", err)
	}
	if client.pairType != pairingTypeScreenCapture {
		t.Fatalf("pairType after StartPINDisplay = %d, want %d", client.pairType, pairingTypeScreenCapture)
	}
	if _, err := client.httpRequest("GET", "/info", "", nil); err != nil {
		t.Fatalf("request after HTTP 453 failed; response body may not have been drained: %v", err)
	}
	waitPairingTestServer(t, serverDone)
}

func TestStartPINDisplayRejectsOtherHTTPStatuses(t *testing.T) {
	for _, status := range []int{400, 403, 455, 500} {
		t.Run(fmt.Sprintf("HTTP_%d", status), func(t *testing.T) {
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			client := &AirPlayClient{
				conn:      clientConn,
				PairingID: "12345678-1234-4234-8234-123456789abc",
			}
			serverDone := make(chan error, 1)
			go func() {
				defer serverConn.Close()
				request, err := readRTSPTestRequest(bufio.NewReader(serverConn))
				if err != nil {
					serverDone <- err
					return
				}
				if request.uri != "/pair-pin-start" {
					serverDone <- fmt.Errorf("request URI = %q, want /pair-pin-start", request.uri)
					return
				}
				serverDone <- writeRTSPTestResponse(serverConn, status, nil, []byte("rejected"))
			}()

			err := client.StartPINDisplay()
			if err == nil {
				t.Fatalf("StartPINDisplay returned nil for HTTP %d", status)
			}
			var statusErr *HTTPStatusError
			if !errors.As(err, &statusErr) {
				t.Fatalf("error %T (%v) does not wrap HTTPStatusError", err, err)
			}
			if statusErr.StatusCode != status {
				t.Fatalf("status = %d, want %d", statusErr.StatusCode, status)
			}
			if got := string(statusErr.Body); got != "rejected" {
				t.Fatalf("error body = %q, want %q", got, "rejected")
			}
			waitPairingTestServer(t, serverDone)
		})
	}
}

func TestPairTransientStopsWhenReceiverRequiresPINPairing(t *testing.T) {
	client := &AirPlayClient{
		info: &ReceiverInfo{StatusFlags: statusFlagPINRequiredForPairing},
	}

	err := client.Pair(context.Background(), "")
	if !errors.Is(err, ErrPINRequired) {
		t.Fatalf("Pair without PIN returned %v, want ErrPINRequired", err)
	}
	if client.PairKeys != nil {
		t.Fatal("PIN-required guard generated transient pairing keys")
	}
	if client.pairType == pairingTypeTransient {
		t.Fatal("PIN-required guard entered transient pairing mode")
	}

	if (&ReceiverInfo{}).RequiresPINPairing() {
		t.Fatal("receiver without PIN-required status flag reports PIN pairing")
	}
	if (&ReceiverInfo{StatusFlags: 1 << 8}).RequiresPINPairing() {
		t.Fatal("per-session PIN status flag reports one-time PIN pairing")
	}
	if (*ReceiverInfo)(nil).RequiresPINPairing() {
		t.Fatal("nil ReceiverInfo reports PIN pairing")
	}
}

func waitPairingTestServer(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pairing test server")
	}
}
