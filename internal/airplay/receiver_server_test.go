package airplay

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"howett.net/plist"
)

func TestReceiverServerEndToEndProfiles(t *testing.T) {
	for _, test := range []struct {
		name            string
		profile         ReceiverProfile
		wantEncrypted   bool
		wantTimingProbe bool
	}{
		{name: "Roku raw and NTP", profile: ReceiverProfileRoku, wantTimingProbe: true},
		{name: "modern HAP and PTP", profile: ReceiverProfileModern, wantEncrypted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, client, ctx := newReceiverServerTestPair(t, ReceiverConfig{
				Profile: test.profile,
				Auth:    ReceiverAuthNone,
			})

			if err := client.Pair(ctx, ""); err != nil {
				t.Fatalf("pair: %v", err)
			}
			if client.encrypted != test.wantEncrypted {
				t.Fatalf("encrypted control = %t, want %t", client.encrypted, test.wantEncrypted)
			}
			if test.profile == ReceiverProfileModern {
				if err := client.FairPlaySetup(ctx); err != nil {
					t.Fatalf("FairPlay setup: %v", err)
				}
			}

			session, err := client.SetupMirror(ctx, StreamConfig{NoAudio: true})
			if err != nil {
				t.Fatalf("setup mirror: %v", err)
			}
			if err := session.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Fatalf("close mirror session: %v", err)
			}

			stats := server.Stats()
			if stats.SetupRequests != 2 && test.profile == ReceiverProfileRoku {
				t.Fatalf("Roku SETUP requests = %d, want 2", stats.SetupRequests)
			}
			if stats.SetupRequests != 3 && test.profile == ReceiverProfileModern {
				t.Fatalf("modern SETUP requests = %d, want 3", stats.SetupRequests)
			}
			if stats.RecordRequests != 1 || stats.FeedbackRequests < 1 || stats.TeardownRequests != 1 {
				t.Fatalf("control stats = %+v", stats)
			}
			wantFairPlay := uint64(0)
			if test.profile == ReceiverProfileModern {
				wantFairPlay = 2
			}
			if stats.FairPlayRequests != wantFairPlay {
				t.Fatalf("FairPlay requests = %d, want %d", stats.FairPlayRequests, wantFairPlay)
			}
			if stats.EventConnections != 1 || stats.VideoConnections != 1 {
				t.Fatalf("media connections = %+v", stats)
			}
			if test.wantTimingProbe && (stats.TimingProbes != 3 || stats.TimingReplies != 3) {
				t.Fatalf("legacy timing stats = %+v", stats)
			}
			if !test.wantTimingProbe && (stats.TimingProbes != 0 || stats.TimingReplies != 0) {
				t.Fatalf("PTP profile unexpectedly used NTP: %+v", stats)
			}
		})
	}
}

func TestReceiverServerCombinedCodePairsAndAnswersDigest(t *testing.T) {
	const code = "password with spaces"
	server, client, ctx := newReceiverServerTestPair(t, ReceiverConfig{
		Profile: ReceiverProfileRoku,
		Auth:    ReceiverAuthCombined,
		Code:    code,
	})

	if !client.info.RequiresPassword() {
		t.Fatal("combined receiver did not advertise a configured password")
	}
	if client.info.RequiresPINPairing() {
		t.Fatal("combined password receiver unexpectedly advertised an on-screen PIN")
	}
	if err := client.Pair(ctx, code); err != nil {
		t.Fatalf("pair with combined code: %v", err)
	}
	if !client.encrypted {
		t.Fatal("code pairing did not establish encrypted HAP control")
	}
	session, err := client.SetupMirror(ctx, StreamConfig{NoAudio: true})
	if err != nil {
		t.Fatalf("Digest-protected setup: %v", err)
	}
	if err := session.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close mirror session: %v", err)
	}

	stats := server.Stats()
	if stats.DigestChallenges != 1 {
		t.Fatalf("Digest challenges = %d, want exactly 1", stats.DigestChallenges)
	}
	if stats.PINStarts != 0 || stats.PairSetup != 3 || stats.PairVerify != 2 || stats.SetupRequests != 2 {
		t.Fatalf("combined auth stats = %+v", stats)
	}
}

func TestReceiverServerRequiresPairVerifyBeforeSessionRequests(t *testing.T) {
	_, client, _ := newReceiverServerTestPair(t, ReceiverConfig{
		Profile: ReceiverProfileRoku,
		Auth:    ReceiverAuthNone,
	})

	for _, request := range []struct {
		name   string
		method string
		uri    string
		body   []byte
	}{
		{name: "SETUP", method: "SETUP", uri: "/session", body: receiverTestSetupBody(t, 96)},
		{name: "RECORD", method: "RECORD", uri: "/session"},
		{name: "SET_PARAMETER", method: "SET_PARAMETER", uri: "/session", body: []byte("volume: 0\r\n")},
		{name: "feedback", method: "POST", uri: "/feedback"},
		{name: "TEARDOWN", method: "TEARDOWN", uri: "/session"},
		{name: "FairPlay", method: "POST", uri: "/fp-setup", body: newFPSAPRecord(1, len(fpsapM1Payload))},
	} {
		t.Run(request.name, func(t *testing.T) {
			err := receiverTestRTSPRequest(client, request.method, request.uri, request.body)
			requireReceiverStatus(t, err, 455)
		})
	}
}

func TestReceiverServerEnforcesModernSessionOrder(t *testing.T) {
	_, client, ctx := newReceiverServerTestPair(t, ReceiverConfig{
		Profile: ReceiverProfileModern,
		Auth:    ReceiverAuthNone,
	})
	if err := client.Pair(ctx, ""); err != nil {
		t.Fatalf("pair: %v", err)
	}
	if err := client.FairPlaySetup(ctx); err != nil {
		t.Fatalf("FairPlay setup: %v", err)
	}

	requireReceiverStatus(t, receiverTestRTSPRequest(client, "SETUP", "/audio", receiverTestSetupBody(t, 96)), 455)
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "RECORD", "/session", nil), 455)
	requireReceiverOK(t, receiverTestRTSPRequest(client, "SETUP", "/session", receiverTestSetupBody(t)))
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "SETUP", "/audio", receiverTestSetupBody(t, 96)), 455)
	requireReceiverOK(t, receiverTestRTSPRequest(client, "RECORD", "/session", nil))
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "SETUP", "/video", receiverTestSetupBody(t, 110)), 455)
	requireReceiverOK(t, receiverTestRTSPRequest(client, "SETUP", "/audio", receiverTestSetupBody(t, 96)))
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "POST", "/feedback", nil), 455)
	requireReceiverOK(t, receiverTestRTSPRequest(client, "SETUP", "/video", receiverTestSetupBody(t, 110)))
	requireReceiverOK(t, receiverTestRTSPRequest(client, "SET_PARAMETER", "/session", []byte("volume: 0\r\n")))
	requireReceiverOK(t, receiverTestRTSPRequest(client, "POST", "/feedback", nil))
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "RECORD", "/session", nil), 455)
	requireReceiverOK(t, receiverTestRTSPRequest(client, "TEARDOWN", "/session", nil))
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "TEARDOWN", "/session", nil), 455)
}

func TestReceiverServerRequiresFairPlayBeforeModernSetup(t *testing.T) {
	server, client, ctx := newReceiverServerTestPair(t, ReceiverConfig{
		Profile: ReceiverProfileModern,
		Auth:    ReceiverAuthNone,
	})
	if err := client.Pair(ctx, ""); err != nil {
		t.Fatalf("pair: %v", err)
	}
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "SETUP", "/session", receiverTestSetupBody(t)), 455)
	if err := client.FairPlaySetup(ctx); err != nil {
		t.Fatalf("FairPlay setup: %v", err)
	}
	requireReceiverOK(t, receiverTestRTSPRequest(client, "SETUP", "/session", receiverTestSetupBody(t)))
	if got := server.Stats().FairPlayRequests; got != 2 {
		t.Fatalf("FairPlay requests = %d, want 2", got)
	}
}

func TestReceiverServerEnforcesRokuSessionOrder(t *testing.T) {
	_, client, ctx := newReceiverServerTestPair(t, ReceiverConfig{
		Profile: ReceiverProfileRoku,
		Auth:    ReceiverAuthNone,
	})
	if err := client.Pair(ctx, ""); err != nil {
		t.Fatalf("pair: %v", err)
	}

	requireReceiverStatus(t, receiverTestRTSPRequest(client, "SETUP", "/session", receiverTestSetupBody(t)), 455)
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "SETUP", "/video", receiverTestSetupBody(t, 110)), 455)
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "RECORD", "/session", nil), 455)
	requireReceiverOK(t, receiverTestRTSPRequest(client, "SETUP", "/audio", receiverTestSetupBody(t, 96)))
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "RECORD", "/session", nil), 455)
	requireReceiverOK(t, receiverTestRTSPRequest(client, "SETUP", "/video", receiverTestSetupBody(t, 110)))
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "SET_PARAMETER", "/session", []byte("volume: 0\r\n")), 455)
	requireReceiverStatus(t, receiverTestRTSPRequest(client, "POST", "/feedback", nil), 455)
	requireReceiverOK(t, receiverTestRTSPRequest(client, "RECORD", "/session", nil))
	requireReceiverOK(t, receiverTestRTSPRequest(client, "SET_PARAMETER", "/session", []byte("volume: 0\r\n")))
	requireReceiverOK(t, receiverTestRTSPRequest(client, "POST", "/feedback", nil))
	requireReceiverOK(t, receiverTestRTSPRequest(client, "TEARDOWN", "/session", nil))
}

func TestReceiverServerPINAndDigestModes(t *testing.T) {
	t.Run("on-screen PIN", func(t *testing.T) {
		const code = "4827"
		server, client, ctx := newReceiverServerTestPair(t, ReceiverConfig{
			Profile: ReceiverProfileModern,
			Auth:    ReceiverAuthPIN,
			Code:    code,
		})
		if !client.info.RequiresPINPairing() {
			t.Fatal("PIN receiver did not advertise PIN-required status")
		}
		if client.info.RequiresPassword() {
			t.Fatal("PIN receiver unexpectedly advertised a configured password")
		}
		if err := client.StartPINDisplay(); err != nil {
			t.Fatalf("start PIN display: %v", err)
		}
		if err := client.Pair(ctx, code); err != nil {
			t.Fatalf("PIN pair: %v", err)
		}
		stats := server.Stats()
		if stats.PINStarts != 1 || stats.PairSetup != 3 || stats.PairVerify != 2 {
			t.Fatalf("PIN stats = %+v", stats)
		}
	})

	t.Run("Digest after raw transient pairing", func(t *testing.T) {
		const code = "digest-only"
		server, client, ctx := newReceiverServerTestPair(t, ReceiverConfig{
			Profile: ReceiverProfileRoku,
			Auth:    ReceiverAuthDigest,
			Code:    code,
		})
		client.SetPassword(code)
		if !client.info.RequiresPassword() {
			t.Fatal("Digest receiver did not advertise a configured password")
		}
		if err := client.Pair(ctx, ""); err != nil {
			t.Fatalf("transient pair: %v", err)
		}
		if client.encrypted {
			t.Fatal("raw Roku pairing unexpectedly encrypted the control channel")
		}
		session, err := client.SetupMirror(ctx, StreamConfig{NoAudio: true})
		if err != nil {
			t.Fatalf("Digest-protected setup: %v", err)
		}
		if err := session.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("close mirror session: %v", err)
		}
		if got := server.Stats().DigestChallenges; got != 1 {
			t.Fatalf("Digest challenges = %d, want 1", got)
		}
	})
}

func TestReceiverServerRejectsWrongPairingCode(t *testing.T) {
	_, client, ctx := newReceiverServerTestPair(t, ReceiverConfig{
		Profile: ReceiverProfileRoku,
		Auth:    ReceiverAuthPassword,
		Code:    "correct",
	})
	if err := client.Pair(ctx, "wrong"); err == nil {
		t.Fatal("pairing accepted the wrong configured password")
	}
}

func TestReceiverServerConfigValidation(t *testing.T) {
	for _, test := range []struct {
		name string
		cfg  ReceiverConfig
	}{
		{name: "unknown profile", cfg: ReceiverConfig{ListenAddress: "127.0.0.1:0", Profile: "other"}},
		{name: "unknown auth", cfg: ReceiverConfig{ListenAddress: "127.0.0.1:0", Auth: "other"}},
		{name: "missing code", cfg: ReceiverConfig{ListenAddress: "127.0.0.1:0", Auth: ReceiverAuthPIN}},
		{name: "unused code", cfg: ReceiverConfig{ListenAddress: "127.0.0.1:0", Auth: ReceiverAuthNone, Code: "1234"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewReceiverServer(test.cfg)
			if err == nil {
				_ = server.Close()
				t.Fatal("expected configuration error")
			}
		})
	}
}

func newReceiverServerTestPair(t *testing.T, cfg ReceiverConfig) (*ReceiverServer, *AirPlayClient, context.Context) {
	t.Helper()
	cfg.ListenAddress = "127.0.0.1:0"
	cfg.Logger = log.New(io.Discard, "", 0)
	server, err := NewReceiverServer(cfg)
	if err != nil {
		t.Fatalf("new receiver server: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("serve receiver: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("receiver Serve did not stop")
		}
	})

	host, portText, err := net.SplitHostPort(server.Addr().String())
	if err != nil {
		t.Fatalf("split receiver address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse receiver port: %v", err)
	}
	client := NewAirPlayClient(host, port)
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect receiver: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.GetInfo(); err != nil {
		t.Fatalf("get receiver info: %v", err)
	}
	return server, client, ctx
}

func receiverTestSetupBody(t *testing.T, streamTypes ...int64) []byte {
	t.Helper()
	setup := make(map[string]any)
	if len(streamTypes) > 0 {
		streams := make([]any, len(streamTypes))
		for i, streamType := range streamTypes {
			streams[i] = map[string]any{"type": streamType}
		}
		setup["streams"] = streams
	}
	body, err := plist.Marshal(setup, plist.BinaryFormat)
	if err != nil {
		t.Fatalf("marshal test SETUP: %v", err)
	}
	return body
}

func receiverTestRTSPRequest(client *AirPlayClient, method, uri string, body []byte) error {
	contentType := ""
	if len(body) > 0 {
		contentType = "application/x-apple-binary-plist"
	}
	_, _, err := client.rtspRequest(method, uri, contentType, body, nil)
	return err
}

func requireReceiverOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("receiver request failed: %v", err)
	}
}

func requireReceiverStatus(t *testing.T, err error, want int) {
	t.Helper()
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != want {
		t.Fatalf("receiver request error = %v, want HTTP %d", err, want)
	}
}
