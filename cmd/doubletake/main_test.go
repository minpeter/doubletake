package main

import (
	"bufio"
	"errors"
	"strings"
	"testing"

	"doubletake/internal/airplay"
)

func TestParsePortRange(t *testing.T) {
	for _, test := range []struct {
		name       string
		value      string
		wantMin    int
		wantMax    int
		wantErrSub string
	}{
		{name: "ephemeral"},
		{name: "valid", value: "60000-60010", wantMin: 60000, wantMax: 60010},
		{name: "whitespace", value: " 60000 - 60002 ", wantMin: 60000, wantMax: 60002},
		{name: "malformed", value: "60000", wantErrSub: "expected MIN-MAX"},
		{name: "minimum invalid", value: "zero-60010", wantErrSub: "min:"},
		{name: "maximum invalid", value: "60000-many", wantErrSub: "max:"},
		{name: "zero", value: "0-2", wantErrSub: "out of bounds"},
		{name: "maximum too high", value: "65534-65536", wantErrSub: "out of bounds"},
		{name: "reversed", value: "60010-60000", wantErrSub: "out of bounds"},
		{name: "too small", value: "60000-60001", wantErrSub: "too small"},
	} {
		t.Run(test.name, func(t *testing.T) {
			gotMin, gotMax, err := parsePortRange(test.value)
			if test.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrSub) {
					t.Fatalf("parsePortRange(%q) error = %v, want substring %q", test.value, err, test.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePortRange(%q): %v", test.value, err)
			}
			if gotMin != test.wantMin || gotMax != test.wantMax {
				t.Fatalf("parsePortRange(%q) = %d-%d, want %d-%d", test.value, gotMin, gotMax, test.wantMin, test.wantMax)
			}
		})
	}
}

func TestReadCredentialPreservesPasswordAndEmptyInput(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("password with spaces\n\n"))
	if got := readCredential(reader, ""); got != "password with spaces" {
		t.Fatalf("password = %q, want %q", got, "password with spaces")
	}
	if got := readCredential(reader, ""); got != "" {
		t.Fatalf("blank password choice = %q, want empty", got)
	}
}

func TestPairingCredentialPrompt(t *testing.T) {
	for _, test := range []struct {
		name       string
		expectPIN  bool
		displayErr error
		want       string
	}{
		{name: "advertised PIN displayed", expectPIN: true, want: "Enter the PIN shown on the receiver: "},
		{name: "advertised PIN display failed", expectPIN: true, displayErr: errors.New("display failed"), want: "Enter the receiver's configured password or pairing PIN: "},
		{name: "unknown mode accepted display request", want: "Enter the receiver's configured password or pairing PIN: "},
		{name: "unknown mode rejected display request", displayErr: errors.New("not supported"), want: "Enter the receiver's configured password or pairing PIN: "},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := pairingCredentialPrompt(test.expectPIN, test.displayErr); got != test.want {
				t.Fatalf("prompt = %q, want %q", got, test.want)
			}
		})
	}
}

func TestPasswordRequiresPairing(t *testing.T) {
	const (
		passwordFlag = uint64(1 << 7)
		pinFlag      = uint64(1 << 9)
	)
	modernFeatures := airplay.FeatureSystemPairing | airplay.FeatureTransientPairing | uint64(1<<38) | uint64(1<<46)
	legacyFeatures := modernFeatures | uint64(1<<51)

	for _, test := range []struct {
		name string
		info *airplay.ReceiverInfo
		want bool
	}{
		{name: "missing info"},
		{name: "legacy configured password", info: &airplay.ReceiverInfo{Features: legacyFeatures, StatusFlags: passwordFlag}, want: true},
		{name: "modern configured password can pair transiently", info: &airplay.ReceiverInfo{Features: modernFeatures, StatusFlags: passwordFlag}},
		{name: "password wins over modern PIN", info: &airplay.ReceiverInfo{Features: modernFeatures, StatusFlags: passwordFlag | pinFlag}, want: true},
		{name: "legacy receiver without password", info: &airplay.ReceiverInfo{Features: legacyFeatures}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := passwordRequiresPairing(test.info); got != test.want {
				t.Fatalf("passwordRequiresPairing() = %t, want %t", got, test.want)
			}
		})
	}
}
