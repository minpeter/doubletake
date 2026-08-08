package airplay

import (
	"bytes"
	"testing"

	"golang.org/x/crypto/chacha20poly1305"
)

func TestIsFirstSlice(t *testing.T) {
	// NAL header 0x61 (type 1), slice header starts with bit 1 → first_mb_in_slice=0
	if !isFirstSlice([]byte{0x61, 0x80}) {
		t.Fatal("expected first slice (first_mb_in_slice=0)")
	}
	// NAL header 0x61, slice header starts with bit 0 → first_mb_in_slice > 0
	if isFirstSlice([]byte{0x61, 0x40}) {
		t.Fatal("expected non-first slice (first_mb_in_slice > 0)")
	}
	// Too short
	if isFirstSlice([]byte{0x61}) {
		t.Fatal("expected false for single-byte NAL")
	}
}

func TestSPSDimensions(t *testing.T) {
	// Baseline-profile SPS (profile_idc=66, level 31) encoding 1280x720,
	// frame_mbs_only=1, no cropping. Hand-encoded RBSP.
	sps := []byte{0x67, 0x42, 0x00, 0x1F, 0xF8, 0x0A, 0x00, 0xB7, 0x20}
	w, h, ok := spsDimensions(sps)
	if !ok {
		t.Fatal("expected SPS to parse")
	}
	if w != 1280 || h != 720 {
		t.Fatalf("expected 1280x720, got %dx%d", w, h)
	}

	// Non-SPS NAL (type 1) must be rejected.
	if _, _, ok := spsDimensions([]byte{0x61, 0x80, 0x00, 0x00}); ok {
		t.Fatal("expected non-SPS NAL to be rejected")
	}
	// Truncated SPS must be rejected.
	if _, _, ok := spsDimensions([]byte{0x67, 0x42}); ok {
		t.Fatal("expected truncated SPS to be rejected")
	}
}

func TestPlistStreamPortsLegacy(t *testing.T) {
	stream := map[string]interface{}{
		"dataPort":    uint64(6100),
		"controlPort": uint64(6101),
	}

	dataPort, controlPort := plistStreamPorts(stream)
	if dataPort != 6100 || controlPort != 6101 {
		t.Fatalf("expected legacy ports 6100/6101, got %d/%d", dataPort, controlPort)
	}
}

func TestPlistStreamPortsStreamConnections(t *testing.T) {
	stream := map[string]interface{}{
		"dataPort":    uint64(6100),
		"controlPort": uint64(6101),
		"streamConnections": map[string]interface{}{
			"streamConnectionTypeRTP": map[string]interface{}{
				"streamConnectionKeyPort": uint64(7100),
			},
			"streamConnectionTypeRTCP": map[string]interface{}{
				"streamConnectionKeyPort": uint64(7101),
			},
		},
	}

	dataPort, controlPort := plistStreamPorts(stream)
	if dataPort != 7100 || controlPort != 7101 {
		t.Fatalf("expected streamConnections ports 7100/7101, got %d/%d", dataPort, controlPort)
	}
}

func TestGenerateAudioChaChaKey(t *testing.T) {
	want := bytes.Repeat([]byte{0x5a}, chacha20poly1305.KeySize)

	key, err := generateAudioChaChaKey(bytes.NewReader(want))
	if err != nil {
		t.Fatalf("generateAudioChaChaKey returned error: %v", err)
	}
	if !bytes.Equal(key, want) {
		t.Fatalf("generated key = %x, want %x", key, want)
	}
}

func TestGenerateAudioChaChaKeyShortRead(t *testing.T) {
	if _, err := generateAudioChaChaKey(bytes.NewReader(make([]byte, 8))); err == nil {
		t.Fatal("expected short reader to fail")
	}
}
