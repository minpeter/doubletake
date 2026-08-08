package airplay

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

func TestUseAudioFECDefaults(t *testing.T) {
	if !useAudioFEC(false) {
		t.Fatal("expected legacy/plaintext sessions to keep FEC by default")
	}
	if useAudioFEC(true) {
		t.Fatal("expected modern encrypted sessions to disable FEC by default")
	}
}

func TestAudioCodecInfoKeepsScreenLatencyMinimumAtZero(t *testing.T) {
	_, _, _, latencyMin, latencyMax, latencySamples := AudioCodecALAC.Info()
	if latencyMin != 0 {
		t.Fatalf("latencyMin = %d, want 0", latencyMin)
	}
	if latencyMax != int64(latencySamples) {
		t.Fatalf("latencyMax = %d, want %d", latencyMax, latencySamples)
	}
}

func TestAudioLatencySamplesForCodec(t *testing.T) {
	defaultLatency := targetLatencySamples44k1()
	tests := []struct {
		name     string
		ct       byte
		override uint32
		want     uint32
	}{
		{name: "default ALAC", ct: byte(AudioCodecALAC), want: defaultLatency},
		{name: "override wins", ct: byte(AudioCodecALAC), override: 11025, want: 11025},
		{name: "unknown codec falls back", ct: 99, want: defaultLatency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := audioLatencySamplesForCodec(tt.ct, tt.override); got != tt.want {
				t.Fatalf("audioLatencySamplesForCodec(%d, %d) = %d, want %d", tt.ct, tt.override, got, tt.want)
			}
		})
	}
}

func TestRandomRTPTime(t *testing.T) {
	got, err := randomRTPTime(bytes.NewReader([]byte{0x12, 0x34, 0x56, 0x78}))
	if err != nil {
		t.Fatal(err)
	}
	if got != 0x12345678 {
		t.Fatalf("RTP timestamp = %#x, want 0x12345678", got)
	}
}

func TestSendSyncPacketUsesNTPFormatWithoutTimeline(t *testing.T) {
	const (
		networkTime = uint64(0x83aa7e8012345678)
		rtpTime     = uint32(5000)
	)
	packet := captureAudioSyncPacket(t, &AudioStream{
		rtpTime:        rtpTime,
		latencySamples: 1000,
	}, networkTime, 0, true)

	if len(packet) != 20 {
		t.Fatalf("NTP sync packet length = %d, want 20", len(packet))
	}
	if packet[0] != 0x90 || packet[1] != audioSyncPayloadTypeNTP {
		t.Fatalf("NTP sync header = %02x%02x, want 90d4", packet[0], packet[1])
	}
	if got := binary.BigEndian.Uint32(packet[4:8]); got != 4000 {
		t.Fatalf("NTP playback RTP = %d, want 4000", got)
	}
	if got := binary.BigEndian.Uint64(packet[8:16]); got != networkTime {
		t.Fatalf("NTP timestamp = 0x%016x, want 0x%016x", got, networkTime)
	}
	if got := binary.BigEndian.Uint32(packet[16:20]); got != rtpTime {
		t.Fatalf("NTP receive RTP = %d, want %d", got, rtpTime)
	}
}

func TestSendSyncPacketUsesPTPTimeAnnounceWithTimeline(t *testing.T) {
	const (
		networkTime = uint64(0x0000000180000000) // 1.5 seconds in seconds.32
		timelineID  = uint64(0x48e15caa8da00008)
		rtpTime     = uint32(5000)
	)
	packet := captureAudioSyncPacket(t, &AudioStream{
		rtpTime:        rtpTime,
		latencySamples: 1000,
	}, networkTime, timelineID, false)

	if len(packet) != 28 {
		t.Fatalf("PTP sync packet length = %d, want 28", len(packet))
	}
	if packet[0] != 0x80 || packet[1] != audioSyncPayloadTypePTP {
		t.Fatalf("PTP sync header = %02x%02x, want 80d7", packet[0], packet[1])
	}
	if got := binary.BigEndian.Uint32(packet[4:8]); got != 4000 {
		t.Fatalf("PTP network-time RTP = %d, want 4000", got)
	}
	if got := binary.BigEndian.Uint32(packet[16:20]); got != rtpTime {
		t.Fatalf("PTP apply RTP = %d, want %d", got, rtpTime)
	}
	if got := binary.BigEndian.Uint64(packet[8:16]); got != 1500000000 {
		t.Fatalf("PTP timestamp = %d ns, want 1500000000", got)
	}
	if got := binary.BigEndian.Uint64(packet[20:28]); got != timelineID {
		t.Fatalf("PTP timeline = 0x%016x, want 0x%016x", got, timelineID)
	}
}

func TestSendSyncPacketWrapsLatencyAdjustedRTP(t *testing.T) {
	rtpTime := uint32(1000)
	latency := uint32(4410)
	packet := captureAudioSyncPacket(t, &AudioStream{
		rtpTime:        rtpTime,
		latencySamples: latency,
	}, 1, 2, true)

	if got, want := binary.BigEndian.Uint32(packet[4:8]), rtpTime-latency; got != want {
		t.Fatalf("wrapped network-time RTP = %#x, want %#x", got, want)
	}
}

func captureAudioSyncPacket(t *testing.T, stream *AudioStream, networkTime, timelineID uint64, isFirst bool) []byte {
	t.Helper()
	receiver, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	sender, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	stream.ctrlConn = sender
	stream.ctrlAddr = receiver.LocalAddr().(*net.UDPAddr)
	if err := stream.sendSyncPacket(networkTime, timelineID, isFirst); err != nil {
		t.Fatal(err)
	}
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 64)
	n, _, err := receiver.ReadFrom(packet)
	if err != nil {
		t.Fatal(err)
	}
	return packet[:n]
}

func TestAudioChaCha64AEADRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, chacha20poly1305.KeySize)
	nonce := bytes.Repeat([]byte{0x11}, audioChaChaNonceSize)
	aad := []byte{0x90, 0x78, 0x56, 0x34, 0x12, 0xef, 0xcd, 0xab}
	plaintext := []byte("doubletake mirrored audio")

	aead, err := newAudioChaCha64AEAD(key)
	if err != nil {
		t.Fatalf("newAudioChaCha64AEAD returned error: %v", err)
	}

	sealed := aead.Seal(nil, nonce, plaintext, aad)
	opened, err := aead.Open(nil, nonce, sealed, aad)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("opened plaintext = %x, want %x", opened, plaintext)
	}
	if len(sealed) != len(plaintext)+aead.Overhead() {
		t.Fatalf("sealed len = %d, want %d", len(sealed), len(plaintext)+aead.Overhead())
	}
}

func TestAudioChaCha64AEADRejectsTampering(t *testing.T) {
	key := bytes.Repeat([]byte{0x24}, chacha20poly1305.KeySize)
	nonce := bytes.Repeat([]byte{0x7b}, audioChaChaNonceSize)
	aad := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	plaintext := []byte("auth must fail when the packet changes")

	aead, err := newAudioChaCha64AEAD(key)
	if err != nil {
		t.Fatalf("newAudioChaCha64AEAD returned error: %v", err)
	}

	sealed := aead.Seal(nil, nonce, plaintext, aad)
	sealed[len(sealed)-1] ^= 0x80
	if _, err := aead.Open(nil, nonce, sealed, aad); err == nil {
		t.Fatal("expected tampered packet to fail authentication")
	}
}

func TestAudioChaCha64AEADEquivalentToZeroPrefixedIETF(t *testing.T) {
	key := bytes.Repeat([]byte{0x35}, chacha20poly1305.KeySize)
	nonce := bytes.Repeat([]byte{0x12}, audioChaChaNonceSize)
	aad := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11}
	plaintext := []byte("original 64-bit nonce variant")

	custom, err := newAudioChaCha64AEAD(key)
	if err != nil {
		t.Fatalf("newAudioChaCha64AEAD returned error: %v", err)
	}
	customSealed := custom.Seal(nil, nonce, plaintext, aad)

	ietf, err := chacha20poly1305.New(key)
	if err != nil {
		t.Fatalf("chacha20poly1305.New returned error: %v", err)
	}
	ietfNonce := make([]byte, chacha20poly1305.NonceSize)
	copy(ietfNonce[4:], nonce)
	ietfSealed := ietf.Seal(nil, ietfNonce, plaintext, aad)

	if !bytes.Equal(customSealed, ietfSealed) {
		t.Fatal("expected the 64-bit nonce construction to match the zero-prefixed IETF form while the counter stays within 32 bits")
	}
}

func TestAudioChaChaAADUsesRTPNetworkOrder(t *testing.T) {
	as := &AudioStream{
		ssrc:          0x11223344,
		chachaAADMode: audioChaChaAADTimestampSSRC,
	}
	header := []byte{0x80, 0x60, 0x00, 0x01, 0x12, 0x34, 0x56, 0x78, 0x11, 0x22, 0x33, 0x44}

	aad := as.audioChaChaAAD(header, 0x12345678)
	want := []byte{0x12, 0x34, 0x56, 0x78, 0x11, 0x22, 0x33, 0x44}
	if !bytes.Equal(aad, want) {
		t.Fatalf("AAD = %x, want %x", aad, want)
	}
}
