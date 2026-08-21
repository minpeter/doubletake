package airplay

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
)

func TestUseAudioFECDefaults(t *testing.T) {
	if !useAudioFEC(AudioCodecALAC, false) {
		t.Fatal("expected legacy/plaintext sessions to keep FEC by default")
	}
	if useAudioFEC(AudioCodecALAC, true) {
		t.Fatal("expected modern encrypted sessions to disable FEC by default")
	}
	if useAudioFEC(AudioCodecAACELD, false) {
		t.Fatal("AAC-ELD must not use ALAC-style redundant retransmits")
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

func TestAACELDCodecInfoMatchesAirPlayCompressionType(t *testing.T) {
	ct, spf, format, latencyMin, latencyMax, latencySamples := AudioCodecAACELD.Info()
	if ct != 8 || spf != 480 || format != 0x1000000 {
		t.Fatalf("AAC-ELD info = ct=%d spf=%d format=0x%x, want ct=8 spf=480 format=0x1000000", ct, spf, format)
	}
	if latencyMin != 0 || latencyMax != int64(latencySamples) {
		t.Fatalf("AAC-ELD latency = min=%d max=%d samples=%d", latencyMin, latencyMax, latencySamples)
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
		{name: "AAC ELD", ct: byte(AudioCodecAACELD), want: defaultLatency},
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

func TestStreamAudioJoinsOwnedWorkersOnReadFailure(t *testing.T) {
	ctrlConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	peer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		ctrlConn.Close()
		t.Fatalf("listen peer socket: %v", err)
	}
	defer peer.Close()

	firstFrame := make(chan struct{})
	close(firstFrame)
	session := &MirrorSession{
		firstFrameSent: firstFrame,
		timingProtocol: timingProtocolNTP,
	}
	stream := &AudioStream{
		ctrlConn:       ctrlConn,
		ctrlAddr:       peer.LocalAddr().(*net.UDPAddr),
		spf:            352,
		latencySamples: targetLatencySamples44k1(),
	}
	capture := &AudioCapture{
		pcmPipe: io.NopCloser(bytes.NewReader(nil)),
		waitCh:  make(chan struct{}),
		codec:   AudioCodecALAC,
	}

	done := make(chan error, 1)
	go func() { done <- session.StreamAudio(context.Background(), capture, stream) }()
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("StreamAudio error = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StreamAudio did not join its control and timing workers")
	}

	if _, err := ctrlConn.WriteTo([]byte{1}, peer.LocalAddr()); err == nil {
		t.Fatal("StreamAudio returned without closing its owned control socket")
	}
}

type gatedPCMReader struct {
	reads  chan struct{}
	frames chan []byte
	done   chan struct{}
	once   sync.Once
}

func newGatedPCMReader() *gatedPCMReader {
	return &gatedPCMReader{
		reads:  make(chan struct{}, 8),
		frames: make(chan []byte),
		done:   make(chan struct{}),
	}
}

func (r *gatedPCMReader) Read(p []byte) (int, error) {
	select {
	case r.reads <- struct{}{}:
	default:
	}
	select {
	case frame := <-r.frames:
		return copy(p, frame), nil
	case <-r.done:
		return 0, io.EOF
	}
}

func (r *gatedPCMReader) Close() error {
	r.once.Do(func() { close(r.done) })
	return nil
}

func TestStreamAudioAnchorsClockOnlyAfterFirstCapturedFrame(t *testing.T) {
	ctrlConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen control socket: %v", err)
	}
	ctrlPeer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		ctrlConn.Close()
		t.Fatalf("listen control peer: %v", err)
	}
	defer ctrlPeer.Close()

	dataConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		ctrlConn.Close()
		t.Fatalf("listen data socket: %v", err)
	}
	dataPeer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		ctrlConn.Close()
		dataConn.Close()
		t.Fatalf("listen data peer: %v", err)
	}
	defer dataPeer.Close()

	firstVideoFrame := make(chan struct{})
	session := &MirrorSession{
		firstFrameSent: firstVideoFrame,
		timingProtocol: timingProtocolNTP,
	}
	stream := &AudioStream{
		conn:            dataConn,
		ctrlConn:        ctrlConn,
		remoteAddr:      dataPeer.LocalAddr().(*net.UDPAddr),
		ctrlAddr:        ctrlPeer.LocalAddr().(*net.UDPAddr),
		spf:             352,
		ct:              byte(AudioCodecALAC),
		latencySamples:  targetLatencySamples44k1(),
		securityMode:    audioSecurityLegacyAES,
		chachaNonceMode: defaultAudioChaChaNonceMode(),
		chachaAADMode:   defaultAudioChaChaAADMode(),
	}
	pcmReader := newGatedPCMReader()
	capture := &AudioCapture{
		pcmPipe: pcmReader,
		waitCh:  make(chan struct{}),
		codec:   AudioCodecALAC,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- session.StreamAudio(ctx, capture, stream) }()
	waitForPCMRead := func(label string) {
		t.Helper()
		select {
		case <-pcmReader.reads:
		case <-time.After(time.Second):
			t.Fatalf("audio capture did not request %s PCM frame", label)
		}
	}
	const pcmFrameBytes = 352 * 2 * 2
	pcmFrame := make([]byte, pcmFrameBytes)
	waitForPCMRead("pre-roll")

	// Starting the process and observing the first video frame are not enough to
	// define an audio clock anchor. No TimeAnnounce may leave until a real sample
	// is available to associate with the initial RTP timestamp.
	if err := ctrlPeer.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	if _, _, err := ctrlPeer.ReadFrom(buf); err == nil {
		t.Fatal("initial TimeAnnounce was sent before the first captured audio frame")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read before first frame: %v", err)
	}

	pcmReader.frames <- pcmFrame
	waitForPCMRead("last pre-roll")
	close(firstVideoFrame)
	if err := ctrlPeer.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ctrlPeer.ReadFrom(buf); err == nil {
		t.Fatal("initial TimeAnnounce was sent before a post-video audio frame was ready")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read before fresh frame: %v", err)
	}

	// The prewarmer may already be blocked in one final read when video becomes
	// ready. Release that read, then withhold the next frame and verify that the
	// clock still has not been anchored.
	pcmReader.frames <- pcmFrame
	waitForPCMRead("first current")
	if err := ctrlPeer.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ctrlPeer.ReadFrom(buf); err == nil {
		t.Fatal("initial TimeAnnounce was sent while only pre-roll audio was available")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("read before held current frame: %v", err)
	}

	pcmReader.frames <- pcmFrame
	if err := ctrlPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	n, _, err := ctrlPeer.ReadFrom(buf)
	if err != nil {
		t.Fatalf("read initial TimeAnnounce: %v", err)
	}
	if n != 20 || buf[0] != 0x90 || buf[1] != audioSyncPayloadTypeNTP {
		t.Fatalf("initial TimeAnnounce = len %d header %02x, want len 20 header 90d4", n, buf[:min(n, 2)])
	}
	if err := dataPeer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := dataPeer.ReadFrom(buf); err != nil {
		t.Fatalf("read first RTP audio packet: %v", err)
	}

	cancel()
	_ = pcmReader.Close()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StreamAudio error = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StreamAudio did not stop")
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
	}, timingProtocolNTP, networkTime, 0, true)

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

func TestSendSyncPacketUsesNegotiatedProtocolInsteadOfTimelinePresence(t *testing.T) {
	packet := captureAudioSyncPacket(t, &AudioStream{}, timingProtocolNTP, 1, 0x1122334455667788, false)
	if len(packet) != 20 || packet[1] != audioSyncPayloadTypeNTP {
		t.Fatalf("explicit NTP sync packet = len %d type 0x%02x, want len 20 type 0x%02x",
			len(packet), packet[1], audioSyncPayloadTypeNTP)
	}

	stream := &AudioStream{ctrlConn: &recordingPacketConn{}, ctrlAddr: &net.UDPAddr{}}
	if err := stream.sendSyncPacket(timingProtocolPTP, 1, 0, false); err == nil {
		t.Fatal("PTP sync succeeded without a receiver timeline ID")
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
	}, timingProtocolPTP, networkTime, timelineID, false)

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
	}, timingProtocolPTP, 1, 2, true)

	if got, want := binary.BigEndian.Uint32(packet[4:8]), rtpTime-latency; got != want {
		t.Fatalf("wrapped network-time RTP = %#x, want %#x", got, want)
	}
}

func captureAudioSyncPacket(t *testing.T, stream *AudioStream, timingProtocol string, networkTime, timelineID uint64, isFirst bool) []byte {
	t.Helper()
	conn := &recordingPacketConn{}
	stream.ctrlConn = conn
	stream.ctrlAddr = &net.UDPAddr{}
	if err := stream.sendSyncPacket(timingProtocol, networkTime, timelineID, isFirst); err != nil {
		t.Fatal(err)
	}
	if len(conn.packets) != 1 {
		t.Fatalf("sent %d sync packets, want 1", len(conn.packets))
	}
	return conn.packets[0]
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
