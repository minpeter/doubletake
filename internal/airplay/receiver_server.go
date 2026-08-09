package airplay

import (
	"bufio"
	"context"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/chacha20poly1305"
	"howett.net/plist"
)

// ReceiverProfile selects a complete receiver protocol personality. Profiles
// deliberately couple pairing, control encryption, SETUP layout, and timing;
// mixing those independently creates combinations that no real receiver uses.
type ReceiverProfile string

const (
	ReceiverProfileModern ReceiverProfile = "modern"
	ReceiverProfileRoku   ReceiverProfile = "roku"
)

// ReceiverAuthMode controls which stages require the single configured Code.
// PIN advertises an on-screen code; password modes advertise a configured
// password instead. Combined requires that password for pair-setup and Digest.
type ReceiverAuthMode string

const (
	ReceiverAuthNone     ReceiverAuthMode = "none"
	ReceiverAuthPIN      ReceiverAuthMode = "pin"
	ReceiverAuthPassword ReceiverAuthMode = "password"
	ReceiverAuthDigest   ReceiverAuthMode = "digest"
	ReceiverAuthCombined ReceiverAuthMode = "combined"
)

// ReceiverConfig configures the in-repository AirPlay receiver. It is intended
// for protocol and end-to-end testing: media is validated and drained rather
// than rendered through a desktop player.
type ReceiverConfig struct {
	ListenAddress string
	Profile       ReceiverProfile
	Auth          ReceiverAuthMode
	Code          string
	Name          string
	Model         string
	DeviceID      string
	Logger        *log.Logger
	Debug         bool
}

// ReceiverStats is a race-free snapshot of protocol and media activity.
type ReceiverStats struct {
	Connections       uint64
	InfoRequests      uint64
	PINStarts         uint64
	PairSetup         uint64
	PairVerify        uint64
	FairPlayRequests  uint64
	DigestChallenges  uint64
	SetupRequests     uint64
	RecordRequests    uint64
	ParameterRequests uint64
	FeedbackRequests  uint64
	TeardownRequests  uint64
	EventConnections  uint64
	VideoConnections  uint64
	VideoPackets      uint64
	VideoBytes        uint64
	AudioPackets      uint64
	AudioBytes        uint64
	TimingProbes      uint64
	TimingReplies     uint64
}

type receiverAtomicStats struct {
	connections       atomic.Uint64
	infoRequests      atomic.Uint64
	pinStarts         atomic.Uint64
	pairSetup         atomic.Uint64
	pairVerify        atomic.Uint64
	fairPlayRequests  atomic.Uint64
	digestChallenges  atomic.Uint64
	setupRequests     atomic.Uint64
	recordRequests    atomic.Uint64
	parameterRequests atomic.Uint64
	feedbackRequests  atomic.Uint64
	teardownRequests  atomic.Uint64
}

type receiverProfileSpec struct {
	name          string
	model         string
	sourceVersion string
	features      uint64
	modernSetup   bool
}

// ReceiverServer implements enough of an AirPlay mirroring receiver to run the
// real doubletake sender end to end without hardware. Pairing and encrypted
// control are protocol-faithful; encoded media is parsed/count-checked and
// discarded.
type ReceiverServer struct {
	cfg         ReceiverConfig
	profile     receiverProfileSpec
	listener    net.Listener
	publicKey   ed25519.PublicKey
	privateKey  ed25519.PrivateKey
	identifier  string
	controllers *receiverControllerStore
	digest      *receiverDigestAuth
	startedAt   time.Time

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
	wg        sync.WaitGroup
	stats     receiverAtomicStats
	controlMu sync.Mutex
	controls  map[net.Conn]struct{}
	serveMu   sync.Mutex
	serving   bool
	serveDone chan struct{}

	mediaMu     sync.Mutex
	activeMedia map[*receiverMediaSession]struct{}
	mediaTotals receiverMediaStats
}

// NewReceiverServer opens the configured control listener. Call Serve to
// accept clients, and Close (or cancel Serve's context) to stop it.
func NewReceiverServer(cfg ReceiverConfig) (*ReceiverServer, error) {
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = "127.0.0.1:7000"
	}
	if cfg.Profile == "" {
		cfg.Profile = ReceiverProfileRoku
	}
	if cfg.Auth == "" {
		cfg.Auth = ReceiverAuthNone
	}
	profile, err := receiverProfile(cfg.Profile)
	if err != nil {
		return nil, err
	}
	switch cfg.Auth {
	case ReceiverAuthNone:
		if cfg.Code != "" {
			return nil, fmt.Errorf("receiver code requires an auth mode")
		}
	case ReceiverAuthPIN, ReceiverAuthPassword, ReceiverAuthDigest, ReceiverAuthCombined:
		if cfg.Code == "" {
			return nil, fmt.Errorf("receiver auth mode %q requires a non-empty code", cfg.Auth)
		}
	default:
		return nil, fmt.Errorf("unknown receiver auth mode %q", cfg.Auth)
	}
	if cfg.Name == "" {
		cfg.Name = profile.name
	}
	if cfg.Model == "" {
		cfg.Model = profile.model
	}
	if cfg.DeviceID == "" {
		cfg.DeviceID, err = randomReceiverDeviceID()
		if err != nil {
			return nil, err
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate receiver identity: %w", err)
	}
	digestPassword := ""
	if cfg.Auth == ReceiverAuthDigest || cfg.Auth == ReceiverAuthCombined {
		digestPassword = cfg.Code
	}
	digest, err := newReceiverDigestAuth(digestPassword)
	if err != nil {
		return nil, fmt.Errorf("configure receiver digest auth: %w", err)
	}
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", cfg.ListenAddress, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := &ReceiverServer{
		cfg:         cfg,
		profile:     profile,
		listener:    listener,
		publicKey:   publicKey,
		privateKey:  privateKey,
		identifier:  generateUUID(),
		controllers: newReceiverControllerStore(),
		digest:      digest,
		startedAt:   time.Now(),
		ctx:         ctx,
		cancel:      cancel,
		activeMedia: make(map[*receiverMediaSession]struct{}),
		controls:    make(map[net.Conn]struct{}),
		serveDone:   make(chan struct{}),
	}
	return server, nil
}

func receiverProfile(profile ReceiverProfile) (receiverProfileSpec, error) {
	switch profile {
	case ReceiverProfileModern:
		return receiverProfileSpec{
			name:          "doubletake modern test receiver",
			model:         "AppleTV-Test",
			sourceVersion: modernAirPlaySourceVersion,
			features: FeatureScreen | FeatureAudio | FeatureFPSAP25 | FeatureHomeKitPairing |
				uint64(1<<38) | FeatureSystemPairing | uint64(1<<46) | FeatureTransientPairing,
			modernSetup: true,
		}, nil
	case ReceiverProfileRoku:
		return receiverProfileSpec{
			name:          "doubletake Roku test receiver",
			model:         "3820R2",
			sourceVersion: legacyAirPlaySourceVersion,
			features:      uint64(0x038bcf46007f8ad0),
		}, nil
	default:
		return receiverProfileSpec{}, fmt.Errorf("unknown receiver profile %q", profile)
	}
}

func randomReceiverDeviceID() (string, error) {
	var address [6]byte
	if _, err := io.ReadFull(rand.Reader, address[:]); err != nil {
		return "", fmt.Errorf("generate receiver device ID: %w", err)
	}
	address[0] = address[0]&0xfe | 0x02
	return fmt.Sprintf("%02X:%02X:%02X:%02X:%02X:%02X",
		address[0], address[1], address[2], address[3], address[4], address[5]), nil
}

// Addr returns the actual bound control address, including an ephemeral port.
func (s *ReceiverServer) Addr() net.Addr { return s.listener.Addr() }

// Serve accepts control connections until ctx is cancelled or Close is called.
func (s *ReceiverServer) Serve(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("receiver Serve context is nil")
	}
	s.serveMu.Lock()
	if s.serving {
		s.serveMu.Unlock()
		return fmt.Errorf("receiver Serve is already running")
	}
	s.serving = true
	s.serveMu.Unlock()
	defer close(s.serveDone)

	stop := context.AfterFunc(ctx, func() { _ = s.Close() })
	defer stop()
	s.logf("listening on %s (profile=%s auth=%s)", s.listener.Addr(), s.cfg.Profile, s.cfg.Auth)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil || ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept receiver control connection: %w", err)
		}
		s.stats.connections.Add(1)
		s.controlMu.Lock()
		s.controls[conn] = struct{}{}
		s.controlMu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				s.controlMu.Lock()
				delete(s.controls, conn)
				s.controlMu.Unlock()
			}()
			if err := s.serveControlConnection(conn); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.logf("control connection %s failed: %v", conn.RemoteAddr(), err)
			}
		}()
	}
}

// Close stops accepting and closes all active media endpoints.
func (s *ReceiverServer) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.closeErr = s.listener.Close()
		s.serveMu.Lock()
		serving := s.serving
		serveDone := s.serveDone
		s.serveMu.Unlock()
		if serving {
			<-serveDone
		}
		s.controlMu.Lock()
		controls := make([]net.Conn, 0, len(s.controls))
		for conn := range s.controls {
			controls = append(controls, conn)
		}
		s.controlMu.Unlock()
		for _, conn := range controls {
			_ = conn.Close()
		}
		s.mediaMu.Lock()
		media := make([]*receiverMediaSession, 0, len(s.activeMedia))
		for session := range s.activeMedia {
			media = append(media, session)
		}
		s.mediaMu.Unlock()
		for _, session := range media {
			_ = session.Close()
		}
	})
	s.wg.Wait()
	if errors.Is(s.closeErr, net.ErrClosed) {
		return nil
	}
	return s.closeErr
}

func (s *ReceiverServer) logf(format string, args ...any) {
	if s.cfg.Debug {
		s.cfg.Logger.Printf("[RECEIVER] "+format, args...)
	}
}

type receiverConnection struct {
	server       *ReceiverServer
	conn         net.Conn
	reader       *bufio.Reader
	pairing      *receiverPairingState
	fairplay     *receiverFPSAPState
	hap          *receiverHAPStream
	media        *receiverMediaSession
	timingProbed bool
	sessionState receiverSessionState
}

type receiverSessionState uint8

const (
	receiverSessionInitial receiverSessionState = iota
	receiverSessionControlPrepared
	receiverSessionRecorded
	receiverSessionAudioPrepared
	receiverSessionVideoPrepared
	receiverSessionReady
	receiverSessionTornDown
)

type receiverSetupKind uint8

const (
	receiverSetupControl receiverSetupKind = iota
	receiverSetupAudio
	receiverSetupVideo
)

func (s *ReceiverServer) serveControlConnection(conn net.Conn) error {
	defer conn.Close()
	pairingCode := ""
	if s.cfg.Auth == ReceiverAuthPIN || s.cfg.Auth == ReceiverAuthPassword || s.cfg.Auth == ReceiverAuthCombined {
		pairingCode = s.cfg.Code
	}
	pairing, err := newReceiverPairingState(receiverPairingConfig{
		identifier:  s.identifier,
		privateKey:  s.privateKey,
		pin:         pairingCode,
		controllers: s.controllers,
	})
	if err != nil {
		return err
	}
	var fairplay *receiverFPSAPState
	if s.profile.features&FeatureFPSAP25 != 0 {
		fairplay, err = newReceiverFPSAPState(rand.Reader)
		if err != nil {
			return fmt.Errorf("initialize receiver FPSAP: %w", err)
		}
	}
	state := &receiverConnection{
		server:   s,
		conn:     conn,
		reader:   bufio.NewReaderSize(conn, receiverHeaderLimit),
		pairing:  pairing,
		fairplay: fairplay,
	}
	defer state.closeMedia()

	for {
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Minute)); err != nil {
			return err
		}
		request, err := readReceiverRequest(state.reader)
		if err != nil {
			return err
		}
		_ = conn.SetReadDeadline(time.Time{})
		s.logf("<- %s %s (body=%d encrypted=%t)", request.method, request.uri, len(request.body), state.hap != nil)
		response, enableKeys := state.dispatch(request)
		if err := state.writeResponse(request, response); err != nil {
			return err
		}
		if enableKeys != nil {
			if err := state.enableEncryption(*enableKeys); err != nil {
				return err
			}
		}
	}
}

type receiverRequest struct {
	method  string
	uri     string
	headers map[string]string
	body    []byte
}

const (
	receiverHeaderLimit = 32 * 1024
	receiverBodyLimit   = 8 * 1024 * 1024
)

func readReceiverRequest(reader *bufio.Reader) (receiverRequest, error) {
	line, used, err := readReceiverLine(reader, receiverHeaderLimit)
	if err != nil {
		return receiverRequest{}, err
	}
	parts := strings.Fields(line)
	if len(parts) != 3 || parts[2] != "RTSP/1.0" {
		return receiverRequest{}, fmt.Errorf("invalid RTSP request line %q", line)
	}
	request := receiverRequest{method: parts[0], uri: parts[1], headers: make(map[string]string)}
	contentLength := 0
	for {
		line, size, err := readReceiverLine(reader, receiverHeaderLimit-used)
		if err != nil {
			return receiverRequest{}, err
		}
		used += size
		if line == "" {
			break
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return receiverRequest{}, fmt.Errorf("invalid RTSP header %q", line)
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		if name == "" {
			return receiverRequest{}, fmt.Errorf("empty RTSP header name")
		}
		if _, duplicate := request.headers[name]; duplicate {
			return receiverRequest{}, fmt.Errorf("duplicate RTSP header %q", name)
		}
		request.headers[name] = value
		if name == "content-length" {
			contentLength, err = strconv.Atoi(value)
			if err != nil || contentLength < 0 || contentLength > receiverBodyLimit {
				return receiverRequest{}, fmt.Errorf("invalid Content-Length %q", value)
			}
		}
	}
	if contentLength > 0 {
		request.body = make([]byte, contentLength)
		if _, err := io.ReadFull(reader, request.body); err != nil {
			return receiverRequest{}, fmt.Errorf("read RTSP body: %w", err)
		}
	}
	return request, nil
}

func readReceiverLine(reader *bufio.Reader, limit int) (string, int, error) {
	if limit <= 0 {
		return "", 0, fmt.Errorf("RTSP headers exceed %d bytes", receiverHeaderLimit)
	}
	var line []byte
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(line)+len(fragment) > limit {
			return "", 0, fmt.Errorf("RTSP headers exceed %d bytes", receiverHeaderLimit)
		}
		line = append(line, fragment...)
		if err == nil {
			break
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return "", 0, err
		}
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", 0, fmt.Errorf("RTSP line is not CRLF terminated")
	}
	return string(line[:len(line)-2]), len(line), nil
}

type receiverResponse struct {
	status      int
	contentType string
	headers     map[string]string
	body        []byte
}

func receiverOK(body []byte, contentType string) receiverResponse {
	return receiverResponse{status: 200, body: body, contentType: contentType}
}

func (c *receiverConnection) dispatch(request receiverRequest) (receiverResponse, *receiverPairingSessionKeys) {
	s := c.server
	path := requestPath(request.uri)
	switch {
	case request.method == "GET" && path == "/info":
		s.stats.infoRequests.Add(1)
		body, err := s.infoPlist()
		if err != nil {
			return receiverError(500, err), nil
		}
		return receiverOK(body, "application/x-apple-binary-plist"), nil

	case request.method == "POST" && path == "/pair-pin-start":
		s.stats.pinStarts.Add(1)
		if s.cfg.Auth != ReceiverAuthPIN {
			return receiverResponse{status: 453}, nil
		}
		s.cfg.Logger.Printf("[RECEIVER] pairing code: %s", s.cfg.Code)
		return receiverOK(nil, ""), nil

	case request.method == "POST" && path == "/pair-setup":
		s.stats.pairSetup.Add(1)
		body, err := c.pairing.pairSetup(request.body)
		if err != nil {
			s.logf("pair-setup rejected: %v", err)
			return receiverError(400, err), nil
		}
		return receiverOK(body, "application/octet-stream"), nil

	case request.method == "POST" && path == "/pair-verify":
		s.stats.pairVerify.Add(1)
		body, err := c.pairing.pairVerify(request.body)
		if err != nil {
			s.logf("pair-verify rejected: %v", err)
			return receiverError(400, err), nil
		}
		response := receiverOK(body, "application/octet-stream")
		keys, verified := c.pairing.sessionKeys()
		if verified && keys.encrypted && c.hap == nil {
			return response, &keys
		}
		return response, nil
	}

	if receiverIsSessionRequest(request, path) || request.method == "POST" && path == "/fp-setup" {
		if _, verified := c.pairing.sessionKeys(); !verified {
			return receiverError(455, fmt.Errorf("pair-verify must complete before %s", request.method)), nil
		}
	}

	if !c.authorize(request) {
		s.stats.digestChallenges.Add(1)
		return receiverResponse{
			status:  401,
			headers: map[string]string{"WWW-Authenticate": s.digest.challengeHeader()},
		}, nil
	}

	switch {
	case request.method == "POST" && path == "/fp-setup":
		s.stats.fairPlayRequests.Add(1)
		if c.fairplay == nil {
			return receiverResponse{status: 404}, nil
		}
		if request.headers["x-apple-et"] != "32" {
			return receiverError(400, fmt.Errorf("fp-setup requires X-Apple-ET: 32")), nil
		}
		body, err := c.fairplay.exchange(request.body)
		if err != nil {
			s.logf("fp-setup rejected: %v", err)
			return receiverError(400, err), nil
		}
		return receiverOK(body, "application/octet-stream"), nil
	case request.method == "SETUP":
		s.stats.setupRequests.Add(1)
		if c.fairplay != nil && !c.fairplay.complete() {
			return receiverError(455, fmt.Errorf("FairPlay SAP must complete before SETUP")), nil
		}
		return c.handleSetup(request), nil
	case request.method == "RECORD":
		s.stats.recordRequests.Add(1)
		return c.handleRecord(), nil
	case request.method == "SET_PARAMETER":
		s.stats.parameterRequests.Add(1)
		if c.sessionState != receiverSessionReady {
			return c.invalidSessionState(request.method), nil
		}
		return receiverOK(nil, ""), nil
	case request.method == "POST" && path == "/feedback":
		s.stats.feedbackRequests.Add(1)
		if c.sessionState != receiverSessionReady {
			return c.invalidSessionState("feedback"), nil
		}
		body, err := plist.Marshal(map[string]any{
			"streams": []any{map[string]any{"type": int64(96), "sr": int64(44100)}},
		}, plist.BinaryFormat)
		if err != nil {
			return receiverError(500, err), nil
		}
		return receiverResponse{status: 200, contentType: "application/x-apple-binary-plist", body: body, headers: s.clockHeaders()}, nil
	case request.method == "TEARDOWN":
		s.stats.teardownRequests.Add(1)
		if c.sessionState == receiverSessionInitial || c.sessionState == receiverSessionTornDown {
			return c.invalidSessionState(request.method), nil
		}
		c.closeMedia()
		c.sessionState = receiverSessionTornDown
		return receiverOK(nil, ""), nil
	default:
		return receiverResponse{status: 404}, nil
	}
}

func receiverIsSessionRequest(request receiverRequest, path string) bool {
	return request.method == "SETUP" ||
		request.method == "RECORD" ||
		request.method == "SET_PARAMETER" ||
		request.method == "TEARDOWN" ||
		request.method == "POST" && path == "/feedback"
}

func requestPath(uri string) string {
	if strings.HasPrefix(uri, "rtsp://") {
		if slash := strings.Index(uri[len("rtsp://"):], "/"); slash >= 0 {
			return uri[len("rtsp://")+slash:]
		}
	}
	return uri
}

func (c *receiverConnection) authorize(request receiverRequest) bool {
	if c.server.digest == nil || !c.server.digest.enabled() {
		return true
	}
	return c.server.digest.authorize(request.method, request.uri, request.headers["authorization"])
}

func receiverError(status int, err error) receiverResponse {
	body := []byte(err.Error())
	return receiverResponse{status: status, contentType: "text/plain", body: body}
}

func (s *ReceiverServer) infoPlist() ([]byte, error) {
	statusFlags := uint64(4)
	if s.cfg.Auth == ReceiverAuthPIN {
		statusFlags |= statusFlagPINRequiredForPairing
	}
	if s.cfg.Auth == ReceiverAuthPassword || s.cfg.Auth == ReceiverAuthDigest || s.cfg.Auth == ReceiverAuthCombined {
		statusFlags |= statusFlagPasswordRequired
	}
	info := map[string]any{
		"name":                     s.cfg.Name,
		"model":                    s.cfg.Model,
		"manufacturer":             "doubletake",
		"deviceID":                 s.cfg.DeviceID,
		"macAddress":               s.cfg.DeviceID,
		"protocolVersion":          "1.1",
		"sourceVersion":            s.profile.sourceVersion,
		"features":                 s.profile.features,
		"statusFlags":              statusFlags,
		"pk":                       []byte(s.publicKey),
		"initialVolume":            float64(0),
		"volumeControlType":        int64(0),
		"keepAliveSendStatsAsBody": true,
		"displays": []any{map[string]any{
			"width": int64(1920), "height": int64(1080),
			"widthPixels": int64(1920), "heightPixels": int64(1080),
			"widthPixelsMax": int64(1920), "heightPixelsMax": int64(1080),
			"maxFPS": int64(60), "uuid": generateUUID(),
		}},
		"audioLatencies": []any{map[string]any{
			"type": int64(100), "ch": int64(2),
			"inputLatencyMicros": int64(0), "outputLatencyMicros": int64(0),
		}},
	}
	return plist.Marshal(info, plist.BinaryFormat)
}

func (c *receiverConnection) handleSetup(request receiverRequest) receiverResponse {
	var setup map[string]any
	if _, err := plist.Unmarshal(request.body, &setup); err != nil {
		return receiverError(400, fmt.Errorf("decode SETUP plist: %w", err))
	}
	streams, err := receiverSetupStreams(setup)
	if err != nil {
		return receiverError(400, err)
	}
	kind, err := receiverSetupKindForStreams(streams)
	if err != nil {
		return receiverError(400, err)
	}
	nextState, err := c.nextSetupState(kind)
	if err != nil {
		return receiverError(455, err)
	}
	if err := c.ensureMedia(); err != nil {
		return receiverError(500, err)
	}
	endpoints := c.media.Endpoints()
	response := map[string]any{"eventPort": int64(endpoints.EventPort), "skipRecord": false}
	if c.server.profile.modernSetup {
		response["timingPeerInfo"] = map[string]any{
			"ClockID":                           int64(0x4454424c54414b45),
			"ID":                                c.server.identifier,
			"DeviceType":                        int64(1),
			"Addresses":                         []any{controlLocalIP(c.conn)},
			"SupportsClockPortMatchingOverride": true,
		}
	}

	if len(streams) == 0 {
		result := c.setupPlistResponse(response)
		if result.status == 200 {
			c.sessionState = nextState
		}
		return result
	}
	streamResponses := make([]any, 0, len(streams))
	for _, stream := range streams {
		switch plistInt(stream["type"]) {
		case 96:
			if !c.server.profile.modernSetup && !c.timingProbed {
				c.probeLegacyTiming(setup)
			}
			audio := map[string]any{
				"type":                     int64(96),
				"dataPort":                 int64(endpoints.AudioRTPPort),
				"controlPort":              int64(endpoints.AudioRTCPPort),
				"arrivalToRenderLatencyMs": int64(0),
			}
			if c.server.profile.modernSetup {
				audio["streamConnections"] = map[string]any{
					"streamConnectionTypeRTP":  map[string]any{"streamConnectionKeyPort": int64(endpoints.AudioRTPPort)},
					"streamConnectionTypeRTCP": map[string]any{"streamConnectionKeyPort": int64(endpoints.AudioRTCPPort)},
				}
			}
			streamResponses = append(streamResponses, audio)
		case 110:
			streamResponses = append(streamResponses, map[string]any{
				"type": int64(110), "dataPort": int64(endpoints.VideoPort),
			})
		default:
			return receiverError(400, fmt.Errorf("unsupported SETUP stream type %d", plistInt(stream["type"])))
		}
	}
	response["streams"] = streamResponses
	result := c.setupPlistResponse(response)
	if result.status == 200 {
		c.sessionState = nextState
	}
	return result
}

func receiverSetupKindForStreams(streams []map[string]any) (receiverSetupKind, error) {
	if len(streams) == 0 {
		return receiverSetupControl, nil
	}
	if len(streams) != 1 {
		return 0, fmt.Errorf("SETUP must contain exactly one media stream")
	}
	switch streamType := plistInt(streams[0]["type"]); streamType {
	case 96:
		return receiverSetupAudio, nil
	case 110:
		return receiverSetupVideo, nil
	default:
		return 0, fmt.Errorf("unsupported SETUP stream type %d", streamType)
	}
}

func (c *receiverConnection) nextSetupState(kind receiverSetupKind) (receiverSessionState, error) {
	if c.server.profile.modernSetup {
		switch {
		case c.sessionState == receiverSessionInitial && kind == receiverSetupControl:
			return receiverSessionControlPrepared, nil
		case c.sessionState == receiverSessionRecorded && kind == receiverSetupAudio:
			return receiverSessionAudioPrepared, nil
		case c.sessionState == receiverSessionAudioPrepared && kind == receiverSetupVideo:
			return receiverSessionReady, nil
		}
	} else {
		switch {
		case c.sessionState == receiverSessionInitial && kind == receiverSetupAudio:
			return receiverSessionAudioPrepared, nil
		case c.sessionState == receiverSessionAudioPrepared && kind == receiverSetupVideo:
			return receiverSessionVideoPrepared, nil
		}
	}
	return 0, fmt.Errorf("SETUP is not valid in receiver session state %d", c.sessionState)
}

func (c *receiverConnection) handleRecord() receiverResponse {
	if c.server.profile.modernSetup {
		if c.sessionState != receiverSessionControlPrepared {
			return c.invalidSessionState("RECORD")
		}
		c.sessionState = receiverSessionRecorded
	} else {
		if c.sessionState != receiverSessionVideoPrepared {
			return c.invalidSessionState("RECORD")
		}
		c.sessionState = receiverSessionReady
	}
	return receiverResponse{status: 200, headers: map[string]string{"Audio-Latency": "22050"}}
}

func (c *receiverConnection) invalidSessionState(operation string) receiverResponse {
	return receiverError(455, fmt.Errorf("%s is not valid in receiver session state %d", operation, c.sessionState))
}

func receiverSetupStreams(setup map[string]any) ([]map[string]any, error) {
	raw, ok := setup["streams"]
	if !ok {
		return nil, nil
	}
	values, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("SETUP streams is not an array")
	}
	streams := make([]map[string]any, 0, len(values))
	for _, value := range values {
		stream, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("SETUP stream is not a dictionary")
		}
		streams = append(streams, stream)
	}
	return streams, nil
}

func (c *receiverConnection) setupPlistResponse(value map[string]any) receiverResponse {
	body, err := plist.Marshal(value, plist.BinaryFormat)
	if err != nil {
		return receiverError(500, err)
	}
	return receiverResponse{
		status: 200, contentType: "application/x-apple-binary-plist", body: body,
		headers: c.server.clockHeaders(),
	}
}

func (c *receiverConnection) ensureMedia() error {
	if c.media != nil {
		return nil
	}
	keys, _ := c.pairing.sessionKeys()
	media, err := newReceiverMediaSession(c.server.ctx, receiverMediaConfig{
		BindIP:            controlLocalIP(c.conn),
		EventEncrypted:    c.hap != nil,
		EventSharedSecret: keys.sharedSecret,
		MaxVideoPayload:   32 * 1024 * 1024,
	})
	if err != nil {
		return fmt.Errorf("start receiver media endpoints: %w", err)
	}
	c.media = media
	c.server.mediaMu.Lock()
	c.server.activeMedia[media] = struct{}{}
	c.server.mediaMu.Unlock()
	return nil
}

func controlLocalIP(conn net.Conn) string {
	if address, ok := conn.LocalAddr().(*net.TCPAddr); ok && address.IP != nil {
		return address.IP.String()
	}
	return "127.0.0.1"
}

func (c *receiverConnection) probeLegacyTiming(setup map[string]any) {
	c.timingProbed = true
	port := plistInt(setup["timingPort"])
	remote, ok := c.conn.RemoteAddr().(*net.TCPAddr)
	if port <= 0 || !ok {
		c.server.logf("legacy SETUP omitted a usable sender timing port")
		return
	}
	ctx, cancel := context.WithTimeout(c.server.ctx, 2*time.Second)
	defer cancel()
	if err := c.media.ProbeLegacyTiming(ctx, &net.UDPAddr{IP: remote.IP, Port: port}, 3); err != nil {
		c.server.logf("legacy timing probe failed: %v", err)
	}
}

func (c *receiverConnection) closeMedia() {
	c.timingProbed = false
	if c.media == nil {
		return
	}
	media := c.media
	c.media = nil
	_ = media.Close()
	snapshot := media.Snapshot()
	c.server.mediaMu.Lock()
	if _, active := c.server.activeMedia[media]; active {
		delete(c.server.activeMedia, media)
		c.server.mediaTotals.add(snapshot)
	}
	c.server.mediaMu.Unlock()
}

func (s *ReceiverServer) clockHeaders() map[string]string {
	millis := time.Since(s.startedAt).Milliseconds()
	if millis < 1 {
		millis = 1
	}
	return map[string]string{
		"X-Apple-RequestReceivedTimestamp": strconv.FormatInt(millis, 10),
		"X-Apple-ProcessingTime":           "1",
	}
}

func (c *receiverConnection) writeResponse(request receiverRequest, response receiverResponse) error {
	if response.status == 0 {
		response.status = 200
	}
	var out strings.Builder
	fmt.Fprintf(&out, "RTSP/1.0 %d %s\r\n", response.status, receiverStatusText(response.status))
	if cseq := request.headers["cseq"]; cseq != "" {
		fmt.Fprintf(&out, "CSeq: %s\r\n", cseq)
	}
	out.WriteString("Server: doubletake-test-receiver/1\r\n")
	for name, value := range response.headers {
		fmt.Fprintf(&out, "%s: %s\r\n", name, value)
	}
	if response.contentType != "" && len(response.body) > 0 {
		fmt.Fprintf(&out, "Content-Type: %s\r\n", response.contentType)
	}
	fmt.Fprintf(&out, "Content-Length: %d\r\n\r\n", len(response.body))
	data := append([]byte(out.String()), response.body...)
	var writer io.Writer = c.conn
	if c.hap != nil {
		writer = c.hap
	}
	if err := writeAll(writer, data); err != nil {
		return fmt.Errorf("write receiver response: %w", err)
	}
	c.server.logf("-> %d for %s %s (body=%d encrypted=%t)", response.status, request.method, request.uri, len(response.body), c.hap != nil)
	return nil
}

func receiverStatusText(status int) string {
	switch status {
	case 200:
		return "OK"
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 404:
		return "Not Found"
	case 453:
		return "Not Enough Bandwidth"
	case 455:
		return "Method Not Valid in This State"
	case 500:
		return "Internal Server Error"
	default:
		return "Error"
	}
}

func (c *receiverConnection) enableEncryption(keys receiverPairingSessionKeys) error {
	hap, err := newReceiverHAPStream(c.conn, keys.readKey, keys.writeKey)
	if err != nil {
		return err
	}
	c.hap = hap
	c.reader = bufio.NewReaderSize(hap, receiverHeaderLimit)
	c.server.logf("control encryption enabled for %s", c.conn.RemoteAddr())
	return nil
}

type receiverHAPStream struct {
	conn        net.Conn
	readCipher  cipher.AEAD
	writeCipher cipher.AEAD
	readNonce   uint64
	writeNonce  uint64
	readBuf     []byte
	writeMu     sync.Mutex
}

func newReceiverHAPStream(conn net.Conn, readKey, writeKey []byte) (*receiverHAPStream, error) {
	if len(readKey) != chacha20poly1305.KeySize || len(writeKey) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("invalid receiver HAP control keys")
	}
	readCipher, err := chacha20poly1305.New(readKey)
	if err != nil {
		return nil, err
	}
	writeCipher, err := chacha20poly1305.New(writeKey)
	if err != nil {
		return nil, err
	}
	return &receiverHAPStream{conn: conn, readCipher: readCipher, writeCipher: writeCipher}, nil
}

func (s *receiverHAPStream) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	if len(s.readBuf) == 0 {
		var sizeBytes [2]byte
		if _, err := io.ReadFull(s.conn, sizeBytes[:]); err != nil {
			return 0, err
		}
		size := int(binary.LittleEndian.Uint16(sizeBytes[:]))
		if size < 1 || size > 1024 {
			return 0, fmt.Errorf("invalid HAP control frame size %d", size)
		}
		sealed := make([]byte, size+s.readCipher.Overhead())
		if _, err := io.ReadFull(s.conn, sealed); err != nil {
			return 0, err
		}
		plain, err := s.readCipher.Open(nil, nonceBytes(s.readNonce), sealed, sizeBytes[:])
		if err != nil {
			return 0, fmt.Errorf("decrypt HAP control frame %d: %w", s.readNonce, err)
		}
		s.readNonce++
		s.readBuf = plain
	}
	n := copy(dst, s.readBuf)
	s.readBuf = s.readBuf[n:]
	return n, nil
}

func (s *receiverHAPStream) Write(plain []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	total := len(plain)
	for len(plain) > 0 {
		chunk := plain
		if len(chunk) > 1024 {
			chunk = chunk[:1024]
		}
		var sizeBytes [2]byte
		binary.LittleEndian.PutUint16(sizeBytes[:], uint16(len(chunk)))
		frame := append([]byte(nil), sizeBytes[:]...)
		frame = s.writeCipher.Seal(frame, nonceBytes(s.writeNonce), chunk, sizeBytes[:])
		if err := writeAll(s.conn, frame); err != nil {
			return total - len(plain), err
		}
		s.writeNonce++
		plain = plain[len(chunk):]
	}
	return total, nil
}

// Stats returns totals including currently active media sessions.
func (s *ReceiverServer) Stats() ReceiverStats {
	stats := ReceiverStats{
		Connections:       s.stats.connections.Load(),
		InfoRequests:      s.stats.infoRequests.Load(),
		PINStarts:         s.stats.pinStarts.Load(),
		PairSetup:         s.stats.pairSetup.Load(),
		PairVerify:        s.stats.pairVerify.Load(),
		FairPlayRequests:  s.stats.fairPlayRequests.Load(),
		DigestChallenges:  s.stats.digestChallenges.Load(),
		SetupRequests:     s.stats.setupRequests.Load(),
		RecordRequests:    s.stats.recordRequests.Load(),
		ParameterRequests: s.stats.parameterRequests.Load(),
		FeedbackRequests:  s.stats.feedbackRequests.Load(),
		TeardownRequests:  s.stats.teardownRequests.Load(),
	}
	s.mediaMu.Lock()
	media := s.mediaTotals
	for session := range s.activeMedia {
		media.add(session.Snapshot())
	}
	s.mediaMu.Unlock()
	stats.EventConnections = media.EventConnections
	stats.VideoConnections = media.VideoConnections
	stats.VideoPackets = media.VideoPackets
	stats.VideoBytes = media.VideoBytes
	stats.AudioPackets = media.AudioPackets
	stats.AudioBytes = media.AudioBytes
	stats.TimingProbes = media.TimingProbes
	stats.TimingReplies = media.TimingReplies
	return stats
}
