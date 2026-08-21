package airplay

import (
	"errors"
	"io"
	"sync"
	"time"
)

const (
	// A video socket can legitimately spend up to two seconds in Write. At the
	// maximum supported 12 Mbit/s bitrate that is about 3 MB of encoded video.
	// Eight MiB leaves ample room for keyframe bursts and scheduler jitter while
	// still putting a firm per-receiver bound on queued payload data.
	broadcastSinkQueueBytes = 8 << 20

	// The byte limit is normally reached first. This second bound prevents a
	// pathological source that returns tiny reads from consuming unbounded slice
	// metadata; it is deliberately generous for normal H.264 buffer cadence.
	broadcastSinkQueueChunks = 4096

	// Source shutdown normally races with consumers draining their last few
	// buffers. Do not let a receiver that stopped reading keep Run alive forever.
	broadcastSinkDrainTimeout = 2 * time.Second
)

var errBroadcastSinkBacklog = errors.New("broadcast sink backlog limit exceeded")

// BroadcastCapture reads from a single ScreenCapture and fans the raw byte
// stream out to multiple registered sinks. Each sink has an independent,
// bounded queue, so a stalled receiver cannot stop capture or its peers.
//
// Usage:
//
//	bc := NewBroadcastCapture(underlying)
//	go bc.Run()          // pumps bytes from the underlying capture
//	sink1 := bc.AddSink()
//	sink2 := bc.AddSink()
//	go session1.StreamFrames(ctx, sink1.AsCapture(), 0)
//	go session2.StreamFrames(ctx, sink2.AsCapture(), 0)
type BroadcastCapture struct {
	src     *ScreenCapture
	mu      sync.Mutex
	done    chan struct{}
	err     error // set before done is closed
	stopped bool
	// sequence is reserved before each source Read. A late sink starts at the
	// following sequence, which gives attachment an exact cutover even when a
	// source read has completed but has not yet been fanned out.
	sequence uint64

	drainTimeout time.Duration

	sinks []*BroadcastSink
}

// BroadcastSink is a reader end of a BroadcastCapture. It satisfies the same
// Read interface as ScreenCapture and can be wrapped into a ScreenCapture-like
// value via AsCapture().
type BroadcastSink struct {
	owner *BroadcastCapture

	mu   sync.Mutex
	cond *sync.Cond

	queue       [][]byte
	headOffset  int
	queuedBytes int

	maxQueuedBytes  int
	maxQueuedChunks int

	inputClosed   bool // the source ended; drain queue, then return EOF
	closed        bool // explicitly removed; discard queue and return EOF
	doneClosed    bool
	done          chan struct{}
	startSequence uint64
}

func newBroadcastSink(owner *BroadcastCapture) *BroadcastSink {
	s := &BroadcastSink{
		owner:           owner,
		maxQueuedBytes:  broadcastSinkQueueBytes,
		maxQueuedChunks: broadcastSinkQueueChunks,
		done:            make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// NewBroadcastCapture wraps src. Sinks may be added before or while Run is
// active; running without sinks simply drains the encoder until a session is
// ready to consume frames.
func NewBroadcastCapture(src *ScreenCapture) *BroadcastCapture {
	return &BroadcastCapture{
		src:          src,
		done:         make(chan struct{}),
		drainTimeout: broadcastSinkDrainTimeout,
	}
}

// AddSink registers a new fan-out reader before or while Run is active.
func (bc *BroadcastCapture) AddSink() *BroadcastSink {
	s := newBroadcastSink(bc)
	bc.mu.Lock()
	if bc.stopped {
		bc.mu.Unlock()
		s.finish()
		return s
	}
	s.startSequence = bc.sequence + 1
	bc.sinks = append(bc.sinks, s)
	bc.mu.Unlock()
	return s
}

// RemoveSink closes and removes a sink so it no longer receives data. Any
// blocked Read is released immediately. It is safe to call concurrently with
// Run or more than once.
func (bc *BroadcastCapture) RemoveSink(s *BroadcastSink) {
	if s == nil {
		return
	}
	s.abort()

	bc.mu.Lock()
	defer bc.mu.Unlock()
	for i, current := range bc.sinks {
		if current == s {
			bc.sinks = append(bc.sinks[:i], bc.sinks[i+1:]...)
			return
		}
	}
}

// Run pumps data from the underlying ScreenCapture to all registered sinks.
// It returns when the capture ends and every active sink has consumed the data
// already queued for it, or after the bounded drain grace period. The caller
// should run this in a dedicated goroutine; an empty sink list is intentionally
// not terminal.
func (bc *BroadcastCapture) Run() error {
	buf := make([]byte, 256*1024)
	for {
		// Reserve this read's sequence before blocking in the source. A sink added
		// at any point after this reservation must begin with the next read, not
		// with bytes that may have been captured before its session was ready.
		bc.mu.Lock()
		bc.sequence++
		sequence := bc.sequence
		bc.mu.Unlock()

		n, readErr := bc.src.Read(buf)
		if n > 0 {
			bc.mu.Lock()
			sinks := make([]*BroadcastSink, 0, len(bc.sinks))
			for _, sink := range bc.sinks {
				if sink.startSequence <= sequence {
					sinks = append(sinks, sink)
				}
			}
			bc.mu.Unlock()

			if len(sinks) > 0 {
				// The payload is immutable after this point and can therefore be
				// shared by all sink queues instead of copied once per receiver.
				chunk := append([]byte(nil), buf[:n]...)
				for _, s := range sinks {
					if err := s.enqueue(chunk); err != nil {
						// A receiver that accumulates more than the bounded
						// backlog is detached without delaying healthy peers.
						bc.RemoveSink(s)
					}
				}
			}
		}
		if readErr != nil {
			bc.finish(readErr)
			return readErr
		}
	}
}

// finish stops accepting sinks, lets existing sinks drain, and only then
// publishes BroadcastCapture completion.
func (bc *BroadcastCapture) finish(err error) {
	bc.mu.Lock()
	bc.err = err
	bc.stopped = true
	sinks := append([]*BroadcastSink(nil), bc.sinks...)
	bc.sinks = nil
	bc.mu.Unlock()

	for _, s := range sinks {
		s.finish()
	}
	timer := time.NewTimer(bc.drainTimeout)
	defer timer.Stop()
	for i, s := range sinks {
		select {
		case <-s.done:
		case <-timer.C:
			// The grace period is shared by every sink, so shutdown is bounded
			// regardless of receiver count. Aborting an already-drained sink is
			// harmless and avoids a second bookkeeping pass.
			for _, pending := range sinks[i:] {
				pending.abort()
			}
			close(bc.done)
			return
		}
	}
	close(bc.done)
}

// Done returns a channel that is closed after Run has finished and all queued
// sink data has either been consumed, explicitly discarded by closing the
// sink, or discarded when the bounded drain grace period expires.
func (bc *BroadcastCapture) Done() <-chan struct{} {
	return bc.done
}

// Err returns the error that caused Run to exit (nil if still running).
func (bc *BroadcastCapture) Err() error {
	select {
	case <-bc.done:
		return bc.err
	default:
		return nil
	}
}

// Source returns the underlying ScreenCapture.
func (bc *BroadcastCapture) Source() *ScreenCapture {
	return bc.src
}

// --- BroadcastSink ---

// enqueue appends immutable capture data without waiting for the receiver. A
// full queue is an error so BroadcastCapture can detach only that receiver.
func (s *BroadcastSink) enqueue(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.inputClosed {
		return io.ErrClosedPipe
	}
	if len(s.queue) >= s.maxQueuedChunks || len(p) > s.maxQueuedBytes-s.queuedBytes {
		return errBroadcastSinkBacklog
	}
	s.queue = append(s.queue, p)
	s.queuedBytes += len(p)
	s.cond.Signal()
	return nil
}

// finish marks source EOF without discarding data already queued.
func (s *BroadcastSink) finish() {
	s.mu.Lock()
	if !s.closed && !s.inputClosed {
		s.inputClosed = true
		if len(s.queue) == 0 {
			s.closeDoneLocked()
		}
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

// abort discards queued data and releases a blocked reader immediately.
func (s *BroadcastSink) abort() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.inputClosed = true
		for i := range s.queue {
			s.queue[i] = nil
		}
		s.queue = nil
		s.headOffset = 0
		s.queuedBytes = 0
		s.closeDoneLocked()
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

func (s *BroadcastSink) closeDoneLocked() {
	if !s.doneClosed {
		s.doneClosed = true
		close(s.done)
	}
}

// Read implements io.Reader. It blocks until data arrives, the source ends, or
// the sink is explicitly closed.
func (s *BroadcastSink) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) == 0 && !s.inputClosed && !s.closed {
		s.cond.Wait()
	}
	if s.closed {
		return 0, io.EOF
	}
	if len(s.queue) == 0 {
		s.closeDoneLocked()
		return 0, io.EOF
	}

	chunk := s.queue[0]
	n := copy(p, chunk[s.headOffset:])
	s.headOffset += n
	s.queuedBytes -= n
	if s.headOffset == len(chunk) {
		s.queue[0] = nil
		s.queue = s.queue[1:]
		s.headOffset = 0
		if len(s.queue) == 0 {
			s.queue = nil
			if s.inputClosed {
				s.closeDoneLocked()
				return n, io.EOF
			}
		}
	}
	return n, nil
}

type broadcastSinkReadCloser struct {
	sink *BroadcastSink
}

func (r broadcastSinkReadCloser) Read(p []byte) (int, error) {
	return r.sink.Read(p)
}

func (r broadcastSinkReadCloser) Close() error {
	r.sink.Close()
	return nil
}

// AsCapture wraps this sink in a synthetic ScreenCapture so it can be passed
// directly to MirrorSession.StreamFrames.
func (s *BroadcastSink) AsCapture() *ScreenCapture {
	return &ScreenCapture{
		stdout: broadcastSinkReadCloser{sink: s},
		waitCh: s.done,
	}
}

// Close closes this sink, discarding queued data and signalling EOF to its
// reader.
func (s *BroadcastSink) Close() {
	if s.owner != nil {
		s.owner.RemoveSink(s)
		return
	}
	s.abort()
}
