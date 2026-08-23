package airplay

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

type sliceVideoAccessUnitReader struct {
	frames []VideoAccessUnit
	index  int
}

func (r *sliceVideoAccessUnitReader) ReadVideoAccessUnit() (VideoAccessUnit, error) {
	if r.index == len(r.frames) {
		return VideoAccessUnit{}, io.EOF
	}
	frame := r.frames[r.index]
	r.index++
	return frame, nil
}

func TestBroadcastCapturePreservesTimestampedAccessUnits(t *testing.T) {
	firstPTS := time.Now().Add(-200 * time.Millisecond)
	frames := []VideoAccessUnit{
		{AnnexB: []byte{0, 0, 0, 1, 0x65, 1}, PTS: firstPTS},
		{AnnexB: []byte{0, 0, 0, 1, 0x61, 2}, PTS: firstPTS.Add(time.Second / 30)},
	}
	capture := &ScreenCapture{
		frames: &sliceVideoAccessUnitReader{frames: frames},
		waitCh: make(chan struct{}),
	}
	broadcast := NewBroadcastCapture(capture)
	firstSink := broadcast.AddSink().AsCapture()
	secondSink := broadcast.AddSink().AsCapture()
	runDone := make(chan error, 1)
	go func() { runDone <- broadcast.Run() }()

	for i, want := range frames {
		for sinkIndex, sink := range []*ScreenCapture{firstSink, secondSink} {
			got, err := sink.ReadVideoAccessUnit()
			if err != nil && !(err == io.EOF && i == len(frames)-1) {
				t.Fatalf("sink %d frame %d: %v", sinkIndex, i, err)
			}
			if !bytes.Equal(got.AnnexB, want.AnnexB) || got.PTS != want.PTS {
				t.Fatalf("sink %d frame %d = {%x %v}, want {%x %v}", sinkIndex, i, got.AnnexB, got.PTS, want.AnnexB, want.PTS)
			}
		}
	}
	select {
	case err := <-runDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("broadcast run = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timestamped broadcast did not finish after both sinks drained")
	}
}

func TestBroadcastCaptureCanAttachSinkAfterRunStarts(t *testing.T) {
	sourceReader, sourceWriter := io.Pipe()
	capture := &ScreenCapture{stdout: sourceReader, waitCh: make(chan struct{})}
	broadcast := NewBroadcastCapture(capture)
	runDone := make(chan error, 1)
	go func() { runDone <- broadcast.Run() }()

	// The encoder must keep draining while a receiver is still negotiating
	// SETUP, without an unconsumed sink blocking established targets.
	drained := make(chan error, 1)
	go func() {
		_, err := sourceWriter.Write([]byte("before setup"))
		drained <- err
	}()
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("drain before sink: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast stopped draining while no sink was attached")
	}

	sink := broadcast.AddSink()
	defer sink.Close()
	// A source Read may already be in flight when the sink attaches. Complete
	// that boundary read, then use the following chunk as the deterministic
	// post-attachment marker. The boundary itself may fall on either side.
	boundary := []byte("attachment boundary")
	if _, err := sourceWriter.Write(boundary); err != nil {
		t.Fatalf("write attachment boundary: %v", err)
	}
	want := []byte("after setup")
	readDone := make(chan []byte, 1)
	markerSeen := make(chan struct{})
	go func() {
		var got []byte
		buf := make([]byte, 64)
		seen := false
		for {
			n, err := sink.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
				if !seen && bytes.Contains(got, want) {
					seen = true
					close(markerSeen)
				}
			}
			if err != nil {
				readDone <- got
				return
			}
		}
	}()
	if _, err := sourceWriter.Write(want); err != nil {
		t.Fatalf("write after sink attachment: %v", err)
	}
	select {
	case <-markerSeen:
	case <-time.After(time.Second):
		t.Fatal("late-attached sink did not receive capture data")
	}
	_ = sourceWriter.Close()
	select {
	case err := <-runDone:
		if err != nil && err != io.EOF {
			t.Fatalf("broadcast run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast did not stop at source EOF")
	}
	select {
	case got := <-readDone:
		if bytes.Contains(got, []byte("before setup")) {
			t.Fatalf("late-attached sink received stale pre-SETUP capture: %q", got)
		}
		if !bytes.Contains(got, want) {
			t.Fatalf("late-attached sink data = %q, want post-SETUP marker %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("late-attached sink did not close at source EOF")
	}
}

type fixedChunkReadCloser struct {
	data      []byte
	chunkSize int
	offset    int
	eof       chan struct{}
	eofSent   bool
}

func (r *fixedChunkReadCloser) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		if !r.eofSent {
			r.eofSent = true
			close(r.eof)
		}
		return 0, io.EOF
	}
	n := min(len(p), min(r.chunkSize, len(r.data)-r.offset))
	copy(p, r.data[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}

func (*fixedChunkReadCloser) Close() error { return nil }

func TestBroadcastCaptureBuffersSchedulerBurstAndDrainsBeforeDone(t *testing.T) {
	// Delay the consumer until every source read has completed. This creates a
	// deterministic scheduler burst that fits within the default chunk limit;
	// a healthy receiver must remain attached and receive it in order.
	want := make([]byte, broadcastSinkQueueChunks/2)
	for i := range want {
		want[i] = byte(i)
	}
	source := &fixedChunkReadCloser{
		data:      want,
		chunkSize: 1,
		eof:       make(chan struct{}),
	}
	capture := &ScreenCapture{stdout: source, waitCh: make(chan struct{})}
	broadcast := NewBroadcastCapture(capture)
	sink := broadcast.AddSink()
	runDone := make(chan error, 1)
	go func() { runDone <- broadcast.Run() }()

	select {
	case <-source.eof:
	case <-time.After(time.Second):
		t.Fatal("broadcast did not consume scheduler burst")
	}
	select {
	case <-broadcast.Done():
		t.Fatal("broadcast completed before its sink drained queued data")
	default:
	}

	got, err := io.ReadAll(sink)
	if err != nil {
		t.Fatalf("read queued burst: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("queued burst mismatch: got %d bytes, want %d", len(got), len(want))
	}
	select {
	case err := <-runDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("broadcast run = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast did not finish after sink drained")
	}
	select {
	case <-broadcast.Done():
	default:
		t.Fatal("Done remained open after queued data drained")
	}
}

func TestBroadcastCaptureSlowSinkDoesNotStallHealthySink(t *testing.T) {
	sourceReader, sourceWriter := io.Pipe()
	capture := &ScreenCapture{stdout: sourceReader, waitCh: make(chan struct{})}
	broadcast := NewBroadcastCapture(capture)
	slow := broadcast.AddSink()
	// Keep this test small while exercising the same production overflow path.
	slow.mu.Lock()
	slow.maxQueuedBytes = 128
	slow.maxQueuedChunks = 2
	slow.mu.Unlock()
	healthy := broadcast.AddSink()

	runDone := make(chan error, 1)
	go func() { runDone <- broadcast.Run() }()
	healthyDone := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, err := io.ReadAll(healthy)
		healthyDone <- struct {
			data []byte
			err  error
		}{data: data, err: err}
	}()

	want := make([]byte, 0, 64*32)
	writeDone := make(chan error, 1)
	go func() {
		for i := 0; i < 32; i++ {
			chunk := bytes.Repeat([]byte{byte(i)}, 64)
			want = append(want, chunk...)
			if _, err := sourceWriter.Write(chunk); err != nil {
				writeDone <- err
				return
			}
		}
		writeDone <- nil
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write source: %v", err)
		}
	case <-time.After(time.Second):
		slow.Close()
		_ = sourceWriter.Close()
		t.Fatal("slow sink stalled the source")
	}
	_ = sourceWriter.Close()

	select {
	case result := <-healthyDone:
		if result.err != nil {
			t.Fatalf("healthy sink read: %v", result.err)
		}
		if !bytes.Equal(result.data, want) {
			t.Fatalf("healthy sink got %d bytes, want %d", len(result.data), len(want))
		}
	case <-time.After(time.Second):
		t.Fatal("healthy sink did not finish")
	}
	select {
	case err := <-runDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("broadcast run = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("broadcast did not finish")
	}

	buf := make([]byte, 1)
	if n, err := slow.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("overflowed sink read = (%d, %v), want (0, EOF)", n, err)
	}
}

func TestBroadcastCaptureRemoveOrCloseUnblocksFanout(t *testing.T) {
	for _, tc := range []struct {
		name   string
		remove func(*BroadcastCapture, *BroadcastSink)
	}{
		{name: "RemoveSink", remove: func(b *BroadcastCapture, s *BroadcastSink) { b.RemoveSink(s) }},
		{name: "Close", remove: func(_ *BroadcastCapture, s *BroadcastSink) { s.Close() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceReader, sourceWriter := io.Pipe()
			capture := &ScreenCapture{stdout: sourceReader, waitCh: make(chan struct{})}
			broadcast := NewBroadcastCapture(capture)
			sink := broadcast.AddSink()
			runDone := make(chan error, 1)
			go func() { runDone <- broadcast.Run() }()

			firstDone := make(chan error, 1)
			go func() {
				_, err := sourceWriter.Write([]byte("first"))
				firstDone <- err
			}()
			select {
			case err := <-firstDone:
				if err != nil {
					t.Fatalf("first source write: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("first source write did not reach fanout")
			}

			secondDone := make(chan error, 1)
			go func() {
				_, err := sourceWriter.Write([]byte("second"))
				secondDone <- err
			}()
			tc.remove(broadcast, sink)
			select {
			case err := <-secondDone:
				if err != nil {
					t.Fatalf("source remained blocked after removal: %v", err)
				}
			case <-time.After(time.Second):
				_ = sourceWriter.Close()
				t.Fatal("removing sink did not promptly unblock fanout")
			}

			_ = sourceWriter.Close()
			select {
			case err := <-runDone:
				if !errors.Is(err, io.EOF) {
					t.Fatalf("broadcast run = %v, want EOF", err)
				}
			case <-time.After(time.Second):
				t.Fatal("broadcast did not finish")
			}
		})
	}
}

func TestBroadcastSinkCloseUnblocksRead(t *testing.T) {
	sink := newBroadcastSink(nil)
	readDone := make(chan error, 1)
	go func() {
		_, err := sink.Read(make([]byte, 1))
		readDone <- err
	}()
	sink.Close()
	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("read after Close = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Read")
	}
}

func TestBroadcastSinkCaptureStopReturnsPromptly(t *testing.T) {
	sink := newBroadcastSink(nil)
	capture := sink.AsCapture()
	stopped := make(chan struct{})
	go func() {
		capture.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stopping a sink capture remained blocked")
	}
}

func TestBroadcastCaptureSourceEOFHasBoundedDrain(t *testing.T) {
	capture := &ScreenCapture{
		stdout: io.NopCloser(bytes.NewReader([]byte("unconsumed"))),
		waitCh: make(chan struct{}),
	}
	broadcast := NewBroadcastCapture(capture)
	broadcast.drainTimeout = 10 * time.Millisecond
	sink := broadcast.AddSink()

	started := time.Now()
	if err := broadcast.Run(); !errors.Is(err, io.EOF) {
		t.Fatalf("broadcast run = %v, want EOF", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("source EOF took %v with an unconsumed sink", elapsed)
	}
	select {
	case <-broadcast.Done():
	default:
		t.Fatal("Done remained open after bounded drain period")
	}
	if n, err := sink.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("timed-out sink read = (%d, %v), want (0, EOF)", n, err)
	}
}

func TestBroadcastCaptureClosesSinkAddedAfterSourceStops(t *testing.T) {
	capture := &ScreenCapture{
		stdout: io.NopCloser(bytes.NewReader(nil)),
		waitCh: make(chan struct{}),
	}
	broadcast := NewBroadcastCapture(capture)
	if err := broadcast.Run(); err != io.EOF {
		t.Fatalf("broadcast run = %v, want EOF", err)
	}

	sink := broadcast.AddSink()
	defer sink.Close()
	readDone := make(chan error, 1)
	go func() {
		_, err := sink.Read(make([]byte, 1))
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("late sink read = %v, want a closed stream", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sink added after capture stop remained open")
	}
}
