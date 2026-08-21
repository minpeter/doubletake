package airplay

import (
	"math"
	"sync/atomic"
	"time"
)

const defaultTargetLatency = 1 * time.Millisecond

// ntpPlayoutLatencyFloor gives the older request/response clock path enough
// lead to schedule RTP packets despite network and userspace jitter. PTP uses a
// shared network timeline and retains the configured low-latency target. This
// is selected by the timing protocol actually negotiated during SETUP, not by
// an unrelated receiver feature or identity.
const ntpPlayoutLatencyFloor = 500 * time.Millisecond

var targetLatencyNS atomic.Int64

func init() {
	targetLatencyNS.Store(int64(defaultTargetLatency))
}

// SetTargetLatency sets the desired end-to-end playout latency target.
// Values are clamped to a sane operational range.
func SetTargetLatency(d time.Duration) {
	if d < 5*time.Millisecond {
		d = 5 * time.Millisecond
	}
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	targetLatencyNS.Store(int64(d))
}

// TargetLatency returns the configured playout latency target.
func TargetLatency() time.Duration {
	d := time.Duration(targetLatencyNS.Load())
	if d <= 0 {
		return defaultTargetLatency
	}
	return d
}

func targetLatencySamples44k1() uint32 {
	return samplesFor44k1(TargetLatency())
}

func samplesFor44k1(d time.Duration) uint32 {
	samples := int64(math.Round(float64(d) * 44100.0 / float64(time.Second)))
	if samples < 1 {
		samples = 1
	}
	if samples > math.MaxUint32 {
		samples = math.MaxUint32
	}
	return uint32(samples)
}
