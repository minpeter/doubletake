package airplay

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/godbus/dbus/v5"
)

func TestValidateHWAccel(t *testing.T) {
	for _, method := range []string{"", "auto", "nvenc", "vaapi", "openh264", "none"} {
		if err := ValidateHWAccel(method); err != nil {
			t.Errorf("ValidateHWAccel(%q): %v", method, err)
		}
	}
	for _, method := range []string{"x264", "OPENH264", "bogus", " auto"} {
		err := ValidateHWAccel(method)
		if err == nil {
			t.Errorf("ValidateHWAccel(%q) succeeded", method)
		} else if !strings.Contains(err.Error(), method) {
			t.Errorf("ValidateHWAccel(%q) error %q does not name the invalid value", method, err)
		}
	}
}

func TestStartTestCaptureRejectsUnknownHWAccel(t *testing.T) {
	capture, err := StartTestCapture(context.Background(), CaptureConfig{HWAccel: "bogus"})
	if err == nil {
		if capture != nil {
			capture.Stop()
		}
		t.Fatal("StartTestCapture accepted an unknown hwaccel value")
	}
	if !strings.Contains(err.Error(), "unknown H.264 encoder") {
		t.Fatalf("StartTestCapture error = %q", err)
	}
}

func TestRecommendedBitrateKbps(t *testing.T) {
	tests := []struct {
		name   string
		width  int
		height int
		fps    int
		want   int
	}{
		{
			name:   "defaults when dimensions invalid",
			width:  0,
			height: 1080,
			fps:    30,
			want:   defaultVideoBitrateKbps,
		},
		{
			name:   "low resolution clamps to floor",
			width:  640,
			height: 360,
			fps:    30,
			want:   minVideoBitrateKbps,
		},
		{
			name:   "720p30 stays near wifi target",
			width:  1280,
			height: 720,
			fps:    30,
			want:   1843,
		},
		{
			name:   "1080p30 uses wifi friendly auto bitrate",
			width:  1920,
			height: 1080,
			fps:    30,
			want:   4147,
		},
		{
			name:   "high resolutions clamp to max",
			width:  3840,
			height: 2160,
			fps:    60,
			want:   maxVideoBitrateKbps,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := recommendedBitrateKbps(tt.width, tt.height, tt.fps); got != tt.want {
				t.Fatalf("recommendedBitrateKbps(%d, %d, %d) = %d, want %d", tt.width, tt.height, tt.fps, got, tt.want)
			}
		})
	}
}

func TestKeyframeIntervalFrames(t *testing.T) {
	if got := keyframeIntervalFrames(30); got != 120 {
		t.Fatalf("keyframeIntervalFrames(30) = %d, want 120", got)
	}
	if got := keyframeIntervalFrames(0); got != 120 {
		t.Fatalf("keyframeIntervalFrames(0) = %d, want 120", got)
	}
}

func TestFrameIntervalMillis(t *testing.T) {
	for _, tt := range []struct {
		fps  int
		want int
	}{
		{fps: 30, want: 33},
		{fps: 60, want: 16},
		{fps: 0, want: 33},
		{fps: 2000, want: 1},
	} {
		if got := frameIntervalMillis(tt.fps); got != tt.want {
			t.Errorf("frameIntervalMillis(%d) = %d, want %d", tt.fps, got, tt.want)
		}
	}
}

func TestPortalStreamDimensions(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value interface{}
		wantW int
		wantH int
		want  bool
	}{
		{name: "signed tuple", value: []int32{1920, 1080}, wantW: 1920, wantH: 1080, want: true},
		{name: "unsigned tuple", value: []uint32{2560, 1440}, wantW: 2560, wantH: 1440, want: true},
		{name: "variant struct", value: []interface{}{int32(3840), int32(2160)}, wantW: 3840, wantH: 2160, want: true},
		{name: "invalid", value: []int32{1920}, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			width, height, ok := portalStreamDimensions(map[string]dbus.Variant{"size": dbus.MakeVariant(tt.value)})
			if width != tt.wantW || height != tt.wantH || ok != tt.want {
				t.Fatalf("portalStreamDimensions() = (%d, %d, %t), want (%d, %d, %t)", width, height, ok, tt.wantW, tt.wantH, tt.want)
			}
		})
	}
}

func TestVbvBufferKbit(t *testing.T) {
	tests := []struct {
		name    string
		bitrate int
		fps     int
		want    int
	}{
		{"invalid returns default", 0, 30, 300},
		{"low bitrate clamps to floor", 1800, 30, 200},
		{"1080p30 auto bitrate", 4147, 30, 276},
		{"high bitrate 60fps", 12000, 60, 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := vbvBufferKbit(tt.bitrate, tt.fps); got != tt.want {
				t.Fatalf("vbvBufferKbit(%d, %d) = %d, want %d", tt.bitrate, tt.fps, got, tt.want)
			}
		})
	}
}

func TestBuildGstVideoPipeline(t *testing.T) {
	encoder := encoderResult{
		parts:     gstStage{"testh264enc", "bitrate=2500"},
		rawFormat: "I420",
	}
	got := buildGstVideoPipeline(
		gstStage{"testsrc", "is-live=true"},
		[]gstStage{{"sourcefilter", "mode=test"}},
		[]gstStage{{"videorate", "drop-only=true"}, frameRateStage(30), lowLatencyVideoQueueStage()},
		encoder,
	)
	want := []string{
		"--quiet", "testsrc", "is-live=true",
		"!", "sourcefilter", "mode=test",
		"!", "videoconvert",
		"!", "video/x-raw,format=I420",
		"!", "videorate", "drop-only=true",
		"!", "video/x-raw,framerate=30/1",
		"!", "queue", "max-size-buffers=1", "max-size-bytes=0", "max-size-time=0", "leaky=downstream",
		"!", "testh264enc", "bitrate=2500",
		"!", "h264parse", "config-interval=-1",
		"!", "video/x-h264,stream-format=byte-stream,alignment=au",
		"!", "fdsink", "fd=1", "sync=false", "async=false",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GStreamer pipeline = %v, want %v", got, want)
	}
}

func TestBuildGstVideoPipelineSharesEncodingSuffix(t *testing.T) {
	encoder := encoderResult{
		parts:       gstStage{"vulkanh264enc", "bitrate=2500"},
		needsVulkan: true,
		rawFormat:   "NV12",
	}
	wayland := buildGstVideoPipeline(
		gstStage{"pipewiresrc", "path=42"},
		[]gstStage{{"vapostproc"}},
		[]gstStage{{"videorate"}, frameRateStage(30), lowLatencyVideoQueueStage()},
		encoder,
	)
	x11 := buildGstVideoPipeline(
		gstStage{"ximagesrc", "display-name=:0"},
		[]gstStage{frameRateStage(30), lowLatencyVideoQueueStage()},
		nil,
		encoder,
	)
	wantSuffix := []string{
		"!", "vulkanupload",
		"!", "vulkanh264enc", "bitrate=2500",
		"!", "h264parse", "config-interval=-1",
		"!", "video/x-h264,stream-format=byte-stream,alignment=au",
		"!", "fdsink", "fd=1", "sync=false", "async=false",
	}
	for name, pipeline := range map[string][]string{"Wayland": wayland, "X11": x11} {
		got := pipeline[len(pipeline)-len(wantSuffix):]
		if !reflect.DeepEqual(got, wantSuffix) {
			t.Errorf("%s encoding suffix = %v, want %v", name, got, wantSuffix)
		}
	}
}

func TestDetectGstEncoderSelectsExplicitOpenH264(t *testing.T) {
	var probes []string
	encoder, err := detectGstEncoderWithProbe(CaptureConfig{
		FPS:     25,
		Bitrate: 2500,
		HWAccel: "openh264",
	}, func(name string) bool {
		probes = append(probes, name)
		return name == "openh264enc"
	})
	if err != nil {
		t.Fatalf("detectGstEncoderWithProbe: %v", err)
	}

	if !reflect.DeepEqual(probes, []string{"openh264enc"}) {
		t.Fatalf("encoder probes = %v, want only openh264enc", probes)
	}
	if encoder.rawFormat != "I420" {
		t.Fatalf("OpenH264 raw format = %q, want I420", encoder.rawFormat)
	}
	if encoder.needsVulkan {
		t.Fatal("OpenH264 unexpectedly requires Vulkan upload")
	}
	wantParts := gstStage{
		"openh264enc",
		"bitrate=2500000",
		"gop-size=100",
		"rate-control=bitrate",
		"usage-type=screen",
	}
	if !reflect.DeepEqual(encoder.parts, wantParts) {
		t.Fatalf("OpenH264 pipeline = %v, want %v", encoder.parts, wantParts)
	}
}

func TestDetectGstEncoderRejectsMissingExplicitOpenH264(t *testing.T) {
	var probes []string
	_, err := detectGstEncoderWithProbe(CaptureConfig{
		FPS:     30,
		Bitrate: 2500,
		HWAccel: "openh264",
	}, func(name string) bool {
		probes = append(probes, name)
		return false
	})

	if !reflect.DeepEqual(probes, []string{"openh264enc"}) {
		t.Fatalf("encoder probes = %v, want only openh264enc", probes)
	}
	if err == nil {
		t.Fatal("missing explicit OpenH264 encoder did not return an error")
	}
	if !strings.Contains(err.Error(), "-hwaccel openh264") || !strings.Contains(err.Error(), "openh264enc") {
		t.Fatalf("missing OpenH264 error = %q", err)
	}
}

func TestDetectGstEncoderSelectionContract(t *testing.T) {
	allProbes := []string{"vulkanh264enc", "nvh264enc", "vah264enc", "openh264enc", "x264enc"}
	tests := []struct {
		name        string
		method      string
		available   map[string]bool
		wantEncoder string
		wantProbes  []string
		wantError   string
	}{
		{
			name:        "auto falls through to OpenH264",
			method:      "auto",
			available:   map[string]bool{"openh264enc": true, "x264enc": true},
			wantEncoder: "openh264enc",
			wantProbes:  allProbes[:4],
		},
		{
			name:        "empty aliases auto and reaches x264",
			available:   map[string]bool{"x264enc": true},
			wantEncoder: "x264enc",
			wantProbes:  allProbes,
		},
		{
			name:       "auto errors when no encoder exists",
			method:     "auto",
			wantProbes: allProbes,
			wantError:  "no supported GStreamer H.264 encoder",
		},
		{
			name:        "nvenc accepts legacy NVENC only",
			method:      "nvenc",
			available:   map[string]bool{"nvh264enc": true, "openh264enc": true},
			wantEncoder: "nvh264enc",
			wantProbes:  []string{"vulkanh264enc", "nvh264enc"},
		},
		{
			name:       "missing nvenc does not cross fallback",
			method:     "nvenc",
			available:  map[string]bool{"vah264enc": true, "openh264enc": true, "x264enc": true},
			wantProbes: []string{"vulkanh264enc", "nvh264enc"},
			wantError:  "-hwaccel nvenc",
		},
		{
			name:        "vaapi selects only VAAPI",
			method:      "vaapi",
			available:   map[string]bool{"vah264enc": true, "openh264enc": true},
			wantEncoder: "vah264enc",
			wantProbes:  []string{"vah264enc"},
		},
		{
			name:       "missing vaapi does not cross fallback",
			method:     "vaapi",
			available:  map[string]bool{"openh264enc": true, "x264enc": true},
			wantProbes: []string{"vah264enc"},
			wantError:  "vah264enc",
		},
		{
			name:        "none forces x264",
			method:      "none",
			available:   map[string]bool{"openh264enc": true, "x264enc": true},
			wantEncoder: "x264enc",
			wantProbes:  []string{"x264enc"},
		},
		{
			name:       "none errors without x264",
			method:     "none",
			available:  map[string]bool{"openh264enc": true},
			wantProbes: []string{"x264enc"},
			wantError:  "x264enc",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var probes []string
			encoder, err := detectGstEncoderWithProbe(CaptureConfig{Bitrate: 2500, HWAccel: test.method}, func(name string) bool {
				probes = append(probes, name)
				return test.available[name]
			})
			if !reflect.DeepEqual(probes, test.wantProbes) {
				t.Fatalf("encoder probes = %v, want %v", probes, test.wantProbes)
			}
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("encoder error = %v, want error containing %q", err, test.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("detectGstEncoderWithProbe: %v", err)
			}
			if len(encoder.parts) == 0 || encoder.parts[0] != test.wantEncoder {
				t.Fatalf("encoder = %#v, want %s", encoder, test.wantEncoder)
			}
		})
	}
}
