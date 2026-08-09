package daemon

import (
	"strings"
	"testing"

	"doubletake/internal/airplay"
)

func TestNewRejectsUnknownHWAccel(t *testing.T) {
	daemon, err := New(Config{HWAccel: "bogus"})
	if err == nil {
		if daemon != nil {
			t.Fatal("New returned a daemon for an unknown hwaccel value")
		}
		t.Fatal("New accepted an unknown hwaccel value")
	}
	if !strings.Contains(err.Error(), `unknown H.264 encoder "bogus"`) {
		t.Fatalf("New error = %q", err)
	}
}

func TestMirrorStreamConfigIncludesDaemonPortRange(t *testing.T) {
	d := &Daemon{cfg: Config{
		FPS:       60,
		Bitrate:   8000,
		PortMin:   60000,
		PortMax:   60010,
		NoEncrypt: true,
		DirectKey: true,
		NoAudio:   true,
	}}

	want := airplay.StreamConfig{
		FPS:       60,
		Bitrate:   8000,
		PortMin:   60000,
		PortMax:   60010,
		NoEncrypt: true,
		DirectKey: true,
		NoAudio:   true,
	}
	if got := d.mirrorStreamConfig(); got != want {
		t.Fatalf("mirror stream config = %+v, want %+v", got, want)
	}
}

func TestValidatePortRange(t *testing.T) {
	for _, test := range []struct {
		name       string
		portMin    int
		portMax    int
		wantErrSub string
	}{
		{name: "ephemeral"},
		{name: "three ports", portMin: 60000, portMax: 60002},
		{name: "larger range", portMin: 60000, portMax: 60010},
		{name: "minimum missing", portMax: 60010, wantErrSub: "out of bounds"},
		{name: "maximum missing", portMin: 60000, wantErrSub: "out of bounds"},
		{name: "reversed", portMin: 60010, portMax: 60000, wantErrSub: "out of bounds"},
		{name: "maximum too high", portMin: 60000, portMax: 65536, wantErrSub: "out of bounds"},
		{name: "too small", portMin: 60000, portMax: 60001, wantErrSub: "too small"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validatePortRange(test.portMin, test.portMax)
			if test.wantErrSub == "" {
				if err != nil {
					t.Fatalf("validatePortRange(%d, %d): %v", test.portMin, test.portMax, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErrSub) {
				t.Fatalf("validatePortRange(%d, %d) error = %v, want substring %q", test.portMin, test.portMax, err, test.wantErrSub)
			}
		})
	}
}
