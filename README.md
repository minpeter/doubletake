# doubletake

AirPlay screen mirroring sender for Linux. Streams your desktop to an Apple TV using the AirPlay mirroring protocol.

## Features

- Full AirPlay mirroring protocol (RTSP/HTTP + encrypted video stream)
- FairPlay SAP authentication (clean Go implementation)
- SRP-6a pairing with PIN and persistent credential storage
- Wayland (PipeWire/xdg-desktop-portal) and X11 screen capture
- H.264 encoding with NVENC, VA-API, OpenH264, and x264
- ChaCha20-Poly1305 stream encryption
- mDNS device discovery
- Daemon mode with multi-target streaming control (`doubletake-ctl`)
- In-process test receiver for hardware-free pairing and media-flow tests
- Configurable latency target (`-target-latency-ms`, default 100ms)
- KDE Plasma widget for quick access (see [plasmoid/](plasmoid/))

## Requirements

- Go 1.23+
- GStreamer 1.0 (with plugins-base, plugins-good, plugins-bad, plugins-ugly, libav)
- PulseAudio utilities (`pactl`; `pulseaudio-utils` on Ubuntu/Debian or the equivalent package on other distributions)
- PipeWire (Wayland) or X11 for screen capture

### Ubuntu/Debian

```sh
sudo apt install libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
  gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad \
  gstreamer1.0-plugins-ugly gstreamer1.0-libav pulseaudio-utils
```

### Arch Linux

```sh
sudo pacman -S gstreamer gst-plugins-base gst-plugins-good gst-plugins-bad \
  gst-plugins-ugly gst-libav libpulse
```

The `openh264` encoder requires the GStreamer `openh264enc` element, normally
provided by the plugins-bad package. Check availability with
`gst-inspect-1.0 openh264enc`.

You can also install from the AUR:

- [`doubletake`](https://aur.archlinux.org/packages/doubletake) (stable release package)
- [`doubletake-git`](https://aur.archlinux.org/packages/doubletake-git) (latest from git)
- [`doubletake-bin`](https://aur.archlinux.org/packages/doubletake-bin) (prebuilt binary package)

## Tested Devices

These are devices that have been tested with doubletake. If there are devices not listed here that you have confirmed working or non-functional, please open an issue.

- AppleTV3,2 (2013 3rd generation) (currently non-functional, see [#17](https://github.com/omarroth/doubletake/issues/17))
- AppleTV11,1 (4K, 2021 2nd gen)
- AppleTV14,1 (4K, 2022 3rd gen) + Homepod (1st gen)
- AppleTV14,1 (4K, 2022 3rd gen)
- Mac17,2 (MacBook Pro, M5 14")
- Mac16,10 (Mac mini, M4)
- Roku Streaming Stick 4K (3820R2)
- Samsung TV TU8300 Series 4K UHD
- Hisense 55A6QU
- Xiaomi 4K HDR TV (AFTBR92D74) (currently non-functional, see [#4](https://github.com/omarroth/doubletake/issues/4))

## Build

```sh
make
```

This builds the three binaries into `bin/`:

- `bin/doubletake`
- `bin/doubletake-ctl`
- `bin/doubletake-test-receiver`

## Install

Install binaries and man pages (default prefix: `/usr/local`):

```sh
sudo make install
```

Use a custom prefix if needed:

```sh
make install PREFIX=$HOME/.local
```

Uninstall:

```sh
sudo make uninstall
```

Run tests:

```sh
make test
```

## Firewall

doubletake reserves three consecutive UDP ports for timing and audio traffic.
Legacy NTP receivers probe the timing port during SETUP; modern PTP sessions do
not advertise it. The event and video data channels are outbound TCP connections
from doubletake to ports returned by the receiver, so they do not require inbound
firewall rules.

By default the OS assigns ephemeral ports. Use `-port-range MIN-MAX` to confine
the UDP ports to a small window you can open in your firewall (needs at least 3
ports):

```sh
doubletake -target 192.168.1.77 -port-range 60000-60010
```

Daemon mode uses the same range for every managed stream. Reserve at least
three available ports per receiver that may stream simultaneously.

Then with UFW:

```sh
sudo ufw allow from any proto udp to any port 60000:60010
```

For nftables/firewalld, add an equivalent rule allowing inbound UDP from the
Apple TV's address on the chosen range.

## Password-protected receivers

If the receiver has **Require Password** enabled (on an Apple TV: Settings →
AirPlay and HomeKit), it challenges the mirroring `SETUP` request with HTTP
Digest auth and mirroring fails with `HTTP 401` until doubletake answers it.
Pass the password with `-code`:

```sh
DOUBLETAKE_CODE='...' doubletake -target 192.168.1.77
```

`-code` carries whatever the receiver is asking for — the onscreen pairing PIN
during `-pair`, or the fixed password when "Require Password" is enabled.
`$DOUBLETAKE_CODE` is preferred over the flag: a command line is visible to
other users via `ps` and lands in shell history. The environment variable takes
precedence when both are set.

Pairing and Digest authentication remain separate wire protocols, but
doubletake deliberately uses one credential flow for both: a PIN/password
entered during pairing is retained for later Digest challenges and reconnects.
A receiver may consume that value during pairing, Digest auth, both, or neither.
Supplying `-code` does not by itself trigger re-pairing.

The CLI and Plasma applet prompt once after inspecting the receiver's security
mode. In the Plasma applet, an on-screen PIN uses a visible four-digit field,
while a configured password uses an unrestricted masked field. Doubletake does
not request or claim that a PIN is visible in password mode. The daemon exposes
the distinction while retaining `doubletake-ctl pin <PIN-or-password>` for
command compatibility.

One thing that makes this confusing to diagnose: **"Require Password" is a
fixed password you set, not a rotating onscreen code.** Nothing appears on the
TV during `-pair`, and the prompt is asking for that configured password.

Run with `-debug` to see the challenge and whether the retry was accepted.

## Hardware-free test receiver

`doubletake-test-receiver` is an in-repository AirPlay receiver for exercising
pairing, encrypted RTSP, SETUP ordering, timing, event channels, and sustained
audio/video traffic without an Apple TV or third-party receiver. It is a
diagnostic sink, not a media player or a receiver-compatibility substitute.

The `modern` profile advertises and exercises FairPlay SAP (FPSAP). The `roku`
profile deliberately omits FPSAP, matching that hardware protocol personality.
The receiver parses the outer AirPlay video framing and counts video and audio
traffic, but encrypted video and audio payloads are not authenticated,
decrypted, or decoded. Nothing is played or displayed; the counters demonstrate
transport flow rather than decoded media correctness.

Start a Roku-compatible receiver whose configured fixed password is required by
both SRP and Digest authentication. This mode does not display a PIN:

```sh
DOUBLETAKE_RECEIVER_CODE='aaaaaaaa' \
  bin/doubletake-test-receiver -profile roku -auth combined -debug
```

Then run the real sender against it from another terminal:

```sh
DOUBLETAKE_CODE='aaaaaaaa' \
  bin/doubletake -target 127.0.0.1 -port 7000 -pair -test
```

The receiver profiles are coherent protocol personalities:

| Profile | Pairing/control | FairPlay | Session setup | Timing |
|---------|-----------------|----------|---------------|--------|
| `roku` | HKP3; raw transient or code-authenticated HAP | deliberately omitted | legacy combined fields | NTP probes |
| `modern` | transient or code-authenticated HAP | FPSAP | control, audio, then video | PTP metadata |

Authentication modes all use the single `-code` value (or
`$DOUBLETAKE_RECEIVER_CODE`):

| Mode | Pair setup | RTSP Digest |
|------|------------|-------------|
| `none` | transient | no |
| `pin` | code required; advertises an on-screen PIN | no |
| `password` | fixed code required | no |
| `digest` | transient | code required |
| `combined` | fixed password required; no on-screen PIN | same password required |

Use `-listen 127.0.0.1:0` to choose an ephemeral port, and
`-stats-interval 1s` to print live counters. See
`doubletake-test-receiver(1)` for all options.

## Usage

```sh
# Discover Apple TVs on the network and stream
doubletake

# Disable audio for video-only mirroring
doubletake -no-audio

# Connect to a specific Apple TV
doubletake -target 192.168.1.77

# First-time pairing with PIN (saves credentials for reuse)
doubletake -target 192.168.1.77 -pair

# Use saved credentials
doubletake -target 192.168.1.77 -creds airplay-credentials.json

# Adjust stream settings (bitrate 0 = auto)
doubletake -target 192.168.1.77 -fps 30 -bitrate 0

# Force a lower bitrate on weaker Wi-Fi
doubletake -target 192.168.1.77 -bitrate 4500

# Set a target playout latency (default is 100ms)
doubletake -target 192.168.1.77 -target-latency-ms 100

# Hardware encoding
doubletake -target 192.168.1.77 -hwaccel nvenc   # NVIDIA
doubletake -target 192.168.1.77 -hwaccel vaapi   # Intel/AMD

# OpenH264 software encoding
doubletake -target 192.168.1.77 -hwaccel openh264

# Debug mode (verbose protocol logging)
doubletake -target 192.168.1.77 -debug

# Run daemon mode and control from a second shell
doubletake -daemonize -port-range 60000-60010
doubletake-ctl status
doubletake-ctl connect 192.168.1.77
doubletake-ctl connect 192.168.1.133
doubletake-ctl disconnect 192.168.1.77
doubletake-ctl disconnect
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-target` | | Apple TV IP (skip mDNS discovery) |
| `-port` | 7000 | AirPlay port |
| `-code` | | Pairing PIN shown on the receiver, or its configured password when "Require Password" is enabled (see [Password-protected receivers](#password-protected-receivers)); prefer `$DOUBLETAKE_CODE` |
| `-port-range` | | Local UDP port range for timing/audio (at least 3 ports) |
| `-cred-backend` | `file` | Credential backend (`file` or `keyring`) |
| `-creds` | `~/.config/doubletake/credentials.json` | Credentials file path |
| `-pair` | false | Force new pairing |
| `-fps` | 30 | Frames per second |
| `-bitrate` | 0 | Video bitrate in kbps (`0` = auto) |
| `-target-latency-ms` | 100 | Target end-to-end latency in milliseconds (audio + video timing) |
| `-hwaccel` | auto | H.264 encoder: `auto`, `nvenc`, `vaapi`, `openh264`, `none` |
| `-no-encrypt` | false | Disable RTSP header encryption (debugging only) |
| `-direct-key` | false | Use `shk`/`shiv` directly without SHA-512 derivation |
| `-no-audio` | false | Disable audio streaming |
| `-test` | false | Use synthetic video source |
| `-daemonize` | false | Run as background daemon with Unix socket control interface |
| `-socket` | `$XDG_RUNTIME_DIR/doubletake.sock` | Daemon control socket path |
| `-debug` | false | Verbose debug logging |

Only `-hwaccel auto` tries fallback encoders, in the order `vulkanh264enc`,
`nvh264enc`, `vah264enc`, `openh264enc`, then `x264enc`. Explicit selections
fail if their required GStreamer encoder is unavailable; `none` forces x264.

### Daemon Control (`doubletake-ctl`)

```sh
doubletake-ctl status
doubletake-ctl discover
doubletake-ctl devices
doubletake-ctl connect [target] [PIN-or-password]
doubletake-ctl pin <PIN-or-password>
doubletake-ctl disconnect [target]
doubletake-ctl mute [target]
doubletake-ctl unmute [target]
```

- `disconnect` without a target stops all active streams.
- `disconnect <target>` stops only that receiver.
- `mute`/`unmute` can operate globally or per target.
- `pin` retains its historical command name, but submits whichever credential
  the daemon requests: an on-screen PIN or a configured password.

## Disclaimer

The majority of code for this project was written by LLMs. I've read through the code to make sure there's nothing obviously stupid, but if you're in a production or security-sensitive environment and need to use AirPlay (for whatever reason), do not use this project.

Since I assume most of the code for this project was trained from [UxPlay](https://github.com/FDH2/UxPlay) and similar projects, I've provided this project under a similar license. Most of the reverse engineering work has already been done by many other people and this project would not be possible without them.

## License

This project is licensed under the [GNU Lesser General Public License v3.0 or later](LICENSE) (`LGPL-3.0-or-later`). See the LICENSE file for the LGPL terms and [COPYING.GPL](COPYING.GPL) for the incorporated GPLv3 terms.

Releases v0.3.2 and earlier were provided under the GNU General Public License v3.0 or later (`GPL-3.0-or-later`).
