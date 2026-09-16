> **This is a fork.** It is
> [Cinnamobot/librespot](https://github.com/Cinnamobot/librespot), a fork of
> [librespot-org/librespot](https://github.com/librespot-org/librespot) that
> adds the crossfade machinery
> [Fastpotify](https://github.com/Cinnamobot/fastpotify)'s automix needs.
> **For librespot itself — what it is, how to install and use it, its options,
> its audio backends, and its releases — read the upstream project's
> documentation.** Everything this fork adds is under
> [Cinnamobot/librespot — the `fastpotify-automix` branch](#cinnamobotlibrespot--the-fastpotify-automix-branch)
> below.

[![Build Status](https://github.com/librespot-org/librespot/workflows/build/badge.svg)](https://github.com/librespot-org/librespot/actions)
[![Gitter chat](https://badges.gitter.im/librespot-org/librespot.png)](https://gitter.im/librespot-org/spotify-connect-resources)
[![Crates.io](https://img.shields.io/crates/v/librespot.svg)](https://crates.io/crates/librespot)

Current maintainers are [listed on GitHub](https://github.com/orgs/librespot-org/people).

# librespot
*librespot* is an open source client library for Spotify. It enables applications to use Spotify's service to control and play music via various backends, and to act as a Spotify Connect receiver. It is an alternative to the official and [now deprecated](https://pyspotify.mopidy.com/en/latest/#libspotify-s-deprecation) closed-source `libspotify`. Additionally, it will provide extra features which are not available in the official library.

_Note: librespot only works with Spotify Premium. This will remain the case. We will not support any features to make librespot compatible with free accounts, such as limited skips and adverts._

## Quick start
We're available on [crates.io](https://crates.io/crates/librespot) as the _librespot_ package. Simply run `cargo install librespot` to install librespot on your system. Check the wiki for more info and possible [usage options](https://github.com/librespot-org/librespot/wiki/Options).

After installation, you can run librespot from the CLI using a command such as `librespot -n "Librespot Speaker" -b 160` to create a speaker called _Librespot Speaker_ serving 160 kbps audio.

## This fork
As the origin by [plietar](https://github.com/plietar/) is no longer actively maintained, this organisation and repository have been set up so that the project may be maintained and upgraded in the future.

## Cinnamobot/librespot — the `fastpotify-automix` branch

**This repository is a fork of
[librespot-org/librespot](https://github.com/librespot-org/librespot).** The
library, its crates, its audio backends, its documentation, and its release
process are the upstream project's work. Where anything below looks like it
describes librespot itself, the upstream README and wiki are the authority, and
this file does not restate them.

The branch `fastpotify-automix` carries what one client needs beyond the
release, for **[Fastpotify](https://github.com/Cinnamobot/fastpotify)**'s
automix: transitions between tracks that are planned rather than timed. It is
not intended to be merged as a whole, and it does not change default
behaviour — a host that never sets a crossfade plan gets exactly upstream
playback.

### What is not this fork's work

The base crossfade is a **cherry-pick of
[librespot-org/librespot#1756](https://github.com/librespot-org/librespot/pull/1756)**,
"feat(playback): crossfade between tracks" by
[@revolutionxk](https://github.com/revolutionxk), which is still open upstream.
That PR is where the second decoder, the equal-power ramp, the mixing before
the sink, and `PlayerConfig::crossfade` come from; this branch only replayed it
onto the fork's own changes. **It remains the right place to discuss that
mechanism**, and this fork's commits should not be read as a competing
implementation of it.

Everything the branch adds on top — planning the overlap, matching tempo,
choosing where each track enters and leaves, the events a host needs to drive
that — is described below.

### What the branch adds

**A planned overlap.** Upstream fades on a timer, between two tracks that are
otherwise unrelated. `CrossfadePlan` lets a host say where the outgoing track
should start leaving, how long the overlap runs, and where in the incoming
track it should begin. The player fires the overlap on that timing.

```rust
pub struct CrossfadePlan {
    pub duration: Duration,             // overlap length
    pub fade_out_before_end: Duration,  // where the exit starts
    pub fade_in_at: Duration,           // where the incoming track starts
    pub tempo_rate: f64,                // the pair's tempo ratio, pitch held
    pub curve: Option<Arc<IncomingCurve>>,
    pub incoming_track: Option<SpotifyUri>,
}
```

`incoming_track` names the track the plan is a transition *into*. A plan
outlives the moment it was made for — it is held while the previous track
plays, and can be replaced or overtaken — so anything acting on it has to be
able to tell whether it is still the plan for the boundary at hand. That
matters most for a manual skip: the position a plan carries is a position in
one particular track, and seeking a different track to it would land nowhere
near the music.

**Tempo matching, pitch held.** The outgoing tail can be keylocked to the
incoming track's tempo, and the two decks can *share* the stretch rather than
one carrying all of it — a pair 26% apart would otherwise be swept 26% on a
single deck, past the point where keylock stays fully pitch-correct. The
incoming deck's half is rendered ahead of the boundary and played as a curve,
rather than run live: a deck fed whole packets while it consumes them at a
swept rate either underruns or runs its decoder ahead of what has been heard.

**A bass handover.** Two tracks overlapping share their bass, and bass is where
the mud is. A shelf at 200 Hz moves the low end from one deck to the other
across the overlap, so only one of them owns it at a time.

**Events a host needs before the preload.**

- `PlayerEvent::UpcomingTrack` — the track that will play next, raised as soon
  as the queue knows one. `TimeToPreloadNextTrack` cannot serve this: it fires
  once the current track is nearly over, because it exists to have the *audio*
  ready in time, whereas a host that wants to look something up about the next
  track needs only its identity, and can have it much earlier.
- `PlayerEvent::IncomingPreloaded` — a short probe of the incoming track's
  audio. Only the outgoing track reaches the sink, so this is how a host gets a
  grid for a track that never plays through the mixer.

**A probe that cannot stall playback.** The probe runs on the same thread as
the loop that feeds the sink, and `AudioFileStreaming::read` blocks on a
condition variable until the bytes it was asked for have arrived. A step taken
before the fetch had caught up therefore stalled the playing track for as long
as the CDN took — measured at a median of 137 ms and a worst case of seconds,
against a sink holding a fraction of that. `StreamLoaderController::read_is_ready`
lets the step be deferred instead.

**A preload ask that can be repeated.** `TimeToPreloadNextTrack` is raised once
per track, and answered with whatever the queue holds at that instant. A track
started from a single-track context is the case that goes wrong: the queue is
still empty when the ask goes out and is filled a moment later, so the ask is
spent with nothing to preload. The ask is now repeated while nothing arrives,
bounded so an empty queue does not produce a command every couple of seconds
for the life of a track.

**Fixes found by running it for hours.** An audio output device that goes away
— an unplugged headset, a machine waking from sleep — pauses the player, which
the state machine read as a broken state and answered with `exit(1)`. A
`spotify:delimiter` marker at the head of the queue was taken for the next
track, so a preload ask was answered with a URI nothing can be loaded from and
the boundary arrived with nothing to mix in.

### Using it

Every librespot crate must come from this branch, so one copy exists:

```toml
[patch.crates-io]
librespot-audio    = { git = "https://github.com/Cinnamobot/librespot", branch = "fastpotify-automix" }
librespot-connect  = { git = "https://github.com/Cinnamobot/librespot", branch = "fastpotify-automix" }
librespot-core     = { git = "https://github.com/Cinnamobot/librespot", branch = "fastpotify-automix" }
librespot-metadata = { git = "https://github.com/Cinnamobot/librespot", branch = "fastpotify-automix" }
librespot-oauth    = { git = "https://github.com/Cinnamobot/librespot", branch = "fastpotify-automix" }
librespot-playback = { git = "https://github.com/Cinnamobot/librespot", branch = "fastpotify-automix" }
librespot-protocol = { git = "https://github.com/Cinnamobot/librespot", branch = "fastpotify-automix" }
```

`Cargo.lock` pins the revision, so the resolution is reproducible.

**The quality workflow does not run on this branch** — upstream's `quality.yml`
and `build.yml` trigger on `dev` and `master` only. Before pushing a change
here, run what those jobs would:

```shell
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

### Where the changes are

| Crate | What it holds |
|---|---|
| `playback/src/player.rs` | `CrossfadePlan`, the deck, the stretch and bass handover, the probe, the events |
| `audio/src/fetch/mod.rs` | `read_is_ready`, so a probe step can be deferred |
| `connect/src/spirc.rs` | Raises `UpcomingTrack` when the queue moves |
| `connect/src/state/tracks.rs` | Names the next *playable* track, skipping the queue's delimiter |
| `protocol/build.rs` | Compiles `cuepoints.proto`, the automix cuepoints the client reads |

Changes are kept as separate commits with their measurements, so any one of
them can be dropped or sent upstream on its own. Patches upstream takes should
be dropped from this branch as they land.

# Documentation
Documentation is currently a work in progress, contributions are welcome!

There is some brief documentation on how the protocol works in the [docs](https://github.com/librespot-org/librespot/tree/master/docs) folder.

[COMPILING.md](https://github.com/librespot-org/librespot/blob/master/COMPILING.md) contains detailed instructions on setting up a development environment, and compiling librespot. More general usage and compilation information is available on the [wiki](https://github.com/librespot-org/librespot/wiki).
[CONTRIBUTING.md](https://github.com/librespot-org/librespot/blob/master/CONTRIBUTING.md) also contains our contributing guidelines.

If you wish to learn more about how librespot works overall, the best way is to simply read the code, and ask any questions you have in our [Gitter Room](https://gitter.im/librespot-org/spotify-connect-resources).

# Issues & Discussions
**We have recently started using Github discussions for general questions and feature requests, as they are a more natural medium for such cases, and allow for upvoting to prioritize feature development. Check them out [here](https://github.com/librespot-org/librespot/discussions). Bugs and issues with the underlying library should still be reported as issues.**

If you run into a bug when using librespot, please search the existing issues before opening a new one. Chances are, we've encountered it before, and have provided a resolution. If not, please open a new one, and where possible, include the backtrace librespot generates on crashing, along with anything we can use to reproduce the issue, e.g. the Spotify URI of the song that caused the crash.

# Building
A quick walkthrough of the build process is outlined below, while a detailed compilation guide can be found [here](https://github.com/librespot-org/librespot/blob/master/COMPILING.md).

## Additional Dependencies
We recently switched to using [Rodio](https://github.com/tomaka/rodio) for audio playback by default, hence for macOS and Windows, you should just be able to clone and build librespot (with the command below).
For Linux, you will need to run the additional commands below, depending on your distro.

On Debian/Ubuntu, the following command will install these dependencies:
```shell
sudo apt-get install build-essential libasound2-dev
```

On Fedora systems, the following command will install these dependencies:
```shell
sudo dnf install alsa-lib-devel make gcc
```

librespot currently offers the following selection of [audio backends](https://github.com/librespot-org/librespot/wiki/Audio-Backends):
```
Rodio (default)
ALSA
GStreamer
PortAudio
PulseAudio
JACK
JACK over Rodio
SDL
Pipe
Subprocess
```
Please check [COMPILING.md](COMPILING.md) for detailed information on TLS, audio, and discovery backend dependencies, or the [Compiling](https://github.com/librespot-org/librespot/wiki/Compiling#general-dependencies) entry on the wiki for additional backend specific dependencies.

Once you've installed the dependencies and cloned this repository you can build *librespot* with the default features using Cargo.
```shell
cargo build --release
```

By default, this builds with native-tls (system TLS), rodio audio backend, and libmdns discovery. See [COMPILING.md](COMPILING.md) for information on selecting different TLS, audio, and discovery backends.

# Packages

librespot is also available via official package system on various operating systems such as Linux, FreeBSD, NetBSD. [Repology](https://repology.org/project/librespot/versions) offers a good overview.

[![Packaging status](https://repology.org/badge/vertical-allrepos/librespot.svg)](https://repology.org/project/librespot/versions)

## Usage
A sample program implementing a headless Spotify Connect receiver is provided.
Once you've built *librespot*, run it using :
```shell
target/release/librespot --name DEVICENAME
```

The above is a minimal example. Here is a more fully fledged one:
```shell
target/release/librespot -n "Librespot" -b 320 -c ./cache --enable-volume-normalisation --initial-volume 75 --device-type avr
```
The above command will create a receiver named ```Librespot```, with bitrate set to 320 kbps, initial volume at 75%, with volume normalisation enabled, and the device displayed in the app as an Audio/Video Receiver. A folder named ```cache``` will be created/used in the current directory, and be used to cache audio data and credentials.

A full list of runtime options is available [here](https://github.com/librespot-org/librespot/wiki/Options).

_Please Note: When using the cache feature, an authentication blob is stored for your account in the cache directory. For security purposes, we recommend that you set directory permissions on the cache directory to `700`._

## Contact
Come and hang out on gitter if you need help or want to offer some:
https://gitter.im/librespot-org/spotify-connect-resources

## Disclaimer
Using this code to connect to Spotify's API is probably forbidden by them.
Use at your own risk.

## License
Everything in this repository is licensed under the MIT license.

## Related Projects
This is a non exhaustive list of projects that either use or have modified librespot. If you'd like to include yours, submit a PR.

- [librespot-golang](https://github.com/librespot-org/librespot-golang) - A golang port of librespot.
- [plugin.audio.spotify](https://github.com/marcelveldt/plugin.audio.spotify) - A Kodi plugin for Spotify.
- [raspotify](https://github.com/dtcooper/raspotify) - A Spotify Connect client that mostly Just Works™
- [Spotifyd](https://github.com/Spotifyd/spotifyd) - A stripped down librespot UNIX daemon.
- [rpi-audio-receiver](https://github.com/nicokaiser/rpi-audio-receiver) - easy Raspbian install scripts for Spotifyd, Bluetooth, Shairport and other audio receivers
- [Spotcontrol](https://github.com/badfortrains/spotcontrol) - A golang implementation of a Spotify Connect controller. No Playback functionality.
- [librespot-java](https://github.com/devgianlu/librespot-java) - A Java port of librespot.
- [ncspot](https://github.com/hrkfdn/ncspot) - Cross-platform ncurses Spotify client.
- [ansible-role-librespot](https://github.com/xMordax/ansible-role-librespot/tree/master) - Ansible role that will build, install and configure Librespot.
- [Spot](https://github.com/xou816/spot) - Gtk/Rust native Spotify client for the GNOME desktop.
- [Snapcast](https://github.com/badaix/snapcast) - synchronised multi-room audio player that uses librespot as its source for Spotify content
- [MuPiBox](https://mupibox.de/) - Portable music box for Spotify and local media based on Raspberry Pi. Operated via touchscreen. Suitable for children and older people.
- [RoPieee](https://ropieee.org) - An easy-to-use Raspberry Pi image for network audio streaming solutions.
