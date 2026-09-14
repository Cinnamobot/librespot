use std::{
    collections::{HashMap, VecDeque},
    f64::consts::FRAC_PI_2,
    fmt, fs,
    fs::File,
    future::Future,
    io::{self, Read, Seek, SeekFrom},
    mem,
    pin::Pin,
    process::exit,
    sync::Mutex,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};

#[cfg(feature = "passthrough-decoder")]
use crate::decoder::PassthroughDecoder;
use crate::{
    audio::{AudioDecrypt, AudioFetchParams, AudioFile, StreamLoaderController},
    audio_backend::Sink,
    config::{Bitrate, NormalisationMethod, NormalisationType, PlayerConfig},
    convert::Converter,
    core::{Error, Session, SpotifyId, SpotifyUri, audio_key::AudioKeyError, util::SeqGenerator},
    decoder::{
        AudioDecoder, AudioPacket, AudioPacketPosition, DecoderError, DecoderResult, SymphoniaDecoder,
    },
    local_file::{LocalFileLookup, create_local_file_lookup},
    metadata::audio::{AudioFileFormat, AudioFiles, AudioItem},
    mixer::VolumeGetter,
};
use futures_util::{
    StreamExt, TryFutureExt, future, future::FusedFuture,
    stream::futures_unordered::FuturesUnordered,
};
use librespot_metadata::{audio::UniqueFields, track::Tracks};

use symphonia::core::io::MediaSource;
use symphonia::core::probe::Hint;
use timestretch::engine::{Engine, EngineConfig, EngineController, EngineProcessor, EngineProfile};
use tokio::sync::{mpsc, oneshot};

use crate::{NUM_CHANNELS, SAMPLE_RATE, SAMPLES_PER_SECOND};

const PRELOAD_NEXT_TRACK_BEFORE_END_DURATION_MS: u32 = 30000;

/// How much earlier than the overlap a crossfade asks for its next track.
/// Loading takes a moment on a slow connection, and a preload that lands
/// after the overlap has begun is of no use.
const CROSSFADE_PRELOAD_SLACK: Duration = Duration::from_secs(20);

const CROSSFADE_MAX: Duration = Duration::from_secs(12);

/// Smallest tempo difference worth keylocking. Below it the two grids stay
/// together across an overlap on their own, and the stretch is not free.
const STRETCH_MIN_RATIO_DIFF: f64 = 0.005;

/// Source ring for the outgoing deck's keylock. The engine checks at build
/// that this covers several callbacks at the fastest rate it supports.
const STRETCH_RING_FRAMES: usize = 32_768;

/// Frames the engine renders per call, and so the granularity the deck
/// reads its output at.
const STRETCH_BLOCK_FRAMES: usize = 1_024;

/// Source fed beyond what the overlap strictly needs, covering the
/// resampler's lookahead so the last output frame is real audio.
const STRETCH_FEED_SLACK: u64 = 4_096;

const CROSSFADE_TAIL_PACKET_FRAMES: usize = 1024;

/// Frames the outgoing deck renders and throws away before it is handed over.
///
/// Long enough for the engine's pipeline — about 12.7 ms in the keylock
/// profile — plus the slowest stage's settle, which measurement puts near
/// 25 ms total. Named rather than local to `Deck::settle` because the prime
/// above it has to feed this much and the pipeline's fill on top, or the
/// settle drains the ring and the deck underruns as it starts.
const SETTLE_FRAMES: usize = 2_048;

fn crossfade_frames(crossfade: Duration) -> u64 {
    let ms = crossfade.min(CROSSFADE_MAX).as_millis() as u64;
    u64::from(SAMPLE_RATE) * ms / 1000
}

/// Where a transition starts, and how long it runs.
///
/// The player can only see the outgoing track's position, so the incoming
/// track's own offset is carried for the caller: a host that plans a
/// beat-matched transition sets this just before loading, and seeks the
/// loaded track to `fade_in_at` itself.
#[derive(Clone, Debug, PartialEq)]
pub struct CrossfadePlan {
    /// Overlap length in seconds.
    pub duration: Duration,
    /// Seconds from the end of the outgoing track where its fade starts.
    pub fade_out_before_end: Duration,
    /// Seconds into the incoming track where its fade starts.
    pub fade_in_at: Duration,
    /// The rate the outgoing tail is played at, with its pitch held, so its
    /// beats land where the incoming track's already are. It is the
    /// incoming track's tempo over the outgoing one's: a slower outgoing
    /// track is sped up. 1.0 leaves the outgoing track alone.
    ///
    /// With [`Self::curve`] set this is where the sweep *ends*: the tail
    /// starts at its own tempo and is pulled onto this one as it hands over.
    pub tempo_rate: f64,
    /// The incoming track's overlap, already rendered under the other half
    /// of the sweep.
    ///
    /// The two decks share the stretch, and the incoming deck's half is
    /// rendered ahead of the boundary rather than run live: it would have to
    /// be fed from the same loop that reports the track's position, and a
    /// deck fed whole packets while it consumes them at a swept rate either
    /// underruns or runs the decoder ahead of what has been heard.
    pub curve: Option<Arc<IncomingCurve>>,
}

impl CrossfadePlan {
    fn clamped(self) -> Self {
        Self {
            duration: self.duration.min(CROSSFADE_MAX),
            fade_out_before_end: self.fade_out_before_end.max(Duration::ZERO),
            fade_in_at: self.fade_in_at.max(Duration::ZERO),
            tempo_rate: if self.tempo_rate.is_finite() {
                self.tempo_rate
            } else {
                1.0
            },
            curve: self.curve,
        }
    }
}

/// Whether a rendered curve belongs to an overlap of `frames`.
///
/// The comparison cannot be exact: the planner builds the overlap's duration
/// from an unrounded number of seconds while the player derives its frame
/// count from truncated milliseconds, so the two land a few dozen frames
/// apart on a long overlap — measured at 40 frames on a 6.6-second one. An
/// exact check therefore discarded a curve that had been rendered perfectly
/// well, and did it silently, so every transition took the one-deck fallback
/// while the log said the pair was shared.
///
/// A millisecond of slack absorbs that rounding, and is far too small to
/// accept a curve belonging to a different boundary: the shortest overlap the
/// planner will make is a second and a half.
fn curve_fits_overlap(curve: &IncomingCurve, frames: u64) -> bool {
    let wanted = frames as usize * NUM_CHANNELS as usize;
    let slack = (SAMPLE_RATE as usize / 1000) * NUM_CHANNELS as usize;
    curve.samples.len().abs_diff(wanted) <= slack
}

struct Ramp {
    left: u64,
    total: u64,
}

impl Ramp {
    fn new(frames: u64) -> Self {
        Self {
            left: frames,
            total: frames,
        }
    }

    fn finished(&self) -> bool {
        self.left == 0
    }

    fn progress(&self) -> f64 {
        // The first frame is the start and the last frame is the end, so the
        // curve reaches exactly 0 and 1 rather than stopping short of them.
        // Otherwise the incoming track never quite arrives at full level and
        // the outgoing one is cut while still audible.
        if self.total <= 1 || self.left == 0 {
            return 1.0;
        }
        (self.total - self.left) as f64 / (self.total - 1) as f64
    }

    fn out_gain(&self) -> f64 {
        (self.progress() * FRAC_PI_2).cos()
    }

    fn in_gain(&self) -> f64 {
        (self.progress() * FRAC_PI_2).sin()
    }

    fn advance(&mut self) {
        self.left = self.left.saturating_sub(1);
    }
}

/// How much of the incoming track's opening to hand to a host that wants to
/// plan a transition.
///
/// Long enough to reach the track's first chorus, which is what a host needs
/// to know to bring the next track in ahead of it rather than during it. The
/// cost is a moment's decoding inside the preload, which happens off the
/// audio path and well before the transition is due.
const INCOMING_PROBE_SECONDS: usize = 90;

/// Audio read from a preloaded track's opening, for a host planning the
/// transition into it.
///
/// The probe is taken by consuming the preloaded decoder and then seeking it
/// back, so the track still plays from where it was prepared to start. A
/// host that only needs the tempo can plan without its own decoder; one that
/// needs a grid runs its own analysis over these samples.
#[derive(Clone, Debug)]
pub struct IncomingProbe {
    /// Interleaved stereo at [`SAMPLE_RATE`].
    pub samples: Vec<f32>,
    /// Where the probes' samples begin, in track time.
    pub position_ms: u32,
}

/// Reads one step of the opening: at most [`PROBE_STEP_SAMPLES`] more
/// samples, stopping early once the whole probe is in.
///
/// Returns whether another step is needed. Keeping each step short is the
/// point: the caller runs this from the loop that also decodes the *playing*
/// track, and a step that took longer than the sink's queue would be heard
/// as a gap in the transition.
fn probe_step(decoder: &mut Decoder, samples: &mut Vec<f32>) -> bool {
    let wanted = INCOMING_PROBE_SECONDS * SAMPLES_PER_SECOND as usize;
    let step_end = (samples.len() + PROBE_STEP_SAMPLES).min(wanted);
    while samples.len() < step_end {
        match decoder.next_packet() {
            Ok(Some((_, AudioPacket::Samples(packet)))) => {
                samples.extend(packet.iter().map(|sample| *sample as f32));
            }
            Ok(Some(_)) => continue,
            // The track ended inside the probe: that is the whole opening.
            Ok(None) | Err(_) => return false,
        }
    }
    samples.len() < wanted
}

/// Frequency the bass swap splits at, in Hz.
///
/// Below this sits the kick and the bassline, which two tracks cannot share
/// without turning to mud. The figure is the usual one for the trick: high
/// enough to catch the low end, low enough to leave the vocal and most
/// instruments untouched.
const BASS_SWAP_HZ: f64 = 200.0;

/// How far down the outgoing deck's low end goes. Deep enough that the
/// incoming track's bass is the only one left, shallow enough that the
/// shelf does not take the body out of the mix.
const BASS_SWAP_DEPTH_DB: f64 = 24.0;

/// A low shelf that can be swept to take a deck's low end out.
///
/// Two tracks overlapping share their bass, and bass is where the mud is.
/// A DJ takes the low end off one deck and then the other, so only one
/// track owns it at a time. This is that shelf: engaged on the outgoing
/// deck as the fade begins, and released on the incoming one as the other
/// lets go.
#[derive(Clone, Copy, Debug, Default)]
struct BassShelf {
    b0: f64,
    b1: f64,
    b2: f64,
    a1: f64,
    a2: f64,
    x1: f64,
    x2: f64,
    y1: f64,
    y2: f64,
}

impl BassShelf {
    /// A low shelf at `hz`, `gain_db` below unity.
    ///
    /// After the Audio EQ Cookbook's shelf, which is what the equalizer in
    /// this crate uses too, so the two sound alike.
    fn new(hz: f64, gain_db: f64) -> Self {
        let a = 10f64.powf(gain_db / 40.0);
        let w0 = std::f64::consts::TAU * hz / f64::from(SAMPLE_RATE);
        let (sin, cos) = w0.sin_cos();
        let alpha = sin / 2.0 * 2f64.sqrt();
        let two_sqrt_a_alpha = 2.0 * a.sqrt() * alpha;
        let a0 = (a + 1.0) + (a - 1.0) * cos + two_sqrt_a_alpha;
        Self {
            b0: a * ((a + 1.0) - (a - 1.0) * cos + two_sqrt_a_alpha) / a0,
            b1: 2.0 * a * ((a - 1.0) - (a + 1.0) * cos) / a0,
            b2: a * ((a + 1.0) - (a - 1.0) * cos - two_sqrt_a_alpha) / a0,
            a1: -2.0 * ((a - 1.0) + (a + 1.0) * cos) / a0,
            a2: ((a + 1.0) + (a - 1.0) * cos - two_sqrt_a_alpha) / a0,
            ..Self::default()
        }
    }

    #[inline]
    fn run(&mut self, x: f64) -> f64 {
        let y =
            self.b0 * x + self.b1 * self.x1 + self.b2 * self.x2 - self.a1 * self.y1 - self.a2 * self.y2;
        self.x2 = self.x1;
        self.x1 = x;
        self.y2 = self.y1;
        self.y1 = y;
        y
    }
}

fn apply_fade_in(samples: &mut [f64], ramp: &mut Ramp) {
    for frame in samples.chunks_mut(NUM_CHANNELS as usize) {
        let gain = ramp.in_gain();
        for sample in frame.iter_mut() {
            *sample *= gain;
        }
        ramp.advance();
    }
}

/// Mixes the outgoing deck under the incoming one, sweeping its bass out
/// as the two overlap.
fn mix_tail_with_bass(samples: &mut [f64], outgoing: &mut Outgoing) {
    let channels = NUM_CHANNELS as usize;
    let frames = samples.len() / channels;
    let tail = outgoing.take(frames);

    // Filter the whole tail once, then blend each frame towards it by how
    // far the sweep has run.
    let Outgoing {
        filtered,
        bass,
        ramp,
        bass_left,
        bass_total,
        ..
    } = outgoing;
    filtered.clear();
    filtered.extend_from_slice(&tail);
    for frame in filtered.chunks_mut(channels) {
        for (sample, shelf) in frame.iter_mut().zip(bass.iter_mut()) {
            *sample = shelf.run(*sample);
        }
    }

    for (frame, (tail_frame, filtered_frame)) in samples
        .chunks_mut(channels)
        .zip(tail.chunks(channels).zip(filtered.chunks(channels)))
    {
        // How far the sweep has run, advanced one frame at a time.
        let mix = if *bass_left == 0 {
            1.0
        } else {
            *bass_left -= 1;
            1.0 - (*bass_left as f64 / (*bass_total).max(1) as f64)
        };
        let gain = ramp.out_gain();
        for ((sample, tail_sample), filtered_sample) in frame
            .iter_mut()
            .zip(tail_frame.iter())
            .zip(filtered_frame.iter())
        {
            // The outgoing track is what is being mixed, so the blend is
            // between its own dry and low-cut copies. Blending against the
            // buffer's contents instead would fold the incoming track into
            // the outgoing one, which is heard as the outgoing track losing
            // its level the moment the overlap starts.
            let blended = tail_sample + (filtered_sample - tail_sample) * mix;
            *sample += blended * gain;
        }
        ramp.advance();
    }
}

/// Feeds one packet of `decoder` into `source`, stopping at `budget` frames.
/// Returns whether anything was fed; `false` means the track ran out.
fn feed_one(
    decoder: &mut Decoder,
    source: &mut timestretch::engine::SourceProducer,
    scratch: &mut Vec<f32>,
    factor: f64,
    fed: &mut u64,
    budget: u64,
) -> bool {
    let packet = match decoder.next_packet() {
        Ok(Some((_, AudioPacket::Samples(samples)))) => samples,
        Ok(Some(_)) => return true,
        Ok(None) | Err(_) => return false,
    };
    scratch.clear();
    scratch.extend(packet.iter().map(|sample| (sample * factor) as f32));
    let mut written = 0;
    while written < scratch.len() {
        if *fed >= budget {
            return true;
        }
        // `push` counts frames, the slice is interleaved samples: never step
        // by the wrong one, or the feed hands the ring half a frame.
        let accepted = source.push(&scratch[written..]);
        if accepted == 0 {
            return true;
        }
        written += accepted * NUM_CHANNELS as usize;
        *fed += accepted as u64;
    }
    true
}

/// Plays the outgoing track's tail at a different tempo with its pitch
/// held, so its beats line up with the incoming track's for the overlap.
///
/// The engine is fed from a thread of its own. Its contract is that the host
/// keeps at least `demand_hint` frames buffered before every call, which
/// means decoding ahead of the read, and the sink thread must never wait on
/// a decode. Feeding less than that is not an error the engine reports: it
/// emits silence for the shortfall, which would be a hole in the middle of
/// the transition. So the feed stays ahead of the engine, and the ring's own
/// backpressure paces it.
///
/// The mix's ramp decides how much is read, and it reaches zero exactly at
/// the last frame of the overlap, so this deck never has to work out where
/// the track ended: it renders what it is asked and the engine pads a track
/// that ran out first.
struct Deck {
    processor: EngineProcessor,
    /// Where the engine reports silence it had to substitute. Non-zero means
    /// the feed fell behind, which is the one way this deck can fail without
    /// sounding like anything in particular.
    controller: EngineController,
    /// Set on drop, so a feed parked on a full ring gives up.
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    /// Rendered output not yet handed to the mix.
    ready: VecDeque<f64>,
    /// Render target, reused so a block costs no allocation.
    out: Vec<f32>,
    underruns: u64,
}

impl Deck {
    /// Starts a keylocked deck to play `frames` of the tail at `rate`.
    ///
    /// Hands the decoder back if the engine will not build, so the caller
    /// can still play the tail as it is.
    ///
    /// A swept tail does not consume the track evenly: it starts on the
    /// outgoing track's own tempo and is pulled onto the incoming one, so
    /// across the overlap it eats somewhere between the two. `budget_rate` is
    /// the fastest it will be retargeted to, and budgeting on that end means
    /// the feed always has enough source; the tail simply stops decoding once
    /// the overlap is over, so nothing past it is pulled in.
    fn new(
        decoder: Decoder,
        rate: f64,
        factor: f64,
        frames: u64,
        budget_rate: f64,
    ) -> Result<Self, Decoder> {
        let handles = match Engine::build(EngineConfig {
            sample_rate: SAMPLE_RATE,
            channels: NUM_CHANNELS as usize,
            profile: EngineProfile::Keylock,
            initial_tempo_rate: rate,
            max_block_frames: STRETCH_BLOCK_FRAMES,
            source_capacity_frames: STRETCH_RING_FRAMES,
            pre_analysis: None,
        }) {
            Ok(handles) => handles,
            Err(e) => {
                warn!("Unable to build the keylock engine, playing the tail as it is: {e}");
                return Err(decoder);
            }
        };

        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let mut source = handles.source;
        // The engine takes source at `rate` frames per frame out, so this is
        // how much of the track the overlap can reach. The rest of the track
        // is left undecoded: a transition out of the outro must not pull in
        // minutes of audio it will never play.
        let budget = (frames as f64 * budget_rate).ceil() as u64 + STRETCH_FEED_SLACK;

        // Prime before returning. The engine substitutes silence for source
        // it does not have yet, so a deck handed straight to the sink would
        // open the transition with a gap while the feed thread catches up.
        //
        // The amount has to cover what `settle` below consumes as well as the
        // pipeline it fills, or the settle drains the ring and the deck
        // underruns the moment it is handed over. It did, by one block: the
        // prime filled a block's worth while the settle walked two thousand
        // frames, which is heard as a gap of exactly that many frames at the
        // start of the transition.
        let mut decoder = decoder;
        let mut fed = 0u64;
        let mut scratch: Vec<f32> = Vec::new();
        let wanted = source.demand_hint(STRETCH_BLOCK_FRAMES, rate.max(1.0))
            + (SETTLE_FRAMES as f64 * rate.max(1.0)).ceil() as usize
            + STRETCH_BLOCK_FRAMES;
        while source.occupied_frames() < wanted && fed < budget && feed_one(
            &mut decoder, &mut source, &mut scratch, factor, &mut fed, budget,
        ) {}

        // Settle the pipeline before the deck is handed over. Without this the
        // outgoing track falls silent for the pipeline's length the instant
        // the transition fires, because from then on the deck is what feeds
        // it; measured at 0.0000 rms for the first ten milliseconds.
        let mut deck = Self {
            processor: handles.processor,
            controller: handles.controller,
            stop,
            handle: None,
            ready: VecDeque::new(),
            out: vec![0.0; STRETCH_BLOCK_FRAMES * NUM_CHANNELS as usize],
            underruns: 0,
        };
        deck.settle();

        let handle = thread::Builder::new()
            .name("crossfade-stretch".into())
            .spawn(move || {
                'feed: while fed < budget {
                    if worker_stop.load(Ordering::Acquire) {
                        return;
                    }
                    match decoder.next_packet() {
                        Ok(Some((_, AudioPacket::Samples(samples)))) => {
                            scratch.clear();
                            scratch.extend(samples.iter().map(|s| (s * factor) as f32));
                            let mut written = 0;
                            while written < scratch.len() {
                                if worker_stop.load(Ordering::Acquire) {
                                    return;
                                }
                                let accepted = source.push(&scratch[written..]);
                                if accepted == 0 {
                                    // The ring is full, so the engine is
                                    // where it should be; let it catch up.
                                    thread::yield_now();
                                    continue;
                                }
                                written += accepted * NUM_CHANNELS as usize;
                                fed += accepted as u64;
                                if fed >= budget {
                                    break 'feed;
                                }
                            }
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) | Err(_) => break,
                    }
                }
                // Padding so the resampler releases the last real frames.
                loop {
                    if source.finish() || worker_stop.load(Ordering::Acquire) {
                        break;
                    }
                    thread::yield_now();
                }
            })
            .expect("spawning the crossfade stretch thread");

        deck.handle = Some(handle);
        Ok(deck)
    }

    /// Retargets the rate the deck plays at, from the next block on.
    ///
    /// A shared transition does not hold one ratio: the two decks move onto
    /// a common tempo across the overlap, so this deck's rate is a curve
    /// rather than the constant it was built with.
    fn set_rate(&self, rate: f64) {
        self.controller.set_tempo_rate(rate);
    }

    /// Runs the pipeline up to steady state and throws the result away, so
    /// the first frame the mix reads is real audio.
    ///
    /// The engine has a pipeline of its own — about 12.7 ms in the keylock
    /// profile — and its stages start cold, taking a little longer than that
    /// to settle. Without this, the moment a transition fires the outgoing
    /// track goes silent while the pipeline fills: measured as 0.0000 rms for
    /// the first ten milliseconds, which is heard as a click.
    ///
    /// What is discarded is the fill, not the track: the pipeline has not
    /// emitted the opening yet, so this costs the tail a few tens of
    /// milliseconds of its start rather than any of the music the listener
    /// was already hearing.
    fn settle(&mut self) {
        let mut discarded = 0;
        let mut scratch = vec![0.0f32; self.out.len()];
        while discarded < SETTLE_FRAMES {
            self.processor.process(&mut scratch);
            discarded += STRETCH_BLOCK_FRAMES;
        }
    }

    /// The next `wanted` interleaved samples, already normalised.
    fn take(&mut self, wanted: usize) -> Vec<f64> {
        while self.ready.len() < wanted {
            self.processor.process(&mut self.out);
            self.ready
                .extend(self.out.iter().map(|sample| f64::from(*sample)));
            let underruns = self.controller.underrun_frames();
            if underruns > self.underruns {
                warn!(
                    "The keylocked tail missed {} frames of source, so the transition has a gap",
                    underruns - self.underruns
                );
                self.underruns = underruns;
            }
        }
        let mut taken: Vec<f64> = self
            .ready
            .drain(..wanted.min(self.ready.len()))
            .collect();
        taken.resize(wanted, 0.0);
        taken
    }
}

impl Drop for Deck {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn mix_tail(samples: &mut [f64], tail: &[f64], ramp: &mut Ramp) {
    let channels = NUM_CHANNELS as usize;
    for (frame, tail_frame) in samples.chunks_mut(channels).zip(tail.chunks(channels)) {
        let gain = ramp.out_gain();
        for (sample, tail_sample) in frame.iter_mut().zip(tail_frame) {
            *sample += tail_sample * gain;
        }
        ramp.advance();
    }
}

/// Where the outgoing deck's samples come from: the decoder as it was, or
/// a keylocked deck playing the tail at the transition's rate.
enum Tail {
    Plain(Decoder),
    Stretched(Box<Deck>),
}

impl Tail {
    /// Retargets the deck, if this tail has one. A plain tail plays at its
    /// own tempo because the pair was too close to be worth stretching.
    fn set_rate(&self, rate: f64) {
        if let Self::Stretched(deck) = self {
            deck.set_rate(rate);
        }
    }
    /// The next `wanted` interleaved samples, already normalised. `None`
    /// once the track is behind this deck.
    fn produce(&mut self, wanted: usize, factor: f64) -> Option<Vec<f64>> {
        match self {
            Self::Plain(decoder) => {
                let mut out: Vec<f64> = Vec::new();
                while out.len() < wanted {
                    match decoder.next_packet() {
                        Ok(Some((_, AudioPacket::Samples(samples)))) => {
                            out.extend(samples.iter().map(|sample| sample * factor));
                        }
                        Ok(Some(_)) => continue,
                        Ok(None) | Err(_) => break,
                    }
                }
                if out.is_empty() { None } else { Some(out) }
            }
            // The deck hands back silence once the track is behind it, so
            // the ramp alone decides when the overlap is over.
            Self::Stretched(deck) => Some(deck.take(wanted)),
        }
    }
}

struct Outgoing {
    tail: Tail,
    normalisation_factor: f64,
    pending: VecDeque<f64>,
    ramp: Ramp,
    /// Takes this deck's low end out, so the incoming track owns the bass
    /// for the length of the overlap. Fixed at full depth; how much of it is
    /// heard is the blend below.
    bass: Vec<BassShelf>,
    /// The filtered copy of the outgoing tail, reused every packet so the
    /// mix does not allocate while the sink is waiting on it.
    filtered: Vec<f64>,
    /// How far the shelf is engaged, swept across the first part of the fade.
    bass_left: u64,
    bass_total: u64,
    ended: bool,
}

impl Outgoing {
    /// `start_rate` is where the tail begins. `end_rate` is where a *shared*
    /// transition sweeps it to: it is `None` when the tail carries the whole
    /// stretch on its own, which is the case that has always existed.
    ///
    /// A shared tail starts at its own tempo and is pulled onto the incoming
    /// track's, so its starting rate is 1.0 — but it must still be keylocked,
    /// because the sweep takes it away from that tempo as it hands over. The
    /// "not worth an engine" shortcut below therefore applies only to a tail
    /// that holds one rate throughout.
    fn new(
        decoder: Decoder,
        normalisation_factor: f64,
        frames: u64,
        start_rate: f64,
        end_rate: Option<f64>,
    ) -> Self {
        // The sweep runs over the first part of the overlap: the low end
        // leaves before the fade is half done, so the two basses never sit
        // together at equal level for long.
        let sweep = frames / 3;
        // The deck is decoded far enough for the fastest rate it will reach,
        // so a sweep that speeds up does not run out of source part way in.
        let budget_rate = end_rate.map_or(start_rate, |end| start_rate.max(end));
        let steady = end_rate.is_none() && (start_rate - 1.0).abs() < STRETCH_MIN_RATIO_DIFF;
        // A rate this close to 1.0 is inaudible as a tempo difference, so
        // the engine is not worth starting for it — unless the tail is
        // sweeping, where 1.0 is only where it starts.
        let tail = if steady {
            Tail::Plain(decoder)
        } else {
            // A tail that cannot be keylocked is still a tail: play it as it
            // is rather than losing the transition.
            match Deck::new(
                decoder,
                start_rate,
                normalisation_factor,
                frames,
                budget_rate,
            ) {
                Ok(deck) => Tail::Stretched(Box::new(deck)),
                Err(decoder) => Tail::Plain(decoder),
            }
        };
        Self {
            tail,
            normalisation_factor,
            pending: VecDeque::new(),
            ramp: Ramp::new(frames),
            bass: (0..NUM_CHANNELS)
                .map(|_| BassShelf::new(BASS_SWAP_HZ, -BASS_SWAP_DEPTH_DB))
                .collect(),
            filtered: Vec::new(),
            bass_left: sweep,
            bass_total: sweep.max(1),
            ended: false,
        }
    }

    fn take(&mut self, frames: usize) -> Vec<f64> {
        let wanted = frames * NUM_CHANNELS as usize;
        while self.pending.len() < wanted && !self.ended {
            let factor = self.normalisation_factor;
            match self.tail.produce(wanted - self.pending.len(), factor) {
                Some(samples) => self.pending.extend(samples),
                None => self.ended = true,
            }
        }
        let mut taken: Vec<f64> = self
            .pending
            .drain(..wanted.min(self.pending.len()))
            .collect();
        taken.resize(wanted, 0.0);
        taken
    }

    fn finished(&self) -> bool {
        self.ramp.finished() || (self.ended && self.pending.is_empty())
    }
}
pub const DB_VOLTAGE_RATIO: f64 = 20.0;
pub const PCM_AT_0DBFS: f64 = 1.0;

// Spotify inserts a custom Ogg packet at the start with custom metadata values, that you would
// otherwise expect in Vorbis comments. This packet isn't well-formed and players may balk at it.
const SPOTIFY_OGG_HEADER_END: u64 = 0xa7;

const LOAD_HANDLES_POISON_MSG: &str = "load handles mutex should not be poisoned";

pub type PlayerResult = Result<(), Error>;

pub struct Player {
    commands: Option<mpsc::UnboundedSender<PlayerCommand>>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum SinkStatus {
    Running,
    Closed,
    TemporarilyClosed,
}

pub type SinkEventCallback = Box<dyn Fn(SinkStatus) + Send>;

struct PlayerInternal {
    session: Session,
    config: PlayerConfig,
    commands: mpsc::UnboundedReceiver<PlayerCommand>,
    load_handles: Arc<Mutex<HashMap<thread::ThreadId, thread::JoinHandle<()>>>>,

    state: PlayerState,
    preload: PlayerPreload,
    sink: Box<dyn Sink>,
    sink_status: SinkStatus,
    sink_event_callback: Option<SinkEventCallback>,
    volume_getter: Box<dyn VolumeGetter + Send>,
    event_senders: Vec<mpsc::UnboundedSender<PlayerEvent>>,
    converter: Converter,

    normalisation_integrators: [f64; 2],
    normalisation_peaks: [f64; 2],
    normalisation_channel: usize,
    normalisation_knee_factor: f64,

    auto_normalise_as_album: bool,

    player_id: usize,
    play_request_id_generator: SeqGenerator<u64>,
    last_progress_update: Instant,

    local_file_lookup: Arc<LocalFileLookup>,

    crossfade: Duration,
    plan: Option<CrossfadePlan>,
    outgoing: Option<Outgoing>,
    /// The ratio the outgoing tail is being swept towards, when this
    /// transition shares its stretch between the decks. `None` means the tail
    /// holds the constant rate it was built with.
    outgoing_sweep: Option<f64>,
    fade_in: Option<Ramp>,
    adopting: Option<SpotifyUri>,
}

static PLAYER_COUNTER: AtomicUsize = AtomicUsize::new(0);

enum PlayerCommand {
    Load {
        track_id: SpotifyUri,
        play: bool,
        position_ms: u32,
    },
    Preload {
        track_id: SpotifyUri,
    },
    Play,
    Pause,
    Stop,
    Seek(u32),
    SetSession(Session),
    AddEventSender(mpsc::UnboundedSender<PlayerEvent>),
    SetSinkEventCallback(Option<SinkEventCallback>),
    EmitVolumeChangedEvent(u16),
    SetAutoNormaliseAsAlbum(bool),
    SetCrossfade(Duration),
    SetCrossfadePlan(Option<CrossfadePlan>),
    EmitSessionDisconnectedEvent {
        connection_id: String,
        user_name: String,
    },
    EmitSessionConnectedEvent {
        connection_id: String,
        user_name: String,
    },
    EmitSessionClientChangedEvent {
        client_id: String,
        client_name: String,
        client_brand_name: String,
        client_model_name: String,
    },
    EmitFilterExplicitContentChangedEvent(bool),
    EmitShuffleChangedEvent(bool),
    EmitRepeatChangedEvent {
        context: bool,
        track: bool,
    },
    EmitAutoPlayChangedEvent(bool),
}

#[derive(Debug, Clone)]
pub enum PlayerEvent {
    // Play request id changed
    PlayRequestIdChanged {
        play_request_id: u64,
    },
    // Fired when the player is stopped (e.g. by issuing a "stop" command to the player).
    Stopped {
        play_request_id: u64,
        track_id: SpotifyUri,
    },
    // The player is delayed by loading a track.
    Loading {
        play_request_id: u64,
        track_id: SpotifyUri,
        position_ms: u32,
    },
    // The player is preloading a track.
    Preloading {
        track_id: SpotifyUri,
    },
    /// The opening of the track being preloaded, so a host can plan the
    /// transition into it. Arrives before [`PlayerEvent::Preloading`].
    IncomingPreloaded {
        track_id: SpotifyUri,
        probe: IncomingProbe,
    },
    // The player is playing a track.
    // This event is issued at the start of playback of whenever the position must be communicated
    // because it is out of sync. This includes:
    // start of a track
    // un-pausing
    // after a seek
    // after a buffer-underrun
    Playing {
        play_request_id: u64,
        track_id: SpotifyUri,
        position_ms: u32,
    },
    // The player entered a paused state.
    Paused {
        play_request_id: u64,
        track_id: SpotifyUri,
        position_ms: u32,
    },
    // The player thinks it's a good idea to issue a preload command for the next track now.
    // This event is intended for use within spirc.
    TimeToPreloadNextTrack {
        play_request_id: u64,
        track_id: SpotifyUri,
    },
    // The player reached the end of a track.
    // This event is intended for use within spirc. Spirc will respond by issuing another command.
    EndOfTrack {
        play_request_id: u64,
        track_id: SpotifyUri,
    },
    // The player was unable to load the requested track.
    Unavailable {
        play_request_id: u64,
        track_id: SpotifyUri,
    },
    // Spotify refused the key required to decrypt the requested track.
    AudioKeyUnavailable {
        play_request_id: u64,
        track_id: SpotifyUri,
    },
    // The mixer volume was set to a new level.
    VolumeChanged {
        volume: u16,
    },
    PositionCorrection {
        play_request_id: u64,
        track_id: SpotifyUri,
        position_ms: u32,
    },
    /// Requires `PlayerConfig::position_update_interval` to be set to Some.
    /// Once set this event will be sent periodically while playing the track to inform about the
    /// current playback position
    PositionChanged {
        play_request_id: u64,
        track_id: SpotifyUri,
        position_ms: u32,
    },
    Seeked {
        play_request_id: u64,
        track_id: SpotifyUri,
        position_ms: u32,
    },
    TrackChanged {
        audio_item: Box<AudioItem>,
    },
    SessionConnected {
        connection_id: String,
        user_name: String,
    },
    SessionDisconnected {
        connection_id: String,
        user_name: String,
    },
    SessionClientChanged {
        client_id: String,
        client_name: String,
        client_brand_name: String,
        client_model_name: String,
    },
    ShuffleChanged {
        shuffle: bool,
    },
    RepeatChanged {
        context: bool,
        track: bool,
    },
    AutoPlayChanged {
        auto_play: bool,
    },
    FilterExplicitContentChanged {
        filter: bool,
    },
}

impl PlayerEvent {
    pub fn get_play_request_id(&self) -> Option<u64> {
        use PlayerEvent::*;
        match self {
            Loading {
                play_request_id, ..
            }
            | Unavailable {
                play_request_id, ..
            }
            | AudioKeyUnavailable {
                play_request_id, ..
            }
            | Playing {
                play_request_id, ..
            }
            | TimeToPreloadNextTrack {
                play_request_id, ..
            }
            | EndOfTrack {
                play_request_id, ..
            }
            | Paused {
                play_request_id, ..
            }
            | Stopped {
                play_request_id, ..
            }
            | PositionCorrection {
                play_request_id, ..
            }
            | Seeked {
                play_request_id, ..
            } => Some(*play_request_id),
            _ => None,
        }
    }
}

pub type PlayerEventChannel = mpsc::UnboundedReceiver<PlayerEvent>;

#[inline]
pub fn db_to_ratio(db: f64) -> f64 {
    f64::powf(10.0, db / DB_VOLTAGE_RATIO)
}

#[inline]
pub fn ratio_to_db(ratio: f64) -> f64 {
    ratio.log10() * DB_VOLTAGE_RATIO
}

pub fn duration_to_coefficient(duration: Duration) -> f64 {
    f64::exp(-1.0 / (duration.as_secs_f64() * SAMPLES_PER_SECOND as f64))
}

pub fn coefficient_to_duration(coefficient: f64) -> Duration {
    Duration::from_secs_f64(-1.0 / f64::ln(coefficient) / SAMPLES_PER_SECOND as f64)
}

#[derive(Clone, Copy, Debug)]
pub struct NormalisationData {
    // Spotify provides these as `f32`, but audio metadata can contain up to `f64`.
    // Also, this negates the need for casting during sample processing.
    pub track_gain_db: f64,
    pub track_peak: f64,
    pub album_gain_db: f64,
    pub album_peak: f64,
}

impl Default for NormalisationData {
    fn default() -> Self {
        Self {
            track_gain_db: 0.0,
            track_peak: 1.0,
            album_gain_db: 0.0,
            album_peak: 1.0,
        }
    }
}

impl NormalisationData {
    fn parse_from_ogg<T: Read + Seek>(mut file: T) -> io::Result<NormalisationData> {
        const SPOTIFY_NORMALIZATION_HEADER_START_OFFSET: u64 = 144;
        const NORMALISATION_DATA_SIZE: usize = 16;

        let newpos = file.seek(SeekFrom::Start(SPOTIFY_NORMALIZATION_HEADER_START_OFFSET))?;
        if newpos != SPOTIFY_NORMALIZATION_HEADER_START_OFFSET {
            error!(
                "NormalisationData::parse_from_file seeking to {SPOTIFY_NORMALIZATION_HEADER_START_OFFSET} but position is now {newpos}"
            );

            error!("Falling back to default (non-track and non-album) normalisation data.");

            return Ok(NormalisationData::default());
        }

        let mut buf = [0u8; NORMALISATION_DATA_SIZE];

        file.read_exact(&mut buf)?;

        let track_gain_db = f32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as f64;
        let track_peak = f32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as f64;
        let album_gain_db = f32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as f64;
        let album_peak = f32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]) as f64;

        Ok(Self {
            track_gain_db,
            track_peak,
            album_gain_db,
            album_peak,
        })
    }

    fn get_factor(config: &PlayerConfig, data: NormalisationData) -> f64 {
        if !config.normalisation {
            return 1.0;
        }

        let (gain_db, gain_peak) = if config.normalisation_type == NormalisationType::Album {
            (data.album_gain_db, data.album_peak)
        } else {
            (data.track_gain_db, data.track_peak)
        };

        // As per the ReplayGain 1.0 & 2.0 (proposed) spec:
        // https://wiki.hydrogenaud.io/index.php?title=ReplayGain_1.0_specification#Clipping_prevention
        // https://wiki.hydrogenaud.io/index.php?title=ReplayGain_2.0_specification#Clipping_prevention
        let normalisation_factor = if config.normalisation_method == NormalisationMethod::Basic {
            // For Basic Normalisation, factor = min(ratio of (ReplayGain + PreGain), 1.0 / peak level).
            // https://wiki.hydrogenaud.io/index.php?title=ReplayGain_1.0_specification#Peak_amplitude
            // https://wiki.hydrogenaud.io/index.php?title=ReplayGain_2.0_specification#Peak_amplitude
            // We then limit that to 1.0 as not to exceed dBFS (0.0 dB).
            let factor = f64::min(
                db_to_ratio(gain_db + config.normalisation_pregain_db),
                PCM_AT_0DBFS / gain_peak,
            );

            if factor > PCM_AT_0DBFS {
                info!(
                    "Lowering gain by {:.2} dB for the duration of this track to avoid potentially exceeding dBFS.",
                    ratio_to_db(factor)
                );

                PCM_AT_0DBFS
            } else {
                factor
            }
        } else {
            // For Dynamic Normalisation it's up to the player to decide,
            // factor = ratio of (ReplayGain + PreGain).
            // We then let the dynamic limiter handle gain reduction.
            let factor = db_to_ratio(gain_db + config.normalisation_pregain_db);
            let threshold_ratio = db_to_ratio(config.normalisation_threshold_dbfs);

            if factor > PCM_AT_0DBFS {
                let factor_db = gain_db + config.normalisation_pregain_db;
                let limiting_db = factor_db + config.normalisation_threshold_dbfs.abs();

                warn!(
                    "This track may exceed dBFS by {factor_db:.2} dB and be subject to {limiting_db:.2} dB of dynamic limiting at its peak."
                );
            } else if factor > threshold_ratio {
                let limiting_db = gain_db
                    + config.normalisation_pregain_db
                    + config.normalisation_threshold_dbfs.abs();

                info!(
                    "This track may be subject to {limiting_db:.2} dB of dynamic limiting at its peak."
                );
            }

            factor
        };

        debug!("Normalisation Data: {data:?}");
        debug!(
            "Calculated Normalisation Factor for {:?}: {:.2}%",
            config.normalisation_type,
            normalisation_factor * 100.0
        );

        normalisation_factor
    }
}

impl Player {
    pub fn new<F>(
        config: PlayerConfig,
        session: Session,
        volume_getter: Box<dyn VolumeGetter + Send>,
        sink_builder: F,
    ) -> Arc<Self>
    where
        F: FnOnce() -> Box<dyn Sink> + Send + 'static,
    {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        if config.normalisation {
            debug!("Normalisation Type: {:?}", config.normalisation_type);
            debug!(
                "Normalisation Pregain: {:.1} dB",
                config.normalisation_pregain_db
            );
            debug!(
                "Normalisation Threshold: {:.1} dBFS",
                config.normalisation_threshold_dbfs
            );
            debug!("Normalisation Method: {:?}", config.normalisation_method);

            if config.normalisation_method == NormalisationMethod::Dynamic {
                // as_millis() has rounding errors (truncates)
                debug!(
                    "Normalisation Attack: {:.0} ms",
                    coefficient_to_duration(config.normalisation_attack_cf).as_secs_f64() * 1000.
                );
                debug!(
                    "Normalisation Release: {:.0} ms",
                    coefficient_to_duration(config.normalisation_release_cf).as_secs_f64() * 1000.
                );
                debug!("Normalisation Knee: {} dB", config.normalisation_knee_db);
            }
        }

        let handle = thread::spawn(move || {
            let player_id = PLAYER_COUNTER.fetch_add(1, Ordering::AcqRel);
            debug!("new Player [{player_id}]");

            let converter = Converter::new(config.ditherer);
            let normalisation_knee_factor = 1.0 / (8.0 * config.normalisation_knee_db);

            // TODO: it would be neat if we could watch for added or modified files in the
            // specified directories, and dynamically update the lookup. Currently, a new player
            // must be created for any new local files to be playable.
            let local_file_lookup =
                create_local_file_lookup(config.local_file_directories.as_slice());

            let crossfade = config.crossfade;

            let internal = PlayerInternal {
                session,
                config,
                commands: cmd_rx,
                load_handles: Arc::new(Mutex::new(HashMap::new())),

                state: PlayerState::Stopped,
                preload: PlayerPreload::None,
                sink: sink_builder(),
                sink_status: SinkStatus::Closed,
                sink_event_callback: None,
                volume_getter,
                event_senders: vec![],
                converter,

                normalisation_peaks: [0.0; 2],
                normalisation_integrators: [0.0; 2],
                normalisation_channel: 0,
                normalisation_knee_factor,

                auto_normalise_as_album: false,

                player_id,
                play_request_id_generator: SeqGenerator::new(0),
                last_progress_update: Instant::now(),

                local_file_lookup: Arc::new(local_file_lookup),

                crossfade: crossfade.min(CROSSFADE_MAX),
                plan: None,
                outgoing: None,
                outgoing_sweep: None,
                fade_in: None,
                adopting: None,
            };

            // While PlayerInternal is written as a future, it still contains blocking code.
            // It must be run by using block_on() in a dedicated thread.
            let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
            runtime.block_on(internal);

            debug!("PlayerInternal thread finished.");
        });

        Arc::new(Self {
            commands: Some(cmd_tx),
            thread_handle: Some(handle),
        })
    }

    pub fn is_invalid(&self) -> bool {
        if let Some(handle) = self.thread_handle.as_ref() {
            return handle.is_finished();
        }
        true
    }

    fn command(&self, cmd: PlayerCommand) {
        if let Some(commands) = self.commands.as_ref() {
            if let Err(e) = commands.send(cmd) {
                error!("Player Commands Error: {e}");
            }
        }
    }

    pub fn load(&self, track_id: SpotifyUri, start_playing: bool, position_ms: u32) {
        self.command(PlayerCommand::Load {
            track_id,
            play: start_playing,
            position_ms,
        });
    }

    pub fn preload(&self, track_id: SpotifyUri) {
        self.command(PlayerCommand::Preload { track_id });
    }

    pub fn play(&self) {
        self.command(PlayerCommand::Play)
    }

    pub fn pause(&self) {
        self.command(PlayerCommand::Pause)
    }

    pub fn stop(&self) {
        self.command(PlayerCommand::Stop)
    }

    pub fn seek(&self, position_ms: u32) {
        self.command(PlayerCommand::Seek(position_ms));
    }

    pub fn set_session(&self, session: Session) {
        self.command(PlayerCommand::SetSession(session));
    }

    pub fn get_player_event_channel(&self) -> PlayerEventChannel {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        self.command(PlayerCommand::AddEventSender(event_sender));
        event_receiver
    }

    pub async fn await_end_of_track(&self) {
        let mut channel = self.get_player_event_channel();
        while let Some(event) = channel.recv().await {
            if matches!(
                event,
                PlayerEvent::EndOfTrack { .. } | PlayerEvent::Stopped { .. }
            ) {
                return;
            }
        }
    }

    pub fn set_sink_event_callback(&self, callback: Option<SinkEventCallback>) {
        self.command(PlayerCommand::SetSinkEventCallback(callback));
    }

    pub fn emit_volume_changed_event(&self, volume: u16) {
        self.command(PlayerCommand::EmitVolumeChangedEvent(volume));
    }

    pub fn set_auto_normalise_as_album(&self, setting: bool) {
        self.command(PlayerCommand::SetAutoNormaliseAsAlbum(setting));
    }

    pub fn set_crossfade(&self, crossfade: Duration) {
        self.command(PlayerCommand::SetCrossfade(crossfade));
    }

    /// Overrides where the next transition starts, in the incoming track's
    /// own terms. Cleared by the load that follows it.
    pub fn set_crossfade_plan(&self, plan: Option<CrossfadePlan>) {
        self.command(PlayerCommand::SetCrossfadePlan(plan));
    }

    pub fn emit_filter_explicit_content_changed_event(&self, filter: bool) {
        self.command(PlayerCommand::EmitFilterExplicitContentChangedEvent(filter));
    }

    pub fn emit_session_connected_event(&self, connection_id: String, user_name: String) {
        self.command(PlayerCommand::EmitSessionConnectedEvent {
            connection_id,
            user_name,
        });
    }

    pub fn emit_session_disconnected_event(&self, connection_id: String, user_name: String) {
        self.command(PlayerCommand::EmitSessionDisconnectedEvent {
            connection_id,
            user_name,
        });
    }

    pub fn emit_session_client_changed_event(
        &self,
        client_id: String,
        client_name: String,
        client_brand_name: String,
        client_model_name: String,
    ) {
        self.command(PlayerCommand::EmitSessionClientChangedEvent {
            client_id,
            client_name,
            client_brand_name,
            client_model_name,
        });
    }

    pub fn emit_shuffle_changed_event(&self, shuffle: bool) {
        self.command(PlayerCommand::EmitShuffleChangedEvent(shuffle));
    }

    pub fn emit_repeat_changed_event(&self, context: bool, track: bool) {
        self.command(PlayerCommand::EmitRepeatChangedEvent { context, track });
    }

    pub fn emit_auto_play_changed_event(&self, auto_play: bool) {
        self.command(PlayerCommand::EmitAutoPlayChangedEvent(auto_play));
    }
}

impl Drop for Player {
    fn drop(&mut self) {
        debug!("Shutting down player thread ...");
        self.commands = None;
        if let Some(handle) = self.thread_handle.take() {
            if let Err(e) = handle.join() {
                error!("Player thread Error: {e:?}");
            }
        }
    }
}

struct PlayerLoadedTrackData {
    decoder: Decoder,
    normalisation_data: NormalisationData,
    stream_loader_controller: StreamLoaderController,
    audio_item: AudioItem,
    bytes_per_second: usize,
    duration_ms: u32,
    stream_position_ms: u32,
    is_explicit: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoadError {
    Unavailable,
    AudioKeyUnavailable,
}

impl LoadError {
    fn after_decoder_failure(audio_key_unavailable: bool) -> Self {
        if audio_key_unavailable {
            Self::AudioKeyUnavailable
        } else {
            Self::Unavailable
        }
    }
}

enum PlayerPreload {
    None,
    Loading {
        track_id: SpotifyUri,
        loader: Pin<Box<dyn FusedFuture<Output = Result<PlayerLoadedTrackData, LoadError>> + Send>>,
    },
    /// Reading the incoming track's opening a step at a time.
    ///
    /// The read has to happen before the track is handed over, but doing it
    /// in one go stops the loop that decodes the *playing* track, and the
    /// sink only holds about 200 ms of audio. A probe of a useful length
    /// takes longer than that, so the queue ran dry and the transition was
    /// heard as a gap. Stepping it keeps both moving.
    Probing {
        track_id: SpotifyUri,
        loaded_track: Box<PlayerLoadedTrackData>,
        samples: Vec<f32>,
        /// Where the decoder started, so it can be put back there.
        position_ms: u32,
    },
    Ready {
        track_id: SpotifyUri,
        loaded_track: Box<PlayerLoadedTrackData>,
    },
}

/// Samples the probe reads per step. Small enough that a step is far
/// shorter than the sink's queue, so the playing track is never starved.
const PROBE_STEP_SAMPLES: usize = 16_384 * NUM_CHANNELS as usize;

type Decoder = Box<dyn AudioDecoder + Send>;

/// The incoming track's overlap, rendered ahead of the boundary under the
/// tempo sweep.
///
/// Both decks move onto a shared tempo across an overlap, and the incoming
/// deck's half of that sweep cannot be run live: it would have to be fed from
/// the same decode loop that reports the track's position, and a deck fed
/// whole packets while it consumes them at a swept rate either underruns or
/// runs the decoder ahead of what has been heard. Rendering the overlap
/// before the boundary removes the deadline — the source is pushed until
/// there is room and the render pulled until the overlap is full — so the two
/// cannot outrun each other.
#[derive(PartialEq)]
pub struct IncomingCurve {
    /// Interleaved stereo at [`SAMPLE_RATE`], exactly the overlap's length.
    pub samples: Arc<Vec<f32>>,
    /// How much of the incoming track the render consumed, in milliseconds.
    ///
    /// The decoder is asked for whole packets while the curve plays at its
    /// own rate, so by the end of the overlap it has travelled further than
    /// the listener has heard. This is the distance to bring it back by, and
    /// it is a property of the render rather than something to re-derive.
    pub consumed_ms: u32,
    /// The pair's tempo ratio, which the overlap's position is computed from.
    pub ratio: f64,
}

impl std::fmt::Debug for IncomingCurve {
    /// The samples are thousands of floats: printing them would drown the
    /// enclosing plan's own fields.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingCurve")
            .field("frames", &(self.samples.len() / NUM_CHANNELS as usize))
            .field("consumed_ms", &self.consumed_ms)
            .field("ratio", &self.ratio)
            .finish()
    }
}

/// How much of the curve is handed out per packet.
const CURVE_PACKET_FRAMES: usize = 4_096;

/// Plays a pre-rendered overlap and then carries on with the track itself.
///
/// The player's loop needs no knowledge of the sweep: it asks this decoder
/// for packets exactly as it asked the real one, and gets the overlap's audio
/// at its own rate. Once the curve is spent the inner decoder is put where
/// the listener actually got to, and every later packet is the track's own.
struct CurvedDecoder {
    curve: Arc<Vec<f32>>,
    cursor: usize,
    inner: Decoder,
    /// Where in the track the overlap began.
    start_ms: u32,
    /// How far the track really advanced across the overlap.
    consumed_ms: u32,
    /// The pair's tempo ratio, which the position integral is computed from.
    ratio: f64,
    /// Whether the inner decoder has been put back on the beat yet.
    handed_over: bool,
}

impl CurvedDecoder {
    fn new(curve: IncomingCurve, inner: Decoder, start_ms: u32) -> Self {
        Self {
            curve: curve.samples,
            cursor: 0,
            inner,
            start_ms,
            consumed_ms: curve.consumed_ms,
            ratio: curve.ratio,
            handed_over: false,
        }
    }

    /// How much of the track the curve has consumed by output position
    /// `progress`, as a fraction of the whole overlap.
    ///
    /// The incoming deck plays at `r^(p-1)` of the track's own tempo, so the
    /// track covered by output position `p` is that rate's integral, over the
    /// whole sweep's integral. Reporting the true figure rather than the wall
    /// clock keeps the position continuous: it starts at the overlap's own
    /// start and finishes exactly at the point the decoder is resumed from,
    /// so the hand-over has no step in it.
    fn consumed_fraction(&self, progress: f64) -> f64 {
        let ratio = self.ratio;
        if (ratio - 1.0).abs() < 1e-9 {
            return progress;
        }
        let ln = ratio.ln();
        let total = (1.0 - 1.0 / ratio) / ln;
        if total <= 0.0 {
            return progress;
        }
        ((ratio.powf(progress - 1.0) - 1.0 / ratio) / ln / total).clamp(0.0, 1.0)
    }

    /// Where in the track the audio just handed out comes from.
    fn position_ms(&self) -> u32 {
        let channels = NUM_CHANNELS as usize;
        let total_frames = self.curve.len() / channels;
        if total_frames == 0 {
            return self.start_ms;
        }
        let progress = (self.cursor / channels) as f64 / total_frames as f64;
        let consumed = self.consumed_ms as f64 * self.consumed_fraction(progress);
        self.start_ms.saturating_add(consumed as u32)
    }
}

impl AudioDecoder for CurvedDecoder {
    fn seek(&mut self, position_ms: u32) -> Result<u32, DecoderError> {
        // A seek abandons the boundary this curve was rendered for: the
        // listener has moved somewhere else entirely.
        self.cursor = self.curve.len();
        self.handed_over = true;
        self.inner.seek(position_ms)
    }

    fn next_packet(&mut self) -> DecoderResult<Option<(AudioPacketPosition, AudioPacket)>> {
        if self.cursor < self.curve.len() {
            let channels = NUM_CHANNELS as usize;
            let end = (self.cursor + CURVE_PACKET_FRAMES * channels).min(self.curve.len());
            let position_ms = self.position_ms();
            let samples: Vec<f64> = self.curve[self.cursor..end]
                .iter()
                .map(|sample| f64::from(*sample))
                .collect();
            self.cursor = end;
            return Ok(Some((
                AudioPacketPosition {
                    position_ms,
                    skipped: false,
                },
                AudioPacket::Samples(samples),
            )));
        }
        if !self.handed_over {
            self.handed_over = true;
            // Past the curve, the track's own audio resumes — from where the
            // listener was actually taken to, which is the overlap's start
            // plus what the sweep consumed.
            let target = self.start_ms.saturating_add(self.consumed_ms);
            if let Err(error) = self.inner.seek(target) {
                warn!("Unable to put the incoming track back on the beat: {error}");
            }
        }
        self.inner.next_packet()
    }
}

enum PlayerState {
    Stopped,
    Loading {
        track_id: SpotifyUri,
        play_request_id: u64,
        start_playback: bool,
        loader: Pin<Box<dyn FusedFuture<Output = Result<PlayerLoadedTrackData, LoadError>> + Send>>,
    },
    Paused {
        track_id: SpotifyUri,
        play_request_id: u64,
        decoder: Decoder,
        audio_item: AudioItem,
        normalisation_data: NormalisationData,
        normalisation_factor: f64,
        stream_loader_controller: StreamLoaderController,
        bytes_per_second: usize,
        duration_ms: u32,
        stream_position_ms: u32,
        suggested_to_preload_next_track: bool,
        is_explicit: bool,
    },
    Playing {
        track_id: SpotifyUri,
        play_request_id: u64,
        decoder: Decoder,
        normalisation_data: NormalisationData,
        audio_item: AudioItem,
        normalisation_factor: f64,
        stream_loader_controller: StreamLoaderController,
        bytes_per_second: usize,
        duration_ms: u32,
        stream_position_ms: u32,
        reported_nominal_start_time: Option<Instant>,
        suggested_to_preload_next_track: bool,
        is_explicit: bool,
    },
    EndOfTrack {
        track_id: SpotifyUri,
        play_request_id: u64,
        loaded_track: PlayerLoadedTrackData,
    },
    Invalid,
}

impl PlayerState {
    fn is_playing(&self) -> bool {
        use self::PlayerState::*;
        match *self {
            Stopped | EndOfTrack { .. } | Paused { .. } | Loading { .. } => false,
            Playing { .. } => true,
            Invalid => {
                error!("PlayerState::is_playing in invalid state");
                exit(1);
            }
        }
    }

    #[allow(dead_code)]
    fn is_stopped(&self) -> bool {
        use self::PlayerState::*;
        matches!(self, Stopped)
    }

    #[allow(dead_code)]
    fn is_loading(&self) -> bool {
        use self::PlayerState::*;
        matches!(self, Loading { .. })
    }

    fn decoder(&mut self) -> Option<&mut Decoder> {
        use self::PlayerState::*;
        match *self {
            Stopped | EndOfTrack { .. } | Loading { .. } => None,
            Paused {
                ref mut decoder, ..
            }
            | Playing {
                ref mut decoder, ..
            } => Some(decoder),
            Invalid => {
                error!("PlayerState::decoder in invalid state");
                exit(1);
            }
        }
    }

    fn playing_to_end_of_track(&mut self) {
        use self::PlayerState::*;
        let new_state = mem::replace(self, Invalid);
        match new_state {
            Playing {
                track_id,
                play_request_id,
                decoder,
                duration_ms,
                bytes_per_second,
                normalisation_data,
                stream_loader_controller,
                stream_position_ms,
                is_explicit,
                audio_item,
                ..
            } => {
                *self = EndOfTrack {
                    track_id,
                    play_request_id,
                    loaded_track: PlayerLoadedTrackData {
                        decoder,
                        normalisation_data,
                        stream_loader_controller,
                        audio_item,
                        bytes_per_second,
                        duration_ms,
                        stream_position_ms,
                        is_explicit,
                    },
                };
            }
            _ => {
                error!("Called playing_to_end_of_track in non-playing state: {new_state:?}");
                exit(1);
            }
        }
    }

    fn paused_to_playing(&mut self) {
        use self::PlayerState::*;
        let new_state = mem::replace(self, Invalid);
        match new_state {
            Paused {
                track_id,
                play_request_id,
                decoder,
                audio_item,
                normalisation_data,
                normalisation_factor,
                stream_loader_controller,
                duration_ms,
                bytes_per_second,
                stream_position_ms,
                suggested_to_preload_next_track,
                is_explicit,
            } => {
                *self = Playing {
                    track_id,
                    play_request_id,
                    decoder,
                    audio_item,
                    normalisation_data,
                    normalisation_factor,
                    stream_loader_controller,
                    duration_ms,
                    bytes_per_second,
                    stream_position_ms,
                    reported_nominal_start_time: Instant::now()
                        .checked_sub(Duration::from_millis(stream_position_ms as u64)),
                    suggested_to_preload_next_track,
                    is_explicit,
                };
            }
            _ => {
                error!("PlayerState::paused_to_playing in invalid state: {new_state:?}");
                exit(1);
            }
        }
    }

    fn playing_to_paused(&mut self) {
        use self::PlayerState::*;
        let new_state = mem::replace(self, Invalid);
        match new_state {
            Playing {
                track_id,
                play_request_id,
                decoder,
                audio_item,
                normalisation_data,
                normalisation_factor,
                stream_loader_controller,
                duration_ms,
                bytes_per_second,
                stream_position_ms,
                suggested_to_preload_next_track,
                is_explicit,
                ..
            } => {
                *self = Paused {
                    track_id,
                    play_request_id,
                    decoder,
                    audio_item,
                    normalisation_data,
                    normalisation_factor,
                    stream_loader_controller,
                    duration_ms,
                    bytes_per_second,
                    stream_position_ms,
                    suggested_to_preload_next_track,
                    is_explicit,
                };
            }
            _ => {
                error!("PlayerState::playing_to_paused in invalid state: {new_state:?}");
                exit(1);
            }
        }
    }
}

struct PlayerTrackLoader {
    session: Session,
    config: PlayerConfig,
    local_file_lookup: Arc<LocalFileLookup>,
}

impl PlayerTrackLoader {
    fn is_audio_key_unavailable(error: &Error) -> bool {
        matches!(
            error.error.downcast_ref::<AudioKeyError>(),
            Some(AudioKeyError::AesKey)
        )
    }

    async fn find_available_alternative(&self, audio_item: AudioItem) -> Option<AudioItem> {
        if let Err(e) = audio_item.availability {
            error!("Track is unavailable: {e}");
            None
        } else if !audio_item.files.is_empty() {
            Some(audio_item)
        } else if let Some(alternatives) = audio_item.alternatives {
            let Tracks(alternatives_vec) = alternatives; // required to make `into_iter` able to move

            let alternatives: FuturesUnordered<_> = alternatives_vec
                .into_iter()
                .map(|alt_id| AudioItem::get_file(&self.session, alt_id))
                .collect();

            alternatives
                .filter_map(|x| future::ready(x.ok()))
                .filter(|x| future::ready(x.availability.is_ok()))
                .next()
                .await
        } else {
            error!("Track should be available, but no alternatives found.");
            None
        }
    }

    fn stream_data_rate(&self, format: AudioFileFormat) -> Option<usize> {
        let kbps = match format {
            AudioFileFormat::OGG_VORBIS_96 => 12.,
            AudioFileFormat::OGG_VORBIS_160 => 20.,
            AudioFileFormat::OGG_VORBIS_320 => 40.,
            AudioFileFormat::MP3_256 => 32.,
            AudioFileFormat::MP3_320 => 40.,
            AudioFileFormat::MP3_160 => 20.,
            AudioFileFormat::MP3_96 => 12.,
            AudioFileFormat::MP3_160_ENC => 20.,
            AudioFileFormat::AAC_24 => 3.,
            AudioFileFormat::AAC_48 => 6.,
            AudioFileFormat::AAC_160 => 20.,
            AudioFileFormat::AAC_320 => 40.,
            AudioFileFormat::MP4_128 => 16.,
            AudioFileFormat::OTHER5 => 40.,
            AudioFileFormat::FLAC_FLAC => 112., // assume 900 kbit/s on average
            AudioFileFormat::XHE_AAC_12 => 1.5,
            AudioFileFormat::XHE_AAC_16 => 2.,
            AudioFileFormat::XHE_AAC_24 => 3.,
            AudioFileFormat::FLAC_FLAC_24BIT => 3.,
        };
        let data_rate: f32 = kbps * 1024.;
        Some(data_rate.ceil() as usize)
    }

    async fn load_track(
        &self,
        track_uri: SpotifyUri,
        position_ms: u32,
    ) -> Result<PlayerLoadedTrackData, LoadError> {
        match track_uri {
            SpotifyUri::Track { .. } | SpotifyUri::Episode { .. } => {
                self.load_remote_track(track_uri, position_ms).await
            }
            SpotifyUri::Local { .. } => self
                .load_local_track(track_uri, position_ms)
                .await
                .ok_or(LoadError::Unavailable),
            _ => {
                error!("Cannot handle load of track with URI: <{track_uri}>",);
                Err(LoadError::Unavailable)
            }
        }
    }

    async fn load_remote_track(
        &self,
        track_uri: SpotifyUri,
        position_ms: u32,
    ) -> Result<PlayerLoadedTrackData, LoadError> {
        let track_id: SpotifyId = match (&track_uri).try_into() {
            Ok(id) => id,
            Err(_) => {
                warn!("<{track_uri}> could not be converted to a base62 ID");
                return Err(LoadError::Unavailable);
            }
        };

        let audio_item = match AudioItem::get_file(&self.session, track_uri).await {
            Ok(audio) => match self.find_available_alternative(audio).await {
                Some(audio) => audio,
                None => {
                    warn!(
                        "spotify:track:<{}> is not available",
                        track_id.to_base62().unwrap_or_default()
                    );
                    return Err(LoadError::Unavailable);
                }
            },
            Err(e) => {
                error!("Unable to load audio item: {e:?}");
                return Err(LoadError::Unavailable);
            }
        };

        info!(
            "Loading <{}> with Spotify URI <{}>",
            audio_item.name, audio_item.uri
        );

        // (Most) podcasts seem to support only 96 kbps Ogg Vorbis, so fall back to it
        let formats = match self.config.bitrate {
            Bitrate::Bitrate96 => [
                AudioFileFormat::OGG_VORBIS_96,
                AudioFileFormat::MP3_96,
                AudioFileFormat::OGG_VORBIS_160,
                AudioFileFormat::MP3_160,
                AudioFileFormat::MP3_256,
                AudioFileFormat::OGG_VORBIS_320,
                AudioFileFormat::MP3_320,
            ],
            Bitrate::Bitrate160 => [
                AudioFileFormat::OGG_VORBIS_160,
                AudioFileFormat::MP3_160,
                AudioFileFormat::OGG_VORBIS_96,
                AudioFileFormat::MP3_96,
                AudioFileFormat::MP3_256,
                AudioFileFormat::OGG_VORBIS_320,
                AudioFileFormat::MP3_320,
            ],
            Bitrate::Bitrate320 => [
                AudioFileFormat::OGG_VORBIS_320,
                AudioFileFormat::MP3_320,
                AudioFileFormat::MP3_256,
                AudioFileFormat::OGG_VORBIS_160,
                AudioFileFormat::MP3_160,
                AudioFileFormat::OGG_VORBIS_96,
                AudioFileFormat::MP3_96,
            ],
        };

        let (format, file_id) =
            match formats
                .iter()
                .find_map(|format| match audio_item.files.get(format) {
                    Some(&file_id) => Some((*format, file_id)),
                    _ => None,
                }) {
                Some(t) => t,
                None => {
                    warn!(
                        "<{}> is not available in any supported format",
                        audio_item.name
                    );
                    return Err(LoadError::Unavailable);
                }
            };

        let bytes_per_second = self
            .stream_data_rate(format)
            .ok_or(LoadError::Unavailable)?;

        // This is only a loop to be able to reload the file if an error occurred
        // while opening a cached file.
        loop {
            let encrypted_file = AudioFile::open(&self.session, file_id, bytes_per_second);

            let encrypted_file = match encrypted_file.await {
                Ok(encrypted_file) => encrypted_file,
                Err(e) => {
                    error!("Unable to load encrypted file: {e:?}");
                    return Err(LoadError::Unavailable);
                }
            };

            let is_cached = encrypted_file.is_cached();

            let stream_loader_controller = encrypted_file
                .get_stream_loader_controller()
                .map_err(|_| LoadError::Unavailable)?;

            // Not all audio files are encrypted. If we can't get a key, try loading the track
            // without decryption. If the file was encrypted after all, the decoder will fail
            // parsing and bail out, so we should be safe from outputting ear-piercing noise.
            let (key, audio_key_unavailable) =
                match self.session.audio_key().request(track_id, file_id).await {
                    Ok(key) => (Some(key), false),
                    Err(e) => {
                        let unavailable = Self::is_audio_key_unavailable(&e);
                        warn!("Unable to load key, continuing without decryption: {e}");
                        (None, unavailable)
                    }
                };

            let mut decrypted_file = AudioDecrypt::new(key, encrypted_file);

            let is_ogg_vorbis = AudioFiles::is_ogg_vorbis(format);
            let (offset, mut normalisation_data) = if is_ogg_vorbis {
                // Spotify stores normalisation data in a custom Ogg packet instead of Vorbis comments.
                let normalisation_data =
                    NormalisationData::parse_from_ogg(&mut decrypted_file).ok();
                (SPOTIFY_OGG_HEADER_END, normalisation_data)
            } else {
                (0, None)
            };

            let audio_file = match Subfile::new(
                decrypted_file,
                offset,
                stream_loader_controller.len() as u64,
            ) {
                Ok(audio_file) => audio_file,
                Err(e) => {
                    error!("PlayerTrackLoader::load_track error opening subfile: {e}");
                    return Err(LoadError::Unavailable);
                }
            };

            let mut symphonia_decoder = |audio_file, format| {
                SymphoniaDecoder::new(audio_file, format).map(|mut decoder| {
                    // For formats other that Vorbis, we'll try getting normalisation data from
                    // ReplayGain metadata fields, if present.
                    if normalisation_data.is_none() {
                        normalisation_data = decoder.normalisation_data();
                    }
                    Box::new(decoder) as Decoder
                })
            };

            let mut hint = Hint::new();
            if let Some(mime_type) = AudioFiles::mime_type(format) {
                hint.mime_type(mime_type);
            }

            #[cfg(feature = "passthrough-decoder")]
            let decoder_type = if self.config.passthrough {
                PassthroughDecoder::new(audio_file, format).map(|x| Box::new(x) as Decoder)
            } else {
                symphonia_decoder(audio_file, hint)
            };

            #[cfg(not(feature = "passthrough-decoder"))]
            let decoder_type = { symphonia_decoder(audio_file, hint) };

            let normalisation_data = normalisation_data.unwrap_or_else(|| {
                warn!("Unable to get normalisation data, continuing with defaults.");
                NormalisationData::default()
            });

            let mut decoder = match decoder_type {
                Ok(decoder) => decoder,
                Err(e) if is_cached => {
                    warn!("Unable to read cached audio file: {e}. Trying to download it.");

                    match self.session.cache() {
                        Some(cache) => {
                            if cache.remove_file(file_id).is_err() {
                                error!("Error removing file from cache");
                                return Err(LoadError::after_decoder_failure(
                                    audio_key_unavailable,
                                ));
                            }
                        }
                        None => {
                            error!("If the audio file is cached, a cache should exist");
                            return Err(LoadError::after_decoder_failure(audio_key_unavailable));
                        }
                    }

                    // Just try it again
                    continue;
                }
                Err(e) => {
                    error!("Unable to read audio file: {e}");
                    return Err(LoadError::after_decoder_failure(audio_key_unavailable));
                }
            };

            let duration_ms = audio_item.duration_ms;
            // Don't try to seek past the track's duration.
            // If the position is invalid just start from
            // the beginning of the track.
            let position_ms = if position_ms > duration_ms {
                warn!(
                    "Invalid start position of {position_ms} ms exceeds track's duration of {duration_ms} ms, starting track from the beginning"
                );
                0
            } else {
                position_ms
            };

            // Ensure the starting position. Even when we want to play from the beginning,
            // the cursor may have been moved by parsing normalisation data. This may not
            // matter for playback (but won't hurt either), but may be useful for the
            // passthrough decoder.
            let stream_position_ms = match decoder.seek(position_ms) {
                Ok(new_position_ms) => new_position_ms,
                Err(e) => {
                    error!(
                        "PlayerTrackLoader::load_track error seeking to starting position {position_ms}: {e}"
                    );
                    return Err(LoadError::Unavailable);
                }
            };

            // Ensure streaming mode now that we are ready to play from the requested position.
            stream_loader_controller.set_stream_mode();

            let is_explicit = audio_item.is_explicit;

            info!("<{}> ({} ms) loaded", audio_item.name, duration_ms);

            return Ok(PlayerLoadedTrackData {
                decoder,
                normalisation_data,
                stream_loader_controller,
                audio_item,
                bytes_per_second,
                duration_ms,
                stream_position_ms,
                is_explicit,
            });
        }
    }

    async fn load_local_track(
        &self,
        track_uri: SpotifyUri,
        position_ms: u32,
    ) -> Option<PlayerLoadedTrackData> {
        info!("Loading local file with Spotify URI <{}>", track_uri);

        let SpotifyUri::Local { duration, .. } = track_uri else {
            error!("Unable to determine track duration for local file: not a local file URI");
            return None;
        };

        let entry = self.local_file_lookup.get(&track_uri);

        let Some(path) = entry else {
            error!("Unable to find file path for local file <{track_uri}>");
            return None;
        };

        let src = match File::open(path) {
            Ok(src) => src,
            Err(e) => {
                error!("Failed to open local file: {e}");
                return None;
            }
        };

        let mut hint = Hint::new();
        if let Some(file_extension) = path.extension().and_then(|e| e.to_str()) {
            hint.with_extension(file_extension);
        }

        let decoder = match SymphoniaDecoder::new(src, hint) {
            Ok(decoder) => decoder,
            Err(e) => {
                error!("Error decoding local file: {e}");
                return None;
            }
        };

        let mut decoder = Box::new(decoder);
        let normalisation_data = decoder.normalisation_data().unwrap_or_else(|| {
            warn!("Unable to get normalisation data, continuing with defaults.");
            NormalisationData::default()
        });

        let local_file_metadata = decoder.local_file_metadata().unwrap_or_default();

        let stream_position_ms = match decoder.seek(position_ms) {
            Ok(new_position_ms) => new_position_ms,
            Err(e) => {
                error!(
                    "PlayerTrackLoader::load_local_track error seeking to starting position {position_ms}: {e}"
                );
                return None;
            }
        };

        let file_size = fs::metadata(path).ok()?.len();
        let bytes_per_second = (file_size / duration.as_secs()) as usize;

        let stream_loader_controller = StreamLoaderController::from_local_file(file_size);

        let name = local_file_metadata.name.unwrap_or_default();

        info!("Loaded <{name}> from path <{}>", path.display());

        Some(PlayerLoadedTrackData {
            decoder,
            normalisation_data,
            stream_loader_controller,
            bytes_per_second,
            duration_ms: duration.as_millis() as u32,
            stream_position_ms,
            is_explicit: false,
            audio_item: AudioItem {
                duration_ms: duration.as_millis() as u32,
                uri: track_uri.to_uri().unwrap_or_default(),
                track_id: track_uri,
                files: Default::default(),
                name,
                // We can't get a CoverImage.URL for the track image, applications will have to parse the file metadata themselves using unique_fields.path
                covers: vec![],
                language: local_file_metadata
                    .language
                    .map(|val| vec![val])
                    .unwrap_or_default(),
                is_explicit: false,
                availability: Ok(()),
                alternatives: None,
                unique_fields: UniqueFields::Local {
                    artists: local_file_metadata.artists,
                    album: local_file_metadata.album,
                    album_artists: local_file_metadata.album_artists,
                    number: local_file_metadata.number,
                    disc_number: local_file_metadata.disc_number,
                    path: path.to_path_buf(),
                },
            },
        })
    }
}

impl Future for PlayerInternal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // While this is written as a future, it still contains blocking code.
        // It must be run on its own thread.
        let passthrough = self.config.passthrough;

        loop {
            let mut all_futures_completed_or_not_ready = true;

            // process commands that were sent to us
            let cmd = match self.commands.poll_recv(cx) {
                Poll::Ready(None) => return Poll::Ready(()), // client has disconnected - shut down.
                Poll::Ready(Some(cmd)) => {
                    all_futures_completed_or_not_ready = false;
                    Some(cmd)
                }
                _ => None,
            };

            if let Some(cmd) = cmd {
                if let Err(e) = self.handle_command(cmd) {
                    error!("Error handling command: {e}");
                }
            }

            // Handle loading of a new track to play
            if let PlayerState::Loading {
                ref mut loader,
                ref track_id,
                start_playback,
                play_request_id,
            } = self.state
            {
                // The loader may be terminated if we are trying to load the same track
                // as before, and that track failed to open before.
                let track_id = track_id.clone();

                if !loader.as_mut().is_terminated() {
                    match loader.as_mut().poll(cx) {
                        Poll::Ready(Ok(loaded_track)) => {
                            self.start_playback(
                                track_id,
                                play_request_id,
                                loaded_track,
                                start_playback,
                            );
                            if let PlayerState::Loading { .. } = self.state {
                                error!("The state wasn't changed by start_playback()");
                                exit(1);
                            }
                        }
                        Poll::Ready(Err(LoadError::Unavailable)) => {
                            error!("Skipping to next track, unable to load track <{track_id:?}>");
                            self.send_event(PlayerEvent::Unavailable {
                                track_id,
                                play_request_id,
                            })
                        }
                        Poll::Ready(Err(LoadError::AudioKeyUnavailable)) => {
                            error!("Unable to play track <{track_id:?}> without its audio key");
                            self.send_event(PlayerEvent::AudioKeyUnavailable {
                                track_id,
                                play_request_id,
                            });
                            self.handle_player_stop();
                        }
                        Poll::Pending => (),
                    }
                }
            }

            // Step the incoming track's probe along. One step per pass
            // keeps this loop returning to decode the playing track, which
            // is what stops the probe from being heard as a gap.
            if let PlayerPreload::Probing { .. } = self.preload {
                // Take it out, step it, put it back: the borrow of
                // `self.preload` cannot be held across `send_event`.
                let probing = mem::replace(&mut self.preload, PlayerPreload::None);
                let PlayerPreload::Probing {
                    track_id,
                    mut loaded_track,
                    mut samples,
                    position_ms,
                } = probing
                else {
                    unreachable!("just matched");
                };
                if probe_step(&mut loaded_track.decoder, &mut samples) {
                    self.preload = PlayerPreload::Probing {
                        track_id,
                        loaded_track,
                        samples,
                        position_ms,
                    };
                } else {
                    // Put the decoder back where the probe found it: the
                    // track has not started, so this is the seek it would
                    // otherwise have made when it loads.
                    match loaded_track.decoder.seek(position_ms) {
                        Ok(position) => loaded_track.stream_position_ms = position,
                        Err(error) => {
                            warn!("Unable to rewind the preloaded track: {error}");
                            continue;
                        }
                    }
                    self.send_event(PlayerEvent::IncomingPreloaded {
                        track_id: track_id.clone(),
                        probe: IncomingProbe {
                            samples,
                            position_ms,
                        },
                    });
                    self.send_event(PlayerEvent::Preloading {
                        track_id: track_id.clone(),
                    });
                    self.preload = PlayerPreload::Ready {
                        track_id,
                        loaded_track,
                    };
                }
            }

            // handle pending preload requests.
            if let PlayerPreload::Loading {
                ref mut loader,
                ref track_id,
            } = self.preload
            {
                let track_id = track_id.clone();
                match loader.as_mut().poll(cx) {
                    Poll::Ready(Ok(loaded_track)) => {
                        // The opening is read a step at a time from the main
                        // loop below, so the track that is still playing is
                        // not starved while it happens.
                        self.preload = PlayerPreload::Probing {
                            track_id,
                            position_ms: loaded_track.stream_position_ms,
                            samples: Vec::new(),
                            loaded_track: Box::new(loaded_track),
                        };
                    }
                    Poll::Ready(Err(error)) => {
                        debug!("Unable to preload {track_id:?}");
                        self.preload = PlayerPreload::None;
                        if error == LoadError::AudioKeyUnavailable {
                            continue;
                        }
                        // Let Spirc know that the track was unavailable.
                        if let PlayerState::Playing {
                            play_request_id, ..
                        }
                        | PlayerState::Paused {
                            play_request_id, ..
                        } = self.state
                        {
                            self.send_event(PlayerEvent::Unavailable {
                                track_id,
                                play_request_id,
                            });
                        }
                    }
                    Poll::Pending => (),
                }
            }

            if self.state.is_playing() {
                self.ensure_sink_running();

                if let PlayerState::Playing {
                    ref track_id,
                    play_request_id,
                    ref mut decoder,
                    normalisation_factor,
                    ref mut stream_position_ms,
                    ref mut reported_nominal_start_time,
                    ..
                } = self.state
                {
                    let track_id = track_id.clone();
                    match decoder.next_packet() {
                        Ok(result) => {
                            if let Some((ref packet_position, ref packet)) = result {
                                let new_stream_position_ms = packet_position.position_ms;
                                let expected_position_ms = std::mem::replace(
                                    &mut *stream_position_ms,
                                    new_stream_position_ms,
                                );

                                if !passthrough {
                                    match packet.samples() {
                                        Ok(_) => {
                                            let new_stream_position = Duration::from_millis(
                                                new_stream_position_ms as u64,
                                            );

                                            let now = Instant::now();

                                            // Only notify if we're skipped some packets *or* we are behind.
                                            // If we're ahead it's probably due to a buffer of the backend
                                            // and we're actually in time.
                                            let notify_about_position =
                                                match *reported_nominal_start_time {
                                                    None => true,
                                                    Some(reported_nominal_start_time) => {
                                                        let mut notify = false;

                                                        if packet_position.skipped {
                                                            if let Some(ahead) = new_stream_position
                                                                .checked_sub(Duration::from_millis(
                                                                    expected_position_ms as u64,
                                                                ))
                                                            {
                                                                notify |=
                                                                    ahead >= Duration::from_secs(1)
                                                            }
                                                        }

                                                        if let Some(lag) = now
                                                            .checked_duration_since(
                                                                reported_nominal_start_time,
                                                            )
                                                        {
                                                            if let Some(lag) =
                                                                lag.checked_sub(new_stream_position)
                                                            {
                                                                notify |=
                                                                    lag >= Duration::from_secs(1)
                                                            }
                                                        }

                                                        notify
                                                    }
                                                };

                                            if notify_about_position {
                                                *reported_nominal_start_time =
                                                    now.checked_sub(new_stream_position);
                                                self.send_event(PlayerEvent::PositionCorrection {
                                                    play_request_id,
                                                    track_id: track_id.clone(),
                                                    position_ms: new_stream_position_ms,
                                                });
                                            }

                                            if let Some(interval) =
                                                self.config.position_update_interval
                                            {
                                                let last_progress_update_since_ms =
                                                    now.duration_since(self.last_progress_update);

                                                if last_progress_update_since_ms > interval {
                                                    self.last_progress_update = now;
                                                    self.send_event(PlayerEvent::PositionChanged {
                                                        play_request_id,
                                                        track_id,
                                                        position_ms: new_stream_position_ms,
                                                    });
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "Skipping to next track, unable to decode samples for track <{track_id:?}>: {e:?}"
                                            );
                                            self.send_event(PlayerEvent::EndOfTrack {
                                                track_id,
                                                play_request_id,
                                            })
                                        }
                                    }
                                }
                            }

                            self.handle_packet(result, normalisation_factor);
                        }
                        Err(e) => {
                            error!(
                                "Skipping to next track, unable to get next packet for track <{track_id:?}>: {e:?}"
                            );
                            self.send_event(PlayerEvent::EndOfTrack {
                                track_id,
                                play_request_id,
                            })
                        }
                    }
                } else {
                    error!("PlayerInternal poll: Invalid PlayerState");
                    exit(1);
                };
            } else if self.outgoing.is_some() {
                self.ensure_sink_running();
                self.write_tail();
            }

            self.maybe_begin_crossfade();

            // Read before borrowing the state: the suggestion below needs
            // both, and the state borrow is exclusive.
            //
            // A plan decides when the transition fires, and it may start
            // much earlier than the configured crossfade: a host that mixes
            // over the last chorus has a lead-in of half a minute or more.
            // The preload has to follow the plan, not the setting, or the
            // incoming track's analysis would land after the transition had
            // already been planned without it.
            let planned_lead = self
                .plan
                .as_ref()
                .map(|plan| plan.fade_out_before_end.max(plan.duration));
            let crossfade_lead_ms = match (planned_lead, self.crossfade().is_zero()) {
                (Some(lead), _) => (lead + CROSSFADE_PRELOAD_SLACK).as_millis() as i64,
                (None, true) => 0,
                (None, false) => {
                    (self.crossfade() + CROSSFADE_PRELOAD_SLACK).as_millis() as i64
                }
            };

            if let PlayerState::Playing {
                ref track_id,
                play_request_id,
                duration_ms,
                stream_position_ms,
                ref mut stream_loader_controller,
                ref mut suggested_to_preload_next_track,
                ..
            }
            | PlayerState::Paused {
                ref track_id,
                play_request_id,
                duration_ms,
                stream_position_ms,
                ref mut stream_loader_controller,
                ref mut suggested_to_preload_next_track,
                ..
            } = self.state
            {
                let track_id = track_id.clone();

                // Normally the suggestion waits until everything left in the
                // track is buffered, because spirc's preload exists to have
                // the next track ready the moment this one ends. A crossfade
                // only needs the next track's opening, and it needs it
                // earlier than the last thirty seconds, so waiting for the
                // whole range would make the overlap miss its cue on a slow
                // connection. Ask as soon as the crossfade's own lead time is
                // in view instead.
                let remaining_ms = duration_ms as i64 - stream_position_ms as i64;
                let wants_preload = if crossfade_lead_ms == 0 {
                    remaining_ms < PRELOAD_NEXT_TRACK_BEFORE_END_DURATION_MS as i64
                        && stream_loader_controller.range_to_end_available()
                } else {
                    remaining_ms < crossfade_lead_ms
                };

                if !*suggested_to_preload_next_track && wants_preload {
                    *suggested_to_preload_next_track = true;
                    self.send_event(PlayerEvent::TimeToPreloadNextTrack {
                        track_id,
                        play_request_id,
                    });
                }
            }

            if (!self.state.is_playing())
                && self.outgoing.is_none()
                && all_futures_completed_or_not_ready
            {
                return Poll::Pending;
            }
        }
    }
}

impl PlayerInternal {
    fn ensure_sink_running(&mut self) {
        if self.sink_status != SinkStatus::Running {
            trace!("== Starting sink ==");
            if let Some(callback) = &mut self.sink_event_callback {
                callback(SinkStatus::Running);
            }
            match self.sink.start() {
                Ok(()) => self.sink_status = SinkStatus::Running,
                Err(e) => {
                    error!("{e}");
                    self.handle_pause();
                }
            }
        }
    }

    fn ensure_sink_stopped(&mut self, temporarily: bool) {
        match self.sink_status {
            SinkStatus::Running => {
                trace!("== Stopping sink ==");
                match self.sink.stop() {
                    Ok(()) => {
                        self.sink_status = if temporarily {
                            SinkStatus::TemporarilyClosed
                        } else {
                            SinkStatus::Closed
                        };
                        if let Some(callback) = &mut self.sink_event_callback {
                            callback(self.sink_status);
                        }
                    }
                    Err(e) => {
                        error!("{e}");
                        exit(1);
                    }
                }
            }
            SinkStatus::TemporarilyClosed => {
                if !temporarily {
                    self.sink_status = SinkStatus::Closed;
                    if let Some(callback) = &mut self.sink_event_callback {
                        callback(SinkStatus::Closed);
                    }
                }
            }
            SinkStatus::Closed => (),
        }
    }

    fn handle_player_stop(&mut self) {
        self.drop_crossfade();
        match self.state {
            PlayerState::Playing {
                ref track_id,
                play_request_id,
                ..
            }
            | PlayerState::Paused {
                ref track_id,
                play_request_id,
                ..
            }
            | PlayerState::EndOfTrack {
                ref track_id,
                play_request_id,
                ..
            }
            | PlayerState::Loading {
                ref track_id,
                play_request_id,
                ..
            } => {
                let track_id = track_id.clone();

                self.ensure_sink_stopped(false);
                self.send_event(PlayerEvent::Stopped {
                    track_id,
                    play_request_id,
                });
                self.state = PlayerState::Stopped;
            }
            PlayerState::Stopped => (),
            PlayerState::Invalid => {
                error!("PlayerInternal::handle_player_stop in invalid state");
                exit(1);
            }
        }
    }

    fn handle_play(&mut self) {
        match self.state {
            PlayerState::Paused {
                ref track_id,
                play_request_id,
                stream_position_ms,
                ..
            } => {
                let track_id = track_id.clone();

                self.state.paused_to_playing();
                self.send_event(PlayerEvent::Playing {
                    track_id,
                    play_request_id,
                    position_ms: stream_position_ms,
                });
                self.ensure_sink_running();
            }
            PlayerState::Loading {
                ref mut start_playback,
                ..
            } => {
                *start_playback = true;
            }
            _ => error!("Player::play called from invalid state: {:?}", self.state),
        }
    }

    fn handle_pause(&mut self) {
        self.drop_crossfade();
        match self.state {
            PlayerState::Paused { .. } => self.ensure_sink_stopped(false),
            PlayerState::Playing {
                ref track_id,
                play_request_id,
                stream_position_ms,
                ..
            } => {
                let track_id = track_id.clone();

                self.state.playing_to_paused();

                self.ensure_sink_stopped(false);
                self.send_event(PlayerEvent::Paused {
                    track_id,
                    play_request_id,
                    position_ms: stream_position_ms,
                });
            }
            PlayerState::Loading {
                ref mut start_playback,
                ..
            } => {
                *start_playback = false;
            }
            _ => error!("Player::pause called from invalid state: {:?}", self.state),
        }
    }

    fn crossfade(&self) -> Duration {
        if self.config.passthrough {
            Duration::ZERO
        } else {
            self.crossfade
        }
    }

    fn crossfading(&self) -> bool {
        self.outgoing.is_some() || self.fade_in.is_some()
    }

    fn drop_crossfade(&mut self) {
        self.outgoing = None;
        self.outgoing_sweep = None;
        self.fade_in = None;
        self.adopting = None;
        self.plan = None;
    }

    fn write_packet(&mut self, packet: AudioPacket) {
        if let Err(e) = self.sink.write(packet, &mut self.converter) {
            error!("{e}");
            self.handle_pause();
        }
    }

    fn write_samples(&mut self, mut data: Vec<f64>) {
        if let Some(ramp) = &mut self.fade_in {
            apply_fade_in(&mut data, ramp);
        }
        self.fade_in.take_if(|ramp| ramp.finished());
        self.mix_outgoing(&mut data);
        self.apply_output_gain(&mut data);
        self.write_packet(AudioPacket::Samples(data));
    }

    fn write_tail(&mut self) {
        let mut data = vec![0.0; CROSSFADE_TAIL_PACKET_FRAMES * NUM_CHANNELS as usize];
        self.mix_outgoing(&mut data);
        self.apply_output_gain(&mut data);
        self.write_packet(AudioPacket::Samples(data));
    }

    fn mix_outgoing(&mut self, data: &mut [f64]) {
        if let Some(outgoing) = &mut self.outgoing {
            // A shared transition sweeps the tail onto the incoming track's
            // tempo as it hands over, so both decks are at their own tempo
            // when they are loudest. The fade ramp is the clock: it spans
            // exactly the overlap and it is what the incoming half was
            // rendered against, so reading progress off it keeps the two
            // halves on the same schedule. Retargeting per packet is enough
            // because the engine only takes a rate per block, and it clamps
            // how far ahead a retarget may be scheduled.
            if let Some(ratio) = self.outgoing_sweep {
                let progress = outgoing.ramp.progress();
                outgoing.tail.set_rate(ratio.powf(progress));
            }
            mix_tail_with_bass(data, outgoing);
        }
        self.outgoing.take_if(|outgoing| outgoing.finished());
    }

    fn apply_output_gain(&mut self, data: &mut [f64]) {
        let volume = self.volume_getter.attenuation_factor();
        match (self.config.normalisation, self.config.normalisation_method) {
            (false, _) | (true, NormalisationMethod::Basic) => {
                if volume < 1.0 {
                    for sample in data.iter_mut() {
                        *sample *= volume;
                    }
                }
            }
            (true, NormalisationMethod::Dynamic) => {
                let threshold_db = self.config.normalisation_threshold_dbfs;
                let knee_db = self.config.normalisation_knee_db;
                let attack_cf = self.config.normalisation_attack_cf;
                let release_cf = self.config.normalisation_release_cf;

                for sample in data.iter_mut() {
                    // Feedforward limiter in the log domain
                    // After: Giannoulis, D., Massberg, M., & Reiss, J.D. (2012).
                    // Digital Dynamic Range Compressor Design—A Tutorial and
                    // Analysis. Journal of The Audio Engineering Society, 60,
                    // 399-408.

                    // This implementation assumes audio is stereo.

                    // step 1-4: half-wave rectification and conversion into dB, and
                    // gain computer with soft knee and subtractor
                    let limiter_db = {
                        // Add slight DC offset. Some samples are silence, which is
                        // -inf dB and gets the limiter stuck. Adding a small
                        // positive offset prevents this.
                        *sample += f64::MIN_POSITIVE;

                        let bias_db = ratio_to_db(sample.abs()) - threshold_db;
                        let knee_boundary_db = bias_db * 2.0;
                        if knee_boundary_db < -knee_db {
                            0.0
                        } else if knee_boundary_db.abs() <= knee_db {
                            let term = knee_boundary_db + knee_db;
                            term * term * self.normalisation_knee_factor
                        } else {
                            bias_db
                        }
                    };

                    // track left/right channel
                    let channel = self.normalisation_channel;
                    self.normalisation_channel ^= 1;

                    // step 5: smooth, decoupled peak detector for each channel
                    // Use direct references to reduce repeated array indexing
                    let integrator = &mut self.normalisation_integrators[channel];
                    let peak = &mut self.normalisation_peaks[channel];

                    *integrator = f64::max(
                        limiter_db,
                        release_cf * *integrator + (1.0 - release_cf) * limiter_db,
                    );
                    *peak = attack_cf * *peak + (1.0 - attack_cf) * *integrator;

                    // steps 6-8: conversion into level and multiplication into gain
                    // stage. Find maximum peak across both channels to couple the
                    // gain and maintain stereo imaging.
                    let max_peak =
                        f64::max(self.normalisation_peaks[0], self.normalisation_peaks[1]);
                    *sample *= db_to_ratio(-max_peak) * volume;
                }
            }
        }
    }

    fn maybe_begin_crossfade(&mut self) {
        let crossfade = self.crossfade();
        if crossfade.is_zero() || self.outgoing.is_some() {
            return;
        }
        let remaining = match &self.state {
            PlayerState::Playing {
                duration_ms,
                stream_position_ms,
                ..
            } => Duration::from_millis(u64::from(duration_ms.saturating_sub(*stream_position_ms))),
            _ => {
                debug!("crossfade: not playing");
                return;
            }
        };
        // A planned transition may be shorter than the configured crossfade,
        // and may start later, so its own overlap governs when it fires.
        let (start_within, duration) = match &self.plan {
            Some(plan) => (plan.fade_out_before_end.max(plan.duration), plan.duration),
            None => (crossfade, crossfade),
        };
        let ready = matches!(self.preload, PlayerPreload::Ready { .. });
        if remaining > start_within || !ready {
            debug!(
                "crossfade: waiting, remaining {:.2}s, start within {:.2}s, preload ready {ready}",
                remaining.as_secs_f64(),
                start_within.as_secs_f64()
            );
            return;
        }
        let rate = self.plan.as_ref().map_or(1.0, |plan| plan.tempo_rate);
        debug!(
            "crossfade: firing with {:.2}s overlap, outgoing tail at {rate:.4}x",
            duration.as_secs_f64()
        );
        self.begin_crossfade(duration, rate);
    }

    fn begin_crossfade(&mut self, crossfade: Duration, rate: f64) {
        let (next_track_id, mut loaded_track) =
            match mem::replace(&mut self.preload, PlayerPreload::None) {
                PlayerPreload::Ready {
                    track_id,
                    loaded_track,
                } => (track_id, loaded_track),
                other => {
                    self.preload = other;
                    return;
                }
            };
        // A planned transition names where in the incoming track its own bar
        // starts. Starting there rather than at the top is what makes the
        // two tracks land their beats together instead of one sliding under
        // the other.
        if let Some(plan) = &self.plan {
            let target_ms = plan.fade_in_at.as_millis() as u32;
            if target_ms != loaded_track.stream_position_ms {
                match loaded_track.decoder.seek(target_ms) {
                    Ok(position) => loaded_track.stream_position_ms = position,
                    Err(error) => {
                        warn!("Unable to start the incoming track on its downbeat: {error}");
                    }
                }
            }
        }
        let frames = crossfade_frames(crossfade);
        // A shared transition: the incoming track's half of the sweep is
        // already rendered, and the outgoing tail sweeps its own half. Taken
        // before the plan is cleared, and only when the curve matches this
        // overlap — a plan whose curve was rendered for a different length
        // would hand the mix audio that does not line up.
        let curve = self
            .plan
            .as_ref()
            .and_then(|plan| plan.curve.clone())
            .filter(|curve| curve_fits_overlap(curve, frames));
        let shared = curve.is_some();
        if let Some(curve) = curve {
            // The incoming decoder hands out the rendered overlap and then
            // resumes the track itself, put back where the listener actually
            // got to. The mix and the player's own loop need no knowledge of
            // the sweep: they ask for packets exactly as they always did.
            let start_ms = loaded_track.stream_position_ms;
            loaded_track.decoder = Box::new(CurvedDecoder::new(
                IncomingCurve {
                    samples: Arc::clone(&curve.samples),
                    consumed_ms: curve.consumed_ms,
                    ratio: curve.ratio,
                },
                loaded_track.decoder,
                start_ms,
            ));
        }
        // A shared transition starts the tail on the outgoing track's own
        // tempo and sweeps it onto the incoming one, which is the other half
        // of the same curve, so the pair keeps a constant quotient. Without a
        // curve the tail carries the whole stretch and holds it.
        let (start_rate, end_rate) = match shared {
            true => (1.0, Some(rate)),
            false => (rate, None),
        };
        let (track_id, play_request_id) = match self.take_outgoing(frames, start_rate, end_rate) {
            Some(taken) => taken,
            None => {
                self.preload = PlayerPreload::Ready {
                    track_id: next_track_id,
                    loaded_track,
                };
                return;
            }
        };
        self.outgoing_sweep = end_rate;
        self.fade_in = Some(Ramp::new(frames));
        self.adopting = Some(next_track_id.clone());
        // The plan describes one boundary: the transition it was made for.
        self.plan = None;
        self.send_event(PlayerEvent::EndOfTrack {
            track_id,
            play_request_id,
        });
        let play_request_id = self.play_request_id_generator.get();
        self.send_event(PlayerEvent::PlayRequestIdChanged { play_request_id });
        self.start_playback(next_track_id, play_request_id, *loaded_track, true);
    }

    /// Takes the outgoing track apart into a deck playing `frames` of its
    /// tail.
    ///
    /// `start_rate` is where the tail begins, and `end_rate` is where a
    /// shared transition sweeps it to — `None` when the tail carries the
    /// whole stretch by itself, which is the constant-rate case.
    fn take_outgoing(
        &mut self,
        frames: u64,
        start_rate: f64,
        end_rate: Option<f64>,
    ) -> Option<(SpotifyUri, u64)> {
        match mem::replace(&mut self.state, PlayerState::Invalid) {
            PlayerState::Playing {
                track_id,
                play_request_id,
                decoder,
                normalisation_factor,
                ..
            } => {
                let factor = if self.config.normalisation {
                    normalisation_factor
                } else {
                    1.0
                };
                self.outgoing = Some(Outgoing::new(decoder, factor, frames, start_rate, end_rate));
                Some((track_id, play_request_id))
            }
            other => {
                self.state = other;
                None
            }
        }
    }

    fn begin_skip_crossfade(&mut self, next: &SpotifyUri) {
        let crossfade = self.crossfade();
        let leaving = match &self.state {
            PlayerState::Playing { track_id, .. } => track_id != next,
            _ => false,
        };
        if crossfade.is_zero() || !leaving {
            return;
        }
        let frames = crossfade_frames(crossfade);
        if self.take_outgoing(frames, 1.0, None).is_some() {
            self.state = PlayerState::Stopped;
            self.fade_in = Some(Ramp::new(frames));
        }
        // A skip is not a planned boundary: the two tracks are unrelated, so
        // they are not stretched onto a shared tempo.
        self.outgoing_sweep = None;
        // A skip cuts the planned boundary short; it must not fire later.
        self.plan = None;
    }

    fn is_adopting(&self, track_id: &SpotifyUri, position_ms: u32) -> bool {
        if position_ms != 0 || !self.crossfading() {
            return false;
        }
        if self.adopting.as_ref() != Some(track_id) {
            return false;
        }
        matches!(
            &self.state,
            PlayerState::Playing { track_id: playing, .. } if playing == track_id
        )
    }

    fn adopt(&mut self, track_id: SpotifyUri, play_request_id: u64) {
        self.adopting = None;
        let position_ms = match &mut self.state {
            PlayerState::Playing {
                play_request_id: current,
                stream_position_ms,
                ..
            } => {
                *current = play_request_id;
                *stream_position_ms
            }
            _ => 0,
        };
        self.send_event(PlayerEvent::Playing {
            track_id,
            play_request_id,
            position_ms,
        });
    }

    fn handle_packet(
        &mut self,
        packet: Option<(AudioPacketPosition, AudioPacket)>,
        normalisation_factor: f64,
    ) {
        match packet {
            Some((_, packet)) => {
                if !packet.is_empty() {
                    match packet {
                        AudioPacket::Samples(mut data) => {
                            if self.config.normalisation && normalisation_factor != 1.0 {
                                for sample in data.iter_mut() {
                                    *sample *= normalisation_factor;
                                }
                            }
                            self.write_samples(data);
                        }
                        raw => self.write_packet(raw),
                    }
                }
            }

            None => {
                self.state.playing_to_end_of_track();
                if let PlayerState::EndOfTrack {
                    ref track_id,
                    play_request_id,
                    ..
                } = self.state
                {
                    self.send_event(PlayerEvent::EndOfTrack {
                        track_id: track_id.clone(),
                        play_request_id,
                    })
                } else {
                    error!("PlayerInternal handle_packet: Invalid PlayerState");
                    exit(1);
                }
            }
        }
    }

    fn start_playback(
        &mut self,
        track_id: SpotifyUri,
        play_request_id: u64,
        loaded_track: PlayerLoadedTrackData,
        start_playback: bool,
    ) {
        let audio_item = Box::new(loaded_track.audio_item.clone());

        self.send_event(PlayerEvent::TrackChanged { audio_item });

        let position_ms = loaded_track.stream_position_ms;

        let mut config = self.config.clone();
        if config.normalisation_type == NormalisationType::Auto {
            if self.auto_normalise_as_album {
                config.normalisation_type = NormalisationType::Album;
            } else {
                config.normalisation_type = NormalisationType::Track;
            }
        };
        let normalisation_factor =
            NormalisationData::get_factor(&config, loaded_track.normalisation_data);
        if let Some(report) = &config.normalisation_report {
            let reported = if config.normalisation {
                normalisation_factor
            } else {
                1.0
            };
            report.store(reported.to_bits(), std::sync::atomic::Ordering::Relaxed);
        }

        if start_playback {
            self.ensure_sink_running();
            self.send_event(PlayerEvent::Playing {
                track_id: track_id.clone(),
                play_request_id,
                position_ms,
            });

            self.state = PlayerState::Playing {
                track_id,
                play_request_id,
                decoder: loaded_track.decoder,
                audio_item: loaded_track.audio_item,
                normalisation_data: loaded_track.normalisation_data,
                normalisation_factor,
                stream_loader_controller: loaded_track.stream_loader_controller,
                duration_ms: loaded_track.duration_ms,
                bytes_per_second: loaded_track.bytes_per_second,
                stream_position_ms: loaded_track.stream_position_ms,
                reported_nominal_start_time: Instant::now()
                    .checked_sub(Duration::from_millis(position_ms as u64)),
                suggested_to_preload_next_track: false,
                is_explicit: loaded_track.is_explicit,
            };
        } else {
            self.ensure_sink_stopped(false);

            self.state = PlayerState::Paused {
                track_id: track_id.clone(),
                play_request_id,
                decoder: loaded_track.decoder,
                audio_item: loaded_track.audio_item,
                normalisation_data: loaded_track.normalisation_data,
                normalisation_factor,
                stream_loader_controller: loaded_track.stream_loader_controller,
                duration_ms: loaded_track.duration_ms,
                bytes_per_second: loaded_track.bytes_per_second,
                stream_position_ms: loaded_track.stream_position_ms,
                suggested_to_preload_next_track: false,
                is_explicit: loaded_track.is_explicit,
            };

            self.send_event(PlayerEvent::Paused {
                track_id,
                play_request_id,
                position_ms,
            });
        }
    }

    fn handle_command_load(
        &mut self,
        track_id: SpotifyUri,
        play_request_id_option: Option<u64>,
        play: bool,
        position_ms: u32,
    ) -> PlayerResult {
        let play_request_id =
            play_request_id_option.unwrap_or(self.play_request_id_generator.get());

        self.send_event(PlayerEvent::PlayRequestIdChanged { play_request_id });

        if !self.config.gapless && self.crossfade().is_zero() {
            self.ensure_sink_stopped(play);
        }

        if matches!(self.state, PlayerState::Invalid) {
            return Err(Error::internal(format!(
                "Player::handle_command_load called from invalid state: {:?}",
                self.state
            )));
        }

        if self.is_adopting(&track_id, position_ms) {
            self.adopt(track_id, play_request_id);
            return Ok(());
        }
        self.adopting = None;
        if play {
            self.begin_skip_crossfade(&track_id);
        } else {
            self.drop_crossfade();
        }

        // Now we check at different positions whether we already have a pre-loaded version
        // of this track somewhere. If so, use it and return.

        // Check if there's a matching loaded track in the EndOfTrack player state.
        // This is the case if we're repeating the same track again.
        if let PlayerState::EndOfTrack {
            track_id: previous_track_id,
            ..
        } = &self.state
        {
            if *previous_track_id == track_id {
                let mut loaded_track = match mem::replace(&mut self.state, PlayerState::Invalid) {
                    PlayerState::EndOfTrack { loaded_track, .. } => loaded_track,
                    _ => {
                        return Err(Error::internal(format!(
                            "PlayerInternal::handle_command_load repeating the same track: invalid state: {:?}",
                            self.state
                        )));
                    }
                };

                if position_ms != loaded_track.stream_position_ms {
                    // This may be blocking.
                    loaded_track.stream_position_ms = loaded_track.decoder.seek(position_ms)?;
                }
                self.preload = PlayerPreload::None;
                self.start_playback(track_id, play_request_id, loaded_track, play);
                if let PlayerState::Invalid = self.state {
                    return Err(Error::internal(format!(
                        "PlayerInternal::handle_command_load repeating the same track: start_playback() did not transition to valid player state: {:?}",
                        self.state
                    )));
                }
                return Ok(());
            }
        }

        // Check if we are already playing the track. If so, just do a seek and update our info.
        if let PlayerState::Playing {
            track_id: ref current_track_id,
            ref mut stream_position_ms,
            ref mut decoder,
            ..
        }
        | PlayerState::Paused {
            track_id: ref current_track_id,
            ref mut stream_position_ms,
            ref mut decoder,
            ..
        } = self.state
        {
            if *current_track_id == track_id {
                // we can use the current decoder. Ensure it's at the correct position.
                if position_ms != *stream_position_ms {
                    // This may be blocking.
                    *stream_position_ms = decoder.seek(position_ms)?;
                }

                // Move the info from the current state into a PlayerLoadedTrackData so we can use
                // the usual code path to start playback.
                let old_state = mem::replace(&mut self.state, PlayerState::Invalid);

                if let PlayerState::Playing {
                    stream_position_ms,
                    decoder,
                    audio_item,
                    stream_loader_controller,
                    bytes_per_second,
                    duration_ms,
                    normalisation_data,
                    is_explicit,
                    ..
                }
                | PlayerState::Paused {
                    stream_position_ms,
                    decoder,
                    audio_item,
                    stream_loader_controller,
                    bytes_per_second,
                    duration_ms,
                    normalisation_data,
                    is_explicit,
                    ..
                } = old_state
                {
                    let loaded_track = PlayerLoadedTrackData {
                        decoder,
                        normalisation_data,
                        stream_loader_controller,
                        audio_item,
                        bytes_per_second,
                        duration_ms,
                        stream_position_ms,
                        is_explicit,
                    };

                    self.preload = PlayerPreload::None;
                    self.start_playback(track_id, play_request_id, loaded_track, play);

                    if let PlayerState::Invalid = self.state {
                        return Err(Error::internal(format!(
                            "PlayerInternal::handle_command_load already playing this track: start_playback() did not transition to valid player state: {:?}",
                            self.state
                        )));
                    }

                    return Ok(());
                } else {
                    return Err(Error::internal(format!(
                        "PlayerInternal::handle_command_load already playing this track: invalid state: {:?}",
                        self.state
                    )));
                }
            }
        }

        // Check if the requested track has been preloaded already. If so use the preloaded data.
        if let PlayerPreload::Ready {
            track_id: loaded_track_id,
            ..
        } = &self.preload
        {
            if track_id == *loaded_track_id {
                let preload = std::mem::replace(&mut self.preload, PlayerPreload::None);
                if let PlayerPreload::Ready {
                    track_id,
                    mut loaded_track,
                } = preload
                {
                    if position_ms != loaded_track.stream_position_ms {
                        // This may be blocking
                        loaded_track.stream_position_ms = loaded_track.decoder.seek(position_ms)?;
                    }
                    self.start_playback(track_id, play_request_id, *loaded_track, play);
                    return Ok(());
                } else {
                    return Err(Error::internal(format!(
                        "PlayerInternal::handle_command_loading preloaded track: invalid state: {:?}",
                        self.state
                    )));
                }
            }
        }

        self.send_event(PlayerEvent::Loading {
            track_id: track_id.clone(),
            play_request_id,
            position_ms,
        });

        // Try to extract a pending loader from the preloading mechanism
        let loader = if let PlayerPreload::Loading {
            track_id: loaded_track_id,
            ..
        } = &self.preload
        {
            if (track_id == *loaded_track_id) && (position_ms == 0) {
                let mut preload = PlayerPreload::None;
                std::mem::swap(&mut preload, &mut self.preload);
                if let PlayerPreload::Loading { loader, .. } = preload {
                    Some(loader)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        self.preload = PlayerPreload::None;

        // If we don't have a loader yet, create one from scratch.
        let loader =
            loader.unwrap_or_else(|| Box::pin(self.load_track(track_id.clone(), position_ms)));

        // Set ourselves to a loading state.
        self.state = PlayerState::Loading {
            track_id,
            play_request_id,
            start_playback: play,
            loader,
        };

        Ok(())
    }

    fn handle_command_preload(&mut self, track_id: SpotifyUri) {
        debug!("Preloading track");
        let mut preload_track = true;
        // check whether the track is already loaded somewhere or being loaded.
        if let PlayerPreload::Loading {
            track_id: currently_loading,
            ..
        }
        | PlayerPreload::Ready {
            track_id: currently_loading,
            ..
        } = &self.preload
        {
            if *currently_loading == track_id {
                // we're already preloading the requested track.
                preload_track = false;
            } else {
                // we're preloading something else - cancel it.
                self.preload = PlayerPreload::None;
            }
        }

        if let PlayerState::Playing {
            track_id: current_track_id,
            ..
        }
        | PlayerState::Paused {
            track_id: current_track_id,
            ..
        }
        | PlayerState::EndOfTrack {
            track_id: current_track_id,
            ..
        } = &self.state
        {
            if *current_track_id == track_id {
                // we already have the requested track loaded.
                preload_track = false;
            }
        }

        // schedule the preload of the current track if desired.
        if preload_track {
            let loader = self.load_track(track_id.clone(), 0);
            self.preload = PlayerPreload::Loading {
                track_id,
                loader: Box::pin(loader),
            }
        }
    }

    fn handle_command_seek(&mut self, position_ms: u32) -> PlayerResult {
        self.drop_crossfade();
        // When we are still loading, the user may immediately ask to
        // seek to another position yet the decoder won't be ready for
        // that. In this case just restart the loading process but
        // with the requested position.
        if let PlayerState::Loading {
            ref track_id,
            play_request_id,
            start_playback,
            ..
        } = self.state
        {
            return self.handle_command_load(
                track_id.clone(),
                Some(play_request_id),
                start_playback,
                position_ms,
            );
        }

        if let Some(decoder) = self.state.decoder() {
            match decoder.seek(position_ms) {
                Ok(new_position_ms) => {
                    if let PlayerState::Playing {
                        ref mut stream_position_ms,
                        ref track_id,
                        play_request_id,
                        ..
                    }
                    | PlayerState::Paused {
                        ref mut stream_position_ms,
                        ref track_id,
                        play_request_id,
                        ..
                    } = self.state
                    {
                        *stream_position_ms = new_position_ms;

                        self.send_event(PlayerEvent::Seeked {
                            play_request_id,
                            track_id: track_id.clone(),
                            position_ms: new_position_ms,
                        });
                    }
                }
                Err(e) => error!("PlayerInternal::handle_command_seek error: {e}"),
            }
        } else {
            error!("Player::seek called from invalid state: {:?}", self.state);
        }

        // ensure we have a bit of a buffer of downloaded data
        self.preload_data_before_playback()?;

        if let PlayerState::Playing {
            ref mut reported_nominal_start_time,
            ..
        } = self.state
        {
            *reported_nominal_start_time =
                Instant::now().checked_sub(Duration::from_millis(position_ms as u64));
        }

        Ok(())
    }

    fn handle_command(&mut self, cmd: PlayerCommand) -> PlayerResult {
        debug!("command={cmd:?}");
        match cmd {
            PlayerCommand::Load {
                track_id,
                play,
                position_ms,
            } => self.handle_command_load(track_id, None, play, position_ms)?,

            PlayerCommand::Preload { track_id } => self.handle_command_preload(track_id),

            PlayerCommand::Seek(position_ms) => self.handle_command_seek(position_ms)?,

            PlayerCommand::Play => self.handle_play(),

            PlayerCommand::Pause => self.handle_pause(),

            PlayerCommand::Stop => self.handle_player_stop(),

            PlayerCommand::SetSession(session) => self.session = session,

            PlayerCommand::AddEventSender(sender) => self.event_senders.push(sender),

            PlayerCommand::SetSinkEventCallback(callback) => self.sink_event_callback = callback,

            PlayerCommand::EmitVolumeChangedEvent(volume) => {
                self.send_event(PlayerEvent::VolumeChanged { volume })
            }

            PlayerCommand::EmitRepeatChangedEvent { context, track } => {
                self.send_event(PlayerEvent::RepeatChanged { context, track })
            }

            PlayerCommand::EmitShuffleChangedEvent(shuffle) => {
                self.send_event(PlayerEvent::ShuffleChanged { shuffle })
            }

            PlayerCommand::EmitAutoPlayChangedEvent(auto_play) => {
                self.send_event(PlayerEvent::AutoPlayChanged { auto_play })
            }

            PlayerCommand::EmitSessionClientChangedEvent {
                client_id,
                client_name,
                client_brand_name,
                client_model_name,
            } => self.send_event(PlayerEvent::SessionClientChanged {
                client_id,
                client_name,
                client_brand_name,
                client_model_name,
            }),

            PlayerCommand::EmitSessionConnectedEvent {
                connection_id,
                user_name,
            } => self.send_event(PlayerEvent::SessionConnected {
                connection_id,
                user_name,
            }),

            PlayerCommand::EmitSessionDisconnectedEvent {
                connection_id,
                user_name,
            } => self.send_event(PlayerEvent::SessionDisconnected {
                connection_id,
                user_name,
            }),

            PlayerCommand::SetAutoNormaliseAsAlbum(setting) => {
                self.auto_normalise_as_album = setting
            }

            PlayerCommand::SetCrossfade(crossfade) => {
                self.crossfade = crossfade.min(CROSSFADE_MAX)
            }
            PlayerCommand::SetCrossfadePlan(plan) => {
                self.plan = plan.map(CrossfadePlan::clamped)
            }
            PlayerCommand::EmitFilterExplicitContentChangedEvent(filter) => {
                self.send_event(PlayerEvent::FilterExplicitContentChanged { filter });

                if filter {
                    if let PlayerState::Playing {
                        ref track_id,
                        play_request_id,
                        is_explicit,
                        ..
                    }
                    | PlayerState::Paused {
                        ref track_id,
                        play_request_id,
                        is_explicit,
                        ..
                    } = self.state
                    {
                        let track_id = track_id.clone();

                        if is_explicit {
                            warn!(
                                "Currently loaded track is explicit, which client setting forbids -- skipping to next track."
                            );
                            self.send_event(PlayerEvent::EndOfTrack {
                                track_id,
                                play_request_id,
                            })
                        }
                    }
                }
            }
        };

        Ok(())
    }

    fn send_event(&mut self, event: PlayerEvent) {
        self.event_senders
            .retain(|sender| sender.send(event.clone()).is_ok());
    }

    fn load_track(
        &mut self,
        spotify_uri: SpotifyUri,
        position_ms: u32,
    ) -> impl FusedFuture<Output = Result<PlayerLoadedTrackData, LoadError>> + Send + 'static {
        // This method creates a future that returns the loaded stream and associated info.
        // Ideally all work should be done using asynchronous code. However, seek() on the
        // audio stream is implemented in a blocking fashion. Thus, we can't turn it into future
        // easily. Instead we spawn a thread to do the work and return a one-shot channel as the
        // future to work with.

        let loader = PlayerTrackLoader {
            session: self.session.clone(),
            config: self.config.clone(),
            local_file_lookup: self.local_file_lookup.clone(),
        };

        let (result_tx, result_rx) = oneshot::channel();

        let load_handles_clone = self.load_handles.clone();
        let handle = tokio::runtime::Handle::current();

        let load_handle = thread::spawn(move || {
            let data = handle.block_on(loader.load_track(spotify_uri, position_ms));
            let _ = result_tx.send(data);

            let mut load_handles = load_handles_clone.lock().expect(LOAD_HANDLES_POISON_MSG);
            load_handles.remove(&thread::current().id());
        });

        let mut load_handles = self.load_handles.lock().expect(LOAD_HANDLES_POISON_MSG);
        load_handles.insert(load_handle.thread().id(), load_handle);

        result_rx
            .map_err(|_| LoadError::Unavailable)
            .and_then(future::ready)
    }

    fn preload_data_before_playback(&mut self) -> PlayerResult {
        if let PlayerState::Playing {
            bytes_per_second,
            ref mut stream_loader_controller,
            ..
        } = self.state
        {
            let read_ahead_during_playback = AudioFetchParams::get().read_ahead_during_playback;
            // Request our read ahead range
            let request_data_length =
                (read_ahead_during_playback.as_secs_f32() * bytes_per_second as f32) as usize;

            // Request the part we want to wait for blocking. This effectively means we wait for the previous request to partially complete.
            let wait_for_data_length =
                (read_ahead_during_playback.as_secs_f32() * bytes_per_second as f32) as usize;

            stream_loader_controller.fetch_next_and_wait(request_data_length, wait_for_data_length)
        } else {
            Ok(())
        }
    }
}

impl Drop for PlayerInternal {
    fn drop(&mut self) {
        debug!("drop PlayerInternal[{}]", self.player_id);

        let handles: Vec<thread::JoinHandle<()>> = {
            // waiting for the thread while holding the mutex would result in a deadlock
            let mut load_handles = self.load_handles.lock().expect(LOAD_HANDLES_POISON_MSG);

            load_handles
                .drain()
                .map(|(_thread_id, handle)| handle)
                .collect()
        };

        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl fmt::Debug for PlayerCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlayerCommand::Load {
                track_id,
                play,
                position_ms,
                ..
            } => f
                .debug_tuple("Load")
                .field(&track_id)
                .field(&play)
                .field(&position_ms)
                .finish(),
            PlayerCommand::Preload { track_id } => {
                f.debug_tuple("Preload").field(&track_id).finish()
            }
            PlayerCommand::Play => f.debug_tuple("Play").finish(),
            PlayerCommand::Pause => f.debug_tuple("Pause").finish(),
            PlayerCommand::Stop => f.debug_tuple("Stop").finish(),
            PlayerCommand::Seek(position) => f.debug_tuple("Seek").field(&position).finish(),
            PlayerCommand::SetSession(_) => f.debug_tuple("SetSession").finish(),
            PlayerCommand::AddEventSender(_) => f.debug_tuple("AddEventSender").finish(),
            PlayerCommand::SetSinkEventCallback(_) => {
                f.debug_tuple("SetSinkEventCallback").finish()
            }
            PlayerCommand::EmitVolumeChangedEvent(volume) => f
                .debug_tuple("EmitVolumeChangedEvent")
                .field(&volume)
                .finish(),
            PlayerCommand::SetAutoNormaliseAsAlbum(setting) => f
                .debug_tuple("SetAutoNormaliseAsAlbum")
                .field(&setting)
                .finish(),
            PlayerCommand::SetCrossfade(crossfade) => {
                f.debug_tuple("SetCrossfade").field(&crossfade).finish()
            }
            PlayerCommand::SetCrossfadePlan(plan) => f
                .debug_tuple("SetCrossfadePlan")
                .field(&plan.as_ref().map(|plan| plan.duration))
                .finish(),
            PlayerCommand::EmitFilterExplicitContentChangedEvent(filter) => f
                .debug_tuple("EmitFilterExplicitContentChangedEvent")
                .field(&filter)
                .finish(),
            PlayerCommand::EmitSessionConnectedEvent {
                connection_id,
                user_name,
            } => f
                .debug_tuple("EmitSessionConnectedEvent")
                .field(&connection_id)
                .field(&user_name)
                .finish(),
            PlayerCommand::EmitSessionDisconnectedEvent {
                connection_id,
                user_name,
            } => f
                .debug_tuple("EmitSessionDisconnectedEvent")
                .field(&connection_id)
                .field(&user_name)
                .finish(),
            PlayerCommand::EmitSessionClientChangedEvent {
                client_id,
                client_name,
                client_brand_name,
                client_model_name,
            } => f
                .debug_tuple("EmitSessionClientChangedEvent")
                .field(&client_id)
                .field(&client_name)
                .field(&client_brand_name)
                .field(&client_model_name)
                .finish(),
            PlayerCommand::EmitShuffleChangedEvent(shuffle) => f
                .debug_tuple("EmitShuffleChangedEvent")
                .field(&shuffle)
                .finish(),
            PlayerCommand::EmitRepeatChangedEvent { context, track } => f
                .debug_tuple("EmitRepeatChangedEvent")
                .field(&context)
                .field(&track)
                .finish(),
            PlayerCommand::EmitAutoPlayChangedEvent(auto_play) => f
                .debug_tuple("EmitAutoPlayChangedEvent")
                .field(&auto_play)
                .finish(),
        }
    }
}

impl fmt::Debug for PlayerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PlayerState::*;
        match self {
            Stopped => f.debug_struct("Stopped").finish(),
            Loading {
                track_id,
                play_request_id,
                ..
            } => f
                .debug_struct("Loading")
                .field("track_id", &track_id)
                .field("play_request_id", &play_request_id)
                .finish(),
            Paused {
                track_id,
                play_request_id,
                ..
            } => f
                .debug_struct("Paused")
                .field("track_id", &track_id)
                .field("play_request_id", &play_request_id)
                .finish(),
            Playing {
                track_id,
                play_request_id,
                ..
            } => f
                .debug_struct("Playing")
                .field("track_id", &track_id)
                .field("play_request_id", &play_request_id)
                .finish(),
            EndOfTrack {
                track_id,
                play_request_id,
                ..
            } => f
                .debug_struct("EndOfTrack")
                .field("track_id", &track_id)
                .field("play_request_id", &play_request_id)
                .finish(),
            Invalid => f.debug_struct("Invalid").finish(),
        }
    }
}

struct Subfile<T: Read + Seek> {
    stream: T,
    offset: u64,
    length: u64,
}

impl<T: Read + Seek> Subfile<T> {
    pub fn new(mut stream: T, offset: u64, length: u64) -> Result<Subfile<T>, io::Error> {
        let target = SeekFrom::Start(offset);
        stream.seek(target)?;

        Ok(Subfile {
            stream,
            offset,
            length,
        })
    }
}

impl<T: Read + Seek> Read for Subfile<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl<T: Read + Seek> Seek for Subfile<T> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let pos = match pos {
            SeekFrom::Start(offset) => SeekFrom::Start(offset + self.offset),
            SeekFrom::End(offset) => {
                if (self.length as i64 - offset) < self.offset as i64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "newpos would be < self.offset",
                    ));
                }
                pos
            }
            _ => pos,
        };

        let newpos = self.stream.seek(pos)?;
        Ok(newpos - self.offset)
    }
}

impl<R> MediaSource for Subfile<R>
where
    R: Read + Seek + Send + Sync,
{
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.length)
    }
}

#[cfg(test)]
mod tests {
    use std::f64::consts::FRAC_1_SQRT_2;
    use std::time::Duration;

    use super::{
        AudioPacket, AudioPacketPosition, BassShelf, BASS_SWAP_DEPTH_DB, BASS_SWAP_HZ,
        CROSSFADE_MAX, CrossfadePlan, Decoder, Deck, NUM_CHANNELS, Outgoing, PROBE_STEP_SAMPLES,
        Ramp, SAMPLE_RATE, Tail, apply_fade_in, crossfade_frames, curve_fits_overlap, mix_tail,
        mix_tail_with_bass, probe_step,
    };
    use super::IncomingCurve;
    use std::sync::Arc;
    use crate::decoder::{AudioDecoder, DecoderError, DecoderResult};
    use super::{LoadError, PlayerEvent, PlayerTrackLoader};
    use crate::core::{Error, SpotifyUri, audio_key::AudioKeyError};


    #[test]
    fn only_an_explicit_audio_key_rejection_is_terminal() {
        let rejected = Error::unavailable(AudioKeyError::AesKey);
        let timeout = Error::aborted(AudioKeyError::Timeout);

        assert!(PlayerTrackLoader::is_audio_key_unavailable(&rejected));
        assert!(!PlayerTrackLoader::is_audio_key_unavailable(&timeout));
        assert_eq!(
            LoadError::after_decoder_failure(true),
            LoadError::AudioKeyUnavailable
        );
        assert_eq!(
            LoadError::after_decoder_failure(false),
            LoadError::Unavailable
        );
    }

    #[test]
    fn audio_key_event_keeps_its_play_request_id() {
        let event = PlayerEvent::AudioKeyUnavailable {
            play_request_id: 42,
            track_id: SpotifyUri::from_uri("spotify:track:14XWXWv5FoCbFzLksawpEe").unwrap(),
        };

        assert_eq!(event.get_play_request_id(), Some(42));
    }


    struct StubDecoder {
        packets: Vec<Vec<f64>>,
    }

    impl AudioDecoder for StubDecoder {
        fn seek(&mut self, position_ms: u32) -> Result<u32, DecoderError> {
            Ok(position_ms)
        }

        fn next_packet(&mut self) -> DecoderResult<Option<(AudioPacketPosition, AudioPacket)>> {
            if self.packets.is_empty() {
                return Ok(None);
            }
            let samples = self.packets.remove(0);
            Ok(Some((
                AudioPacketPosition {
                    position_ms: 0,
                    skipped: false,
                },
                AudioPacket::Samples(samples),
            )))
        }
    }

    /// A tone at `hz`, as one packet of `frames` stereo frames.
    fn tone(hz: f64, frames: usize) -> Vec<f64> {
        tone_from(hz, frames, 0)
    }

    /// The same tone, starting at `offset` frames into its cycle.
    ///
    /// A decoder hands out several packets and the deck plays them back to
    /// back, so each packet has to continue the previous one's phase. A tone
    /// that restarts at zero every packet has a discontinuity at every
    /// boundary, which destroys any measurement that correlates against it.
    fn tone_from(hz: f64, frames: usize, offset: usize) -> Vec<f64> {
        let mut out = Vec::with_capacity(frames * NUM_CHANNELS as usize);
        for frame in 0..frames {
            let t = (offset + frame) as f64;
            let value = (2.0 * std::f64::consts::PI * hz * t / f64::from(SAMPLE_RATE)).sin();
            for _ in 0..NUM_CHANNELS {
                out.push(value);
            }
        }
        out
    }

    /// A run of packets whose phases continue across the boundaries.
    fn continuous_tone(hz: f64, packets: usize, frames_each: usize) -> Vec<Vec<f64>> {
        (0..packets)
            .map(|packet| tone_from(hz, frames_each, packet * frames_each))
            .collect()
    }

    /// Counting zero crossings gives the tone's frequency, which is the whole
    /// point of keylock: the deck plays the tail faster, so the same audio
    /// arrives sooner, but the pitch the listener hears must not move.
    fn dominant_frequency(samples: &[f64]) -> f64 {
        let mono: Vec<f64> = samples.chunks(NUM_CHANNELS as usize).map(|f| f[0]).collect();
        // The tail of the buffer is dropped: the deck pads its end with
        // silence once the track is behind it, and zero crossings through
        // silence say nothing about pitch. The head is kept, because the deck
        // is settled before it is handed over and must start at full level —
        // see `a_deck_starts_at_full_level`.
        let skip = (mono.len() / 4).max(1);
        let body = &mono[..mono.len().saturating_sub(skip)];
        let crossings = body
            .windows(2)
            .filter(|pair| (pair[0] < 0.0) != (pair[1] < 0.0))
            .count();
        crossings as f64 * f64::from(SAMPLE_RATE) / (2.0 * body.len() as f64)
    }

    /// The bug this covers: the moment a transition fires, the outgoing
    /// track stops coming from the plain decode path and starts coming out of
    /// the keylock engine. The engine's pipeline is about 12.7 ms long and its
    /// stages start cold, so its first frames were silence — measured as
    /// 0.0000 rms for the first ten milliseconds, which a listener hears as
    /// the track being cut for an instant.
    ///
    /// The deck is settled before it is handed over, so the first frame the
    /// mix reads is already at full level.

    #[test]
    fn a_deck_starts_at_full_level() {
        let decoder: Box<dyn AudioDecoder + Send> = Box::new(StubDecoder {
            packets: vec![tone(220.0, 44_100 * 2)],
        });
        let mut deck = match Deck::new(decoder, 1.06, 1.0, 44_100, 1.06) {
            Ok(deck) => deck,
            Err(_) => panic!("the engine builds"),
        };

        // 5 ms buckets across the first 50 ms.
        let block = 220;
        let mut levels = Vec::new();
        for _ in 0..10 {
            let samples = deck.take(block * NUM_CHANNELS as usize);
            let energy: f64 = samples.iter().map(|sample| sample * sample).sum();
            levels.push((energy / samples.len() as f64).sqrt());
        }
        let steady = levels.iter().sum::<f64>() / levels.len() as f64;
        assert!(steady > 0.1, "the deck produced no audio to measure");

        // Every bucket must be near the steady level: a silent or half-level
        // opening is the click this guards against.
        for (index, level) in levels.iter().enumerate() {
            assert!(
                *level > steady * 0.5,
                "the deck was at {level:.4} rms in bucket {index} (steady {steady:.4}); \
                 the transition would be heard as a cut"
            );
        }
    }

    /// The deck must hand back the overlap's worth of audio, at the requested
    /// rate and with the pitch held. A deck that stretched the pitch along
    /// with the tempo would be a tape deck, not a keylocked one.
    #[test]
    fn the_keylocked_tail_changes_tempo_and_keeps_pitch() {
        let hz = 220.0;
        // Enough audio that the stretched read stays inside the track: the
        // overlap asks the engine for more source frames than it plays.
        let frames = 16_384usize;
        let overlap = 4_000u64;
        let rate = 1.06;
        let decoder: Box<dyn AudioDecoder + Send> = Box::new(StubDecoder {
            packets: vec![tone(hz, frames)],
        });
        let mut deck = match Deck::new(decoder, rate, 1.0, overlap, rate) {
            Ok(deck) => deck,
            Err(_) => panic!("the engine builds"),
        };

        // Read the overlap's worth, as the mix does across the ramp.
        let wanted = overlap as usize * NUM_CHANNELS as usize;
        let mut rendered: Vec<f64> = Vec::with_capacity(wanted);
        while rendered.len() < wanted {
            rendered.extend(deck.take(NUM_CHANNELS as usize * 256));
        }
        rendered.truncate(wanted);

        let heard = dominant_frequency(&rendered);
        assert!(
            (heard - hz).abs() < 8.0,
            "keylock moved the pitch: heard {heard:.1} Hz for a {hz:.0} Hz tone"
        );
        assert_eq!(
            deck.controller.underrun_frames(),
            0,
            "the feed fell behind, leaving a gap in the overlap"
        );
        // That the deck produced real audio for the whole overlap, rather
        // than silence, is what makes the pitch reading above meaningful.
        assert!(
            rendered.iter().any(|sample| sample.abs() > 1e-6),
            "the deck rendered no audio, so the pitch reading proves nothing"
        );
        // The rate's own direction is not asserted here. `source_position`
        // reports a figure that does not line up with the crate's contract
        // (`rate` source frames per output frame), so it cannot settle the
        // direction; see `Param::TempoRate` in the crate's control.rs, which
        // says 1.02 is 2% faster.
    }

    /// A shared transition starts the tail at its own tempo, so a check on
    /// the *starting* rate alone would decide the engine is not worth
    /// building — and then the sweep would have nowhere to happen, because a
    /// plain tail ignores every retarget. That was a real bug: the incoming
    /// deck would have swept while the outgoing one stood still, and the two
    /// would have drifted apart.
    #[test]
    fn a_swept_tail_is_keylocked_even_though_it_starts_at_its_own_tempo() {
        let decoder: Box<dyn AudioDecoder + Send> =
            Box::new(StubDecoder { packets: vec![tone(220.0, 44_100)] });
        // Starts at 1.0, ends at 1.25: the shared-transition shape.
        let outgoing = Outgoing::new(decoder, 1.0, 4_096, 1.0, Some(1.25));
        assert!(
            matches!(outgoing.tail, Tail::Stretched(_)),
            "a tail that is swept must have a deck to sweep"
        );
    }


    /// The comparison that decides whether a rendered curve is used at all.
    ///
    /// Getting this wrong is silent: the transition still fires, it just
    /// falls back to stretching one deck, and the only evidence is a listener
    /// saying it sounds wrong. Measured on a real boundary, the rounding is
    /// 40 frames on a 6.6-second overlap — so the tolerance is not slack for
    /// tidiness, it is the difference between the feature working and not.
    #[test]
    fn a_curve_is_matched_to_its_overlap_within_the_rounding() {
        let curve = |frames: usize| IncomingCurve {
            samples: Arc::new(vec![0.0; frames * NUM_CHANNELS as usize]),
            consumed_ms: 1_000,
            ratio: 1.25,
        };
        let overlap = 6_624u64;
        assert!(curve_fits_overlap(&curve(overlap as usize), overlap));
        // The planner's unrounded duration against the player's truncated
        // milliseconds: 40 frames on this overlap.
        assert!(
            curve_fits_overlap(&curve(overlap as usize + 40), overlap),
            "the rounding must not discard a curve that fits"
        );
        assert!(curve_fits_overlap(&curve(overlap as usize - 40), overlap));
        // A curve rendered for a different boundary is still refused: the
        // shortest overlap the planner makes is a second and a half.
        assert!(
            !curve_fits_overlap(&curve(overlap as usize + 4_410), overlap),
            "a curve for another boundary must not be used"
        );
        assert!(!curve_fits_overlap(
            &curve(overlap as usize / 2),
            overlap
        ));
    }

    /// The whole point of the sweep, on the outgoing side: retargeting the
    /// deck must actually change the tempo it plays at, or the pair would
    /// stop being locked the moment the incoming deck moved.
    #[test]
    fn a_stretched_tail_follows_its_retargets() {
        let decoder: Box<dyn AudioDecoder + Send> = Box::new(StubDecoder {
            packets: continuous_tone(220.0, 64, 4_096),
        });
        let outgoing = Outgoing::new(decoder, 1.0, 8_192, 1.0, Some(1.25));
        let Tail::Stretched(deck) = &outgoing.tail else {
            panic!("a swept tail is stretched");
        };
        // Retargeting is a no-op on a plain tail, so this both exercises the
        // path and asserts it exists.
        deck.set_rate(1.25);
        assert!(
            (deck.controller.tempo_rate_target() - 1.25).abs() < 1e-9,
            "the deck ignored the retarget: target is {}",
            deck.controller.tempo_rate_target()
        );
    }

    /// A decoder that hands out its audio in small packets, so a step can be
    /// told apart by how much it consumed rather than by one big read.
    struct DripDecoder {
        packets: Vec<Vec<f64>>,
    }

    impl AudioDecoder for DripDecoder {
        fn seek(&mut self, position_ms: u32) -> Result<u32, DecoderError> {
            Ok(position_ms)
        }

        fn next_packet(&mut self) -> DecoderResult<Option<(AudioPacketPosition, AudioPacket)>> {
            if self.packets.is_empty() {
                return Ok(None);
            }
            let samples = self.packets.remove(0);
            Ok(Some((
                AudioPacketPosition {
                    position_ms: 0,
                    skipped: false,
                },
                AudioPacket::Samples(samples),
            )))
        }
    }

    /// The bug this covers: the whole probe used to be read in one pass from
    /// the loop that also decodes the *playing* track. A 90-second probe took
    /// longer than the sink's queue holds, so the queue ran dry and the
    /// listener heard the transition as a gap (measured: 623 ms of silence).
    ///
    /// Each step must therefore be bounded by `PROBE_STEP_SAMPLES`, so the
    /// loop gets back to the playing track promptly.
    #[test]
    fn a_probe_reads_a_bounded_step_at_a_time() {
        let packet_frames = 2_048;
        let packets: Vec<Vec<f64>> = (0..40).map(|_| tone(440.0, packet_frames)).collect();
        let mut decoder: Decoder = Box::new(DripDecoder { packets });

        let mut samples: Vec<f32> = Vec::new();
        let mut steps = 0;
        let mut last = 0usize;
        while probe_step(&mut decoder, &mut samples) {
            steps += 1;
            // The bound is per step: no single pass may outlast the sink's
            // queue, which is what keeps the playing track fed.
            let gained = samples.len() - last;
            last = samples.len();
            assert!(
                gained <= PROBE_STEP_SAMPLES,
                "step {steps} read {gained} samples, past the {PROBE_STEP_SAMPLES} bound"
            );
            assert!(steps < 1_000, "the probe must terminate");
        }
        assert!(steps > 1, "a 90s probe cannot be one step");
        assert!(!samples.is_empty(), "the probe read nothing");
    }

    /// A track shorter than the probe must end the read rather than spin.
    #[test]
    fn a_probe_stops_when_the_track_ends() {
        let mut decoder: Decoder = Box::new(DripDecoder {
            packets: vec![tone(440.0, 1_024)],
        });
        let mut samples: Vec<f32> = Vec::new();
        let mut steps = 0;
        while probe_step(&mut decoder, &mut samples) {
            steps += 1;
            assert!(steps < 10, "a spent track must end the probe");
        }
        assert!(!samples.is_empty(), "the opening was still read");
    }

    /// The amplitude of `hz` in `samples`, by correlation.
    ///
    /// Two tones at different frequencies are orthogonal over a long enough
    /// window, so this reads one deck's contribution out of the mix without
    /// the other interfering — which a plain level cannot do.
    fn component_at(samples: &[f64], hz: f64) -> f64 {
        let channels = NUM_CHANNELS as usize;
        let frames = samples.len() / channels;
        if frames == 0 {
            return 0.0;
        }
        let sum: f64 = samples
            .chunks(channels)
            .enumerate()
            .map(|(index, frame)| {
                let phase = std::f64::consts::TAU * hz * index as f64 / f64::from(SAMPLE_RATE);
                frame[0] * phase.sin()
            })
            .sum();
        2.0 * sum / frames as f64
    }

    /// The bug this covers: the bass blend used the output buffer's contents
    /// as its "dry" signal. During a crossfade that buffer already holds the
    /// incoming track, so the blend folded the two together and the outgoing
    /// track's own level collapsed — heard as the previous track suddenly
    /// sounding small, separately from the fade.
    ///
    /// It only bites while the sweep is still running: once the blend reaches
    /// 1.0 both the broken and the correct arithmetic reduce to the low-cut
    /// tail, which is why this measures the start of the overlap.
    ///
    /// The two decks are given different amplitudes as well as different
    /// frequencies. With equal amplitudes the broken arithmetic is
    /// indistinguishable from the correct one, which is how an earlier
    /// version of this test passed with the bug still in place.
    #[test]
    fn the_outgoing_track_keeps_its_level_under_an_incoming_one() {
        const OUTGOING_HZ: f64 = 5_000.0;
        const INCOMING_HZ: f64 = 8_000.0;
        // The incoming track arrives quieter than the outgoing one, as it
        // does at the start of an overlap. Equal levels would hide the fault.
        const INCOMING_GAIN: f64 = 0.3;

        let crossfade_frames = u64::from(SAMPLE_RATE) * 5;
        let mut outgoing = Outgoing::new(
            Box::new(StubDecoder {
                packets: continuous_tone(OUTGOING_HZ, 400, 4_096),
            }),
            1.0,
            crossfade_frames,
            1.0,
            None,
        );

        let buffer_frames = 22_050usize;
        let mut mixed: Vec<f64> = tone(INCOMING_HZ, buffer_frames)
            .iter()
            .map(|sample| sample * INCOMING_GAIN)
            .collect();
        mix_tail_with_bass(&mut mixed, &mut outgoing);

        let heard = component_at(&mixed, OUTGOING_HZ);
        // The shelf is transparent this far above its corner and the fade has
        // barely started, so the outgoing track must arrive at its own level.
        assert!(
            heard > 0.75,
            "the outgoing track arrived at {heard:.4} rms against an incoming one at \
             {INCOMING_GAIN}; it started at full level and the fade has barely begun"
        );
        assert!(
            heard < 1.3,
            "the outgoing track arrived at {heard:.4}, louder than it is"
        );
    }

    /// The two decks must sum to constant power across the whole overlap,
    /// which is what stops the middle of a transition sounding dipped. This
    /// checks the composite rather than either curve on its own, because a
    /// ramp pair that is individually right can still be mismatched in
    /// length and leave a hole.
    #[test]
    fn the_two_decks_hold_constant_power_across_the_overlap() {
        let frames = 1000u64;
        let mut out = Ramp::new(frames);
        let mut incoming = Ramp::new(frames);
        let mut lowest = f64::MAX;
        for _ in 0..frames {
            let power = out.out_gain().powi(2) + incoming.in_gain().powi(2);
            lowest = lowest.min(power);
            out.advance();
            incoming.advance();
        }
        assert!(
            (lowest - 1.0).abs() < 1e-9,
            "power fell to {lowest} during the overlap"
        );
        // Both ramps finish together, so neither deck is left playing alone
        // past the overlap the other was planned for.
        assert!(out.finished() && incoming.finished());
    }

    /// The fade-in must cover the overlap and not end early, or the incoming
    /// track stays quiet for part of it and then jumps to full level.
    #[test]
    fn the_fade_in_covers_every_frame_of_the_overlap() {
        let frames = 64usize;
        let mut ramp = Ramp::new(frames as u64);
        let channels = crate::NUM_CHANNELS as usize;
        let mut samples = vec![1.0f64; frames * channels];
        apply_fade_in(&mut samples, &mut ramp);
        assert!(ramp.finished(), "the ramp must consume the whole overlap");
        // Silent at the first frame, full at the last, rising throughout.
        assert!(samples[0].abs() < 1e-9);
        assert!((samples[frames * channels - 1] - 1.0).abs() < 1e-9);
        let gains: Vec<f64> = samples.chunks(channels).map(|f| f[0]).collect();
        assert!(gains.windows(2).all(|pair| pair[1] >= pair[0]));
    }

    /// A ramp pair of different lengths would leave one deck playing after
    /// the other stopped. The overlap is one number for both.
    #[test]
    fn both_decks_are_built_with_the_same_overlap_length() {
        let frames = crossfade_frames(Duration::from_secs(6));
        let out = Ramp::new(frames);
        let incoming = Ramp::new(frames);
        assert_eq!(out.total, incoming.total);
    }

    /// The shelf must actually take the low end out, or the swap does
    /// nothing and both tracks fight over the bass.
    #[test]
    fn the_bass_shelf_cuts_the_low_end() {
        let rate = f64::from(crate::SAMPLE_RATE);
        let low = 60.0;
        let shelf = BassShelf::new(BASS_SWAP_HZ, -BASS_SWAP_DEPTH_DB);

        // Measure a low tone and a high one through the same filter.
        let amplitude = |hz: f64, mut filter: BassShelf| {
            let mut peak: f64 = 0.0;
            // Let the filter settle before measuring.
            for index in 0..(rate as usize / 4) {
                let t = index as f64 / rate;
                let out = filter.run((std::f64::consts::TAU * hz * t).sin());
                if index > rate as usize / 8 {
                    peak = peak.max(out.abs());
                }
            }
            peak
        };
        let low_out = amplitude(low, shelf);
        let high_out = amplitude(4000.0, shelf);
        assert!(
            low_out < 0.5,
            "60 Hz came through at {low_out}, the low cut is not working"
        );
        assert!(
            high_out > 0.9,
            "4 kHz was cut to {high_out}, the shelf reaches too high"
        );
    }

    /// The sweep must start with the bass intact and end with it gone, so
    /// the handover happens across the overlap rather than all at once.
    #[test]
    fn the_bass_sweep_runs_from_untouched_to_cut() {
        let frames = 90u64;
        let channels = crate::NUM_CHANNELS as usize;
        let total = frames as usize * channels;
        let decoder: Box<dyn AudioDecoder + Send> = Box::new(StubDecoder {
            packets: vec![vec![0.3; total]],
        });
        let mut outgoing = Outgoing::new(decoder, 1.0, frames, 1.0, None);
        assert_eq!(
            outgoing.bass_left, outgoing.bass_total,
            "the sweep starts unengaged"
        );

        let mut data = vec![0.0f64; total];
        mix_tail_with_bass(&mut data, &mut outgoing);
        assert!(
            outgoing.bass_left < outgoing.bass_total,
            "the sweep must advance as the overlap plays"
        );
        assert_eq!(outgoing.bass_left, 0, "one packet spans the whole sweep");
    }

    /// A shelf at zero depth must be transparent: it is the state the
    /// incoming deck's filter sits in when it owns the bass.
    #[test]
    fn the_bass_shelf_leaves_the_signal_alone_at_zero_depth() {
        let mut filter = BassShelf::new(BASS_SWAP_HZ, 0.0);
        let rate = f64::from(crate::SAMPLE_RATE);
        let mut peak: f64 = 0.0;
        for index in 0..(rate as usize / 4) {
            let t = index as f64 / rate;
            let out = filter.run((std::f64::consts::TAU * 60.0 * t).sin());
            if index > rate as usize / 8 {
                peak = peak.max(out.abs());
            }
        }
        assert!(
            (peak - 1.0).abs() < 0.02,
            "a zero-depth shelf changed the level to {peak}"
        );
    }

    #[test]
    fn ramp_runs_from_one_track_to_the_other() {
        let mut ramp = Ramp::new(100);
        assert!((ramp.out_gain() - 1.0).abs() < 1e-9);
        assert!(ramp.in_gain().abs() < 1e-9);
        for _ in 0..100 {
            ramp.advance();
        }
        assert!(ramp.finished());
        assert!(ramp.out_gain().abs() < 1e-9);
        assert!((ramp.in_gain() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn ramp_holds_its_level_at_the_midpoint() {
        // The curve spans the first frame to the last inclusive, so the
        // halfway point falls between two frames. Either side of it must
        // still sit within half a step of equal power.
        let total = 100u64;
        let mut ramp = Ramp::new(total);
        for _ in 0..(total / 2) {
            ramp.advance();
        }
        let power = ramp.out_gain().powi(2) + ramp.in_gain().powi(2);
        assert!(
            (power - 1.0).abs() < 1e-9,
            "the two decks must stay at equal power, got {power}"
        );
        // Equal power at the middle means each deck is 1/sqrt(2), within the
        // one-frame rounding the inclusive curve introduces.
        let step = std::f64::consts::FRAC_PI_2 / (total - 1) as f64;
        assert!(ramp.out_gain() <= FRAC_1_SQRT_2);
        assert!(ramp.out_gain() >= FRAC_1_SQRT_2 - step);
    }

    #[test]
    fn ramp_holds_its_target_once_finished() {
        let mut ramp = Ramp::new(10);
        for _ in 0..100 {
            ramp.advance();
        }
        assert!(ramp.out_gain().abs() < 1e-9);
    }

    #[test]
    fn fade_in_steps_once_per_frame() {
        let mut ramp = Ramp::new(4);
        let mut samples = vec![1.0f64; 8];
        apply_fade_in(&mut samples, &mut ramp);
        for frame in samples.chunks(2) {
            assert_eq!(frame[0], frame[1]);
        }
        assert!(samples[0].abs() < 1e-9);
        assert!(samples[6] > samples[4]);
        assert!(ramp.finished());
    }

    #[test]
    fn tail_is_mixed_frame_for_frame() {
        let mut ramp = Ramp::new(4);
        let mut samples = vec![0.0f64; 8];
        mix_tail(&mut samples, &vec![1.0f64; 8], &mut ramp);
        assert!((samples[0] - 1.0).abs() < 1e-9);
        assert!(samples[6] < samples[4]);
        assert!(ramp.finished());
    }

    #[test]
    fn tail_spans_packets_of_different_lengths() {
        let decoder = StubDecoder {
            packets: vec![vec![1.0; 6], vec![1.0; 2], vec![1.0; 10]],
        };
        let mut outgoing = Outgoing::new(Box::new(decoder), 1.0, 100, 1.0, None);
        assert_eq!(outgoing.take(4).len(), 8);
        assert_eq!(outgoing.take(4).len(), 8);
    }

    #[test]
    fn tail_pads_with_silence_once_spent() {
        let decoder = StubDecoder {
            packets: vec![vec![1.0; 4]],
        };
        let mut outgoing = Outgoing::new(Box::new(decoder), 1.0, 100, 1.0, None);
        assert_eq!(
            outgoing.take(4),
            vec![1.0, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0]
        );
        assert!(outgoing.finished());
    }

    #[test]
    fn tail_carries_its_own_normalisation() {
        let decoder = StubDecoder {
            packets: vec![vec![1.0; 4]],
        };
        let mut outgoing = Outgoing::new(Box::new(decoder), 0.5, 100, 1.0, None);
        assert_eq!(outgoing.take(2), vec![0.5, 0.5, 0.5, 0.5]);
    }

    #[test]
    fn crossfade_frames_follow_the_sample_rate_and_the_cap() {
        assert_eq!(crossfade_frames(Duration::from_secs(1)), 44_100);
        assert_eq!(crossfade_frames(Duration::ZERO), 0);
        assert_eq!(
            crossfade_frames(Duration::from_secs(60)),
            crossfade_frames(CROSSFADE_MAX)
        );
    }

    /// A plan decides when its transition fires, independently of the
    /// configured crossfade length: a short planned overlap must still wait
    /// for its own lead-in rather than firing at the global setting.
    #[test]
    fn a_plan_sets_its_own_lead_in_and_length() {
        let plan = CrossfadePlan {
            duration: Duration::from_secs(3),
            fade_out_before_end: Duration::from_secs(9),
            fade_in_at: Duration::from_millis(500),
            tempo_rate: 1.0,
            curve: None,
        }
        .clamped();
        assert_eq!(plan.duration, Duration::from_secs(3));
        // The trigger waits for the later of the two lead-ins.
        let start_within = plan.fade_out_before_end.max(plan.duration);
        assert_eq!(start_within, Duration::from_secs(9));
        // ...and the overlap is the planned length, not the lead-in.
        assert_eq!(plan.duration, Duration::from_secs(3));
    }

    #[test]
    fn a_plan_is_capped_at_the_crossfade_maximum() {
        let plan = CrossfadePlan {
            duration: Duration::from_secs(60),
            fade_out_before_end: Duration::from_secs(60),
            fade_in_at: Duration::ZERO,
            tempo_rate: 1.0,
            curve: None,
        }
        .clamped();
        assert_eq!(plan.duration, CROSSFADE_MAX);
    }

    #[test]
    fn a_plan_never_asks_for_a_negative_offset() {
        let plan = CrossfadePlan {
            duration: Duration::from_secs(2),
            fade_out_before_end: Duration::ZERO,
            fade_in_at: Duration::ZERO,
            tempo_rate: 1.0,
            curve: None,
        }
        .clamped();
        assert_eq!(plan.fade_out_before_end, Duration::ZERO);
        assert_eq!(plan.fade_in_at, Duration::ZERO);
    }
}

