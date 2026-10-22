//! The libmoq service, behind this plugin's own boundary.
//!
//! `moqsink` and `moqsrc` reach libmoq through the handful of operations
//! gathered here rather than through `moq5` directly. The point is testing:
//! the element logic -- the track barrier, caps and codec validation, the
//! CMSF contentProtections, the sender state machine, moqsrc's track
//! selection -- is what the unit tests assert, and a test double for the
//! service lets them run without a relay while recording what the element
//! asked the service to do.
//!
//! The implementation is chosen at compile time, so a normal build has no
//! indirection and no dynamic dispatch: [`real`] wraps `moq5` in `#[inline]`
//! newtypes, and a test build swaps in [`fake`]. `real` is compiled either
//! way, so both are type-checked by `cargo build` and `cargo test`
//! respectively -- a signature that drifts fails one of them. Behaviour that
//! drifts is what the e2e suite is for: a double cannot notice libmoq
//! changing underneath it.
//!
//! Types that libmoq hands back as plain data with public fields
//! (`Terminal`, `SenderStats`, `WriteError`, `TrackEventKind`, the handles)
//! are `moq5`'s own, re-exported, because a double can build those directly.
//! Only what `moq5` keeps opaque -- a track event, a media object, a track
//! description -- gets a facade type here.

// Compiled in a test build too, so its signatures stay checked against the
// elements even while the double is the one they call.
#[cfg_attr(test, allow(dead_code))]
pub(crate) mod real;

#[cfg(test)]
pub(crate) mod fake;

// Named by the elements only where a signature spells the type out; the rest
// reach them through inference, and the fake build names more of them.
#[allow(unused_imports)]
#[cfg(not(test))]
pub(crate) use real::{Desc, Endpoint, Object, Receiver, SendTrack, Sender, Track, TrackEvt};

#[allow(unused_imports)]
#[cfg(test)]
pub(crate) use fake::{Desc, Endpoint, Object, Receiver, SendTrack, Sender, Track, TrackEvt};

// -- what the elements author ----------------------------------------------
//
// `moq5`'s own configuration builders keep every field private, so a double
// cannot see what the element asked for. These carry the same settings as
// plain data: the real adapter replays them onto `moq5`'s builders, and the
// fake reads them to assert what the element authored -- the track set, and
// the CMSF contentProtections in particular, which this plugin derives.
//
// The DRM records themselves stay `moq5`'s (`ContentProtection`, `DrmSystem`,
// `CmsfUrl`): those are public-field structs a double can already read.

use std::time::Duration;

use moq5::{ContentProtection, MediaType, Namespace, Packaging, TerminalReason};

/// Why an endpoint ended, as the elements log it. `moq5::Terminal` is
/// `#[non_exhaustive]`, so a double cannot build one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TerminalInfo {
    pub(crate) reason: TerminalReason,
    pub(crate) detail_code: u64,
}

/// How both elements dial the relay. `moq5::EndpointConfig` keeps its
/// fields private, so the double could not otherwise check that the element
/// pinned the right draft or left certificate verification on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EndpointCfg {
    pub(crate) url: String,
    pub(crate) insecure: bool,
    /// The MoQT draft to offer exactly, or 0 to offer all.
    pub(crate) draft: u32,
}

/// The receiver settings `moqsrc` authors.
#[derive(Clone, Debug)]
pub(crate) struct ReceiverCfg {
    pub(crate) namespace: Namespace,
    pub(crate) auto_subscribe: bool,
}

impl ReceiverCfg {
    pub(crate) fn live(namespace: Namespace) -> ReceiverCfg {
        ReceiverCfg {
            namespace,
            auto_subscribe: true,
        }
    }
    #[must_use]
    pub(crate) fn auto_subscribe(mut self, auto: bool) -> ReceiverCfg {
        self.auto_subscribe = auto;
        self
    }
}

/// The sender settings `moqsink` authors.
#[cfg_attr(test, allow(dead_code))]
#[derive(Clone, Debug)]
pub(crate) struct SenderCfg {
    pub(crate) namespace: Namespace,
    pub(crate) publish_tracks: bool,
    pub(crate) catalog_refresh: Duration,
    pub(crate) content_protections: Vec<ContentProtection>,
}

impl SenderCfg {
    pub(crate) fn live(namespace: Namespace) -> SenderCfg {
        SenderCfg {
            namespace,
            publish_tracks: false,
            catalog_refresh: Duration::ZERO,
            content_protections: Vec::new(),
        }
    }
    #[must_use]
    pub(crate) fn publish_tracks(mut self, publish: bool) -> SenderCfg {
        self.publish_tracks = publish;
        self
    }
    #[must_use]
    pub(crate) fn catalog_refresh_interval(mut self, interval: Duration) -> SenderCfg {
        self.catalog_refresh = interval;
        self
    }
    #[must_use]
    pub(crate) fn content_protections(mut self, entries: Vec<ContentProtection>) -> SenderCfg {
        self.content_protections = entries;
        self
    }
}

/// The per-track settings `moqsink` authors, in catalog terms.
#[cfg_attr(test, allow(dead_code))]
#[derive(Clone, Debug)]
pub(crate) struct TrackCfg {
    pub(crate) name: String,
    pub(crate) media_type: MediaType,
    pub(crate) packaging: Packaging,
    pub(crate) codec: String,
    pub(crate) timescale: Option<u32>,
    pub(crate) init_data: Option<Vec<u8>>,
    pub(crate) dimensions: Option<(u32, u32)>,
    pub(crate) framerate_millis: Option<u64>,
    pub(crate) samplerate: Option<u32>,
    pub(crate) channel_config: Option<String>,
    pub(crate) bitrate: Option<u64>,
    pub(crate) sap_timeline: Option<u32>,
    pub(crate) cp_ref_ids: Vec<String>,
}

impl TrackCfg {
    pub(crate) fn new(
        name: impl Into<String>,
        media_type: MediaType,
        packaging: Packaging,
        codec: impl Into<String>,
    ) -> TrackCfg {
        TrackCfg {
            name: name.into(),
            media_type,
            packaging,
            codec: codec.into(),
            timescale: None,
            init_data: None,
            dimensions: None,
            framerate_millis: None,
            samplerate: None,
            channel_config: None,
            bitrate: None,
            sap_timeline: None,
            cp_ref_ids: Vec::new(),
        }
    }
    #[must_use]
    pub(crate) fn timescale(mut self, timescale: u32) -> TrackCfg {
        self.timescale = Some(timescale);
        self
    }
    #[must_use]
    pub(crate) fn init_data(mut self, data: impl Into<Vec<u8>>) -> TrackCfg {
        self.init_data = Some(data.into());
        self
    }
    #[must_use]
    pub(crate) fn dimensions(mut self, width: u32, height: u32) -> TrackCfg {
        self.dimensions = Some((width, height));
        self
    }
    #[must_use]
    pub(crate) fn framerate_millis(mut self, millis: u64) -> TrackCfg {
        self.framerate_millis = Some(millis);
        self
    }
    #[must_use]
    pub(crate) fn samplerate(mut self, rate: u32) -> TrackCfg {
        self.samplerate = Some(rate);
        self
    }
    #[must_use]
    pub(crate) fn channel_config(mut self, config: impl Into<String>) -> TrackCfg {
        self.channel_config = Some(config.into());
        self
    }
    #[must_use]
    pub(crate) fn bitrate(mut self, bitrate: u64) -> TrackCfg {
        self.bitrate = Some(bitrate);
        self
    }
    #[must_use]
    pub(crate) fn sap_timeline(mut self, history_groups: u32) -> TrackCfg {
        self.sap_timeline = Some(history_groups);
        self
    }
    #[must_use]
    pub(crate) fn content_protection_ref_ids(mut self, ids: Vec<String>) -> TrackCfg {
        self.cp_ref_ids = ids;
        self
    }
}
