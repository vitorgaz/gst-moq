//! libmoq itself: `#[inline]` newtypes over the `moq5` crate.
//!
//! Every method forwards one `moq5` call, so a release build is the same code
//! the elements ran before this boundary existed.

use std::time::Duration;

use moq5::{
    EndpointConfig, Packaging, Protocol, ReceiverConfig, Result, SendObject, SendTrackConfig,
    SenderConfig, SenderStats, SubscribeOptions, TrackEventKind, Version, VersionOffer,
    WaitResult, WriteError,
};

use super::{EndpointCfg, ReceiverCfg, SenderCfg, TerminalInfo, TrackCfg};

/// Replay the dialled endpoint onto `moq5`'s builder.
fn endpoint_config(cfg: &EndpointCfg) -> EndpointConfig {
    let built = EndpointConfig::new(cfg.url.clone())
        .protocol(Protocol::WebTransport)
        .insecure_skip_verify(cfg.insecure);
    if cfg.draft == 0 {
        built
    } else {
        built.versions(VersionOffer::Exact(Version::from_draft(cfg.draft)))
    }
}

fn receiver_config(cfg: &ReceiverCfg) -> ReceiverConfig {
    ReceiverConfig::live(cfg.namespace.clone()).auto_subscribe(cfg.auto_subscribe)
}

/// Replay the authored sender settings onto `moq5`'s builder.
fn sender_config(cfg: &SenderCfg) -> SenderConfig {
    SenderConfig::live(cfg.namespace.clone())
        .publish_tracks(cfg.publish_tracks)
        .catalog_refresh_interval(cfg.catalog_refresh)
        .content_protections(cfg.content_protections.clone())
}

/// Replay the authored track settings onto `moq5`'s builder. Only the fields
/// the element set are applied, so libmoq's own defaults stand for the rest.
fn track_config(cfg: &TrackCfg) -> SendTrackConfig {
    let mut tc = SendTrackConfig::new(
        cfg.name.clone(),
        cfg.media_type,
        cfg.packaging,
        cfg.codec.clone(),
    );
    if let Some(timescale) = cfg.timescale {
        tc = tc.timescale(timescale);
    }
    if let Some(data) = cfg.init_data.clone() {
        tc = tc.init_data(data);
    }
    if let Some((width, height)) = cfg.dimensions {
        tc = tc.dimensions(width, height);
    }
    if let Some(millis) = cfg.framerate_millis {
        tc = tc.framerate_millis(millis);
    }
    if let Some(rate) = cfg.samplerate {
        tc = tc.samplerate(rate);
    }
    if let Some(config) = cfg.channel_config.clone() {
        tc = tc.channel_config(config);
    }
    if let Some(bitrate) = cfg.bitrate {
        tc = tc.bitrate(bitrate);
    }
    if let Some(groups) = cfg.sap_timeline {
        tc = tc.sap_timeline(groups);
    }
    if !cfg.cp_ref_ids.is_empty() {
        tc = tc.content_protection_ref_ids(cfg.cp_ref_ids.clone());
    }
    tc
}

/// Handles and plain records stay `moq5`'s own: they are opaque identifiers
/// or public-field structs, so nothing here needs to re-describe them.
pub(crate) use moq5::{SendTrack, Track};

// -- endpoint ---------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct Endpoint(moq5::Endpoint);

impl Endpoint {
    #[inline]
    pub(crate) fn connect(config: &EndpointCfg) -> Result<Endpoint> {
        moq5::Endpoint::connect(&endpoint_config(config)).map(Endpoint)
    }
    #[inline]
    pub(crate) fn drain(&self, timeout: Duration) -> Result<WaitResult> {
        self.0.drain(timeout)
    }
    #[inline]
    pub(crate) fn set_interrupted(&self, interrupted: bool) {
        self.0.set_interrupted(interrupted)
    }
    #[inline]
    pub(crate) fn terminal(&self) -> Result<TerminalInfo> {
        self.0.terminal().map(|t| TerminalInfo {
            reason: t.reason,
            detail_code: t.detail_code,
        })
    }
    #[inline]
    pub(crate) fn wait(&self, timeout: Duration) -> Result<WaitResult> {
        self.0.wait(timeout)
    }
}

// -- sender -----------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Sender(moq5::Sender);

impl Sender {
    #[inline]
    pub(crate) fn attach(endpoint: &Endpoint, config: &SenderCfg) -> Result<Sender> {
        moq5::Sender::attach(&endpoint.0, &sender_config(config)).map(Sender)
    }
    #[inline]
    pub(crate) fn add_track(&self, config: &TrackCfg) -> Result<SendTrack> {
        self.0.add_track(&track_config(config))
    }
    #[inline]
    pub(crate) fn write(
        &self,
        track: SendTrack,
        object: SendObject,
    ) -> std::result::Result<(), WriteError> {
        self.0.write(track, object)
    }
    #[inline]
    pub(crate) fn end_track(&self, track: SendTrack) -> Result<()> {
        self.0.end_track(track)
    }
    #[inline]
    pub(crate) fn stats(&self) -> Result<SenderStats> {
        self.0.stats()
    }
    #[inline]
    pub(crate) fn is_ready(&self) -> bool {
        self.0.is_ready()
    }
    #[inline]
    pub(crate) fn is_fatal(&self) -> bool {
        self.0.is_fatal()
    }
    #[inline]
    pub(crate) fn fatal_code(&self) -> u64 {
        self.0.fatal_code()
    }
    #[inline]
    pub(crate) fn wait(&self, timeout: Duration) -> Result<WaitResult> {
        self.0.wait(timeout)
    }
}

// -- receiver ---------------------------------------------------------------

#[derive(Debug)]
pub(crate) struct Receiver(moq5::Receiver);

impl Receiver {
    #[inline]
    pub(crate) fn attach(endpoint: &Endpoint, config: &ReceiverCfg) -> Result<Receiver> {
        moq5::Receiver::attach(&endpoint.0, &receiver_config(config)).map(Receiver)
    }
    #[inline]
    pub(crate) fn subscribe_track(
        &self,
        track: Track,
        options: Option<SubscribeOptions>,
    ) -> Result<()> {
        self.0.subscribe_track(track, options)
    }
    #[inline]
    pub(crate) fn poll_track(&self) -> Result<Option<TrackEvt<'_>>> {
        Ok(self.0.poll_track()?.map(TrackEvt))
    }
    #[inline]
    pub(crate) fn poll_object(&self) -> Result<Option<Object>> {
        Ok(self.0.poll_object()?.map(Object))
    }
    #[inline]
    pub(crate) fn wait(&self, timeout: Duration) -> Result<WaitResult> {
        self.0.wait(timeout)
    }
    #[inline]
    pub(crate) fn is_fatal(&self) -> bool {
        self.0.is_fatal()
    }
    #[inline]
    pub(crate) fn fatal_code(&self) -> u64 {
        self.0.fatal_code()
    }
    /// The MoQT draft the receiver's endpoint negotiated, 0 when none.
    #[inline]
    pub(crate) fn negotiated_draft(&self) -> u32 {
        self.0
            .endpoint()
            .negotiated_version()
            .map_or(0, |v| v.draft())
    }
}

// -- what the receiver hands back -------------------------------------------

pub(crate) struct TrackEvt<'r>(moq5::TrackEvent<'r>);

impl<'r> TrackEvt<'r> {
    #[inline]
    pub(crate) fn kind(&self) -> TrackEventKind {
        self.0.kind()
    }
    #[inline]
    pub(crate) fn track(&self) -> Option<Track> {
        self.0.track()
    }
    #[inline]
    pub(crate) fn description(&self) -> Result<Option<Desc<'r>>> {
        Ok(self.0.description()?.map(Desc))
    }
    /// The raw event kind, for the codes this crate logs but does not model.
    #[inline]
    pub(crate) fn raw_kind(&self) -> u32 {
        self.0.raw().kind
    }
}

pub(crate) struct Desc<'r>(moq5::TrackDescription<'r>);

impl<'r> Desc<'r> {
    #[inline]
    pub(crate) fn name(&self) -> &'r [u8] {
        self.0.name()
    }
    #[inline]
    pub(crate) fn codec(&self) -> &'r [u8] {
        self.0.codec()
    }
    #[inline]
    pub(crate) fn packaging(&self) -> Option<Packaging> {
        self.0.packaging()
    }
    #[inline]
    pub(crate) fn init_data(&self) -> &'r [u8] {
        self.0.init_data()
    }
    /// The decoder config from the catalog's init info. Only this is needed,
    /// and handing out an `InitInfo` would tie a double to a C record full of
    /// raw pointers.
    #[inline]
    pub(crate) fn codec_config(&self) -> &[u8] {
        self.0.init_info().codec_config()
    }
    #[inline]
    pub(crate) fn samplerate(&self) -> Option<u32> {
        self.0.samplerate()
    }
    #[inline]
    pub(crate) fn channel_config(&self) -> &'r [u8] {
        self.0.channel_config()
    }
    /// Whether the catalog carried a track name at all, which an empty name
    /// does not distinguish.
    #[inline]
    pub(crate) fn has_name(&self) -> bool {
        !self.0.raw().name.data.is_null()
    }
}

pub(crate) struct Object(moq5::MediaObject);

impl Object {
    #[inline]
    pub(crate) fn track(&self) -> Option<Track> {
        self.0.track()
    }
    #[inline]
    pub(crate) fn packaging(&self) -> Option<Packaging> {
        self.0.packaging()
    }
    #[inline]
    pub(crate) fn is_keyframe(&self) -> bool {
        self.0.is_keyframe()
    }
    #[inline]
    pub(crate) fn presentation_time_us(&self) -> u64 {
        self.0.presentation_time_us()
    }
    #[inline]
    pub(crate) fn payload(&self) -> &[u8] {
        self.0.payload()
    }
    #[inline]
    pub(crate) fn fragment(&self) -> &[u8] {
        self.0.fragment()
    }
}
