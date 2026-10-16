//! MSF catalog codec strings.

use moq5::cmaf::{CodecKind, InitInfo};

/// MSF codec string for a parsed CMAF init segment: "avc1.PPCCLL",
/// "mp4a.40.<aot>" or "opus". `None` for codecs this plugin does not publish
/// (HEVC, AV1) or when the codec config is too short to read.
pub fn codec_string_from_init(info: InitInfo<'_>) -> Option<String> {
    let d = info.codec_config();
    match info.codec_kind() {
        CodecKind::Avc => {
            // AVCDecoderConfigurationRecord: version, profile, compat, level
            if d.len() < 4 || d[0] != 1 {
                return None;
            }
            Some(format!("avc1.{:02x}{:02x}{:02x}", d[1], d[2], d[3]))
        }
        CodecKind::Aac => codec_string_from_aac_asc(d),
        CodecKind::Opus => Some("opus".into()),
        _ => None,
    }
}

/// MSF codec string for a bare AudioSpecificConfig (the `codec_data` an AAC
/// caps carries): "mp4a.40.<audioObjectType>". `None` when the ASC is too
/// short or names an object type this plugin does not publish.
pub fn codec_string_from_aac_asc(asc: &[u8]) -> Option<String> {
    // AudioSpecificConfig: audioObjectType is the top 5 bits. 31 is the escape
    // form for types >= 32, which this plugin does not publish.
    let aot = asc.first()? >> 3;
    if aot == 0 || aot == 31 {
        return None;
    }
    Some(format!("mp4a.40.{aot}"))
}

/// Sample rate and channel count for the supported AAC-LC ASC subset. `None`
/// unless the ASC is the two-byte AAC-LC form without extensions, core
/// dependency, or an explicit sampling frequency. Caps values must match the
/// ASC when present.
pub fn aac_asc_params(asc: &[u8]) -> Option<(i32, i32)> {
    // ISO/IEC 14496-3 1.6.2.1: audioObjectType(5), samplingFrequencyIndex(4),
    // then either a 24-bit explicit frequency (index 15) or straight on to
    // channelConfiguration(4).
    const FREQ: [i32; 13] = [
        96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350,
    ];
    if asc.len() != 2 || (asc[0] >> 3) != 2 || (asc[1] & 0x03) != 0 {
        return None;
    }
    let idx = (((asc[0] & 0x07) << 1) | (asc[1] >> 7)) as usize;
    // 13/14 reserved, 15 = explicit; not published
    let rate = *FREQ.get(idx)?;
    let chan = (asc[1] >> 3) & 0x0f;
    if chan == 0 || chan > 7 {
        return None; // 0 = defined in the payload, 8..15 reserved
    }
    let channels = if chan == 7 { 8 } else { chan as i32 }; // 7 means 7.1
    Some((rate, channels))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::span;
    use moq5::sys::*;

    /// The codec string of a hand-built init info.
    fn codec_of(i: &moq_cmaf_init_info_t) -> Option<String> {
        // SAFETY: the spans point at the test's arrays, alive for the call.
        codec_string_from_init(unsafe { InitInfo::from_raw(i) })
    }

    fn info(kind: moq_cmaf_codec_kind_t, config: &[u8]) -> moq_cmaf_init_info_t {
        let mut i = moq_cmaf_init_info_t::default();
        unsafe { moq_cmaf_init_info_init(&mut i) };
        i.codec_kind = kind;
        i.codec_config = span(config);
        i
    }

    #[test]
    fn avc() {
        // avcC: version 1, profile 0x42, compat 0xc0, level 0x1e, ...
        let avcc = [1, 0x42, 0xc0, 0x1e, 0xff, 0xe1];
        let i = info(MOQ_CMAF_CODEC_AVC, &avcc);
        assert_eq!(codec_of(&i).as_deref(), Some("avc1.42c01e"));
    }

    #[test]
    fn aac_lc() {
        // AudioSpecificConfig: AOT 2 (AAC-LC) = 00010 in the top 5 bits -> 0x12 0x10
        let asc = [0x12, 0x10];
        let i = info(MOQ_CMAF_CODEC_AAC, &asc);
        assert_eq!(codec_of(&i).as_deref(), Some("mp4a.40.2"));
    }

    #[test]
    fn he_aac() {
        // AOT 5 (SBR) = 00101 -> 0x2a
        let asc = [0x2a, 0x10];
        let i = info(MOQ_CMAF_CODEC_AAC, &asc);
        assert_eq!(codec_of(&i).as_deref(), Some("mp4a.40.5"));
    }

    #[test]
    fn opus() {
        let i = info(MOQ_CMAF_CODEC_OPUS, &[]);
        assert_eq!(codec_of(&i).as_deref(), Some("opus"));
    }

    #[test]
    fn unsupported() {
        let i = info(MOQ_CMAF_CODEC_HEVC, &[]);
        assert_eq!(codec_of(&i), None);

        // AVC with a truncated avcC
        let short_avcc = [1, 0x42];
        let i = info(MOQ_CMAF_CODEC_AVC, &short_avcc);
        assert_eq!(codec_of(&i), None);
    }

    #[test]
    fn asc_params() {
        let lc = [0x11, 0x90];
        let he = [0x2a, 0x10];
        let explicit_rate = [0x17, 0x90];
        let pce = [0x11, 0x80];
        let escape = [0xf9, 0x90];
        let surround = [0x11, 0xb8];
        assert_eq!(aac_asc_params(&lc), Some((48000, 2)));
        assert_eq!(aac_asc_params(&surround).map(|p| p.1), Some(8));
        let bad: [&[u8]; 5] = [&he, &explicit_rate, &pce, &escape, &lc[..1]];
        for b in bad {
            assert_eq!(aac_asc_params(b), None, "{b:02x?}");
        }
        let dependent = [0x11, 0x92];
        let extended = [0x11, 0x91];
        let trailing = [0x11, 0x90, 0x56];
        assert_eq!(aac_asc_params(&dependent), None);
        assert_eq!(aac_asc_params(&extended), None);
        assert_eq!(aac_asc_params(&trailing), None);
        assert_eq!(aac_asc_params(&[]), None);
    }
}
