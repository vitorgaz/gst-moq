//! Offline fixture inventory through the plugin splitter and exact SDK parser.
//!
//! Usage: inspect_cmaf_fixture <file.mp4>
//! Prints one JSON line per init segment and fragment; exits 1 on any parse
//! failure, a trailing partial box, or a file with no fragments.

use gst::glib;
use gstmoq::codec::codec_string_from_init;
use gstmoq::fmp4::{Kind, Splitter};
use moq5::cmaf::{parse_fragment, CmafInit};

fn sha256(data: &[u8]) -> String {
    glib::compute_checksum_for_data(glib::ChecksumType::Sha256, data)
        .map(|s| s.to_string())
        .unwrap_or_default()
}

fn run() -> i32 {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        return 2;
    }
    // GLib reads and reports exactly as the C tool did (g_printerr converts
    // to the locale charset).
    let data = match glib::file_get_contents(&args[1]) {
        Ok(d) => d,
        Err(e) => {
            glib::g_printerr!("{}\n", e.message());
            return 1;
        }
    };
    let mut splitter = Splitter::new(16 << 20);
    for chunk in data.chunks(7) {
        if let Err(e) = splitter.push(chunk) {
            glib::g_printerr!("{}\n", e);
            return 1;
        }
    }
    drop(data);
    if splitter.has_partial() {
        return 1;
    }

    let mut fragments = 0u32;
    let mut init: Option<CmafInit> = None;
    while let Some(unit) = splitter.pull() {
        let hash = sha256(&unit.data);
        match unit.kind {
            Kind::Init => {
                let size = unit.data.len();
                let Ok(parsed) = CmafInit::parse(unit.data) else {
                    return 1;
                };
                let info = init.insert(parsed).info();
                let codec = codec_string_from_init(info).unwrap_or_default();
                println!(
                    "{{\"kind\":\"init\",\"size\":{},\"sha256\":\"{}\",\"timescale\":{},\"codec\":\"{}\"}}",
                    size,
                    hash,
                    info.timescale(),
                    codec
                );
            }
            Kind::Fragment => {
                let Some(init) = init.as_ref() else {
                    return 1;
                };
                // The C tool parsed into a fixed 256-sample table.
                let fragment = match parse_fragment(&unit.data) {
                    Ok(f) if f.samples.len() <= 256 => f,
                    _ => return 1,
                };
                let Some(decode_ticks) = fragment.base_decode_time else {
                    return 1;
                };
                if fragment.track_id != init.info().track_id() {
                    return 1;
                }
                println!(
                    "{{\"kind\":\"fragment\",\"index\":{},\"size\":{},\"sha256\":\"{}\",\"decode_ticks\":{},\"sample_count\":{}}}",
                    fragments,
                    unit.data.len(),
                    hash,
                    decode_ticks,
                    fragment.samples.len()
                );
                fragments += 1;
            }
        }
    }
    if fragments == 0 {
        1
    } else {
        0
    }
}

fn main() {
    std::process::exit(run());
}
