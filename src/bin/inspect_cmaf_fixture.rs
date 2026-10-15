//! Offline fixture inventory through the plugin splitter and exact SDK parser.
//!
//! Usage: inspect_cmaf_fixture <file.mp4>
//! Prints one JSON line per init segment and fragment; exits 1 on any parse
//! failure, a trailing partial box, or a file with no fragments.

use gst::glib;
use gstmoq::codec::codec_string_from_init;
use gstmoq::fmp4::{Kind, Splitter};
use gstmoq::moq::*;

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
    let data = match std::fs::read(&args[1]) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Failed to open file “{}”: {}", args[1], e);
            return 1;
        }
    };
    let mut splitter = Splitter::new(16 << 20);
    for chunk in data.chunks(7) {
        if let Err(e) = splitter.push(chunk) {
            eprintln!("{e}");
            return 1;
        }
    }
    drop(data);
    if splitter.has_partial() {
        return 1;
    }

    let mut fragments = 0u32;
    let mut init: Option<Vec<u8>> = None;
    let mut info = moq_cmaf_init_info_t::default();
    unsafe { moq_cmaf_init_info_init(&mut info) };
    while let Some(unit) = splitter.pull() {
        let hash = sha256(&unit.data);
        match unit.kind {
            Kind::Init => {
                // `info` borrows from the init bytes, so keep them alive in
                // `init` before parsing.
                let bytes = init.insert(unit.data);
                if unsafe { moq_cmaf_parse_init(span(bytes), &mut info) } != MOQ_OK {
                    return 1;
                }
                let codec = codec_string_from_init(&info).unwrap_or_default();
                println!(
                    "{{\"kind\":\"init\",\"size\":{},\"sha256\":\"{}\",\"timescale\":{},\"codec\":\"{}\"}}",
                    bytes.len(),
                    hash,
                    info.timescale,
                    codec
                );
            }
            Kind::Fragment => {
                let mut samples = vec![moq_cmaf_sample_t::default(); 256];
                let mut fragment = moq_cmaf_fragment_info_t::default();
                let ok = init.is_some()
                    && unsafe {
                        moq_cmaf_fragment_info_init(
                            &mut fragment,
                            samples.as_mut_ptr(),
                            samples.len(),
                        );
                        moq_cmaf_parse_fragment(span(&unit.data), &mut fragment)
                    } == MOQ_OK
                    && fragment.has_base_decode_time
                    && fragment.track_id == info.track_id;
                if !ok {
                    return 1;
                }
                println!(
                    "{{\"kind\":\"fragment\",\"index\":{},\"size\":{},\"sha256\":\"{}\",\"decode_ticks\":{},\"sample_count\":{}}}",
                    fragments,
                    unit.data.len(),
                    hash,
                    fragment.base_decode_time,
                    fragment.sample_count
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
