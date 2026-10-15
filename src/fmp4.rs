//! Fragmented-MP4 top-level box splitter.
//!
//! Feed it the byte stream a fragmented muxer produces (mp4mux
//! fragment-duration>0 / cmafmux). It groups `ftyp`+`moov` into one INIT unit
//! and every `moof`+`mdat` pair into one FRAGMENT unit. `styp`, `sidx`,
//! `prft`, `emsg`, `free` and any other top-level box between fragments is
//! skipped.

use std::collections::VecDeque;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// ftyp + moov
    Init,
    /// moof + mdat
    Fragment,
}

#[derive(Debug)]
pub struct Unit {
    pub kind: Kind,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// size < header, size == 0, or > max_box
    BoxSize(String),
    /// moof before moov, mdat without moof, ...
    Order(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::BoxSize(m) | Error::Order(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
pub struct Splitter {
    /// unparsed input
    pub(crate) buf: Vec<u8>,
    /// accumulating ftyp + moov
    init: Vec<u8>,
    init_emitted: bool,
    /// pending moof waiting for its mdat
    pub(crate) moof: Option<Vec<u8>>,
    /// largest single box accepted
    max_box: usize,
    out: VecDeque<Unit>,
    /// top-level boxes ignored so far
    skipped: u32,
}

fn fourcc_str(f: &[u8]) -> String {
    String::from_utf8_lossy(f).into_owned()
}

/// Parse a box header. `None` when more bytes are needed.
fn read_header(p: &[u8]) -> Option<(u64, usize)> {
    if p.len() < 8 {
        return None;
    }
    let s32 = u32::from_be_bytes([p[0], p[1], p[2], p[3]]);
    if s32 == 1 {
        if p.len() < 16 {
            return None;
        }
        let s64 = u64::from_be_bytes(p[8..16].try_into().unwrap());
        Some((s64, 16))
    } else {
        Some((s32 as u64, 8))
    }
}

impl Splitter {
    pub fn new(max_box: usize) -> Self {
        Splitter {
            buf: Vec::new(),
            init: Vec::new(),
            init_emitted: false,
            moof: None,
            max_box,
            out: VecDeque::new(),
            skipped: 0,
        }
    }

    pub fn max_box(&self) -> usize {
        self.max_box
    }

    pub fn skipped(&self) -> u32 {
        self.skipped
    }

    pub fn pull(&mut self) -> Option<Unit> {
        self.out.pop_front()
    }

    /// Put an unprocessed unit back at the head of the queue.
    pub fn unpull(&mut self, u: Unit) {
        self.out.push_front(u);
    }

    /// A partial box or a moof still waiting for its mdat.
    pub fn has_partial(&self) -> bool {
        !self.buf.is_empty() || self.moof.is_some()
    }

    fn emit(&mut self, kind: Kind, data: Vec<u8>) {
        self.out.push_back(Unit { kind, data });
    }

    pub fn push(&mut self, data: &[u8]) -> Result<(), Error> {
        self.buf.extend_from_slice(data);

        loop {
            let Some((size, hdr)) = read_header(&self.buf) else {
                return Ok(()); // need more bytes
            };
            let fourcc: [u8; 4] = self.buf[4..8].try_into().unwrap();
            if size < hdr as u64 || size > self.max_box as u64 {
                return Err(Error::BoxSize(format!(
                    "box '{}' has invalid size {} (max {})",
                    fourcc_str(&fourcc),
                    size,
                    self.max_box
                )));
            }
            let size = size as usize;
            if self.buf.len() < size {
                return Ok(()); // box not complete yet
            }

            match &fourcc {
                b"ftyp" | b"moov" => {
                    if self.moof.is_some() {
                        return Err(Error::Order(format!(
                            "box '{}' between moof and mdat",
                            fourcc_str(&fourcc)
                        )));
                    }
                    if self.init_emitted {
                        // a new init segment begins
                        self.init.clear();
                        self.init_emitted = false;
                    }
                    self.init.extend_from_slice(&self.buf[..size]);
                }
                b"moof" => {
                    if self.moof.is_some() {
                        return Err(Error::Order("moof not followed by mdat".into()));
                    }
                    if self.init.is_empty() && !self.init_emitted {
                        return Err(Error::Order("moof before any moov".into()));
                    }
                    if !self.init_emitted {
                        let init = self.init.clone();
                        self.emit(Kind::Init, init);
                        self.init_emitted = true;
                    }
                    let mut moof = Vec::with_capacity(size);
                    moof.extend_from_slice(&self.buf[..size]);
                    self.moof = Some(moof);
                }
                b"mdat" => {
                    let Some(mut frag) = self.moof.take() else {
                        return Err(Error::Order("mdat without a preceding moof".into()));
                    };
                    frag.extend_from_slice(&self.buf[..size]);
                    self.emit(Kind::Fragment, frag);
                }
                _ => {
                    if self.moof.is_some() {
                        return Err(Error::Order(format!(
                            "box '{}' between moof and mdat",
                            fourcc_str(&fourcc)
                        )));
                    }
                    self.skipped += 1;
                }
            }

            self.buf.drain(..size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build one box: 32-bit size, fourcc, payload.
    fn bx(fourcc: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let size = (8 + payload.len()) as u32;
        let mut b = size.to_be_bytes().to_vec();
        b.extend_from_slice(fourcc);
        b.extend_from_slice(payload);
        b
    }

    fn sample_stream() -> Vec<u8> {
        let p1 = [1u8; 4];
        let p2 = [2u8; 6];
        [
            bx(b"ftyp", &p1),
            bx(b"moov", &p2),
            bx(b"styp", &p1), // skipped
            bx(b"moof", &p1),
            bx(b"mdat", &p2),
            bx(b"sidx", &p1), // skipped
            bx(b"moof", &p2),
            bx(b"mdat", &p1),
        ]
        .concat()
    }

    #[test]
    fn whole_stream() {
        let input = sample_stream();
        let mut s = Splitter::new(1 << 20);
        s.push(&input).unwrap();

        let u = s.pull().unwrap();
        assert_eq!(u.kind, Kind::Init);
        assert_eq!(u.data.len(), 12 + 14);

        let u = s.pull().unwrap();
        assert_eq!(u.kind, Kind::Fragment);
        assert_eq!(u.data.len(), 12 + 14);
        assert_eq!(&u.data[4..8], b"moof");

        let u = s.pull().unwrap();
        assert_eq!(u.kind, Kind::Fragment);
        assert_eq!(u.data.len(), 14 + 12);

        assert!(s.pull().is_none());
        assert_eq!(s.skipped(), 2);
    }

    /// Same stream, fed one byte at a time and in 3- and 7-byte chunks.
    #[test]
    fn odd_chunks() {
        for chunk in [1usize, 3, 7] {
            let input = sample_stream();
            let mut s = Splitter::new(1 << 20);
            let mut kinds = Vec::new();
            for part in input.chunks(chunk) {
                s.push(part).unwrap();
                while let Some(u) = s.pull() {
                    assert!(kinds.len() < 3);
                    kinds.push(u.kind);
                }
            }
            assert_eq!(kinds, [Kind::Init, Kind::Fragment, Kind::Fragment]);
        }
    }

    #[test]
    fn largesize() {
        // size == 1 with a 64-bit largesize of 20: 16-byte header + 4 payload.
        let p = [9u8; 4];
        let mut input = vec![0, 0, 0, 1, b'f', b't', b'y', b'p', 0, 0, 0, 0, 0, 0, 0, 20];
        input.extend_from_slice(&p);
        input.extend(bx(b"moov", &p));
        input.extend(bx(b"moof", &p));
        input.extend(bx(b"mdat", &p));

        let mut s = Splitter::new(1 << 20);
        s.push(&input).unwrap();
        let u = s.pull().unwrap();
        assert_eq!(u.kind, Kind::Init);
        assert_eq!(u.data.len(), 20 + 12);
    }

    #[test]
    fn bad_sizes() {
        let zero_size = [0, 0, 0, 0, b'f', b't', b'y', b'p'];
        let tiny = [0, 0, 0, 4, b'f', b't', b'y', b'p'];
        let huge = [0, 1, 0, 0, b'm', b'd', b'a', b't']; // 65536
        for input in [zero_size, tiny, huge] {
            let mut s = Splitter::new(1024);
            assert!(matches!(s.push(&input), Err(Error::BoxSize(_))));
        }
    }

    #[test]
    fn bad_order() {
        let p = [0u8; 4];

        // moof before any moov
        let mut s = Splitter::new(1024);
        assert!(matches!(s.push(&bx(b"moof", &p)), Err(Error::Order(_))));

        // mdat without moof
        let input = [bx(b"ftyp", &p), bx(b"moov", &p), bx(b"mdat", &p)].concat();
        let mut s = Splitter::new(1024);
        assert!(matches!(s.push(&input), Err(Error::Order(_))));

        // a box between moof and mdat
        let input = [
            bx(b"ftyp", &p),
            bx(b"moov", &p),
            bx(b"moof", &p),
            bx(b"free", &p),
        ]
        .concat();
        let mut s = Splitter::new(1024);
        assert!(matches!(s.push(&input), Err(Error::Order(_))));
    }

    /// A second ftyp+moov after fragments starts a new init segment.
    #[test]
    fn reinit() {
        let p = [0u8; 4];
        let input = [
            bx(b"ftyp", &p),
            bx(b"moov", &p),
            bx(b"moof", &p),
            bx(b"mdat", &p),
            bx(b"ftyp", &p),
            bx(b"moov", &p),
            bx(b"moof", &p),
            bx(b"mdat", &p),
        ]
        .concat();
        let mut s = Splitter::new(1024);
        s.push(&input).unwrap();
        for expect in [Kind::Init, Kind::Fragment, Kind::Init, Kind::Fragment] {
            assert_eq!(s.pull().unwrap().kind, expect);
        }
        assert!(s.pull().is_none());
    }
}
