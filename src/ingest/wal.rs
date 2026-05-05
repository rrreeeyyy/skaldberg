//! Append-only write-ahead log.
//!
//! Frame format (all little-endian):
//!
//! ```text
//!   +0   u32   payload_len            (size in bytes of the user payload)
//!   +4   u32   crc32                  (over record_seq bytes + payload bytes)
//!   +8   u64   record_seq             (monotonic across all WAL files)
//!   +16  ...   payload (payload_len bytes)
//!   total: 16 + payload_len bytes
//! ```
//!
//! The CRC covers `record_seq` and `payload` so that a torn write at any
//! position will either:
//!   - be caught by `read_exact` returning UnexpectedEof (length truncation), or
//!   - fail CRC verification (silent corruption).
//!
//! Records can be split across files: the writer rotates to a new file
//! whenever the current file reaches `rotation_bytes`. The replay iterator
//! transparently advances to the next file at clean EOF.

use std::convert::TryInto;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;
use thiserror::Error;

const HEADER_BYTES: usize = 4 + 4 + 8;
pub const DEFAULT_ROTATION_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("invalid wal file name: {0:?}")]
    InvalidFileName(PathBuf),
    #[error("payload too large for one record: {0} bytes")]
    PayloadTooLarge(usize),
}

#[derive(Debug, Error)]
pub enum WalReadError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("crc mismatch at record seq {0}")]
    CrcMismatch(u64),
    #[error("torn record at file tail: {0}")]
    Truncated(String),
}

/// Append-only writer. Holds a single open file handle and rotates when
/// the current file exceeds `rotation_bytes`.
pub struct WalWriter {
    dir: PathBuf,
    file: File,
    file_seq: u64,
    file_bytes: u64,
    next_record_seq: u64,
    rotation_bytes: u64,
}

impl WalWriter {
    pub fn open(dir: &Path) -> Result<Self, WalError> {
        Self::open_with_rotation(dir, DEFAULT_ROTATION_BYTES)
    }

    pub fn open_with_rotation(dir: &Path, rotation_bytes: u64) -> Result<Self, WalError> {
        fs::create_dir_all(dir)?;
        let entries = list_wal_files(dir)?;
        let file_seq = entries.last().map(|(s, _)| *s).unwrap_or(0);
        // True max across all files (defensive — in practice only the highest
        // file has the latest records, but a crash mid-rotation could leave
        // an empty current file with the real max in the previous file).
        let mut next_record_seq = 1u64;
        for (_, path) in &entries {
            let m = scan_max_record_seq(path)?;
            if m + 1 > next_record_seq {
                next_record_seq = m + 1;
            }
        }
        let path = wal_path(dir, file_seq);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let file_bytes = file.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            file,
            file_seq,
            file_bytes,
            next_record_seq,
            rotation_bytes,
        })
    }

    /// Append one payload. Returns the assigned record_seq. The on-disk
    /// state is `fsync`'d before returning.
    pub fn append(&mut self, payload: &[u8]) -> Result<u64, WalError> {
        if payload.len() > u32::MAX as usize {
            return Err(WalError::PayloadTooLarge(payload.len()));
        }
        let record_seq = self.next_record_seq;
        let mut hasher = Hasher::new();
        hasher.update(&record_seq.to_le_bytes());
        hasher.update(payload);
        let crc = hasher.finalize();

        let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&record_seq.to_le_bytes());
        frame.extend_from_slice(payload);

        self.file.write_all(&frame)?;
        // sync_data == fdatasync — flushes file contents but skips metadata
        // updates the ack doesn't depend on. Faster than sync_all.
        self.file.sync_data()?;

        self.file_bytes += frame.len() as u64;
        self.next_record_seq += 1;

        if self.file_bytes >= self.rotation_bytes {
            self.rotate()?;
        }
        Ok(record_seq)
    }

    fn rotate(&mut self) -> Result<(), WalError> {
        self.file_seq += 1;
        let path = wal_path(&self.dir, self.file_seq);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        self.file = file;
        self.file_bytes = 0;
        Ok(())
    }

    /// Delete WAL files whose records are all `<= flushed_through_seq`.
    /// Never deletes the file currently being appended to.
    pub fn truncate_through(&mut self, flushed_through_seq: u64) -> Result<(), WalError> {
        let entries = list_wal_files(&self.dir)?;
        for (file_seq, path) in entries {
            if file_seq == self.file_seq {
                continue;
            }
            let max = scan_max_record_seq(&path)?;
            if max <= flushed_through_seq {
                fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    pub fn next_record_seq(&self) -> u64 {
        self.next_record_seq
    }

    #[cfg(test)]
    pub fn current_file_seq(&self) -> u64 {
        self.file_seq
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub record_seq: u64,
    pub payload: Vec<u8>,
}

/// Replay iterator. Yields records in seq order across all WAL files.
/// Stops cleanly at the first corruption — caller decides whether to
/// continue (skipping bad records) or refuse to start.
pub struct WalIter {
    files: std::vec::IntoIter<(u64, PathBuf)>,
    current: Option<BufReader<File>>,
}

impl WalIter {
    pub fn open(dir: &Path) -> Result<Self, WalReadError> {
        let files = if dir.exists() {
            list_wal_files(dir).map_err(|e| match e {
                WalError::Io(io) => WalReadError::Io(io),
                other => WalReadError::Io(io::Error::new(io::ErrorKind::Other, other.to_string())),
            })?
        } else {
            Vec::new()
        };
        Ok(Self {
            files: files.into_iter(),
            current: None,
        })
    }
}

impl Iterator for WalIter {
    type Item = Result<WalRecord, WalReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current.is_none() {
                let (_, path) = self.files.next()?;
                match File::open(&path) {
                    Ok(f) => self.current = Some(BufReader::new(f)),
                    Err(e) => return Some(Err(e.into())),
                }
            }
            let r = self.current.as_mut().unwrap();
            let mut hdr = [0u8; HEADER_BYTES];
            match r.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    self.current = None;
                    continue;
                }
                Err(e) => return Some(Err(e.into())),
            }
            let payload_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
            let record_seq = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
            let mut payload = vec![0u8; payload_len];
            match r.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Some(Err(WalReadError::Truncated(format!(
                        "record seq {} declares {} byte payload but file ended early",
                        record_seq, payload_len
                    ))));
                }
                Err(e) => return Some(Err(e.into())),
            }
            let mut hasher = Hasher::new();
            hasher.update(&record_seq.to_le_bytes());
            hasher.update(&payload);
            if hasher.finalize() != crc {
                return Some(Err(WalReadError::CrcMismatch(record_seq)));
            }
            return Some(Ok(WalRecord {
                record_seq,
                payload,
            }));
        }
    }
}

fn wal_path(dir: &Path, file_seq: u64) -> PathBuf {
    dir.join(format!("{:010}.log", file_seq))
}

fn list_wal_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) != Some("log") {
            continue;
        }
        let stem = p
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| WalError::InvalidFileName(p.clone()))?;
        let seq: u64 = stem
            .parse()
            .map_err(|_| WalError::InvalidFileName(p.clone()))?;
        out.push((seq, p));
    }
    out.sort_by_key(|(s, _)| *s);
    Ok(out)
}

fn scan_max_record_seq(path: &Path) -> Result<u64, WalError> {
    let f = File::open(path)?;
    let mut r = BufReader::new(f);
    let mut max_seq: u64 = 0;
    loop {
        let mut hdr = [0u8; HEADER_BYTES];
        if r.read_exact(&mut hdr).is_err() {
            break;
        }
        let payload_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
        let record_seq = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
        let mut skip = vec![0u8; payload_len];
        if r.read_exact(&mut skip).is_err() {
            break;
        }
        if record_seq > max_seq {
            max_seq = record_seq;
        }
    }
    Ok(max_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn append_and_read_back() {
        let d = tmp();
        let mut w = WalWriter::open(d.path()).unwrap();
        let s1 = w.append(b"hello").unwrap();
        let s2 = w.append(b"world").unwrap();
        assert_eq!(s1, 1);
        assert_eq!(s2, 2);
        drop(w);

        let recs: Vec<WalRecord> = WalIter::open(d.path())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].record_seq, 1);
        assert_eq!(recs[0].payload, b"hello");
        assert_eq!(recs[1].record_seq, 2);
        assert_eq!(recs[1].payload, b"world");
    }

    #[test]
    fn empty_dir_iter_is_empty() {
        let d = tmp();
        let recs: Vec<_> = WalIter::open(d.path())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(recs.is_empty());
    }

    #[test]
    fn nonexistent_dir_iter_is_empty() {
        let d = tmp();
        let nope = d.path().join("nope");
        let recs: Vec<_> = WalIter::open(&nope)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(recs.is_empty());
    }

    #[test]
    fn reopen_continues_record_seq() {
        let d = tmp();
        let mut w1 = WalWriter::open(d.path()).unwrap();
        w1.append(b"a").unwrap();
        w1.append(b"b").unwrap();
        drop(w1);

        let mut w2 = WalWriter::open(d.path()).unwrap();
        let seq = w2.append(b"c").unwrap();
        assert_eq!(seq, 3);
        drop(w2);

        let recs: Vec<u64> = WalIter::open(d.path())
            .unwrap()
            .filter_map(|r| r.ok().map(|r| r.record_seq))
            .collect();
        assert_eq!(recs, vec![1, 2, 3]);
    }

    #[test]
    fn rotation_on_size() {
        let d = tmp();
        // 64 byte rotation; each "hello" record is 16+5=21 bytes.
        let mut w = WalWriter::open_with_rotation(d.path(), 64).unwrap();
        for _ in 0..10 {
            w.append(b"hello").unwrap();
        }
        let files = list_wal_files(d.path()).unwrap();
        assert!(files.len() >= 2, "expected rotation, got {} files", files.len());
        drop(w);

        let recs: Vec<_> = WalIter::open(d.path())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(recs.len(), 10);
        for (i, r) in recs.iter().enumerate() {
            assert_eq!(r.record_seq, (i + 1) as u64);
            assert_eq!(r.payload, b"hello");
        }
    }

    #[test]
    fn torn_tail_yields_truncated_error() {
        let d = tmp();
        let mut w = WalWriter::open(d.path()).unwrap();
        w.append(b"first").unwrap();
        w.append(b"second").unwrap();
        drop(w);

        // Chop a few bytes off the end to simulate crash mid-write.
        let files = list_wal_files(d.path()).unwrap();
        let path = &files.last().unwrap().1;
        let len = std::fs::metadata(path).unwrap().len();
        let f = OpenOptions::new().write(true).open(path).unwrap();
        f.set_len(len - 3).unwrap();
        drop(f);

        let mut iter = WalIter::open(d.path()).unwrap();
        let r1 = iter.next().unwrap().unwrap();
        assert_eq!(r1.payload, b"first");
        let r2 = iter.next().unwrap();
        assert!(matches!(r2, Err(WalReadError::Truncated(_))));
    }

    #[test]
    fn crc_mismatch_detected() {
        let d = tmp();
        let mut w = WalWriter::open(d.path()).unwrap();
        w.append(b"hello").unwrap();
        drop(w);

        let files = list_wal_files(d.path()).unwrap();
        let path = &files[0].1;
        let mut bytes = std::fs::read(path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(path, &bytes).unwrap();

        let mut iter = WalIter::open(d.path()).unwrap();
        let r = iter.next().unwrap();
        assert!(matches!(r, Err(WalReadError::CrcMismatch(_))));
    }

    #[test]
    fn truncate_through_deletes_finished_files() {
        let d = tmp();
        let mut w = WalWriter::open_with_rotation(d.path(), 30).unwrap();
        for i in 0..5 {
            w.append(format!("rec-{:02}", i).as_bytes()).unwrap();
        }
        let files_before = list_wal_files(d.path()).unwrap();
        assert!(files_before.len() >= 2);
        let last_file_seq = files_before.last().unwrap().0;

        // Flushed up to record_seq 3 — files whose max <= 3 are deletable.
        w.truncate_through(3).unwrap();
        let files_after = list_wal_files(d.path()).unwrap();

        // The current file is never touched.
        assert!(files_after.iter().any(|(s, _)| *s == last_file_seq));

        // Records 4 and 5 should still be present and readable.
        let seqs: Vec<u64> = WalIter::open(d.path())
            .unwrap()
            .filter_map(|r| r.ok().map(|r| r.record_seq))
            .collect();
        assert!(seqs.contains(&4), "seq 4 missing: {:?}", seqs);
        assert!(seqs.contains(&5), "seq 5 missing: {:?}", seqs);
    }

    #[test]
    fn appended_payload_with_zero_bytes_roundtrips() {
        // The header is fixed-size; zero bytes inside payload must not confuse anything.
        let d = tmp();
        let mut w = WalWriter::open(d.path()).unwrap();
        let payload: Vec<u8> = vec![0x00, 0x01, 0x00, 0xff, 0x00];
        w.append(&payload).unwrap();
        drop(w);

        let recs: Vec<WalRecord> = WalIter::open(d.path())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].payload, payload);
    }
}
