//! In-process indexing of an untrusted pack: the replacement for
//! `git index-pack` against an empty object database.
//!
//! The pack's bytes are never rewritten. Cairn keeps the received pack as it
//! arrived and needs an index that describes exactly those bytes, so gix's
//! bundle writer, which turns ref-deltas into offset-deltas while it resolves
//! them, is not usable here. Instead entries are streamed once to verify their
//! zlib streams, CRCs and the trailing checksum, then every object is decoded
//! from the mapped pack, with a ref-delta's base resolved only from objects in
//! the same pack. A delta whose base the pack does not carry is a thin pack and
//! is refused, the property `index-pack` gave by indexing against an empty
//! object directory.
//!
//! The index is written in version 2 format, byte for byte what `git
//! index-pack --index-version=2` writes for the same pack, because receivers
//! compare the derived index with the one the producer built.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use gix_hash::{Kind as HashKind, ObjectId};
use gix_object::Kind as ObjectKind;
use gix_pack::cache::DecodeEntry;
use gix_pack::data::decode::entry::ResolvedBase;
use gix_pack::data::decode::Error as DecodeError;
use gix_pack::data::input::{BytesToEntriesIter, EntryDataMode, Mode};

const HASH_KIND: HashKind = HashKind::Sha1;
const HASH_LEN: u64 = 20;
const PACK_HEADER_LEN: u64 = 12;
const IDX_V2_MAGIC: [u8; 4] = [0xff, b't', b'O', b'c'];
const LARGE_OFFSET_THRESHOLD: u64 = 0x7fff_ffff;
/// Decoded objects kept for delta bases. Bases almost always precede their
/// deltas closely, so a small window turns chain decoding from quadratic into
/// linear without holding the whole pack inflated.
const DELTA_BASE_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// One object of an indexed pack, in pack order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexedObject {
    pub oid: String,
    pub kind: &'static str,
    /// The object's own size, resolved through any delta chain.
    pub size: u64,
}

/// An indexed pack: its trailing checksum, its objects, and the version 2
/// index describing it.
#[derive(Debug)]
pub(crate) struct IndexedPack {
    pub pack_checksum: String,
    pub objects: Vec<IndexedObject>,
    pub index: Vec<u8>,
}

/// Why a pack could not be indexed.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IndexError {
    /// The pack is malformed, truncated, thin, or otherwise not a complete
    /// self-contained pack.
    Invalid(String),
    /// The pack declares more objects than `max_objects`.
    TooManyObjects { actual: u64 },
    /// Resolving the pack's objects passed `max_inflated_bytes`.
    TooLarge { actual: u64 },
}

/// The bounds an untrusted pack is held to while it is indexed, enforced as
/// early as each can be known so a hostile pack cannot spend more than them.
#[derive(Clone, Copy, Debug)]
pub(crate) struct IndexBounds {
    pub max_objects: u64,
    pub max_inflated_bytes: u64,
}

fn invalid(detail: impl std::fmt::Display) -> IndexError {
    IndexError::Invalid(detail.to_string())
}

struct Located {
    pack_offset: u64,
    crc32: u32,
}

struct Resolved {
    oid: ObjectId,
    kind: ObjectKind,
    size: u64,
}

/// Index the pack at `pack_path`, which must be a complete pack carrying every
/// delta base it uses.
pub(crate) fn index_pack(pack_path: &Path, bounds: IndexBounds) -> Result<IndexedPack, IndexError> {
    let file_len = std::fs::metadata(pack_path)
        .map_err(|e| invalid(format!("reading pack: {e}")))?
        .len();
    let pack = gix_pack::data::File::at(pack_path, HASH_KIND)
        .map_err(|e| invalid(format!("pack header: {e}")))?
        .with_alloc_limit_bytes(Some(
            usize::try_from(bounds.max_inflated_bytes).unwrap_or(usize::MAX),
        ));
    let declared = u64::from(pack.num_objects());
    if declared > bounds.max_objects {
        return Err(IndexError::TooManyObjects { actual: declared });
    }

    let (located, pack_checksum) = scan_entries(pack_path, file_len)?;
    if located.len() as u64 != declared {
        return Err(invalid(format!(
            "pack declares {declared} objects but carries {}",
            located.len()
        )));
    }
    let resolved = resolve_objects(&pack, &located, bounds)?;

    let mut objects = Vec::with_capacity(resolved.len());
    let mut sorted = Vec::with_capacity(resolved.len());
    for (entry, object) in located.iter().zip(&resolved) {
        objects.push(IndexedObject {
            oid: object.oid.to_hex().to_string(),
            kind: kind_name(object.kind),
            size: object.size,
        });
        sorted.push((object.oid, entry.crc32, entry.pack_offset));
    }
    sorted.sort_unstable_by_key(|entry| entry.0);
    if let Some(pair) = sorted.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(invalid(format!("duplicate object {} in pack", pair[0].0)));
    }
    let index = encode_index_v2(&sorted, &pack_checksum)?;
    Ok(IndexedPack {
        pack_checksum: pack_checksum.to_hex().to_string(),
        objects,
        index,
    })
}

/// Stream every entry once, verifying each zlib stream, computing each CRC and
/// checking the trailing checksum, and refuse bytes past the trailer.
fn scan_entries(pack_path: &Path, file_len: u64) -> Result<(Vec<Located>, ObjectId), IndexError> {
    let reader =
        BufReader::new(File::open(pack_path).map_err(|e| invalid(format!("opening pack: {e}")))?);
    let entries =
        BytesToEntriesIter::new_from_header(reader, Mode::Verify, EntryDataMode::Crc32, HASH_KIND)
            .map_err(invalid)?;
    let mut located = Vec::new();
    let mut end = PACK_HEADER_LEN;
    let mut trailer = None;
    for entry in entries {
        let entry = entry.map_err(invalid)?;
        end = entry.pack_offset + u64::from(entry.header_size) + entry.compressed_size;
        trailer = entry.trailer.or(trailer);
        located.push(Located {
            pack_offset: entry.pack_offset,
            crc32: entry
                .crc32
                .ok_or_else(|| invalid("pack entry without a CRC"))?,
        });
    }
    let checksum = match trailer {
        Some(checksum) => checksum,
        None => empty_pack_checksum(pack_path, file_len)?,
    };
    if file_len != end + HASH_LEN {
        return Err(invalid(format!(
            "pack has {} bytes after its trailer",
            file_len.saturating_sub(end + HASH_LEN)
        )));
    }
    Ok((located, checksum))
}

/// An empty pack yields no entries to carry the verified trailer, so its
/// checksum is verified here.
fn empty_pack_checksum(pack_path: &Path, file_len: u64) -> Result<ObjectId, IndexError> {
    if file_len != PACK_HEADER_LEN + HASH_LEN {
        return Err(invalid("truncated empty pack"));
    }
    let bytes = std::fs::read(pack_path).map_err(|e| invalid(format!("reading pack: {e}")))?;
    let mut hasher = gix_hash::hasher(HASH_KIND);
    hasher.update(&bytes[..PACK_HEADER_LEN as usize]);
    let computed = hasher.try_finalize().map_err(invalid)?;
    let stored = ObjectId::from_bytes_or_panic(&bytes[PACK_HEADER_LEN as usize..]);
    if computed != stored {
        return Err(invalid("empty pack checksum mismatch"));
    }
    Ok(stored)
}

/// Decode every object, resolving a ref-delta's base only from objects of
/// this pack. A ref-delta whose base has not been resolved yet waits on it, so
/// bases may appear in any order; one the pack never supplies makes it thin.
fn resolve_objects(
    pack: &gix_pack::data::File,
    located: &[Located],
    bounds: IndexBounds,
) -> Result<Vec<Resolved>, IndexError> {
    let mut resolved: Vec<Option<Resolved>> = (0..located.len()).map(|_| None).collect();
    let by_oid: RefCell<HashMap<ObjectId, u64>> =
        RefCell::new(HashMap::with_capacity(located.len()));
    let mut waiting: HashMap<ObjectId, Vec<usize>> = HashMap::new();
    let mut cache = BaseCache::default();
    let mut inflate = gix_features::zlib::Inflate::default();
    let mut buffer = Vec::new();
    let mut inflated: u64 = 0;

    let resolve = |base: &gix_hash::oid, _out: &mut Vec<u8>| -> Option<ResolvedBase> {
        let offset = by_oid.borrow().get(base).copied()?;
        pack.entry(offset).ok().map(ResolvedBase::InPack)
    };

    let mut ready: Vec<usize> = (0..located.len()).rev().collect();
    while let Some(index) = ready.pop() {
        let offset = located[index].pack_offset;
        let entry = pack
            .entry(offset)
            .map_err(|e| invalid(format!("pack entry at {offset}: {e}")))?;
        // A whole object's size is in its entry header: refuse it before
        // inflating it. A delta's result size is only known while decoding,
        // where the allocation cap on the mapped pack refuses it instead.
        if entry.header.as_kind().is_some() {
            let after = inflated.saturating_add(entry.decompressed_size);
            if after > bounds.max_inflated_bytes {
                return Err(IndexError::TooLarge { actual: after });
            }
        }
        let outcome =
            match pack.decode_entry(entry, &mut buffer, &mut inflate, &resolve, &mut cache) {
                Ok(outcome) => outcome,
                Err(DecodeError::DeltaBaseUnresolved(base)) => {
                    waiting.entry(base).or_default().push(index);
                    continue;
                }
                Err(DecodeError::OutOfMemory) => {
                    return Err(IndexError::TooLarge {
                        actual: bounds.max_inflated_bytes.saturating_add(1),
                    })
                }
                Err(error) => return Err(invalid(format!("decoding object at {offset}: {error}"))),
            };
        let size = outcome.object_size;
        inflated = inflated.saturating_add(size);
        if inflated > bounds.max_inflated_bytes {
            return Err(IndexError::TooLarge { actual: inflated });
        }
        let oid = gix_object::compute_hash(HASH_KIND, outcome.kind, &buffer).map_err(invalid)?;
        if by_oid
            .borrow_mut()
            .insert(oid, located[index].pack_offset)
            .is_some()
        {
            return Err(invalid(format!("duplicate object {oid} in pack")));
        }
        resolved[index] = Some(Resolved {
            oid,
            kind: outcome.kind,
            size,
        });
        if let Some(dependents) = waiting.remove(&oid) {
            ready.extend(dependents);
        }
    }
    if let Some(base) = waiting.keys().min() {
        return Err(invalid(format!(
            "thin pack: delta base {base} is not in the pack"
        )));
    }
    resolved
        .into_iter()
        .map(|object| object.ok_or_else(|| invalid("unresolved pack entry")))
        .collect()
}

/// A bounded window of recently decoded objects, keyed by pack offset.
#[derive(Default)]
struct BaseCache {
    objects: HashMap<u64, (ObjectKind, Vec<u8>)>,
    order: VecDeque<u64>,
    bytes: usize,
}

impl DecodeEntry for BaseCache {
    fn put(
        &mut self,
        _pack_id: u32,
        offset: u64,
        data: &[u8],
        kind: ObjectKind,
        _compressed_size: usize,
    ) {
        if data.len() > DELTA_BASE_CACHE_BYTES || self.objects.contains_key(&offset) {
            return;
        }
        while self.bytes + data.len() > DELTA_BASE_CACHE_BYTES {
            let Some(evicted) = self.order.pop_front() else {
                break;
            };
            if let Some((_, bytes)) = self.objects.remove(&evicted) {
                self.bytes -= bytes.len();
            }
        }
        self.bytes += data.len();
        self.order.push_back(offset);
        self.objects.insert(offset, (kind, data.to_vec()));
    }

    fn get(
        &mut self,
        _pack_id: u32,
        offset: u64,
        out: &mut Vec<u8>,
    ) -> Option<(ObjectKind, usize)> {
        let (kind, data) = self.objects.get(&offset)?;
        out.clear();
        out.extend_from_slice(data);
        Some((*kind, data.len()))
    }
}

/// Encode a version 2 pack index over `sorted` (object id, CRC32, pack offset),
/// ordered by object id. Offsets past 31 bits go to the large-offset table in
/// the same order git assigns them.
fn encode_index_v2(
    sorted: &[(ObjectId, u32, u64)],
    pack_checksum: &ObjectId,
) -> Result<Vec<u8>, IndexError> {
    let mut out = Vec::with_capacity(8 + 256 * 4 + sorted.len() * 28 + 40);
    out.extend_from_slice(&IDX_V2_MAGIC);
    out.extend_from_slice(&2u32.to_be_bytes());
    let mut fanout = [0u32; 256];
    for (oid, _, _) in sorted {
        fanout[usize::from(oid.as_bytes()[0])] += 1;
    }
    let mut cumulative = 0u32;
    for count in fanout {
        cumulative += count;
        out.extend_from_slice(&cumulative.to_be_bytes());
    }
    for (oid, _, _) in sorted {
        out.extend_from_slice(oid.as_bytes());
    }
    for (_, crc32, _) in sorted {
        out.extend_from_slice(&crc32.to_be_bytes());
    }
    let mut large = Vec::new();
    for (_, _, offset) in sorted {
        if *offset > LARGE_OFFSET_THRESHOLD {
            let slot = u32::try_from(large.len()).map_err(|_| invalid("too many large offsets"))?;
            out.extend_from_slice(&(0x8000_0000 | slot).to_be_bytes());
            large.push(*offset);
        } else {
            out.extend_from_slice(&(*offset as u32).to_be_bytes());
        }
    }
    for offset in large {
        out.extend_from_slice(&offset.to_be_bytes());
    }
    out.extend_from_slice(pack_checksum.as_bytes());
    let mut hasher = gix_hash::hasher(HASH_KIND);
    hasher.update(&out);
    let checksum = hasher.try_finalize().map_err(invalid)?;
    out.extend_from_slice(checksum.as_bytes());
    Ok(out)
}

fn kind_name(kind: ObjectKind) -> &'static str {
    match kind {
        ObjectKind::Commit => "commit",
        ObjectKind::Tree => "tree",
        ObjectKind::Blob => "blob",
        ObjectKind::Tag => "tag",
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::process::{Command, Stdio};

    use gix_pack::data::entry::Header;

    use super::*;
    use crate::testutil::{commit_all, git, init_repo, write_file};

    const UNBOUNDED: IndexBounds = IndexBounds {
        max_objects: u64::MAX,
        max_inflated_bytes: u64::MAX,
    };

    fn run_git(dir: &Path, args: &[&str], stdin: &[u8]) -> std::process::Output {
        let mut child = Command::new("git")
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn git");
        let mut pipe = child.stdin.take().unwrap();
        let input = stdin.to_vec();
        let writer = std::thread::spawn(move || pipe.write_all(&input));
        let output = child.wait_with_output().unwrap();
        writer.join().unwrap().unwrap();
        output
    }

    /// `git index-pack` of `pack` against an empty object directory: the
    /// reference this indexer replaces. Returns the index and pack checksum.
    fn git_index(pack: &[u8]) -> Result<(Vec<u8>, String), String> {
        let dir = tempfile::tempdir().unwrap();
        let git_dir = dir.path().join("quarantine.git");
        // host-default-branch: an empty quarantine for index-pack; no ref is read.
        git(
            dir.path(),
            &["init", "--bare", "-q", git_dir.to_str().unwrap()],
        );
        let pack_path = dir.path().join("incoming.pack");
        std::fs::write(&pack_path, pack).unwrap();
        let index_path = dir.path().join("incoming.idx");
        let output = Command::new("git")
            .args(["index-pack", "--index-version=2", "-o"])
            .arg(&index_path)
            .arg(&pack_path)
            .env("GIT_DIR", &git_dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .output()
            .unwrap();
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned());
        }
        Ok((
            std::fs::read(index_path).unwrap(),
            String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        ))
    }

    fn index_bytes(pack: &[u8], bounds: IndexBounds) -> Result<IndexedPack, IndexError> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("incoming.pack");
        std::fs::write(&path, pack).unwrap();
        index_pack(&path, bounds)
    }

    /// A history whose versions of one text file delta well against each
    /// other, beside a nested tree.
    fn history() -> (tempfile::TempDir, Vec<String>) {
        let repo = tempfile::tempdir().unwrap();
        init_repo(repo.path());
        let mut text = String::new();
        for line in 0..2000 {
            text.push_str(&format!("line {line} of a file that deltas well\n"));
        }
        let mut tips = Vec::new();
        for version in 0..4 {
            write_file(
                repo.path(),
                "notes.txt",
                format!("{text}version {version}\n").as_bytes(),
            );
            write_file(
                repo.path(),
                &format!("nested/dir/{version}.txt"),
                format!("payload {version}").as_bytes(),
            );
            tips.push(commit_all(repo.path(), &format!("version {version}")));
        }
        (repo, tips)
    }

    fn pack_all(repo: &Path, flags: &[&str]) -> Vec<u8> {
        let objects = git(repo, &["rev-list", "--objects", "--all"]);
        let mut args = vec!["pack-objects", "--stdout", "--quiet"];
        args.extend_from_slice(flags);
        let output = run_git(repo, &args, objects.as_bytes());
        assert!(output.status.success());
        output.stdout
    }

    fn count_entries(pack: &[u8], matches: fn(&Header) -> bool) -> usize {
        BytesToEntriesIter::new_from_header(
            std::io::BufReader::new(pack),
            Mode::Verify,
            EntryDataMode::Ignore,
            HASH_KIND,
        )
        .unwrap()
        .filter(|entry| matches(&entry.as_ref().unwrap().header))
        .count()
    }

    /// Hold the in-process index to git's on `pack`: the same index bytes,
    /// the same checksum, and each object's real kind and size.
    fn assert_indexes_like_git(pack: &[u8], repo: Option<&Path>) -> IndexedPack {
        let (index, checksum) = git_index(pack).expect("git indexes the pack");
        let indexed = index_bytes(pack, UNBOUNDED).expect("the pack indexes in process");
        assert_eq!(indexed.pack_checksum, checksum);
        assert!(
            indexed.index == index,
            "the index differs from git index-pack's"
        );
        if let Some(repo) = repo {
            let input: String = indexed
                .objects
                .iter()
                .map(|object| format!("{}\n", object.oid))
                .collect();
            let output = run_git(repo, &["cat-file", "--batch-check"], input.as_bytes());
            let expected: Vec<String> = String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect();
            let actual: Vec<String> = indexed
                .objects
                .iter()
                .map(|object| format!("{} {} {}", object.oid, object.kind, object.size))
                .collect();
            assert_eq!(actual, expected);
        }
        indexed
    }

    #[test]
    fn transfer_ref_delta_and_offset_delta_packs_index_exactly_as_git_does() {
        let (repo, _) = history();
        let is_ref = |header: &Header| matches!(header, Header::RefDelta { .. });
        let is_ofs = |header: &Header| matches!(header, Header::OfsDelta { .. });

        let transfer = pack_all(repo.path(), &["--window=0"]);
        assert_eq!(
            count_entries(&transfer, is_ref) + count_entries(&transfer, is_ofs),
            0
        );
        assert_indexes_like_git(&transfer, Some(repo.path()));

        // Plain `pack-objects`, as the archival builder runs it, names delta
        // bases by object id.
        let by_id = pack_all(repo.path(), &[]);
        assert!(count_entries(&by_id, is_ref) > 0);
        assert_indexes_like_git(&by_id, Some(repo.path()));

        let by_offset = pack_all(repo.path(), &["--delta-base-offset"]);
        assert!(count_entries(&by_offset, is_ofs) > 0);
        assert_indexes_like_git(&by_offset, Some(repo.path()));
    }

    fn deflate(bytes: &[u8]) -> Vec<u8> {
        let mut writer = gix_features::zlib::stream::deflate::Write::new(Vec::new());
        writer.write_all(bytes).unwrap();
        writer.flush().unwrap();
        writer.into_inner()
    }

    fn entry_header(kind: u8, size: usize) -> Vec<u8> {
        let mut size = size;
        let mut first = (kind << 4) | (size & 0x0f) as u8;
        size >>= 4;
        let mut out = Vec::new();
        if size > 0 {
            first |= 0x80;
        }
        out.push(first);
        while size > 0 {
            let mut byte = (size & 0x7f) as u8;
            size >>= 7;
            if size > 0 {
                byte |= 0x80;
            }
            out.push(byte);
        }
        out
    }

    fn finish_pack(count: u32, entries: &[u8]) -> Vec<u8> {
        let mut pack = b"PACK".to_vec();
        pack.extend_from_slice(&2u32.to_be_bytes());
        pack.extend_from_slice(&count.to_be_bytes());
        pack.extend_from_slice(entries);
        let mut hasher = gix_hash::hasher(HASH_KIND);
        hasher.update(&pack);
        pack.extend_from_slice(hasher.try_finalize().unwrap().as_bytes());
        pack
    }

    /// A ref-delta written before the base it names, which git accepts and
    /// no Cairn producer writes, but a hostile or foreign one may.
    fn delta_before_base() -> (Vec<u8>, ObjectId) {
        let base = b"the base blob, carried after its delta\n";
        let result = b"the delta result\n";
        let mut delta = vec![base.len() as u8, result.len() as u8, result.len() as u8];
        delta.extend_from_slice(result);
        let base_id = gix_object::compute_hash(HASH_KIND, ObjectKind::Blob, base).unwrap();
        let mut entries = entry_header(7, delta.len());
        entries.extend_from_slice(base_id.as_bytes());
        entries.extend_from_slice(&deflate(&delta));
        entries.extend_from_slice(&entry_header(3, base.len()));
        entries.extend_from_slice(&deflate(base));
        (finish_pack(2, &entries), base_id)
    }

    #[test]
    fn a_ref_delta_may_precede_its_base_in_the_pack() {
        let (pack, base_id) = delta_before_base();
        let indexed = assert_indexes_like_git(&pack, None);
        let expected_result =
            gix_object::compute_hash(HASH_KIND, ObjectKind::Blob, b"the delta result\n").unwrap();
        assert_eq!(
            indexed.objects,
            vec![
                IndexedObject {
                    oid: expected_result.to_hex().to_string(),
                    kind: "blob",
                    size: 17,
                },
                IndexedObject {
                    oid: base_id.to_hex().to_string(),
                    kind: "blob",
                    size: 39,
                },
            ]
        );
    }

    #[test]
    fn an_empty_pack_indexes_like_git() {
        let indexed = assert_indexes_like_git(&finish_pack(0, &[]), None);
        assert!(indexed.objects.is_empty());
    }

    #[test]
    fn a_thin_pack_is_refused_naming_the_base_it_lacks() {
        let (repo, tips) = history();
        let revs = format!("{}\n^{}\n", tips[3], tips[2]);
        let output = run_git(
            repo.path(),
            &["pack-objects", "--stdout", "--quiet", "--revs", "--thin"],
            revs.as_bytes(),
        );
        assert!(output.status.success());
        let thin = output.stdout;
        assert!(git_index(&thin).is_err(), "git refuses the thin pack too");
        let IndexError::Invalid(detail) = index_bytes(&thin, UNBOUNDED).unwrap_err() else {
            panic!("a thin pack is invalid, not over a limit");
        };
        assert!(detail.starts_with("thin pack: delta base "), "{detail}");

        // The hand-built pack without its base is thin in the same way.
        let (_, base_id) = delta_before_base();
        let mut delta = vec![39, 17, 17];
        delta.extend_from_slice(b"the delta result\n");
        let mut entries = entry_header(7, delta.len());
        entries.extend_from_slice(base_id.as_bytes());
        entries.extend_from_slice(&deflate(&delta));
        let IndexError::Invalid(detail) =
            index_bytes(&finish_pack(1, &entries), UNBOUNDED).unwrap_err()
        else {
            panic!("a thin pack is invalid");
        };
        assert_eq!(
            detail,
            format!("thin pack: delta base {base_id} is not in the pack")
        );
    }

    #[test]
    fn bytes_after_the_trailer_and_a_wrong_count_are_refused() {
        let (repo, _) = history();
        let pack = pack_all(repo.path(), &["--window=0"]);
        let mut junk = pack.clone();
        junk.extend_from_slice(b"junk");
        assert!(git_index(&junk).is_err());
        assert!(matches!(
            index_bytes(&junk, UNBOUNDED),
            Err(IndexError::Invalid(_))
        ));

        let mut miscounted = pack[..pack.len() - 20].to_vec();
        let count = u32::from_be_bytes(miscounted[8..12].try_into().unwrap());
        miscounted[8..12].copy_from_slice(&(count + 1).to_be_bytes());
        let miscounted = finish_pack(count + 1, &miscounted[12..]);
        assert!(git_index(&miscounted).is_err());
        assert!(matches!(
            index_bytes(&miscounted, UNBOUNDED),
            Err(IndexError::Invalid(_))
        ));
    }

    #[test]
    fn the_inflated_bound_counts_what_deltas_expand_to() {
        let (repo, _) = history();
        let pack = pack_all(repo.path(), &[]);
        let indexed = assert_indexes_like_git(&pack, Some(repo.path()));
        let inflated: u64 = indexed.objects.iter().map(|object| object.size).sum();

        // `git verify-pack -v` reports a deltified object's delta size, so a
        // bound summed from it sat far below what the pack expands to.
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("p.idx");
        std::fs::write(dir.path().join("p.pack"), &pack).unwrap();
        std::fs::write(&index_path, &indexed.index).unwrap();
        let listing = git(
            dir.path(),
            &["verify-pack", "-v", index_path.to_str().unwrap()],
        );
        let reported: u64 = listing
            .lines()
            .filter_map(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                (fields.len() >= 5 && fields[0].len() == 40)
                    .then(|| fields[2].parse::<u64>().unwrap())
            })
            .sum();
        assert!(reported < inflated, "{reported} < {inflated}");

        let bounds = |max_objects, max_inflated_bytes| IndexBounds {
            max_objects,
            max_inflated_bytes,
        };
        assert!(matches!(
            index_bytes(&pack, bounds(u64::MAX, reported)),
            Err(IndexError::TooLarge { actual }) if actual > reported
        ));
        assert!(index_bytes(&pack, bounds(u64::MAX, inflated)).is_ok());
        let count = indexed.objects.len() as u64;
        assert_eq!(
            index_bytes(&pack, bounds(count - 1, u64::MAX)).unwrap_err(),
            IndexError::TooManyObjects { actual: count }
        );
    }
}
