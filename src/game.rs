use super::{is_zip, Error};
use core::num::ParseIntError;
use fxhash::{FxHashMap, FxHashSet};
use indicatif::{ProgressBar, ProgressStyle};
use prettytable::Table;
use serde_derive::{Deserialize, Serialize};
use sha1_smol::Sha1;
use std::cmp::Ordering;
use std::collections::hash_map::OccupiedEntry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io::{Read, Seek};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

const CACHE_XATTR: &str = "user.emupart";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GameDb {
    pub description: String,
    pub date: Option<String>,
    pub games: HashMap<String, Game>,
}

impl GameDb {
    #[inline]
    pub fn is_game(&self, game: &str) -> bool {
        self.games.contains_key(game)
    }

    #[inline]
    pub fn game(&self, game: &str) -> Option<&Game> {
        self.games.get(game)
    }

    #[inline]
    pub fn games_iter(&self) -> impl ExactSizeIterator<Item = &Game> {
        self.games.values()
    }

    #[inline]
    pub fn all_games<C: FromIterator<String>>(&self) -> C {
        self.games.keys().cloned().collect()
    }

    #[inline]
    pub fn retain_working(&mut self) {
        self.games.retain(|_, game| game.is_working())
    }

    pub fn validate_games<I>(&self, games: I) -> Result<(), Error>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        games.into_iter().try_for_each(|s| {
            if self.is_game(s.as_ref()) {
                Ok(())
            } else {
                Err(Error::NoSuchSoftware(s.as_ref().to_string()))
            }
        })
    }

    pub fn required_parts<I>(&self, games: I) -> Result<FxHashSet<Part>, Error>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut parts = FxHashSet::default();
        games
            .into_iter()
            .try_for_each(|game| {
                if let Some(game) = self.games.get(game.as_ref()) {
                    parts.extend(game.parts.values().cloned());
                    Ok(())
                } else {
                    Err(Error::NoSuchSoftware(game.as_ref().to_string()))
                }
            })
            .map(|()| parts)
    }

    pub fn verify<'a>(
        &self,
        root: &Path,
        games: &'a HashSet<String>,
    ) -> BTreeMap<&'a str, Vec<VerifyFailure>> {
        use indicatif::ParallelProgressIterator;
        use rayon::prelude::*;

        let pbar = ProgressBar::new(games.len() as u64).with_style(verify_style());
        pbar.set_message("verifying games");

        games
            .par_iter()
            .progress_with(pbar)
            .map(|game| (game.as_str(), self.verify_game(root, game)))
            .collect()
    }

    fn verify_game(&self, root: &Path, game_name: &str) -> Vec<VerifyFailure> {
        if let Some(game) = self.games.get(game_name) {
            let mut results = game.verify(&root.join(game_name));
            results.extend(
                game.devices
                    .iter()
                    .flat_map(|device| self.verify_game(root, device)),
            );
            results
        } else {
            Vec::new()
        }
    }

    pub fn list_results(&self, search: Option<&str>, simple: bool) -> Vec<GameRow> {
        if let Some(search) = search {
            self.games
                .values()
                .filter(|g| !g.is_device)
                .map(|g| g.report(simple))
                .filter(|g| g.matches(search))
                .collect()
        } else {
            self.games
                .values()
                .filter(|g| !g.is_device)
                .map(|g| g.report(simple))
                .collect()
        }
    }

    pub fn list(&self, search: Option<&str>, sort: GameColumn, simple: bool) {
        let mut results = self.list_results(search, simple);
        results.sort_by(|a, b| a.compare(b, sort));
        GameDb::display_report(&results)
    }

    pub fn games<I>(&self, games: I, simple: bool)
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        GameDb::display_report(
            &games
                .into_iter()
                .filter_map(|g| self.games.get(g.as_ref()).map(|g| g.report(simple)))
                .collect::<Vec<GameRow>>(),
        )
    }

    pub fn report_results(
        &self,
        games: &HashSet<String>,
        search: Option<&str>,
        simple: bool,
    ) -> Vec<GameRow> {
        let mut results: Vec<GameRow> = games
            .iter()
            .filter_map(|g| {
                self.games
                    .get(g)
                    .filter(|g| !g.is_device)
                    .map(|g| g.report(simple))
            })
            .collect();

        if let Some(search) = search {
            results.retain(|g| g.matches(search));
        }

        results
    }

    pub fn report(
        &self,
        games: &HashSet<String>,
        search: Option<&str>,
        sort: GameColumn,
        simple: bool,
    ) {
        let mut results = self.report_results(games, search, simple);
        results.sort_by(|a, b| a.compare(b, sort));
        GameDb::display_report(&results)
    }

    fn display_report(games: &[GameRow]) {
        use prettytable::{cell, format, row};

        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.get_format().column_separator('\u{2502}');

        for game in games {
            let description = game.description;
            let creator = game.creator;
            let year = game.year;
            let name = game.name;

            table.add_row(match game.status {
                Status::Working => row![description, creator, year, name],
                Status::Partial => row![FY => description, creator, year, name],
                Status::NotWorking => row![FR => description, creator, year, name],
            });
        }

        table.printstd();
    }

    pub fn display_parts(&self, name: &str) -> Result<(), Error> {
        use prettytable::{cell, format, row};

        let game = self
            .game(name)
            .ok_or_else(|| Error::NoSuchSoftware(name.to_string()))?;

        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.get_format().column_separator('\u{2502}');

        let devices: BTreeMap<&str, &Game> = game
            .devices
            .iter()
            .map(|dev| self.game(dev).expect("unknown device in game"))
            .filter(|game| !game.parts.is_empty())
            .map(|game| (game.name.as_str(), game))
            .collect();

        if devices.is_empty() {
            game.display_parts(&mut table);
        } else {
            table.add_row(row![H3cu->name]);
            game.display_parts(&mut table);
            for (dev_name, dev) in devices.into_iter() {
                table.add_row(row![H3cu->dev_name]);
                dev.display_parts(&mut table);
            }
        }

        table.printstd();
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum Status {
    Working,
    Partial,
    NotWorking,
}

impl Default for Status {
    fn default() -> Self {
        Status::Working
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Game {
    pub name: String,
    pub description: String,
    pub creator: String,
    pub year: String,
    pub status: Status,
    pub is_device: bool,
    pub parts: HashMap<String, Part>,
    pub devices: Vec<String>,
}

impl Game {
    #[inline]
    pub fn is_working(&self) -> bool {
        match self.status {
            Status::Working | Status::Partial => true,
            Status::NotWorking => false,
        }
    }

    pub fn report(&self, simple: bool) -> GameRow {
        #[inline]
        fn no_parens(s: &str) -> &str {
            if let Some(index) = s.find('(') {
                s[0..index].trim_end()
            } else {
                s
            }
        }

        #[inline]
        fn no_slashes(s: &str) -> &str {
            if let Some(index) = s.find(" / ") {
                s[0..index].trim_end()
            } else {
                s
            }
        }

        GameRow {
            name: &self.name,
            description: if simple {
                no_slashes(no_parens(&self.description))
            } else {
                &self.description
            },
            creator: if simple {
                no_parens(&self.creator)
            } else {
                &self.creator
            },
            year: &self.year,
            status: self.status,
        }
    }

    fn verify(&self, game_root: &Path) -> Vec<VerifyFailure> {
        use dashmap::DashMap;
        use rayon::prelude::*;
        use std::fs::read_dir;

        let mut failures = Vec::new();

        // turn files on disk into a map, so extra files can be located
        let files_on_disk: DashMap<String, PathBuf> = DashMap::new();

        match read_dir(game_root) {
            Ok(dir) => read_game_dir(
                dir,
                |name, pb| {
                    files_on_disk.insert(name, pb);
                },
                &mut failures,
            ),

            // no directory to read and no parts to check,
            // so no failures are possible
            Err(_) => return failures,
        }

        // verify all game parts
        failures.extend(
            self.parts
                .par_iter()
                .filter_map(|(name, part)| match files_on_disk.remove(name) {
                    Some((_, pathbuf)) => part.verify_cached(pathbuf).err(),
                    None => Some(VerifyFailure::Missing {
                        path: game_root.join(name),
                        part: part.clone(),
                    }),
                })
                .collect::<Vec<VerifyFailure>>()
                .into_iter(),
        );

        // mark any leftover files on disk as extras
        failures.extend(
            files_on_disk
                .into_iter()
                .map(|(_, pathbuf)| VerifyFailure::extra(pathbuf)),
        );

        failures
    }

    pub fn add_and_verify<F>(
        &self,
        rom_sources: &mut RomSources,
        target_dir: &Path,
        mut progress: F,
    ) -> Result<Vec<VerifyFailure>, Error>
    where
        F: FnMut(ExtractedPart<'_>),
    {
        use std::fs::read_dir;

        let mut failures = Vec::new();

        let game_root = target_dir.join(&self.name);

        // turn files on disk into a map, so extra files can be located
        let mut files_on_disk: HashMap<String, PathBuf> = HashMap::new();

        if let Ok(dir) = read_dir(&game_root) {
            read_game_dir(
                dir,
                |name, pb| {
                    files_on_disk.insert(name, pb);
                },
                &mut failures,
            );
        }

        // verify all game parts
        for (name, part) in self.parts.iter() {
            match files_on_disk.remove(name) {
                Some(target) => {
                    if let Err(failure) = part.verify_cached(target) {
                        match failure.populate(rom_sources)? {
                            Ok(d) => progress(d),
                            Err(failure) => failures.push(failure),
                        }
                    }
                }
                None => {
                    match (VerifyFailure::Missing {
                        part: part.clone(),
                        path: game_root.join(name),
                    })
                    .populate(rom_sources)?
                    {
                        Ok(d) => progress(d),
                        Err(failure) => failures.push(failure),
                    }
                }
            }
        }

        // mark any leftover files on disk as extras
        failures.extend(
            files_on_disk
                .into_iter()
                .map(|(_, pathbuf)| VerifyFailure::extra(pathbuf)),
        );

        Ok(failures)
    }

    pub fn rename(
        &self,
        target: &Path,
        file_move: fn(&Path, &Path) -> Result<(), std::io::Error>,
    ) -> Result<(), Error> {
        use std::fs::read_dir;

        let target_dir = target.join(&self.name);

        let dir = match read_dir(&target_dir) {
            Ok(dir) => dir,
            Err(_) => return Ok(()),
        };

        let parts: FxHashMap<Part, PathBuf> = self
            .parts
            .iter()
            .map(|(name, part)| (part.clone(), target_dir.join(name)))
            .collect();

        for entry in dir.filter_map(|e| e.ok()) {
            let entry_path = entry.path();
            if let Ok(part) = Part::from_path(&entry_path) {
                if let Some(target_path) = parts.get(&part) {
                    file_move(&entry_path, target_path)?;
                }
            }
        }

        Ok(())
    }

    pub fn display_parts(&self, table: &mut Table) {
        use prettytable::{cell, row};

        let parts: BTreeMap<&str, &Part> = self
            .parts
            .iter()
            .map(|(name, part)| (name.as_str(), part))
            .collect();

        if !parts.is_empty() {
            for (name, part) in parts {
                table.add_row(row![name, part.digest()]);
            }
        }
    }
}

fn read_game_dir<I, F>(dir: I, mut insert: F, failures: &mut Vec<VerifyFailure>)
where
    I: Iterator<Item = std::io::Result<std::fs::DirEntry>>,
    F: FnMut(String, PathBuf),
{
    for entry in dir
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
    {
        match entry.file_name().into_string() {
            Ok(name) => {
                insert(name, entry.path());
            }
            Err(_) => failures.push(VerifyFailure::extra(entry.path())),
        }
    }
}

pub struct GameRow<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub creator: &'a str,
    pub year: &'a str,
    pub status: Status,
}

impl<'a> GameRow<'a> {
    pub fn matches(&self, search: &str) -> bool {
        self.name.starts_with(search)
            || self.description.contains(search)
            || self.creator.contains(search)
            || (self.year == search)
    }

    fn sort_key(&self, sort: GameColumn) -> (&str, &str, &str) {
        match sort {
            GameColumn::Description => (self.description, self.creator, self.year),
            GameColumn::Creator => (self.creator, self.description, self.year),
            GameColumn::Year => (self.year, self.description, self.creator),
        }
    }

    pub fn compare(&self, other: &GameRow, sort: GameColumn) -> Ordering {
        self.sort_key(sort).cmp(&other.sort_key(sort))
    }
}

pub enum VerifyFailure {
    Missing {
        path: PathBuf,
        part: Part,
    },
    Extra {
        path: PathBuf,
        part: Result<Part, std::io::Error>,
    },
    Bad {
        path: PathBuf,
        expected: Part,
        actual: Part,
    },
    Error {
        path: PathBuf,
        err: std::io::Error,
    },
}

impl VerifyFailure {
    #[inline]
    fn extra(path: PathBuf) -> Self {
        Self::Extra {
            part: Part::from_path(&path),
            path,
        }
    }
}

impl VerifyFailure {
    // attempt to fix failure by populating missing/bad ROMs from rom_sources
    fn populate<'u>(
        self,
        rom_sources: &mut RomSources<'u>,
    ) -> Result<Result<ExtractedPart<'u>, Self>, Error> {
        use std::collections::hash_map::Entry;

        match self {
            VerifyFailure::Bad {
                path,
                expected,
                actual,
            } => match rom_sources.entry(expected.clone()) {
                Entry::Occupied(entry) => {
                    std::fs::remove_file(&path)?;
                    Self::extract_to(entry, path, &expected).map(Ok)
                }

                Entry::Vacant(_) => Ok(Err(VerifyFailure::Bad {
                    path,
                    expected,
                    actual,
                })),
            },

            VerifyFailure::Missing { path, part } => match rom_sources.entry(part.clone()) {
                Entry::Occupied(entry) => {
                    std::fs::create_dir_all(path.parent().unwrap())?;
                    Self::extract_to(entry, path, &part).map(Ok)
                }

                Entry::Vacant(_) => Ok(Err(VerifyFailure::Missing { path, part })),
            },

            extra @ VerifyFailure::Extra { .. } => Ok(Err(extra)),

            err @ VerifyFailure::Error { .. } => Ok(Err(err)),
        }
    }

    fn extract_to<'u>(
        mut entry: OccupiedEntry<'_, Part, RomSource<'u>>,
        target: PathBuf,
        part: &Part,
    ) -> Result<ExtractedPart<'u>, Error> {
        let source = entry.get();

        match source.extract(target.as_ref())? {
            extracted @ Extracted::Copied => {
                part.set_xattr(&target);

                Ok(ExtractedPart {
                    extracted,
                    source: entry.insert(RomSource::File {
                        file: Arc::new(target.clone()),
                        has_xattr: true,
                        zip_parts: ZipParts::default(),
                    }),
                    target,
                })
            }

            extracted @ Extracted::Linked { has_xattr } => {
                if !has_xattr {
                    part.set_xattr(&target);
                }

                Ok(ExtractedPart {
                    extracted,
                    source: source.clone(),
                    target,
                })
            }
        }
    }
}

impl fmt::Display for VerifyFailure {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VerifyFailure::Missing { path, .. } => {
                write!(f, "MISSING : {}", path.display())
            }
            VerifyFailure::Extra { path, .. } => write!(f, "EXTRA : {}", path.display()),
            VerifyFailure::Bad { path, .. } => write!(f, "BAD : {}", path.display()),
            VerifyFailure::Error { path, err } => {
                write!(f, "ERROR : {} : {}", path.display(), err)
            }
        }
    }
}

pub struct ExtractedPart<'u> {
    extracted: Extracted,
    source: RomSource<'u>,
    target: PathBuf,
}

impl<'u> fmt::Display for ExtractedPart<'u> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.extracted {
            Extracted::Copied => write!(f, "{} => {}", self.source, self.target.display()),
            Extracted::Linked { .. } => {
                write!(f, "{} -> {}", self.source, self.target.display())
            }
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct FileId {
    pub dev: u64,
    pub ino: u64,
}

impl FileId {
    #[cfg(target_os = "linux")]
    pub fn new(path: &Path) -> Result<Self, std::io::Error> {
        use std::os::linux::fs::MetadataExt;

        path.metadata().map(|m| Self {
            dev: m.st_dev(),
            ino: m.st_ino(),
        })
    }

    #[cfg(target_os = "macos")]
    pub fn new(path: &Path) -> Result<Self, std::io::Error> {
        use std::os::macos::fs::MetadataExt;

        path.metadata().map(|m| Self {
            dev: m.st_dev(),
            ino: m.st_ino(),
        })
    }

    #[cfg(target_os = "unix")]
    pub fn new(path: &Path) -> Result<Self, std::io::Error> {
        use std::os::unix::fs::MetadataExt;

        path.metadata().map(|m| Self {
            dev: m.dev(),
            ino: m.ino(),
        })
    }

    #[cfg(target_os = "windows")]
    pub fn new(path: &Path) -> Result<Self, std::io::Error> {
        use std::os::windows::fs::MetadataExt;

        path.metadata().map(|m| Self {
            dev: m.volume_serial_number().unwrap().into(),
            ino: m.file_index().unwrap(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Part {
    Rom { sha1: [u8; 20] },
    Disk { sha1: [u8; 20] },
}

impl Part {
    #[inline]
    pub fn new_rom(sha1: &str) -> Result<Self, Sha1ParseError> {
        parse_sha1(sha1).map(|sha1| Part::Rom { sha1 })
    }

    #[inline]
    pub fn new_disk(sha1: &str) -> Result<Self, Sha1ParseError> {
        parse_sha1(sha1).map(|sha1| Part::Disk { sha1 })
    }

    #[inline]
    pub fn name_to_chd(name: &str) -> String {
        let mut d = name.to_string();
        d.push_str(".chd");
        d
    }

    #[inline]
    pub fn digest(&self) -> Digest {
        match self {
            Part::Rom { sha1, .. } => Digest(sha1),
            Part::Disk { sha1 } => Digest(sha1),
        }
    }

    #[inline]
    pub fn from_path(path: &Path) -> Result<Self, std::io::Error> {
        use std::fs::File;
        use std::io::BufReader;

        File::open(path)
            .map(BufReader::new)
            .and_then(|mut r| Part::from_reader(&mut r))
    }

    fn from_cached_path(path: &Path) -> Result<Self, std::io::Error> {
        use dashmap::DashMap;
        use fxhash::FxBuildHasher;
        use once_cell::sync::OnceCell;

        static PART_CACHE: OnceCell<DashMap<FileId, Part, FxBuildHasher>> = OnceCell::new();

        let file_id = FileId::new(path)?;

        // using DashMap's Entry API leaves the map locked
        // while generating the Part from path
        // which locks out other threads until finished
        // whereas a get()/insert() pair does not
        let map = PART_CACHE.get_or_init(DashMap::default);

        match map.get(&file_id) {
            Some(part) => Ok(part.clone()),
            None => {
                let part = Self::from_disk_cached_path(path)?;
                map.insert(file_id, part.clone());
                Ok(part)
            }
        }
    }

    #[inline]
    pub fn get_xattr(path: &Path) -> Option<Self> {
        match xattr::get(path, CACHE_XATTR) {
            Ok(Some(v)) => ciborium::de::from_reader(std::io::Cursor::new(v)).ok(),
            _ => None,
        }
    }

    #[inline]
    pub fn set_xattr(&self, path: &Path) {
        let mut attr = Vec::new();
        ciborium::ser::into_writer(self, &mut attr).unwrap();
        let _ = xattr::set(path, CACHE_XATTR, &attr);
    }

    #[inline]
    pub fn has_xattr(path: &Path) -> Result<bool, std::io::Error> {
        xattr::list(path).map(|mut iter| iter.any(|s| s == CACHE_XATTR))
    }

    #[inline]
    pub fn remove_xattr(path: &Path) -> Result<(), std::io::Error> {
        xattr::remove(path, CACHE_XATTR)
    }

    fn from_disk_cached_path(path: &Path) -> Result<Self, std::io::Error> {
        match Part::get_xattr(path) {
            Some(part) => Ok(part),
            None => {
                let part = Self::from_path(path)?;
                part.set_xattr(path);
                Ok(part)
            }
        }
    }

    #[inline]
    fn from_slice(bytes: &[u8]) -> Result<Self, std::io::Error> {
        Self::from_reader(std::io::Cursor::new(bytes))
    }

    fn from_reader<R: Read>(r: R) -> Result<Self, std::io::Error> {
        use std::io::{copy, sink};

        let mut r = Sha1Reader::new(r);
        match Part::disk_from_reader(&mut r) {
            Ok(Some(part)) => Ok(part),
            Ok(None) => copy(&mut r, &mut sink()).map(|_| r.into()),
            Err(err) => Err(err),
        }
    }

    fn disk_from_reader<R: Read>(mut r: R) -> Result<Option<Self>, std::io::Error> {
        fn skip<R: Read>(mut r: R, to_skip: usize) -> Result<(), std::io::Error> {
            let mut buf = vec![0; to_skip];
            r.read_exact(buf.as_mut_slice())
        }

        let mut tag = [0; 8];

        if r.read_exact(&mut tag).is_err() || &tag != b"MComprHD" {
            // non-CHD files might be less than 8 bytes
            return Ok(None);
        }

        // at this point we'll treat the file as a CHD

        skip(&mut r, 4)?; // unused length field

        let mut version = [0; 4];
        r.read_exact(&mut version)?;

        let bytes_to_skip = match u32::from_be_bytes(version) {
            3 => (32 + 32 + 32 + 64 + 64 + 8 * 16 + 8 * 16 + 32) / 8,
            4 => (32 + 32 + 32 + 64 + 64 + 32) / 8,
            5 => (32 * 4 + 64 + 64 + 64 + 32 + 32 + 8 * 20) / 8,
            _ => return Ok(None),
        };
        skip(&mut r, bytes_to_skip)?;

        let mut sha1 = [0; 20];
        r.read_exact(&mut sha1)?;
        Ok(Some(Part::Disk { sha1 }))
    }

    fn verify<F>(&self, from: F, part_path: PathBuf) -> Result<(), VerifyFailure>
    where
        F: FnOnce(&Path) -> Result<Self, std::io::Error>,
    {
        match from(part_path.as_ref()) {
            Ok(ref disk_part) if self == disk_part => Ok(()),
            Ok(ref disk_part) => Err(VerifyFailure::Bad {
                path: part_path,
                expected: self.clone(),
                actual: disk_part.clone(),
            }),
            Err(err) => Err(VerifyFailure::Error {
                path: part_path,
                err,
            }),
        }
    }

    #[inline]
    pub fn verify_cached(&self, part_path: PathBuf) -> Result<(), VerifyFailure> {
        self.verify(Part::from_cached_path, part_path)
    }

    #[inline]
    pub fn verify_uncached(&self, part_path: PathBuf) -> Result<(), VerifyFailure> {
        self.verify(Part::from_path, part_path)
    }
}

struct Sha1Reader<R> {
    reader: R,
    sha1: Sha1,
}

impl<R> Sha1Reader<R> {
    #[inline]
    fn new(reader: R) -> Self {
        Sha1Reader {
            reader,
            sha1: Sha1::new(),
        }
    }
}

impl<R: Read> Read for Sha1Reader<R> {
    fn read(&mut self, data: &mut [u8]) -> Result<usize, std::io::Error> {
        let bytes = self.reader.read(data)?;
        self.sha1.update(&data[0..bytes]);
        Ok(bytes)
    }
}

impl<R> From<Sha1Reader<R>> for Part {
    #[inline]
    fn from(other: Sha1Reader<R>) -> Part {
        Part::Rom {
            sha1: other.sha1.digest().bytes(),
        }
    }
}

pub fn parse_sha1(hex: &str) -> Result<[u8; 20], Sha1ParseError> {
    let mut hex = hex.trim();
    let mut bin = [0; 20];

    if hex.len() != 40 {
        return Err(Sha1ParseError::IncorrectLength);
    }
    if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Sha1ParseError::NonHexDigits);
    }

    for c in bin.iter_mut() {
        let (first, rest) = hex.split_at(2);
        *c = u8::from_str_radix(first, 16).unwrap();
        hex = rest;
    }

    Ok(bin)
}

#[derive(Debug)]
pub enum Sha1ParseError {
    IncorrectLength,
    NonHexDigits,
}

impl std::fmt::Display for Sha1ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Sha1ParseError::IncorrectLength => write!(f, "incorrect SHA1 hash length"),
            Sha1ParseError::NonHexDigits => write!(f, "non hex digits in SHA1 hash"),
        }
    }
}

impl std::error::Error for Sha1ParseError {}

pub struct Digest<'a>(&'a [u8]);

impl<'a> fmt::Display for Digest<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{:02x}", b))
    }
}

#[derive(Copy, Clone)]
pub enum GameColumn {
    Description,
    Creator,
    Year,
}

impl FromStr for GameColumn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "description" => Ok(GameColumn::Description),
            "creator" => Ok(GameColumn::Creator),
            "year" => Ok(GameColumn::Year),
            _ => Err("invalid sort by value".to_string()),
        }
    }
}

#[inline]
pub fn find_files_style() -> ProgressStyle {
    ProgressStyle::default_spinner().template("{spinner} {wide_msg} {pos}")
}

#[inline]
pub fn verify_style() -> ProgressStyle {
    ProgressStyle::default_bar().template("{spinner} {wide_msg} {pos} / {len}")
}

fn subdir_files(root: &Path) -> Vec<PathBuf> {
    use indicatif::ProgressIterator;
    use walkdir::WalkDir;

    let pbar = ProgressBar::new_spinner().with_style(find_files_style());
    pbar.set_message("locating files");
    pbar.set_draw_delta(100);

    let walkdir = WalkDir::new(root).into_iter().progress_with(pbar.clone());

    let results = if cfg!(unix) {
        use nohash_hasher::IntSet;
        use walkdir::DirEntryExt;

        let mut files = IntSet::default();

        walkdir
            .filter_map(|e| {
                e.ok()
                    .filter(|e| e.file_type().is_file() && files.insert(e.ino()))
                    .map(|e| e.into_path())
            })
            .collect()
    } else {
        walkdir
            .filter_map(|e| {
                e.ok()
                    .filter(|e| e.file_type().is_file())
                    .map(|e| e.into_path())
            })
            .collect()
    };

    pbar.finish_and_clear();

    results
}

type ZipParts = Vec<usize>;

#[derive(Clone, Debug)]
pub enum RomSource<'u> {
    File {
        file: Arc<PathBuf>,
        has_xattr: bool,
        zip_parts: ZipParts,
    },
    Url {
        url: &'u str,
        data: Arc<[u8]>,
        zip_parts: ZipParts,
    },
}

impl<'u> RomSource<'u> {
    pub fn from_path(pb: PathBuf) -> Result<Vec<(Part, RomSource<'u>)>, Error> {
        use std::fs::File;
        use std::io::BufReader;

        // if the file already has a cached xattr set,
        // return it as-is without any further parsing
        // and flag it so we don't attempt to set the xattr again
        if let Some(part) = Part::get_xattr(&pb) {
            return Ok(vec![(
                part,
                RomSource::File {
                    file: Arc::new(pb),
                    has_xattr: true,
                    zip_parts: ZipParts::default(),
                },
            )]);
        }

        let file = Arc::new(pb);
        let mut r = File::open(file.as_ref()).map(BufReader::new)?;

        let mut result = vec![(
            Part::from_reader(&mut r)?,
            RomSource::File {
                file: file.clone(),
                has_xattr: false,
                zip_parts: ZipParts::default(),
            },
        )];

        r.seek(std::io::SeekFrom::Start(0))?;

        if is_zip(&mut r).unwrap_or(false) {
            result.extend(unpack_zip_parts(r).into_iter().map(|(part, zip_parts)| {
                (
                    part,
                    RomSource::File {
                        file: file.clone(),
                        has_xattr: false,
                        zip_parts,
                    },
                )
            }));
        }

        Ok(result)
    }

    pub fn from_url(url: &'u str) -> Result<Vec<(Part, RomSource<'u>)>, Error> {
        let data: Arc<[u8]> = crate::http::fetch_url_data(url).map(Arc::from)?;

        let mut result = vec![(
            Part::from_slice(&data)?,
            RomSource::Url {
                url,
                data: data.clone(),
                zip_parts: ZipParts::default(),
            },
        )];

        if matches!(data[..], [0x50, 0x4B, 0x03, 0x04, ..]) {
            result.extend(
                unpack_zip_parts(std::io::Cursor::new(data.clone()))
                    .into_iter()
                    .map(|(part, zip_parts)| {
                        (
                            part,
                            RomSource::Url {
                                url,
                                data: data.clone(),
                                zip_parts,
                            },
                        )
                    }),
            );
        }

        Ok(result)
    }

    fn extract(&self, target: &Path) -> Result<Extracted, Error> {
        use std::fs::{copy, hard_link, File};

        match self {
            RomSource::File {
                file: source,
                has_xattr,
                zip_parts,
            } => match zip_parts.split_first() {
                None => hard_link(source.as_path(), &target)
                    .map(|()| Extracted::Linked {
                        has_xattr: *has_xattr,
                    })
                    .or_else(|_| {
                        copy(source.as_path(), &target)
                            .map_err(Error::IO)
                            .map(|_| Extracted::Copied)
                    }),

                Some((index, rest)) => extract_from_zip_file(
                    rest,
                    zip::ZipArchive::new(File::open(source.as_ref())?)?.by_index(*index)?,
                    target,
                ),
            },

            RomSource::Url {
                data, zip_parts, ..
            } => extract_from_zip_file(zip_parts, std::io::Cursor::new(data), target),
        }
    }
}

impl fmt::Display for RomSource<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RomSource::File {
                file, zip_parts, ..
            } => file
                .display()
                .fmt(f)
                .and_then(|()| zip_parts.iter().try_for_each(|part| write!(f, ":{}", part))),
            RomSource::Url { url, zip_parts, .. } => url
                .fmt(f)
                .and_then(|()| zip_parts.iter().try_for_each(|part| write!(f, ":{}", part))),
        }
    }
}

fn extract_from_zip_file<R: Read>(
    indexes: &[usize],
    mut r: R,
    target: &Path,
) -> Result<Extracted, Error> {
    match indexes.split_first() {
        None => std::fs::File::create(target)
            .and_then(|mut w| std::io::copy(&mut r, &mut w))
            .map_err(Error::IO)
            .map(|_| Extracted::Copied),

        Some((index, rest)) => {
            let mut zip_data = Vec::new();
            r.read_to_end(&mut zip_data)?;
            extract_from_zip_file(
                rest,
                zip::ZipArchive::new(std::io::Cursor::new(zip_data))?.by_index(*index)?,
                target,
            )
        }
    }
}

fn unpack_zip_parts<F: Read + Seek>(zip: F) -> Vec<(Part, ZipParts)> {
    // a valid ROM might be an invalid Zip file
    // so a failure to unpack Zip parts from a file
    // should not be considered a fatal error

    fn is_zip<R: Read>(mut reader: R) -> bool {
        let mut buf = [0; 4];
        match reader.read_exact(&mut buf) {
            Ok(()) => &buf == b"\x50\x4b\x03\x04",
            Err(_) => false,
        }
    }

    fn unpack<F: Read + Seek>(zip: F) -> Result<Vec<(Part, ZipParts)>, Error> {
        let mut zip = zip::ZipArchive::new(zip)?;
        let mut results = Vec::new();

        for index in 0..zip.len() {
            if is_zip(zip.by_index(index)?) {
                let mut zip_data = Vec::new();

                zip.by_index(index)?.read_to_end(&mut zip_data)?;

                results.extend(
                    unpack_zip_parts(std::io::Cursor::new(zip_data))
                        .into_iter()
                        .map(|(part, mut zip_parts)| {
                            zip_parts.insert(0, index);
                            (part, zip_parts)
                        }),
                )
            } else {
                results.push((Part::from_reader(zip.by_index(index)?)?, vec![index]))
            }
        }

        Ok(results)
    }

    unpack(zip).unwrap_or_default()
}

#[derive(Copy, Clone)]
enum Extracted {
    Copied,
    Linked { has_xattr: bool },
}

pub type RomSources<'u> = FxHashMap<Part, RomSource<'u>>;

fn file_rom_sources<F>(root: &Path, part_filter: F) -> RomSources
where
    F: Fn(&Part) -> bool + Sync + Send,
{
    use indicatif::ParallelProgressIterator;
    use rayon::prelude::*;

    let files = subdir_files(root);

    let pbar = ProgressBar::new(files.len() as u64).with_style(verify_style());
    pbar.set_message("cataloging files");
    pbar.set_draw_delta(files.len() as u64 / 1000);

    let results = files
        .into_par_iter()
        .progress_with(pbar.clone())
        .flat_map(|pb| {
            RomSource::from_path(pb)
                .unwrap_or_else(|_| Vec::new())
                .into_par_iter()
        })
        .filter(|(part, _)| part_filter(part))
        .collect();

    pbar.finish_and_clear();

    results
}

#[inline]
fn url_rom_sources<F>(url: &str, part_filter: F) -> RomSources
where
    F: Fn(&Part) -> bool + Sync + Send,
{
    RomSource::from_url(url)
        .map(|v| {
            v.into_iter()
                .filter(|(part, _)| part_filter(part))
                .collect()
        })
        .unwrap_or_default()
}

fn multi_rom_sources<'u, F>(
    roots: &'u [PathBuf],
    urls: &'u [String],
    part_filter: F,
) -> RomSources<'u>
where
    F: Fn(&Part) -> bool + Sync + Send + Copy,
{
    urls.iter()
        .map(|url| url_rom_sources(url, part_filter))
        .chain(roots.iter().map(|root| file_rom_sources(root, part_filter)))
        .reduce(|mut acc, item| {
            acc.extend(item);
            acc
        })
        .unwrap_or_else(|| file_rom_sources(Path::new("."), part_filter))
}

#[inline]
pub fn all_rom_sources<'u>(roots: &'u [PathBuf], urls: &'u [String]) -> RomSources<'u> {
    multi_rom_sources(roots, urls, |_| true)
}

#[inline]
pub fn get_rom_sources<'u>(
    roots: &'u [PathBuf],
    urls: &'u [String],
    required: FxHashSet<Part>,
) -> RomSources<'u> {
    multi_rom_sources(roots, urls, |part| required.contains(part))
}

pub fn file_move(source: &Path, target: &Path) -> Result<(), std::io::Error> {
    if (source != target) && !target.exists() {
        use std::fs::rename;
        rename(source, target)?;
        println!("{} -> {}", source.display(), target.display());
    }
    Ok(())
}

pub fn file_move_dry_run(source: &Path, target: &Path) -> Result<(), std::io::Error> {
    if (source != target) && !target.exists() {
        println!("{} -> {}", source.display(), target.display());
    }
    Ok(())
}

pub fn display_all_results(game: &str, failures: &[VerifyFailure]) {
    if failures.is_empty() {
        println!("{} : OK", game);
    } else {
        display_bad_results(game, failures)
    }
}

pub fn display_bad_results(game: &str, failures: &[VerifyFailure]) {
    if !failures.is_empty() {
        use std::io::{stdout, Write};

        // ensure results are generated as a unit
        let stdout = stdout();
        let mut handle = stdout.lock();
        for failure in failures {
            writeln!(&mut handle, "{game} : {failure}").unwrap();
        }
    }
}

#[inline]
pub fn parse_int(s: &str) -> Result<u64, ParseIntError> {
    // MAME's use of integer values is a horror show
    let s = s.trim();

    u64::from_str(s)
        .or_else(|_| u64::from_str_radix(s, 16))
        .or_else(|e| {
            if let Some(stripped) = s.strip_prefix("0x") {
                u64::from_str_radix(stripped, 16)
            } else {
                dbg!(s);
                Err(e)
            }
        })
}
