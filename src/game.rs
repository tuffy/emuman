use super::{is_zip, Error};
use core::num::ParseIntError;
use fxhash::{FxHashMap, FxHashSet};
use indicatif::{ProgressBar, ProgressStyle};
use prettytable::Table;
use serde_derive::{Deserialize, Serialize};
use sha1::Sha1;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io::Read;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

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
    ) -> BTreeMap<&'a str, Vec<VerifyFailure<PathBuf>>> {
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

    fn verify_game(&self, root: &Path, game_name: &str) -> Vec<VerifyFailure<PathBuf>> {
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

    fn verify(&self, game_root: &Path) -> Vec<VerifyFailure<PathBuf>> {
        use dashmap::DashMap;
        use rayon::prelude::*;
        use std::fs::read_dir;

        let mut failures = Vec::new();

        // turn files on disk into a map, so extra files can be located
        let files_on_disk: DashMap<String, PathBuf> = DashMap::new();

        if let Ok(dir) = read_dir(game_root) {
            for entry in dir
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            {
                match entry.file_name().into_string() {
                    Ok(name) => {
                        files_on_disk.insert(name, entry.path()).map(|_| ());
                    }
                    Err(_) => failures.push(VerifyFailure::extra(entry.path())),
                }
            }
        } else if self.parts.is_empty() {
            // no directory to read and no parts to check,
            // so no failures are possible
            return failures;
        }

        // verify all game parts
        failures.extend(
            self.parts
                .par_iter()
                .filter_map(|(name, part)| match files_on_disk.remove(name) {
                    Some((_, pathbuf)) => part.verify(pathbuf).err(),
                    None => Some(VerifyFailure::Missing {
                        path: game_root.join(name),
                        part: part.clone(),
                    }),
                })
                .collect::<Vec<VerifyFailure<PathBuf>>>()
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

    pub fn add_and_verify(
        &self,
        rom_sources: &mut RomSources,
        target_dir: &Path,
        progress: &ProgressBar,
    ) -> Result<Vec<VerifyFailure<PathBuf>>, Error> {
        use std::fs::read_dir;

        let mut failures = Vec::new();

        let game_root = target_dir.join(&self.name);

        // turn files on disk into a map, so extra files can be located
        let mut files_on_disk: HashMap<String, PathBuf> = HashMap::new();

        if let Ok(dir) = read_dir(&game_root) {
            for entry in dir
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            {
                match entry.file_name().into_string() {
                    Ok(name) => {
                        files_on_disk.insert(name, entry.path());
                    }
                    Err(_) => failures.push(VerifyFailure::extra(entry.path())),
                }
            }
        }

        // verify all game parts
        for (name, part) in self.parts.iter() {
            use std::collections::hash_map::Entry;
            use std::fs::remove_file;

            match files_on_disk.remove(name) {
                Some(target) => {
                    // if file exists on disk
                    if let Err(failure) = part.verify(&target).map_err(|err| err.with_path(())) {
                        // but is not correct
                        match rom_sources.entry(part.clone()) {
                            Entry::Occupied(mut entry) => {
                                // if part exists in rom_sources
                                // replace incorrect file with file from rom_sources
                                let source = entry.get();
                                remove_file(&target).map_err(Error::IO)?;
                                match source.extract(&target)? {
                                    Extracted::Copied => {
                                        progress.println(format!(
                                            "{} => {}",
                                            source,
                                            target.display()
                                        ));
                                        entry.insert(RomSource::Disk(target));
                                    }
                                    Extracted::Linked => progress.println(format!(
                                        "{} -> {}",
                                        source,
                                        target.display()
                                    )),
                                }
                            }
                            Entry::Vacant(_) => {
                                // if part missing in rom_sources, forward error
                                failures.push(failure.with_path(target));
                            }
                        }
                    }
                }
                None => {
                    // if file does not already exist on disk

                    use std::fs::create_dir_all;

                    let target = game_root.join(name);

                    match rom_sources.entry(part.clone()) {
                        Entry::Occupied(mut entry) => {
                            // and if part exists in rom_sources
                            // link/copy file from rom_sources
                            let source = entry.get();
                            create_dir_all(target.parent().unwrap())?;
                            match source.extract(&target)? {
                                Extracted::Copied => {
                                    progress.println(format!("{} => {}", source, target.display()));
                                    entry.insert(RomSource::Disk(target));
                                }
                                Extracted::Linked => {
                                    progress.println(format!("{} -> {}", source, target.display()))
                                }
                            }
                        }
                        Entry::Vacant(_) => {
                            // otherwise mark as missing
                            failures.push(VerifyFailure::Missing {
                                path: target,
                                part: part.clone(),
                            });
                        }
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

        Ok(cleanup_failures(failures, progress))
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

pub enum VerifyFailure<P> {
    Missing {
        path: P,
        part: Part,
    },
    Extra {
        path: P,
        part: Result<Part, std::io::Error>,
    },
    Bad {
        path: P,
        expected: Part,
        actual: Part,
    },
    Error {
        path: P,
        err: std::io::Error,
    },
}

impl<P: AsRef<Path>> VerifyFailure<P> {
    #[inline]
    fn extra(path: P) -> Self {
        Self::Extra {
            part: Part::from_path(path.as_ref()),
            path,
        }
    }
}

impl<P> VerifyFailure<P> {
    #[inline]
    fn with_path<Q>(self, path: Q) -> VerifyFailure<Q> {
        match self {
            VerifyFailure::Missing { part, path: _ } => VerifyFailure::Missing { path, part },
            VerifyFailure::Extra { part, path: _ } => VerifyFailure::Extra { path, part },
            VerifyFailure::Bad {
                expected,
                actual,
                path: _,
            } => VerifyFailure::Bad {
                path,
                expected,
                actual,
            },
            VerifyFailure::Error { err, path: _ } => VerifyFailure::Error { path, err },
        }
    }
}

impl<P: AsRef<Path>> fmt::Display for VerifyFailure<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VerifyFailure::Missing { path, .. } => {
                write!(f, "MISSING : {}", path.as_ref().display())
            }
            VerifyFailure::Extra { path, .. } => write!(f, "EXTRA : {}", path.as_ref().display()),
            VerifyFailure::Bad { path, .. } => write!(f, "BAD : {}", path.as_ref().display()),
            VerifyFailure::Error { path, err } => {
                write!(f, "ERROR : {} : {}", path.as_ref().display(), err)
            }
        }
    }
}

#[derive(Clone, Hash, PartialEq, Eq)]
enum FileId {
    Posix { dev: u64, ino: u64 },
    Unknown(PathBuf),
}

impl FileId {
    fn new(path: &Path) -> Result<Self, std::io::Error> {
        #[cfg(target_os = "linux")]
        use std::os::linux::fs::MetadataExt;
        #[cfg(target_os = "macos")]
        use std::os::macos::fs::MetadataExt;

        if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
            path.metadata().map(|m| Self::Posix {
                dev: m.st_dev(),
                ino: m.st_ino(),
            })
        } else {
            Ok(FileId::Unknown(path.to_path_buf()))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Part {
    Rom { sha1: [u8; 20] },
    Disk { sha1: [u8; 20] },
}

impl Part {
    #[inline]
    pub fn new_rom(sha1: &str) -> Self {
        Part::Rom {
            sha1: parse_sha1(sha1),
        }
    }

    #[inline]
    pub fn new_disk(sha1: &str) -> Self {
        Part::Disk {
            sha1: parse_sha1(sha1),
        }
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
    fn from_path(path: &Path) -> Result<Self, std::io::Error> {
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
                let part = Self::from_path(path)?;
                map.insert(file_id, part.clone());
                Ok(part)
            }
        }
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

    fn verify<P: AsRef<Path>>(&self, part_path: P) -> Result<(), VerifyFailure<P>> {
        match Part::from_cached_path(part_path.as_ref()) {
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

pub fn parse_sha1(hex: &str) -> [u8; 20] {
    let mut hex = hex.trim();
    let mut bin = [0; 20];

    if hex.len() != 40 {
        panic!("sha1 sum \"{}\" not 40 characters", hex);
    }
    if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        panic!("sha1 sum \"{}\" not all hex digits", hex);
    }

    for c in bin.iter_mut() {
        let (first, rest) = hex.split_at(2);
        *c = u8::from_str_radix(first, 16).unwrap();
        hex = rest;
    }

    bin
}

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
fn find_files_style() -> ProgressStyle {
    ProgressStyle::default_spinner().template("{spinner} {wide_msg} {pos}")
}

#[inline]
fn verify_style() -> ProgressStyle {
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

pub enum RomSource {
    Disk(PathBuf),
    Zip {
        file: Arc<PathBuf>,
        index: usize,
    },
    SubZip {
        file: Arc<PathBuf>,
        index: usize,
        sub_index: usize,
    },
}

impl RomSource {
    pub fn from_path(pb: PathBuf) -> Result<Vec<(Part, RomSource)>, Error> {
        use std::fs::File;
        use std::io::{BufReader, Cursor, Seek, SeekFrom};

        let mut r = File::open(&pb).map(BufReader::new)?;

        let mut result = vec![(Part::from_reader(&mut r)?, RomSource::Disk(pb.clone()))];

        // valid ROMs might also be potentially invalid Zip archives
        // so the extra error handling ensures original file(s)
        // are returned as part of the result if something goes bad
        // after the initial read

        if r.seek(SeekFrom::Start(0)).is_err() {
            return Ok(result);
        }

        if is_zip(&mut r).unwrap_or(false) {
            let file = Arc::new(pb);
            let mut zip = match zip::ZipArchive::new(r) {
                Ok(z) => z,
                Err(_) => return Ok(result),
            };

            for index in 0..zip.len() {
                let mut file_data = Vec::new();
                match zip.by_index(index) {
                    Ok(mut r) => {
                        if r.read_to_end(&mut file_data).is_err() {
                            return Ok(result);
                        }
                    }
                    Err(_) => return Ok(result),
                }

                let mut reader = Cursor::new(file_data);
                if let Ok(part) = Part::from_reader(&mut reader) {
                    result.push((
                        part,
                        RomSource::Zip {
                            file: file.clone(),
                            index,
                        },
                    ));
                }

                if reader.seek(SeekFrom::Start(0)).is_err() {
                    return Ok(result);
                }

                if is_zip(&mut reader).unwrap_or(false) {
                    let mut sub_zip = match zip::ZipArchive::new(reader) {
                        Ok(z) => z,
                        Err(_) => return Ok(result),
                    };

                    for sub_index in 0..sub_zip.len() {
                        let part = match sub_zip.by_index(sub_index) {
                            Ok(z) => match Part::from_reader(z) {
                                Ok(part) => part,
                                Err(_) => return Ok(result),
                            },
                            Err(_) => return Ok(result),
                        };

                        result.push((
                            part,
                            RomSource::SubZip {
                                file: file.clone(),
                                index,
                                sub_index,
                            },
                        ))
                    }
                }
            }
        }

        Ok(result)
    }

    fn extract(&self, target: &Path) -> Result<Extracted, Error> {
        match self {
            RomSource::Disk(source) => {
                use std::fs::{copy, hard_link};

                if hard_link(source, &target).is_ok() {
                    Ok(Extracted::Linked)
                } else {
                    copy(source, &target)
                        .map_err(Error::IO)
                        .map(|_| Extracted::Copied)
                }
            }
            RomSource::Zip { file, index } => {
                use std::fs::File;
                use std::io::{copy, BufReader};
                use zip::ZipArchive;

                copy(
                    &mut ZipArchive::new(File::open(file.as_ref()).map(BufReader::new)?)?
                        .by_index(*index)?,
                    &mut File::create(&target)?,
                )
                .map_err(Error::IO)
                .map(|_| Extracted::Copied)
            }
            RomSource::SubZip {
                file,
                index,
                sub_index,
            } => {
                use std::fs::File;
                use std::io::{copy, BufReader, Cursor};
                use zip::ZipArchive;

                let mut file_data = Vec::new();
                ZipArchive::new(File::open(file.as_ref()).map(BufReader::new)?)?
                    .by_index(*index)?
                    .read_to_end(&mut file_data)?;

                let reader = Cursor::new(file_data);
                copy(
                    &mut ZipArchive::new(reader)?.by_index(*sub_index)?,
                    &mut File::create(&target)?,
                )
                .map_err(Error::IO)
                .map(|_| Extracted::Copied)
            }
        }
    }
}

impl fmt::Display for RomSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RomSource::Disk(source) => source.display().fmt(f),
            RomSource::Zip { file, index } => write!(f, "{}:{}", file.display(), index),
            RomSource::SubZip {
                file,
                index,
                sub_index,
            } => write!(f, "{}:{}:{}", file.display(), index, sub_index),
        }
    }
}

enum Extracted {
    Copied,
    Linked,
}

pub type RomSources = FxHashMap<Part, RomSource>;

fn rom_sources<F>(root: &Path, part_filter: F) -> RomSources
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

pub fn all_rom_sources(root: &Path) -> RomSources {
    rom_sources(root, |_| true)
}

pub fn get_rom_sources(root: &Path, required: FxHashSet<Part>) -> RomSources {
    rom_sources(root, |part| required.contains(part))
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

pub fn display_all_results(game: &str, failures: &[VerifyFailure<PathBuf>]) {
    if failures.is_empty() {
        println!("{} : OK", game);
    } else {
        use std::io::{stdout, Write};

        // ensure results are generated as a unit
        let stdout = stdout();
        let mut handle = stdout.lock();
        writeln!(&mut handle, "{} : BAD", game).unwrap();
        for failure in failures {
            writeln!(&mut handle, "  {}", failure).unwrap();
        }
    }
}

pub fn display_bad_results(game: &str, failures: &[VerifyFailure<PathBuf>]) {
    if !failures.is_empty() {
        use std::io::{stdout, Write};

        // ensure results are generated as a unit
        let stdout = stdout();
        let mut handle = stdout.lock();
        writeln!(&mut handle, "{} : BAD", game).unwrap();
        for failure in failures {
            writeln!(&mut handle, "  {}", failure).unwrap();
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

fn cleanup_failures<P: AsRef<Path>>(
    failures: Vec<VerifyFailure<P>>,
    progress: &ProgressBar,
) -> Vec<VerifyFailure<P>> {
    if failures.is_empty() {
        failures
    } else {
        let mut extras = HashMap::new();
        let mut to_cleanup = Vec::new();

        for failure in failures {
            match failure {
                VerifyFailure::Extra {
                    path,
                    part: Ok(part),
                } => {
                    extras.insert(part, path);
                }
                other => {
                    to_cleanup.push(other);
                }
            }
        }

        if extras.is_empty() {
            to_cleanup
        } else {
            use std::fs::rename;

            let mut failures = Vec::new();

            for cleanup in to_cleanup {
                match cleanup {
                    VerifyFailure::Missing {
                        path: missing_path,
                        part,
                    } => match extras.remove(&part) {
                        Some(extra_path) => {
                            if rename(extra_path.as_ref(), missing_path.as_ref()).is_ok() {
                                progress.println(format!(
                                    "{} -> {}",
                                    extra_path.as_ref().display(),
                                    missing_path.as_ref().display()
                                ));
                            } else {
                                failures.push(VerifyFailure::Missing {
                                    path: missing_path,
                                    part,
                                });
                            }
                        }
                        None => failures.push(VerifyFailure::Missing {
                            path: missing_path,
                            part,
                        }),
                    },
                    VerifyFailure::Bad {
                        path: bad_path,
                        expected,
                        actual,
                    } => match extras.remove(&expected) {
                        Some(extra_path) => {
                            if std::fs::remove_file(bad_path.as_ref())
                                .and_then(|()| rename(extra_path.as_ref(), bad_path.as_ref()))
                                .is_ok()
                            {
                                progress.println(format!(
                                    "{} -> {}",
                                    extra_path.as_ref().display(),
                                    bad_path.as_ref().display()
                                ));
                            } else {
                                failures.push(VerifyFailure::Bad {
                                    path: bad_path,
                                    expected,
                                    actual,
                                });
                            }
                        }
                        None => failures.push(VerifyFailure::Bad {
                            path: bad_path,
                            expected,
                            actual,
                        }),
                    },
                    error @ VerifyFailure::Error { .. } => failures.push(error),
                    extra @ VerifyFailure::Extra { .. } => failures.push(extra),
                }
            }

            failures.extend(extras.into_iter().filter_map(
                |(part, path)| match std::fs::remove_file(path.as_ref()) {
                    Ok(()) => {
                        progress.println(format!("deleted : {}", path.as_ref().display()));
                        None
                    }
                    Err(_) => Some(VerifyFailure::Extra {
                        part: Ok(part),
                        path,
                    }),
                },
            ));

            failures
        }
    }
}
