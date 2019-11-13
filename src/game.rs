use super::{Error, is_zip};
use fxhash::{FxHashMap, FxHashSet};
use indicatif::{ProgressBar, ProgressStyle};
use serde_derive::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io::{BufRead, Read, Seek};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GameDb {
    pub description: String,
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

    pub fn list(&self, search: Option<&str>, sort: GameColumn, simple: bool) {
        let mut results: Vec<GameRow> = if let Some(search) = search {
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
        };

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

    pub fn report(
        &self,
        games: &HashSet<String>,
        search: Option<&str>,
        sort: GameColumn,
        simple: bool,
    ) {
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

        results.sort_by(|a, b| a.compare(b, sort));
        GameDb::display_report(&results)
    }

    fn display_report(games: &[GameRow]) {
        use prettytable::{cell, format, row, Table};

        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
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
        use rayon::prelude::*;
        use std::fs::read_dir;
        use std::sync::Mutex;

        let mut failures = Vec::new();

        // turn files on disk into a map, so extra files can be located
        let mut files_on_disk: HashMap<String, PathBuf> = HashMap::new();

        if let Ok(dir) = read_dir(game_root) {
            for entry in dir.filter_map(|e| e.ok()) {
                if let Ok(name) = entry.file_name().into_string() {
                    files_on_disk.insert(name, entry.path());
                } else {
                    // anything that's not UTF-8 is an extra
                    failures.push(VerifyFailure::Extra(entry.path()));
                }
            }
        } else if self.parts.is_empty() {
            // no directory to read and no parts to check,
            // so no failures are possible
            return failures;
        }

        // verify all game parts
        let files_on_disk = Mutex::new(files_on_disk);
        failures.extend(
            self.parts
                .par_iter()
                .filter_map(|(name, part)| {
                    if let Some(pathbuf) = files_on_disk.lock().unwrap().remove(name) {
                        part.verify(pathbuf)
                    } else {
                        Some(VerifyFailure::Missing(game_root.join(name)))
                    }
                })
                .collect::<Vec<VerifyFailure<PathBuf>>>()
                .into_iter(),
        );

        // mark any leftover files on disk as extras
        failures.extend(
            files_on_disk
                .into_inner()
                .unwrap()
                .into_iter()
                .map(|(_, pathbuf)| VerifyFailure::Extra(pathbuf)),
        );

        failures
    }

    pub fn add(
        &self,
        rom_sources: &mut RomSources,
        target: &Path,
        extract: ExtractFn,
    ) -> Result<(), Error> {
        let target_dir = target.join(&self.name);
        self.parts
            .iter()
            .try_for_each(|(rom_name, part)| extract(rom_sources, part, target_dir.join(rom_name)))
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
                    file_move(&entry_path, &target_path)?;
                }
            }
        }

        Ok(())
    }

    pub fn display_parts(&self) {
        use prettytable::{cell, format, row, Table};

        let mut parts: Vec<(&str, &Part)> = self
            .parts
            .iter()
            .map(|(name, part)| (name.as_str(), part))
            .collect();

        if !parts.is_empty() {
            parts.sort_by(|x, y| x.0.cmp(y.0));

            let mut table = Table::new();
            table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
            for (name, part) in parts {
                if let Some(size) = part.size() {
                    table.add_row(row![name, size, part.digest()]);
                } else {
                    table.add_row(row![name, "", part.digest()]);
                }
            }
            table.printstd();
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
    Missing(P),
    Extra(P),
    Bad(P),
    Error(P, std::io::Error),
}

impl<P: AsRef<Path>> fmt::Display for VerifyFailure<P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VerifyFailure::Missing(pb) => write!(f, "MISSING : {}", pb.as_ref().display()),
            VerifyFailure::Extra(pb) => write!(f, "EXTRA : {}", pb.as_ref().display()),
            VerifyFailure::Bad(pb) => write!(f, "BAD : {}", pb.as_ref().display()),
            VerifyFailure::Error(pb, err) => {
                write!(f, "ERROR : {} : {}", pb.as_ref().display(), err)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Part {
    ROM { sha1: [u8; 20], size: u64 },
    Disk { sha1: [u8; 20] },
}

impl Part {
    #[inline]
    pub fn new_rom(sha1: &str, size: u64) -> Self {
        Part::ROM {
            sha1: parse_sha1(sha1),
            size,
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
    fn digest(&self) -> Digest {
        match self {
            Part::ROM { sha1, .. } => Digest(sha1),
            Part::Disk { sha1 } => Digest(sha1),
        }
    }

    #[inline]
    fn size(&self) -> Option<u64> {
        match self {
            Part::ROM { size, .. } => Some(*size),
            Part::Disk { .. } => None,
        }
    }

    #[inline]
    fn from_path(path: &Path) -> Result<Self, std::io::Error> {
        use std::fs::File;
        use std::io::BufReader;

        File::open(path)
            .map(BufReader::new)
            .and_then(|mut r| Part::from_bufreader(&mut r))
    }

    fn from_bufreader<R>(mut r: R) -> Result<Self, std::io::Error>
    where
        R: BufRead + Seek,
    {
        use std::io::SeekFrom;

        match Part::disk_from_bufreader(&mut r) {
            Ok(Some(disk)) => Ok(disk),
            Ok(None) => {
                r.seek(SeekFrom::Start(0))?;
                Part::rom_from_bufreader(r)
            }
            Err(err) => Err(err),
        }
    }

    fn disk_from_bufreader<R: BufRead>(mut r: R) -> Result<Option<Self>, std::io::Error> {
        fn skip<R: BufRead>(mut r: R, mut to_skip: usize) -> Result<(), std::io::Error> {
            while to_skip > 0 {
                let consumed = r.fill_buf()?.len().min(to_skip);
                r.consume(consumed);
                to_skip -= consumed;
            }
            Ok(())
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

    fn rom_from_bufreader<B: BufRead>(mut r: B) -> Result<Self, std::io::Error> {
        use sha1::Sha1;

        let mut sha1 = Sha1::new();
        let mut size = 0;
        loop {
            let buf = r.fill_buf()?;
            let len = if buf.is_empty() {
                return Ok(Part::ROM {
                    sha1: sha1.digest().bytes(),
                    size,
                });
            } else {
                sha1.update(buf);
                size += buf.len() as u64;
                buf.len()
            };
            r.consume(len);
        }
    }

    fn rom_from_reader<R: Read>(mut r: R) -> Result<Self, std::io::Error> {
        use sha1::Sha1;

        let mut sha1 = Sha1::new();
        let mut size = 0;
        let mut buf = [0; 4096];
        loop {
            match r.read(&mut buf) {
                Ok(0) => {
                    break Ok(Part::ROM {
                        sha1: sha1.digest().bytes(),
                        size,
                    })
                }
                Ok(bytes) => {
                    sha1.update(&buf[0..bytes]);
                    size += bytes as u64;
                }
                Err(e) => break Err(e),
            }
        }
    }

    fn verify<P: AsRef<Path>>(&self, part_path: P) -> Option<VerifyFailure<P>> {
        match Part::from_path(part_path.as_ref()) {
            Ok(ref disk_part) if self == disk_part => None,
            Ok(_) => Some(VerifyFailure::Bad(part_path)),
            Err(err) => Some(VerifyFailure::Error(part_path, err)),
        }
    }

    fn is_bad<P: AsRef<Path>>(&self, part_path: P) -> bool {
        if let Some(VerifyFailure::Bad(_)) = self.verify(part_path) {
            true
        } else {
            false
        }
    }
}

pub fn parse_sha1(mut hex: &str) -> [u8; 20] {
    let mut bin = [0; 20];

    assert_eq!(hex.len(), 40);
    assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));

    for c in bin.iter_mut() {
        let (first, rest) = hex.split_at(2);
        *c = u8::from_str_radix(first, 16).unwrap();
        hex = rest;
    }

    bin
}

struct Digest<'a>(&'a [u8]);

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

    let results = WalkDir::new(root)
        .into_iter()
        .progress_with(pbar.clone())
        .filter_map(|e| {
            e.ok()
                .filter(|e| e.file_type().is_file())
                .map(|e| e.into_path())
        })
        .collect();

    pbar.finish_and_clear();

    results
}

pub enum RomSource {
    Disk(PathBuf),
    Zip { file: Arc<PathBuf>, index: usize },
}

impl RomSource {
    fn from_path(pb: PathBuf) -> Result<Vec<(Part, RomSource)>, Error> {
        use std::fs::File;
        use std::io::BufReader;

        let mut r = File::open(&pb).map(BufReader::new)?;

        // valid parts might be less than 4 bytes,
        // so fallback to reading a raw part
        // if some error occurs testing for zipfiles
        if is_zip(&mut r).unwrap_or(false) {
            let file = Arc::new(pb);
            let mut zip = zip::ZipArchive::new(r)?;
            (0..zip.len())
                .map(|index| {
                    Ok((
                        Part::rom_from_reader(zip.by_index(index)?)?,
                        RomSource::Zip {
                            file: file.clone(),
                            index,
                        },
                    ))
                })
                .collect()
        } else {
            Ok(vec![(Part::from_bufreader(r)?, RomSource::Disk(pb))])
        }
    }
}

type RomSources = FxHashMap<Part, RomSource>;

fn rom_sources<F>(root: &Path, filter: F) -> RomSources
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
        .filter(|(part, _)| filter(part))
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

pub type ExtractFn = fn(&mut RomSources, &Part, PathBuf) -> Result<(), Error>;

pub fn extract(roms: &mut RomSources, part: &Part, target: PathBuf) -> Result<(), Error> {
    use std::collections::hash_map::Entry;

    if !target.exists() {
        if let Entry::Occupied(mut entry) = roms.entry(part.clone()) {
            use std::fs::create_dir_all;

            create_dir_all(target.parent().unwrap())?;

            match entry.get() {
                RomSource::Disk(source) => {
                    use std::fs::{copy, hard_link};

                    hard_link(source, &target).or_else(|_| copy(source, &target).map(|_| ()))?;
                    println!("{} -> {}", source.display(), target.display());
                }
                RomSource::Zip { file, index } => {
                    use std::fs::File;
                    use std::io::{copy, BufReader};
                    use zip::ZipArchive;

                    copy(
                        &mut ZipArchive::new(File::open(file.as_ref()).map(BufReader::new)?)?
                            .by_index(*index)?,
                        &mut File::create(&target)?,
                    )?;
                    println!("{}:{} -> {}", file.display(), index, target.display());

                    // replace zip file source with on-disk source
                    // in RomSources database so that if this part
                    // is extracted again, it will be hard-linked
                    // to the file we've just finished extracting
                    entry.insert(RomSource::Disk(target));
                }
            }
        }
        Ok(())
    } else if let Some(VerifyFailure::Bad(target)) = part.verify(target) {
        use std::fs::remove_file;

        remove_file(&target)
            .map_err(Error::IO)
            .and_then(|()| extract(roms, part, target))
    } else {
        Ok(())
    }
}

pub fn extract_dry_run(roms: &mut RomSources, part: &Part, target: PathBuf) -> Result<(), Error> {
    use std::collections::hash_map::Entry;

    if !target.exists() || part.is_bad(&target) {
        if let Entry::Occupied(entry) = roms.entry(part.clone()) {
            match entry.get() {
                RomSource::Disk(source) => {
                    println!("{} -> {}", source.display(), target.display());
                }
                RomSource::Zip {
                    file: source,
                    index,
                } => {
                    println!("{}:{} -> {}", source.display(), index, target.display());
                }
            }
        }
    }
    Ok(())
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
