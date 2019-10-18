use super::Error;
use hash_hasher::{HashedMap, HashedSet};
use indicatif::{ProgressBar, ProgressStyle};
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::BufRead;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::str::FromStr;

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

    pub fn required_parts<I>(&self, games: I) -> Result<HashedSet<Part>, Error>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut parts = HashedSet::default();
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
    ) -> Vec<(&'a str, Vec<VerifyFailure<PathBuf>>)> {
        use indicatif::ParallelProgressIterator;
        use rayon::prelude::*;

        let pbar = ProgressBar::new(games.len() as u64).with_style(verify_style());
        pbar.set_message("verifying games");

        let mut results = games
            .par_iter()
            .progress_with(pbar)
            .map(|game| (game.as_str(), self.verify_game(root, game)))
            .collect::<Vec<(&'a str, Vec<VerifyFailure<PathBuf>>)>>();

        results.sort_unstable_by_key(|&(name, _)| name);

        results
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

    pub fn list(&self, search: Option<&str>, sort: SortBy, simple: bool) {
        let mut results: Vec<&Game> = if let Some(search) = search {
            self.games
                .values()
                .filter(|g| !g.is_device && g.matches(search))
                .collect()
        } else {
            self.games.values().filter(|g| !g.is_device).collect()
        };

        Game::sort_report(&mut results, sort);
        GameDb::display_report(&results, simple)
    }

    pub fn games<I>(&self, games: I, simple: bool)
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        GameDb::display_report(
            &games
                .into_iter()
                .filter_map(|g| self.games.get(g.as_ref()))
                .collect::<Vec<&Game>>(),
            simple,
        )
    }

    pub fn report(
        &self,
        games: &HashSet<String>,
        search: Option<&str>,
        sort: SortBy,
        simple: bool,
    ) {
        let mut results: Vec<&Game> = games
            .iter()
            .filter_map(|g| self.games.get(g).filter(|g| !g.is_device))
            .collect();
        if let Some(search) = search {
            results.retain(|g| g.matches(search));
        }

        Game::sort_report(&mut results, sort);
        GameDb::display_report(&results, simple)
    }

    fn display_report(games: &[&Game], simple: bool) {
        use prettytable::{cell, format, row, Table};

        #[inline]
        pub fn no_parens(s: &str) -> &str {
            if let Some(index) = s.find('(') {
                s[0..index].trim_end()
            } else {
                s
            }
        }

        #[inline]
        pub fn no_slashes(s: &str) -> &str {
            if let Some(index) = s.find(" / ") {
                s[0..index].trim_end()
            } else {
                s
            }
        }

        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        for game in games {
            let description = if simple {
                no_slashes(no_parens(&game.description))
            } else {
                &game.description
            };
            let creator = if simple {
                no_parens(&game.creator)
            } else {
                &game.creator
            };
            let year = &game.year;
            let name = &game.name;

            table.add_row(match game.status {
                Status::Working => row![description, creator, year, name],
                Status::Partial => row![FY => description, creator, year, name],
                Status::NotWorking => row![FR => description, creator, year, name],
            });
        }

        table.printstd();
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

    pub fn matches(&self, search: &str) -> bool {
        self.name.starts_with(search)
            || self.description.contains(search)
            || self.creator.contains(search)
            || (self.year == search)
    }

    pub fn sort_key(&self, sort: SortBy) -> &str {
        match sort {
            SortBy::Description => &self.description,
            SortBy::Creator => &self.creator,
            SortBy::Year => &self.year,
        }
    }

    pub fn sort_report(games: &mut Vec<&Game>, sort: SortBy) {
        match sort {
            SortBy::Description => {
                games.sort_unstable_by_key(|x| x.sort_key(SortBy::Year));
                games.sort_by_key(|x| x.sort_key(SortBy::Creator));
                games.sort_by_key(|x| x.sort_key(SortBy::Description));
            }
            SortBy::Creator => {
                games.sort_unstable_by_key(|x| x.sort_key(SortBy::Year));
                games.sort_by_key(|x| x.sort_key(SortBy::Description));
                games.sort_by_key(|x| x.sort_key(SortBy::Creator));
            }
            SortBy::Year => {
                games.sort_unstable_by_key(|x| x.sort_key(SortBy::Creator));
                games.sort_by_key(|x| x.sort_key(SortBy::Description));
                games.sort_by_key(|x| x.sort_key(SortBy::Year));
            }
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
        rom_sources: &HashedMap<Part, PathBuf>,
        target: &Path,
        copy: fn(&Part, &Path, &Path) -> Result<(), std::io::Error>,
    ) -> Result<(), Error> {
        let target_dir = target.join(&self.name);
        self.parts
            .iter()
            .try_for_each(|(rom_name, part)| {
                if let Some(source) = rom_sources.get(&part) {
                    copy(&part, source, &target_dir.join(rom_name))
                } else {
                    Ok(())
                }
            })
            .map_err(Error::IO)
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

        let parts: HashedMap<Part, PathBuf> = self
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
                table.add_row(row![name, part.digest()]);
            }
            table.printstd();
        }
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
    ROM { sha1: [u8; 20] },
    Disk { sha1: [u8; 20] },
}

impl Part {
    #[inline]
    pub fn new_rom(sha1: &str) -> Self {
        Part::ROM {
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
    fn digest(&self) -> Digest {
        match self {
            Part::ROM {sha1} => Digest(sha1),
            Part::Disk {sha1} => Digest(sha1),
        }
    }

    pub fn from_path(path: &Path) -> Result<Self, std::io::Error> {
        use std::fs::File;
        use std::io::{BufReader, Seek, SeekFrom};

        let mut r = BufReader::new(File::open(path)?);

        match Part::disk_from_reader(&mut r) {
            Ok(Some(disk)) => Ok(disk),
            Ok(None) => {
                r.seek(SeekFrom::Start(0))?;
                Part::rom_from_reader(r)
            }
            Err(err) => Err(err),
        }
    }

    fn disk_from_reader<R: BufRead>(mut r: R) -> Result<Option<Self>, std::io::Error> {
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

    fn rom_from_reader<B: BufRead>(mut r: B) -> Result<Self, std::io::Error> {
        use sha1::Sha1;

        let mut sha1 = Sha1::new();
        loop {
            let buf = r.fill_buf()?;
            let len = if buf.is_empty() {
                return Ok(Part::ROM {
                    sha1: sha1.digest().bytes(),
                });
            } else {
                sha1.update(buf);
                buf.len()
            };
            r.consume(len);
        }
    }

    fn verify<P: AsRef<Path>>(&self, part_path: P) -> Option<VerifyFailure<P>> {
        match Part::from_path(part_path.as_ref()) {
            Ok(ref disk_part) if self == disk_part => None,
            Ok(_) => Some(VerifyFailure::Bad(part_path)),
            Err(err) => Some(VerifyFailure::Error(part_path, err)),
        }
    }
}

pub fn parse_sha1(mut hex: &str) -> [u8; 20] {
    let mut bin = [0; 20];

    assert_eq!(hex.len(), 40);
    assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));

    for c in bin.iter_mut() {
        *c = u8::from_str_radix(&hex[0..2], 16).unwrap();
        hex = &hex[2..];
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
pub enum SortBy {
    Description,
    Creator,
    Year,
}

impl FromStr for SortBy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "description" => Ok(SortBy::Description),
            "creator" => Ok(SortBy::Creator),
            "year" => Ok(SortBy::Year),
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

fn subdir_files(root: &Path, progress: ProgressBar) -> Vec<PathBuf> {
    use indicatif::ProgressIterator;
    use walkdir::WalkDir;

    WalkDir::new(root)
        .into_iter()
        .progress_with(progress)
        .filter_map(|e| {
            e.ok()
                .filter(|e| e.file_type().is_file())
                .map(|e| e.into_path())
        })
        .collect()
}

fn rom_sources<F>(root: &Path, check: F) -> HashedMap<Part, PathBuf>
where
    F: Fn(PathBuf) -> Option<(Part, PathBuf)> + Sync + Send,
{
    use indicatif::ParallelProgressIterator;
    use rayon::prelude::*;

    let pbar = ProgressBar::new_spinner().with_style(find_files_style());
    pbar.set_message("locating files");
    pbar.set_draw_delta(100);

    let files = subdir_files(root, pbar.clone());

    pbar.set_message("cataloging files");
    pbar.set_style(verify_style());
    pbar.set_position(0);
    pbar.set_length(files.len() as u64);
    pbar.set_draw_delta(files.len() as u64 / 1000);

    let results = files
        .into_par_iter()
        .progress_with(pbar.clone())
        .filter_map(check)
        .collect();

    pbar.finish_and_clear();

    results
}

pub fn all_rom_sources(root: &Path) -> HashedMap<Part, PathBuf> {
    rom_sources(root, |pb| Part::from_path(&pb).ok().map(|part| (part, pb)))
}

pub fn get_rom_sources(root: &Path, required: HashedSet<Part>) -> HashedMap<Part, PathBuf> {
    rom_sources(root, |pb| {
        Part::from_path(&pb)
            .ok()
            .filter(|part| required.contains(part))
            .map(|part| (part, pb))
    })
}

pub fn copy(part: &Part, source: &Path, target: &Path) -> Result<(), std::io::Error> {
    use std::fs::{copy, create_dir_all, hard_link, remove_file};

    if !target.exists() {
        create_dir_all(target.parent().unwrap())?;
        hard_link(source, target).or_else(|_| copy(source, target).map(|_| ()))?;
        println!("{} -> {}", source.display(), target.display());
    } else if let Some(VerifyFailure::Bad(target)) = part.verify(target) {
        remove_file(target)?;
        hard_link(source, target).or_else(|_| copy(source, target).map(|_| ()))?;
        println!("{} -> {}", source.display(), target.display());
    }
    Ok(())
}

pub fn copy_dry_run(part: &Part, source: &Path, target: &Path) -> Result<(), std::io::Error> {
    if !target.exists() {
        println!("{} -> {}", source.display(), target.display());
    } else if let Some(VerifyFailure::Bad(target)) = part.verify(target.to_path_buf()) {
        println!("{} -> {}", source.display(), target.display());
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
