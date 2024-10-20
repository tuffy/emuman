use super::{is_zip, Error};
use comfy_table::Table;
use core::num::ParseIntError;
use dashmap::mapref::entry::OccupiedEntry;
use dashmap::DashMap;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde_derive::{Deserialize, Serialize};
use sha1_smol::Sha1;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::io::{Read, Seek};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

const CACHE_XATTR: &str = "user.emupart";

type PartMap<T> = DashMap<Part, T, fnv::FnvBuildHasher>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct GameDb {
    description: String,
    games: HashMap<String, Game>,
}

impl GameDb {
    #[inline]
    pub fn new(description: String, games: HashMap<String, Game>) -> Self {
        Self { description, games }
    }

    #[inline]
    pub fn description(&self) -> &str {
        self.description.as_str()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.games.len()
    }

    #[inline]
    pub fn game(&self, game: &str) -> Option<&Game> {
        self.games.get(game)
    }

    #[inline]
    pub fn remove_game(&mut self, game: &str) -> Option<Game> {
        self.games.remove(game)
    }

    #[inline]
    pub fn games_iter(&self) -> impl ExactSizeIterator<Item = &Game> {
        self.games.values()
    }

    #[inline]
    pub fn into_games(self) -> impl ExactSizeIterator<Item = Game> {
        self.games.into_values()
    }

    #[inline]
    pub fn games_map(&self) -> &HashMap<String, Game> {
        &self.games
    }

    #[inline]
    fn valid_game(&self, game: &str) -> Result<&Game, Error> {
        self.games
            .get(game)
            .ok_or_else(|| Error::NoSuchSoftware(game.to_string()))
    }

    #[inline]
    pub fn valid_games<'g, I, C>(&'g self, games: I) -> Result<C, Error>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
        C: FromIterator<&'g Game>,
    {
        games
            .into_iter()
            .map(|name| self.valid_game(name.as_ref()))
            .collect()
    }

    pub fn verify<'g>(&'g self, root: &Path, game: &'g Game) -> Vec<VerifyFailure<'g>> {
        let mut results = game.parts.verify_failures(&root.join(&game.name));
        results.extend(
            game.devices
                .iter()
                .filter_map(|device| self.game(device))
                .flat_map(|device| self.verify(root, device)),
        );
        results
    }

    pub fn list_results(&self, search: Option<&str>, simple: bool) -> Vec<GameRow> {
        if let Some(search) = search {
            self.games_iter()
                .filter(|g| !g.is_device)
                .map(|g| g.report(simple))
                .filter(|g| g.matches(search))
                .collect()
        } else {
            self.games_iter()
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
                .filter_map(|g| self.game(g.as_ref()).map(|g| g.report(simple)))
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
                self.game(g)
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
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::{Cell, Color};

        let mut table = Table::new();
        table
            .set_header(vec!["Game", "Creator", "Year", "Shortname"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        for GameRow {
            description,
            creator,
            year,
            name,
            status,
        } in games
        {
            table.add_row(vec![
                match status {
                    Status::Working => Cell::new(description),
                    Status::Partial => Cell::new(description).fg(Color::Yellow),
                    Status::NotWorking => Cell::new(description).fg(Color::Red),
                },
                Cell::new(creator),
                Cell::new(year),
                Cell::new(name),
            ]);
        }

        println!("{table}");
    }

    pub fn display_parts(&self, name: &str) -> Result<(), Error> {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::{Attribute, Cell, CellAlignment};

        let game = self
            .game(name)
            .ok_or_else(|| Error::NoSuchSoftware(name.to_string()))?;

        let mut table = Table::new();
        table
            .set_header(vec!["Part", "SHA1 Hash"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

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
            table.add_row(vec![Cell::new(name)
                .set_alignment(CellAlignment::Center)
                .add_attribute(Attribute::Bold)]);
            game.display_parts(&mut table);
            for (dev_name, dev) in devices.into_iter() {
                table.add_row(vec![Cell::new(dev_name)
                    .set_alignment(CellAlignment::Center)
                    .add_attribute(Attribute::Bold)]);
                dev.display_parts(&mut table);
            }
        }

        println!("{table}");
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Default)]
pub enum Status {
    #[default]
    Working,
    Partial,
    NotWorking,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Game {
    pub name: String,
    pub description: String,
    pub creator: String,
    pub year: String,
    pub status: Status,
    pub is_device: bool,
    pub parts: GameParts,
    pub devices: Vec<String>,
}

impl Game {
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

    // appends game's name to root automatically
    #[inline]
    pub fn add_and_verify(
        &self,
        rom_sources: &RomSources,
        target_dir: &Path,
        handle_repair: impl Fn(Repaired<'_>) -> Option<PathBuf> + Send + Sync + Copy,
    ) -> Result<Vec<VerifyFailure>, Error> {
        self.parts
            .add_and_verify_failures(rom_sources, &target_dir.join(&self.name), handle_repair)
    }

    pub fn display_parts(&self, table: &mut Table) {
        let parts: BTreeMap<&str, &Part> = self
            .parts
            .iter()
            .map(|(name, part)| (name.as_str(), part))
            .collect();

        for (name, part) in parts {
            table.add_row(vec![name, &part.digest().to_string()]);
        }
    }
}

#[derive(Default)]
pub struct GameDir<F, D, E> {
    pub files: F,
    pub dirs: D,
    pub failures: E,
}

impl<'s, F, D, E> GameDir<F, D, E>
where
    F: Default + ExtendOne<(String, PathBuf)>,
    D: Default + ExtendOne<(String, PathBuf)>,
    E: Default + ExtendOne<VerifyFailure<'s>>,
{
    pub fn open(root: &Path) -> Self {
        fn entry_to_part(entry: std::fs::DirEntry) -> Result<(String, PathBuf), PathBuf> {
            match entry.file_name().into_string() {
                Ok(name) => Ok((name, entry.path())),
                Err(_) => Err(entry.path()),
            }
        }

        let dir = match std::fs::read_dir(root) {
            Ok(dir) => dir,
            Err(_) => return Self::default(),
        };

        let mut files = F::default();
        let mut dirs = D::default();
        let mut failures = E::default();

        for entry in dir.filter_map(|e| e.ok()) {
            match entry.file_type() {
                Ok(t) if t.is_file() => match entry_to_part(entry) {
                    Ok(pair) => files.extend_item(pair),
                    Err(pb) => failures.extend_item(VerifyFailure::extra(pb)),
                },
                Ok(t) if t.is_dir() => match entry_to_part(entry) {
                    Ok(pair) => dirs.extend_item(pair),
                    Err(pb) => failures.extend_item(VerifyFailure::extra_dir(pb)),
                },
                Ok(_) => { /* neither file or dir, so do nothing */ }
                Err(err) => failures.extend_item(VerifyFailure::error(entry.path(), err)),
            }
        }

        Self {
            files,
            dirs,
            failures,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GameParts {
    parts: HashMap<String, Part>,
}

impl FromIterator<(String, Part)> for GameParts {
    #[inline]
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (String, Part)>,
    {
        Self {
            parts: HashMap::from_iter(iter),
        }
    }
}

impl Extend<(String, Part)> for GameParts {
    #[inline]
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (String, Part)>,
    {
        self.parts.extend(iter)
    }
}

impl GameParts {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.parts.len()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Part)> {
        self.parts.iter()
    }

    #[inline]
    pub fn into_iter(self) -> impl Iterator<Item = (String, Part)> {
        self.parts.into_iter()
    }

    #[inline]
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.parts.keys()
    }

    #[inline]
    pub fn paths<'r>(&'r self, root: &'r Path) -> impl Iterator<Item = PathBuf> + 'r {
        self.keys().map(|name| root.join(name))
    }

    #[inline]
    pub fn size(&self, root: &Path) -> FileSize {
        self.paths(root)
            .map(|pb| FileSize::new(&pb).unwrap_or_default())
            .sum()
    }

    #[inline]
    pub fn insert(&mut self, k: String, v: Part) -> Option<Part> {
        self.parts.insert(k, v)
    }

    #[inline]
    pub fn remove(&mut self, name: &str) -> Option<Part> {
        self.parts.remove(name)
    }

    // game_root is the root directory to start looking for files
    // increment_progress is called once per (name, part) pair
    // handle_failure is an attempt to recover from failures
    pub fn process_parts<'s, S, F, E>(
        &'s self,
        game_root: &Path,
        increment_progress: impl Fn() + Send + Sync,
        handle_failure: impl Fn(VerifyFailure) -> Result<Result<Option<PathBuf>, VerifyFailure>, E>
            + Send
            + Sync,
    ) -> Result<(S, F), E>
    where
        S: Default + ExtendOne<VerifySuccess> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
        E: Send,
    {
        let GameDir {
            files,
            dirs,
            mut failures,
        }: GameDir<DashMap<_, _>, Vec<_>, F> = GameDir::open(game_root);

        failures.extend(
            dirs.into_iter()
                .map(|(_, dir)| VerifyFailure::extra_dir(dir)),
        );

        let successes = self.process(
            files,
            &mut failures,
            |name| game_root.join(name),
            increment_progress,
            handle_failure,
        )?;

        Ok((successes, failures))
    }

    // files is a map of files to be processed
    // failures is a running total of existing validation failures
    // missing_path takes a ROM name and returns its desired path
    // increment_progress is called once per (name, part) pair
    // handle failure is how to handle failures that might occur
    pub fn process<'s, S, F, E>(
        &'s self,
        files: DashMap<String, PathBuf>,
        failures: &mut F,
        missing_path: impl Fn(&str) -> PathBuf + Send + Sync,
        increment_progress: impl Fn() + Send + Sync,
        handle_failure: impl Fn(VerifyFailure) -> Result<Result<Option<PathBuf>, VerifyFailure>, E>
            + Send
            + Sync,
    ) -> Result<S, E>
    where
        S: Default + ExtendOne<VerifySuccess> + Send,
        F: ExtendOne<VerifyFailure<'s>> + Send,
        E: Send,
    {
        use rayon::prelude::*;
        use std::sync::Mutex;

        let successes = Mutex::new(S::default());
        let missing;
        let failures = Mutex::new(failures);

        // verify all game parts
        if files.is_empty() {
            missing = Mutex::new(self.parts.iter().collect());
        } else {
            missing = Mutex::new(Vec::new());

            self.parts.par_iter().try_for_each(|(name, part)| {
                match files.remove(name) {
                    Some((_, path)) => {
                        match part.verify(name, path) {
                            Ok(success) => successes.lock().unwrap().extend_item(success),

                            Err(failure) => match handle_failure(failure)? {
                                Ok(Some(_)) => successes.lock().unwrap().extend_item(VerifySuccess),

                                Ok(None) => { /* file deleted, so do nothing */ }

                                Err(failure) => failures.lock().unwrap().extend_item(failure),
                            },
                        }

                        increment_progress();
                    }

                    None => missing.lock().unwrap().push((name, part)),
                }

                Ok(())
            })?;
        }

        // process anything left over on disk
        let extras = PartMap::default();

        files.into_par_iter().try_for_each(|(_, path)| {
            match Part::from_cached_path(&path) {
                Ok(part) => {
                    // populate extras map
                    if let Some(path) = extras.insert(part.clone(), path) {
                        // treat multiple files that hash the same as extras
                        if let Err(failure) = handle_failure(VerifyFailure::Extra {
                            path,
                            part: Ok(part),
                        })? {
                            // leftover Extras can't be promoted to successes
                            // so don't worry about Ok case
                            failures.lock().unwrap().extend_item(failure)
                        }
                    }
                }

                // treat everything we can't read as extras
                part @ Err(_) => failures
                    .lock()
                    .unwrap()
                    .extend_item(VerifyFailure::Extra { path, part }),
            };
            Ok::<(), E>(())
        })?;

        // process everything tagged as missing
        missing
            .into_inner()
            .unwrap()
            .into_par_iter()
            .try_for_each(|(name, part)| {
                let destination = missing_path(name);

                match handle_failure(match extras.remove(part) {
                    // if the missing file is in the extras pile
                    // treat it as a rename and handle it
                    Some((_, source)) => VerifyFailure::Rename {
                        source,
                        destination,
                    },

                    // otherwise, treat it as a missing file and handle it
                    None => VerifyFailure::Missing {
                        path: destination,
                        name,
                        part,
                    },
                })? {
                    Ok(Some(_)) => successes.lock().unwrap().extend_item(VerifySuccess),

                    Ok(None) => { /* file deleted, so do nothing (shouldn't happen) */ }

                    Err(failure) => failures.lock().unwrap().extend_item(failure),
                }

                increment_progress();

                Ok(())
            })?;

        // nothing left to run in parallel, so dispose of the mutex
        let failures = failures.into_inner().unwrap();

        // any leftover extras are handled
        for extra in extras.into_iter().map(|(part, path)| VerifyFailure::Extra {
            part: Ok(part),
            path,
        }) {
            // at this point, any misnamed files have already been handled
            // and Extra files can't be promoted to VerifySuccesses
            // (since they have no valid names)
            // so only the Err case needs to be handled
            if let Err(failure) = handle_failure(extra)? {
                failures.extend_item(failure);
            }
        }

        Ok(successes.into_inner().unwrap())
    }

    #[inline]
    pub fn verify_with_progress<'s, S, F>(
        &'s self,
        game_root: &Path,
        increment_progress: impl Fn() + Send + Sync,
    ) -> (S, F)
    where
        S: Default + ExtendOne<VerifySuccess> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
    {
        self.process_parts(game_root, increment_progress, |failure| {
            Ok::<_, Never>(Err(failure))
        })
        .unwrap()
    }

    #[inline]
    pub fn verify<'s, S, F>(&'s self, game_root: &Path) -> (S, F)
    where
        S: Default + ExtendOne<VerifySuccess> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
    {
        self.verify_with_progress(game_root, || {})
    }

    #[inline]
    pub fn verify_failures<'s>(&'s self, game_root: &Path) -> Vec<VerifyFailure<'s>> {
        let (_, failures): (ExtendSink<_>, _) = self.verify(game_root);
        failures
    }

    #[inline]
    pub fn add_and_verify_with_progress<'s, S, F>(
        &'s self,
        rom_sources: &RomSources,
        game_root: &Path,
        increment_progress: impl Fn() + Send + Sync,
        handle_repair: impl Fn(Repaired<'_>) -> Option<PathBuf> + Send + Sync + Copy,
    ) -> Result<(S, F), Error>
    where
        S: Default + ExtendOne<VerifySuccess> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
    {
        self.process_parts(game_root, increment_progress, |failure| {
            failure.try_fix(rom_sources).map(|r| r.map(handle_repair))
        })
    }

    #[inline]
    pub fn add_and_verify<'s, S, F>(
        &'s self,
        rom_sources: &RomSources,
        game_root: &Path,
        handle_repair: impl Fn(Repaired<'_>) -> Option<PathBuf> + Send + Sync + Copy,
    ) -> Result<(S, F), Error>
    where
        S: Default + ExtendOne<VerifySuccess> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
    {
        self.add_and_verify_with_progress(rom_sources, game_root, || {}, handle_repair)
    }

    #[inline]
    pub fn add_and_verify_failures(
        &self,
        rom_sources: &RomSources,
        game_root: &Path,
        handle_repair: impl Fn(Repaired<'_>) -> Option<PathBuf> + Send + Sync + Copy,
    ) -> Result<Vec<VerifyFailure>, Error> {
        self.add_and_verify(rom_sources, game_root, handle_repair)
            .map(|(_, failures): (ExtendSink<_>, _)| failures)
    }
}

#[derive(Copy, Clone, Default, Ord, PartialOrd, Eq, PartialEq)]
pub struct FileSize {
    pub real: u64,
    pub len: u64,
}

impl FileSize {
    fn new(path: &Path) -> Result<Self, std::io::Error> {
        let metadata = path.metadata()?;
        Ok(Self {
            len: metadata.len(),
            real: filesize::file_real_size_fast(path, &metadata)?,
        })
    }
}

impl std::ops::Add for FileSize {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            len: self.len + other.len,
            real: self.real + other.real,
        }
    }
}

impl std::iter::Sum<FileSize> for FileSize {
    fn sum<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = FileSize>,
    {
        iter.into_iter().fold(Self::default(), |acc, x| acc + x)
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

#[derive(Debug)]
pub struct VerifySuccess;

#[derive(Debug)]
pub enum VerifyFailure<'s> {
    Missing {
        path: PathBuf,
        name: &'s str,
        part: &'s Part,
    },
    Extra {
        path: PathBuf,
        part: Result<Part, std::io::Error>,
    },
    Rename {
        source: PathBuf,
        destination: PathBuf,
    },
    ExtraDir {
        path: PathBuf,
    },
    Bad {
        path: PathBuf,
        name: &'s str,
        expected: &'s Part,
        actual: Part,
    },
    Error {
        path: PathBuf,
        err: std::io::Error,
    },
}

impl<'s> VerifyFailure<'s> {
    #[inline]
    fn extra(path: PathBuf) -> Self {
        Self::Extra {
            part: Part::from_path(&path),
            path,
        }
    }

    #[inline]
    pub fn extra_dir(path: PathBuf) -> Self {
        Self::ExtraDir { path }
    }

    #[inline]
    pub fn error(path: PathBuf, err: std::io::Error) -> Self {
        Self::Error { path, err }
    }

    #[inline]
    pub fn path(&self) -> &Path {
        match self {
            VerifyFailure::Missing { path, .. }
            | VerifyFailure::Extra { path, .. }
            | VerifyFailure::Rename { source: path, .. }
            | VerifyFailure::ExtraDir { path, .. }
            | VerifyFailure::Bad { path, .. }
            | VerifyFailure::Error { path, .. } => path.as_path(),
        }
    }

    // attempt to fix failure, returning either:
    // repair successful            - Ok(Ok(Repaired))
    // unable to repair             - Ok(Err(Self))
    // error occurred during repair - Err(Error)
    pub fn try_fix<'u>(
        self,
        rom_sources: &RomSources<'u>,
    ) -> Result<Result<Repaired<'u>, Self>, Error> {
        use dashmap::mapref::entry::Entry;

        fn extract_to<'u>(
            mut entry: OccupiedEntry<'_, Part, RomSource<'u>>,
            target: PathBuf,
            part: &Part,
        ) -> Result<Repaired<'u>, Error> {
            let source = entry.get();

            match source.extract(target.as_ref())? {
                extracted @ Extracted::Copied { .. } => {
                    part.set_xattr(&target);

                    Ok(Repaired::Extracted {
                        extracted,
                        source: entry.insert(RomSource::File {
                            file: Arc::from(target.clone()),
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

                    Ok(Repaired::Extracted {
                        extracted,
                        source: source.clone(),
                        target,
                    })
                }
            }
        }

        match self {
            VerifyFailure::Bad {
                path,
                name,
                expected,
                actual,
            } => match rom_sources.entry(expected.clone()) {
                Entry::Occupied(entry) => {
                    std::fs::remove_file(&path)?;
                    extract_to(entry, path, expected).map(Ok)
                }

                Entry::Vacant(_) => Ok(Err(VerifyFailure::Bad {
                    path,
                    name,
                    expected,
                    actual,
                })),
            },

            VerifyFailure::Missing { path, part, name } => match rom_sources.entry(part.clone()) {
                Entry::Occupied(entry) => {
                    std::fs::create_dir_all(path.parent().unwrap())?;
                    extract_to(entry, path, part).map(Ok)
                }

                Entry::Vacant(_) => Ok(Err(VerifyFailure::Missing { path, part, name })),
            },

            VerifyFailure::Rename {
                source,
                destination,
                ..
            } => {
                std::fs::rename(&source, &destination)?;
                Ok(Ok(Repaired::Moved {
                    source,
                    destination,
                }))
            }

            VerifyFailure::Extra { path, part: Ok(_) } => {
                std::fs::remove_file(&path)?;
                Ok(Ok(Repaired::Deleted(path)))
            }

            failure => Ok(Err(failure)),
        }
    }
}

impl fmt::Display for VerifyFailure<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VerifyFailure::Missing { path, .. } => {
                write!(f, " MISSING : {}", path.display())
            }
            VerifyFailure::Extra { path, .. } | VerifyFailure::ExtraDir { path } => {
                write!(f, "   EXTRA : {}", path.display())
            }
            VerifyFailure::Rename { source, .. } => {
                write!(f, "MISNAMED : {}", source.display())
            }
            VerifyFailure::Bad { path, .. } => write!(f, "     BAD : {}", path.display()),
            VerifyFailure::Error { path, err } => {
                write!(f, "   ERROR : {} : {}", path.display(), err)
            }
        }
    }
}

pub enum Repaired<'u> {
    Extracted {
        extracted: Extracted,
        source: RomSource<'u>,
        target: PathBuf,
    },
    Moved {
        source: PathBuf,
        destination: PathBuf,
    },
    Deleted(PathBuf),
}

impl<'u> Repaired<'u> {
    pub fn into_fixed_pathbuf(self) -> Option<PathBuf> {
        match self {
            Self::Extracted { target, .. } => Some(target),
            Self::Moved { destination, .. } => Some(destination),
            Self::Deleted(_) => None,
        }
    }
}

impl<'u> fmt::Display for Repaired<'u> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Extracted {
                extracted: Extracted::Copied { rate: None },
                source,
                target,
            } => {
                write!(f, "{} \u{21D2} {}", source, target.display())
            }
            Self::Extracted {
                extracted: Extracted::Copied { rate: Some(rate) },
                source,
                target,
            } => {
                write!(f, "{} \u{21D2} {} ({})", source, target.display(), rate)
            }
            Self::Extracted {
                extracted: Extracted::Linked { .. },
                source,
                target,
            } => {
                write!(f, "{} \u{2192} {}", source, target.display())
            }
            Self::Moved {
                source,
                destination,
            } => {
                write!(f, "{} \u{2192} {}", source.display(), destination.display())
            }
            Self::Deleted(path) => write!(f, "removed : {}", path.display()),
        }
    }
}

// a simple polyfill until extend_one stabilizes in the Extend trait
pub trait ExtendOne<I>: Extend<I> {
    fn extend_item(&mut self, item: I);
}

impl<I> ExtendOne<I> for Vec<I> {
    #[inline]
    fn extend_item(&mut self, item: I) {
        self.push(item);
    }
}

impl<K: Eq + std::hash::Hash, V> ExtendOne<(K, V)> for HashMap<K, V> {
    #[inline]
    fn extend_item(&mut self, (key, value): (K, V)) {
        self.insert(key, value);
    }
}

impl<K: Eq + std::hash::Hash, V> ExtendOne<(K, V)> for DashMap<K, V> {
    #[inline]
    fn extend_item(&mut self, (key, value): (K, V)) {
        self.insert(key, value);
    }
}

pub struct ExtendSink<I>(std::marker::PhantomData<I>);

impl<I> Default for ExtendSink<I> {
    #[inline]
    fn default() -> Self {
        ExtendSink(std::marker::PhantomData)
    }
}

impl<I> Extend<I> for ExtendSink<I> {
    #[inline]
    fn extend<T>(&mut self, _: T)
    where
        T: IntoIterator<Item = I>,
    {
        // do nothing
    }
}

impl<I> ExtendOne<I> for ExtendSink<I> {
    #[inline]
    fn extend_item(&mut self, _: I) {
        // do nothing
    }
}

pub struct ExtendCounter<I> {
    pub total: usize,
    phantom: std::marker::PhantomData<I>,
}

impl<I> Extend<I> for ExtendCounter<I> {
    #[inline]
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = I>,
    {
        self.total += iter.into_iter().count();
    }
}

impl<I> ExtendOne<I> for ExtendCounter<I> {
    #[inline]
    fn extend_item(&mut self, _: I) {
        self.total += 1;
    }
}

impl<I> Default for ExtendCounter<I> {
    #[inline]
    fn default() -> Self {
        Self {
            total: 0,
            phantom: std::marker::PhantomData,
        }
    }
}

pub struct First<T>(pub Option<T>);

impl<T> Default for First<T> {
    #[inline]
    fn default() -> Self {
        Self(None)
    }
}

impl<T> First<T> {
    #[inline]
    fn insert(&mut self, item: T) {
        match &mut self.0 {
            Some(_) => { /* do nothing */ }
            first @ None => {
                *first = Some(item);
            }
        }
    }
}

impl<T> Extend<T> for First<T> {
    #[inline]
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = T>,
    {
        for i in iter {
            self.insert(i);
        }
    }
}

impl<T> ExtendOne<T> for First<T> {
    #[inline]
    fn extend_item(&mut self, item: T) {
        self.insert(item);
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
    pub fn new_rom(sha1: &str) -> Result<Self, hex::FromHexError> {
        parse_sha1(sha1).map(|sha1| Part::Rom { sha1 })
    }

    #[inline]
    pub fn new_disk(sha1: &str) -> Result<Self, hex::FromHexError> {
        parse_sha1(sha1).map(|sha1| Part::Disk { sha1 })
    }

    #[inline]
    pub fn new_empty() -> Self {
        Self::from_slice(b"").unwrap()
    }

    #[inline]
    pub fn digest(&self) -> Digest {
        match self {
            Part::Rom { sha1 } => Digest(sha1),
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
        use fxhash::FxBuildHasher;
        use std::sync::OnceLock as OnceCell;

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

    #[cfg(not(target_os = "windows"))]
    pub fn get_xattr(path: &Path) -> Option<Self> {
        if xattr::SUPPORTED_PLATFORM {
            xattr::get(path, CACHE_XATTR)
                .ok()
                .flatten()
                .and_then(|v| match v.as_slice() {
                    [b'r', sha1_hex @ ..] => {
                        let mut sha1 = [0; 20];
                        hex::decode_to_slice(sha1_hex, &mut sha1)
                            .map(|()| Self::Rom { sha1 })
                            .ok()
                    }
                    [b'd', sha1_hex @ ..] => {
                        let mut sha1 = [0; 20];
                        hex::decode_to_slice(sha1_hex, &mut sha1)
                            .map(|()| Self::Disk { sha1 })
                            .ok()
                    }
                    _ => None,
                })
        } else {
            None
        }
    }

    #[cfg(target_os = "windows")]
    pub fn get_xattr(_path: &Path) -> Option<Self> {
        None
    }

    #[cfg(not(target_os = "windows"))]
    pub fn set_xattr(&self, path: &Path) {
        if xattr::SUPPORTED_PLATFORM {
            let mut attr = [0; 41];
            match self {
                Self::Rom { sha1 } => {
                    attr[0] = b'r';
                    hex::encode_to_slice(sha1, &mut attr[1..]).unwrap();
                }
                Self::Disk { sha1 } => {
                    attr[0] = b'd';
                    hex::encode_to_slice(sha1, &mut attr[1..]).unwrap();
                }
            }

            let _ = xattr::set(path, CACHE_XATTR, &attr);
        }
    }

    #[cfg(target_os = "windows")]
    pub fn set_xattr(&self, _path: &Path) {
        // do nothing
    }

    #[cfg(not(target_os = "windows"))]
    pub fn has_xattr(path: &Path) -> Result<bool, std::io::Error> {
        if xattr::SUPPORTED_PLATFORM {
            xattr::list(path).map(|mut iter| iter.any(|s| s == CACHE_XATTR))
        } else {
            Ok(false)
        }
    }

    #[cfg(target_os = "windows")]
    pub fn has_xattr(_path: &Path) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    #[cfg(not(target_os = "windows"))]
    pub fn remove_xattr(path: &Path) -> Result<(), std::io::Error> {
        if xattr::SUPPORTED_PLATFORM {
            xattr::remove(path, CACHE_XATTR)
        } else {
            Ok(())
        }
    }

    #[cfg(target_os = "windows")]
    pub fn remove_xattr(_path: &Path) -> Result<(), std::io::Error> {
        Ok(())
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

    fn disk_from_reader<R: Read>(r: R) -> Result<Option<Self>, std::io::Error> {
        use bitstream_io::{BigEndian, ByteRead, ByteReader};

        let mut r = ByteReader::endian(r, BigEndian);

        if r.read::<[u8; 8]>()
            .map(|tag| &tag != b"MComprHD")
            .unwrap_or(true)
        {
            return Ok(None);
        }

        // at this point we'll treat the file as a CHD

        r.skip(4)?; // unused length field

        let bytes_to_skip = match r.read::<u32>()? {
            3 => (32 + 32 + 32 + 64 + 64 + 8 * 16 + 8 * 16 + 32) / 8,
            4 => (32 + 32 + 32 + 64 + 64 + 32) / 8,
            5 => (32 * 4 + 64 + 64 + 64 + 32 + 32 + 8 * 20) / 8,
            _ => return Ok(None),
        };
        r.skip(bytes_to_skip)?;

        Ok(Some(Part::Disk { sha1: r.read()? }))
    }

    pub fn verify<'s>(
        &'s self,
        name: &'s str,
        path: PathBuf,
    ) -> Result<VerifySuccess, VerifyFailure<'s>> {
        match Part::from_cached_path(path.as_ref()) {
            Ok(ref disk_part) if self == disk_part => Ok(VerifySuccess),
            Ok(disk_part) => Err(VerifyFailure::Bad {
                path,
                name,
                expected: self,
                actual: disk_part,
            }),
            Err(err) => Err(VerifyFailure::Error { path, err }),
        }
    }

    #[inline]
    pub fn is_valid(&self, path: &Path) -> Result<bool, std::io::Error> {
        Part::from_path(path).map(|disk_part| self == &disk_part)
    }

    #[inline]
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Part::Disk{ sha1 } if sha1 == b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" )
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

#[inline]
pub fn parse_sha1(hex: &str) -> Result<[u8; 20], hex::FromHexError> {
    let mut bin = [0; 20];

    hex::decode_to_slice(hex.trim().as_bytes(), &mut bin).map(|()| bin)
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
pub fn find_files_style() -> ProgressStyle {
    ProgressStyle::default_spinner()
        .template("{spinner} {wide_msg} {pos}")
        .unwrap()
}

#[inline]
pub fn verify_style() -> ProgressStyle {
    ProgressStyle::default_bar()
        .template("{spinner} {wide_msg} {pos} / {len}")
        .unwrap()
}

#[derive(Clone, Debug)]
pub enum Compression {
    Zip { index: usize },
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Compression::Zip { index } => write!(f, "{}", index),
        }
    }
}

impl Compression {
    fn extract<R, W>(&self, i: R, mut o: W) -> Result<u64, Error>
    where
        R: Read + Seek,
        W: std::io::Write,
    {
        match self {
            Self::Zip { index } => {
                std::io::copy(&mut zip::ZipArchive::new(i)?.by_index(*index)?, &mut o)
                    .map_err(Error::IO)
            }
        }
    }

    fn extract_to_buf<R>(&self, i: R) -> Result<std::io::Cursor<Vec<u8>>, Error>
    where
        R: Read + Seek,
    {
        let mut buf = Vec::new();
        self.extract(i, &mut buf).map(|_| std::io::Cursor::new(buf))
    }
}

type ZipParts = Vec<Compression>;

#[derive(Clone, Debug)]
pub enum RomSource<'u> {
    File {
        file: Arc<Path>,
        has_xattr: bool,
        zip_parts: ZipParts,
    },

    Url {
        url: &'u str,
        data: Arc<[u8]>,
        zip_parts: ZipParts,
    },

    Empty,
}

impl<'u> RomSource<'u> {
    // returns true if this source is more "local" than the other,
    // (that is, local files are more local than remote URLs,
    //  and non-zipped files are more local than zipped files)
    pub fn more_local_than(&self, other: &Self) -> bool {
        match (self, other) {
            (
                RomSource::File {
                    zip_parts: parts_a, ..
                },
                RomSource::File {
                    zip_parts: parts_b, ..
                },
            )
            | (
                RomSource::Url {
                    zip_parts: parts_a, ..
                },
                RomSource::Url {
                    zip_parts: parts_b, ..
                },
            ) => parts_a.len() < parts_b.len(),
            (RomSource::File { .. }, _) => true,
            (RomSource::Url { .. }, _) => false,
            (RomSource::Empty, RomSource::Empty) => false,
            (RomSource::Empty, RomSource::File { .. }) => false,
            (RomSource::Empty, RomSource::Url { .. }) => true,
        }
    }

    pub fn from_path(pb: PathBuf) -> Result<Vec<(Part, Self)>, Error> {
        use std::fs::File;
        use std::io::BufReader;

        // if the file already has a cached xattr set,
        // return it as-is without any further parsing
        // and flag it so we don't attempt to set the xattr again
        if let Some(part) = Part::get_xattr(&pb) {
            return Ok(vec![(
                part,
                RomSource::File {
                    file: Arc::from(pb),
                    has_xattr: true,
                    zip_parts: ZipParts::default(),
                },
            )]);
        }

        let file = Arc::from(pb);
        let mut r = File::open(&file).map(BufReader::new)?;

        Ok(if is_zip(&mut r).unwrap_or(false) {
            unpack_zip_parts(r, File::open(&file).map(BufReader::new)?)
                .into_iter()
                .map(|(part, zip_parts)| {
                    (
                        part,
                        RomSource::File {
                            file: Arc::clone(&file),
                            has_xattr: false,
                            zip_parts: zip_parts.into(),
                        },
                    )
                })
                .collect()
        } else {
            vec![(
                Part::from_reader(&mut r)?,
                RomSource::File {
                    file: Arc::clone(&file),
                    has_xattr: false,
                    zip_parts: ZipParts::default(),
                },
            )]
        })
    }

    pub fn from_url(url: &'u str, progress: &MultiProgress) -> Result<Vec<(Part, Self)>, Error> {
        let data: Arc<[u8]> =
            crate::http::fetch_url_data_with_progress(url, progress).map(Arc::from)?;

        let mut result = vec![(
            Part::from_slice(&data)?,
            RomSource::Url {
                url,
                data: data.clone(),
                zip_parts: ZipParts::default(),
            },
        )];

        if matches!(data[..], [0x50, 0x4B, 0x03, 0x04, ..]) {
            let sub_zip = std::io::Cursor::new(data.clone());

            result.extend(unpack_zip_parts(sub_zip.clone(), sub_zip).into_iter().map(
                |(part, zip_parts)| {
                    (
                        part,
                        RomSource::Url {
                            url,
                            data: data.clone(),
                            zip_parts: zip_parts.into(),
                        },
                    )
                },
            ));
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
            } => match zip_parts.as_slice() {
                [] => hard_link(source, target)
                    .map(|()| Extracted::Linked {
                        has_xattr: *has_xattr,
                    })
                    .or_else(|_| {
                        Rate::from_copy(|| copy(source, target))
                            .map(|rate| Extracted::Copied { rate })
                            .map_err(Error::IO)
                    }),

                [c] => std::fs::File::create(target)
                    .map_err(Error::IO)
                    .and_then(|w| Rate::from_copy(|| c.extract(File::open(source.as_ref())?, w)))
                    .map(|rate| Extracted::Copied { rate }),

                [c, rest @ ..] => extract_from_zip_file(
                    rest,
                    c.extract_to_buf(File::open(source.as_ref())?)?,
                    target,
                ),
            },

            RomSource::Url {
                data, zip_parts, ..
            } => extract_from_zip_file(zip_parts, std::io::Cursor::new(data), target),

            RomSource::Empty => File::create(target)
                .map(|_| Extracted::Copied { rate: None })
                .map_err(Error::IO),
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

            RomSource::Empty => write!(f, "\u{2039}EMPTY\u{203A}"),
        }
    }
}

fn extract_from_zip_file<R: Read + Seek>(
    indexes: &[Compression],
    mut r: R,
    target: &Path,
) -> Result<Extracted, Error> {
    match indexes {
        [] => std::fs::File::create(target)
            .and_then(|mut w| Rate::from_copy(|| std::io::copy(&mut r, &mut w)))
            .map(|rate| Extracted::Copied { rate })
            .map_err(Error::IO),

        [c] => std::fs::File::create(target)
            .map_err(Error::IO)
            .and_then(|w| Rate::from_copy(|| c.extract(r, w)))
            .map(|rate| Extracted::Copied { rate }),

        [c, rest @ ..] => extract_from_zip_file(rest, c.extract_to_buf(r)?, target),
    }
}

fn unpack_zip_parts<Z, F>(mut zip: Z, whole_file: F) -> Vec<(Part, VecDeque<Compression>)>
where
    Z: Read + Seek + Send,
    F: Read + Send + 'static,
{
    // a valid ROM might be an invalid Zip file
    // so a failure to unpack Zip parts from a file
    // should not be considered a fatal error

    fn unpack<F: Read + Seek>(zip: F) -> Result<Vec<(Part, VecDeque<Compression>)>, Error> {
        fn is_zip<R: Read>(mut reader: R) -> bool {
            let mut buf = [0; 4];
            match reader.read_exact(&mut buf) {
                Ok(()) => &buf == b"\x50\x4b\x03\x04",
                Err(_) => false,
            }
        }

        let mut zip = zip::ZipArchive::new(zip)?;
        let mut results = Vec::new();

        for index in 0..zip.len() {
            if is_zip(zip.by_index(index)?) {
                let mut zip_data = Vec::new();

                zip.by_index(index)?.read_to_end(&mut zip_data)?;

                let sub_zip = std::io::Cursor::new(zip_data);

                results.extend(unpack_zip_parts(sub_zip.clone(), sub_zip).into_iter().map(
                    |(part, mut zip_parts)| {
                        zip_parts.push_front(Compression::Zip { index });
                        (part, zip_parts)
                    },
                ))
            } else {
                results.push((
                    Part::from_reader(zip.by_index(index)?)?,
                    vec![Compression::Zip { index }].into(),
                ))
            }
        }

        Ok(results)
    }

    let (mut unpacked, whole) = rayon::join(
        || unpack(&mut zip).unwrap_or_default(),
        || Part::from_reader(whole_file),
    );

    if let Ok(part) = whole {
        unpacked.push((part, VecDeque::default()));
    }

    unpacked
}

#[derive(Copy, Clone)]
pub enum Extracted {
    Copied { rate: Option<Rate> },
    Linked { has_xattr: bool },
}

#[derive(Copy, Clone)]
pub struct Rate {
    bytes_per_sec: f64,
}

impl Rate {
    #[inline]
    fn new(bytes: u64, dur: std::time::Duration) -> Rate {
        Rate {
            bytes_per_sec: bytes as f64 / dur.as_secs_f64(),
        }
    }

    #[inline]
    fn from_copy<E>(copy: impl FnOnce() -> Result<u64, E>) -> Result<Option<Self>, E> {
        use std::time::SystemTime;

        let start = SystemTime::now();
        let bytes = copy()?;
        Ok(SystemTime::now()
            .duration_since(start)
            .ok()
            .map(|duration| Self::new(bytes, duration)))
    }
}

impl fmt::Display for Rate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        const K: f64 = (1 << 10) as f64;
        const M: f64 = (1 << 20) as f64;
        const G: f64 = (1 << 30) as f64;

        match self.bytes_per_sec {
            b if (b.is_infinite() || b.is_nan()) => write!(f, "N/A"),
            b if b < K => write!(f, "{:.2} B/s", b),
            b if b < M => write!(f, "{:.2} KiB/s", b / K),
            b if b < G => write!(f, "{:.2} MiB/s", b / M),
            b => write!(f, "{:.2} GiB/s", b / G),
        }
    }
}

pub fn with_progress<T>(
    multi_progress: &indicatif::MultiProgress,
    bar: indicatif::ProgressBar,
    f: impl FnOnce(indicatif::ProgressBar) -> T,
) -> T {
    let pbar = multi_progress.add(bar);
    let results = f(pbar.clone());
    multi_progress.remove(&pbar);
    results
}

pub type RomSources<'u> = PartMap<RomSource<'u>>;

pub fn empty_rom_sources<'r>() -> RomSources<'r> {
    let map = RomSources::default();
    map.insert(Part::new_empty(), RomSource::Empty);
    map
}

pub fn file_rom_sources<'r>(root: &Path, progress: &MultiProgress) -> RomSources<'r> {
    use indicatif::ParallelProgressIterator;
    use nohash::IntSet;
    use rayon::prelude::*;
    #[cfg(unix)]
    use walkdir::DirEntryExt;

    let mut seen = IntSet::default();

    with_progress(
        progress,
        ProgressBar::new_spinner()
            .with_style(find_files_style())
            .with_message("locating files"),
        |pbar| {
            walkdir::WalkDir::new(root)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| {
                    if cfg!(unix) {
                        seen.insert(e.ino())
                    } else {
                        true
                    }
                })
                .map(|e| e.into_path())
                .par_bridge()
                .progress_with(pbar)
                .flat_map(|pb| RomSource::from_path(pb).unwrap_or_default().into_par_iter())
                .collect()
        },
    )
}

#[inline]
pub fn url_rom_sources<'u>(url: &'u str, progress: &MultiProgress) -> RomSources<'u> {
    RomSource::from_url(url, progress)
        .map(|v| v.into_iter().collect())
        .unwrap_or_default()
}

#[derive(Default)]
pub struct VerifyResultsSummary {
    pub successes: usize,
    pub total: usize,
}

impl VerifyResultsSummary {
    pub fn row(&self, name: &str) -> Vec<comfy_table::Cell> {
        use comfy_table::{Cell, CellAlignment, Color};

        let successes = Cell::new(self.successes).set_alignment(CellAlignment::Right);

        vec![
            Cell::new(self.total).set_alignment(CellAlignment::Right),
            if self.successes != self.total {
                successes.fg(Color::Red)
            } else {
                successes
            },
            Cell::new(name),
        ]
    }
}

impl fmt::Display for VerifyResultsSummary {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{total:5} tested, {successes:5} OK",
            total = self.total,
            successes = self.successes
        )
    }
}

impl std::ops::AddAssign for VerifyResultsSummary {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.successes += rhs.successes;
        self.total += rhs.total;
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
                Err(e)
            }
        })
}

// an error that never happens
#[derive(Debug)]
pub enum Never {}
