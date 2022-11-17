use super::{is_zip, Error};
use comfy_table::Table;
use core::num::ParseIntError;
use dashmap::mapref::entry::OccupiedEntry;
use dashmap::DashMap;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde_derive::{Deserialize, Serialize};
use sha1_smol::Sha1;
use std::cmp::Ordering;
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

    pub fn display_all_games(self, simple: bool) {
        GameDb::display_report(
            &self
                .games_iter()
                .map(|g| g.report(simple))
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
    pub fn add_and_verify<H>(
        &self,
        rom_sources: &RomSources,
        target_dir: &Path,
        handle_failure: H,
    ) -> Result<Vec<VerifyFailure>, Error>
    where
        H: Fn(ExtractedPart<'_>) + Send + Sync + Copy,
    {
        self.parts.add_and_verify_failures(
            rom_sources,
            &target_dir.join(&self.name),
            handle_failure,
        )
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
                Err(_) => failures.extend_item(VerifyFailure::extra(entry.path())),
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
    pub fn process_parts<'s, S, F, I, H, E>(
        &'s self,
        game_root: &Path,
        increment_progress: I,
        handle_failure: H,
    ) -> Result<(S, F), E>
    where
        S: Default + ExtendOne<VerifySuccess<'s>> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
        I: Fn() + Send + Sync,
        H: Fn(VerifyFailure) -> Result<Result<(), VerifyFailure>, E> + Send + Sync,
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

        self.process(
            files,
            failures,
            |name, part| VerifyFailure::missing(game_root, name, part),
            increment_progress,
            handle_failure,
        )
    }

    // files is a map of files to be processed
    // failures is a running total of existing validation failures
    // missing is how to build failures from missing files (maybe handled later)
    // increment_progress is called once per (name, part) pair
    // handle failure is how to handle failures that might occur
    pub fn process<'s, S, F, M, I, H, E>(
        &'s self,
        files: DashMap<String, PathBuf>,
        failures: F,
        missing: M,
        increment_progress: I,
        handle_failure: H,
    ) -> Result<(S, F), E>
    where
        S: Default + ExtendOne<VerifySuccess<'s>> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
        M: Fn(&'s str, &'s Part) -> VerifyFailure<'s> + Send + Sync,
        I: Fn() + Send + Sync,
        H: Fn(VerifyFailure) -> Result<Result<(), VerifyFailure>, E> + Send + Sync,
        E: Send,
    {
        use rayon::prelude::*;
        use std::sync::Mutex;

        let successes = Mutex::new(S::default());
        let failures = Mutex::new(failures);

        // verify all game parts
        self.parts.par_iter().try_for_each(|(name, part)| {
            match files.remove(name) {
                Some((_, pathbuf)) => match part.verify(name, pathbuf) {
                    Ok(success) => successes.lock().unwrap().extend_item(success),

                    Err(failure) => match handle_failure(failure)? {
                        Ok(()) => successes
                            .lock()
                            .unwrap()
                            .extend_item(VerifySuccess { name, part }),

                        Err(failure) => failures.lock().unwrap().extend_item(failure),
                    },
                },

                None => match handle_failure(missing(name, part))? {
                    Ok(()) => successes
                        .lock()
                        .unwrap()
                        .extend_item(VerifySuccess { name, part }),

                    Err(failure) => failures.lock().unwrap().extend_item(failure),
                },
            }

            increment_progress();

            Ok(())
        })?;

        let mut failures = failures.into_inner().unwrap();

        // mark any leftover files on disk as extras
        failures.extend_many(files.into_iter().map(|(_, pb)| VerifyFailure::extra(pb)));

        Ok((successes.into_inner().unwrap(), failures))
    }

    #[inline]
    pub fn verify_with_progress<'s, S, F, I>(
        &'s self,
        game_root: &Path,
        increment_progress: I,
    ) -> (S, F)
    where
        I: Fn() + Send + Sync,
        S: Default + ExtendOne<VerifySuccess<'s>> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
    {
        self.process_parts(
            game_root,
            increment_progress,
            |failure| -> Result<_, Never> { Ok(Err(failure)) },
        )
        .unwrap()
    }

    #[inline]
    pub fn verify<'s, S, F>(&'s self, game_root: &Path) -> (S, F)
    where
        S: Default + ExtendOne<VerifySuccess<'s>> + Send,
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
    pub fn add_and_verify_with_progress<'s, S, F, I, H>(
        &'s self,
        rom_sources: &RomSources,
        game_root: &Path,
        increment_progress: I,
        handle_failure: H,
    ) -> Result<(S, F), Error>
    where
        S: Default + ExtendOne<VerifySuccess<'s>> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
        I: Fn() + Send + Sync,
        H: Fn(ExtractedPart<'_>) + Send + Sync + Copy,
    {
        self.process_parts(game_root, increment_progress, |failure| {
            failure.try_fix(rom_sources).map(|r| r.map(handle_failure))
        })
    }

    #[inline]
    pub fn add_and_verify<'s, S, F, H>(
        &'s self,
        rom_sources: &RomSources,
        game_root: &Path,
        handle_failure: H,
    ) -> Result<(S, F), Error>
    where
        S: Default + ExtendOne<VerifySuccess<'s>> + Send,
        F: Default + ExtendOne<VerifyFailure<'s>> + Send,
        H: Fn(ExtractedPart<'_>) + Send + Sync + Copy,
    {
        self.add_and_verify_with_progress(rom_sources, game_root, || {}, handle_failure)
    }

    #[inline]
    pub fn add_and_verify_failures<'s, H>(
        &'s self,
        rom_sources: &RomSources,
        game_root: &Path,
        handle_failure: H,
    ) -> Result<Vec<VerifyFailure>, Error>
    where
        H: Fn(ExtractedPart<'_>) + Send + Sync + Copy,
    {
        self.add_and_verify(rom_sources, game_root, handle_failure)
            .map(|(_, failures): (ExtendSink<_>, _)| failures)
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
pub struct VerifySuccess<'s> {
    pub name: &'s str,
    pub part: &'s Part,
}

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
    pub fn missing(root: &Path, name: &'s str, part: &'s Part) -> Self {
        Self::Missing {
            path: root.join(name),
            name,
            part,
        }
    }

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
    pub fn path(&self) -> &Path {
        match self {
            VerifyFailure::Missing { path, .. }
            | VerifyFailure::Extra { path, .. }
            | VerifyFailure::ExtraDir { path, .. }
            | VerifyFailure::Bad { path, .. }
            | VerifyFailure::Error { path, .. } => path.as_path(),
        }
    }

    // attempt to fix failure by populating missing/bad ROMs from rom_sources
    pub fn try_fix<'u>(
        self,
        rom_sources: &RomSources<'u>,
    ) -> Result<Result<ExtractedPart<'u>, Self>, Error> {
        use dashmap::mapref::entry::Entry;

        match self {
            VerifyFailure::Bad {
                path,
                name,
                expected,
                actual,
            } => match rom_sources.entry(expected.clone()) {
                Entry::Occupied(entry) => {
                    std::fs::remove_file(&path)?;
                    Self::extract_to(entry, path, expected).map(Ok)
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
                    Self::extract_to(entry, path, part).map(Ok)
                }

                Entry::Vacant(_) => Ok(Err(VerifyFailure::Missing { path, part, name })),
            },

            extra @ VerifyFailure::Extra { .. } => Ok(Err(extra)),

            extra @ VerifyFailure::ExtraDir { .. } => Ok(Err(extra)),

            err @ VerifyFailure::Error { .. } => Ok(Err(err)),
        }
    }

    fn extract_to<'u, S: std::hash::BuildHasher>(
        mut entry: OccupiedEntry<'_, Part, RomSource<'u>, S>,
        target: PathBuf,
        part: &Part,
    ) -> Result<ExtractedPart<'u>, Error> {
        let source = entry.get();

        match source.extract(target.as_ref())? {
            extracted @ Extracted::Copied { .. } => {
                part.set_xattr(&target);

                Ok(ExtractedPart {
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

                Ok(ExtractedPart {
                    extracted,
                    source: source.clone(),
                    target,
                })
            }
        }
    }
}

impl fmt::Display for VerifyFailure<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VerifyFailure::Missing { path, .. } => {
                write!(f, "MISSING : {}", path.display())
            }
            VerifyFailure::Extra { path, .. } | VerifyFailure::ExtraDir { path } => {
                write!(f, "EXTRA : {}", path.display())
            }
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
            Extracted::Copied { rate: None } => {
                write!(f, "{} \u{21D2} {}", self.source, self.target.display())
            }
            Extracted::Copied { rate: Some(rate) } => {
                write!(
                    f,
                    "{} \u{21D2} {} ({})",
                    self.source,
                    self.target.display(),
                    rate
                )
            }
            Extracted::Linked { .. } => {
                write!(f, "{} \u{2192} {}", self.source, self.target.display())
            }
        }
    }
}

// a simple polyfill until extend_one stabilizes in the Extend trait
pub trait ExtendOne<I>: Extend<I> {
    fn extend_item(&mut self, item: I);

    #[inline]
    fn extend_many<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = I>,
    {
        self.extend(iter);
    }
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
    pub value: usize,
    phantom: std::marker::PhantomData<I>,
}

impl<I> Extend<I> for ExtendCounter<I> {
    #[inline]
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = I>,
    {
        self.value += iter.into_iter().count();
    }
}

impl<I> ExtendOne<I> for ExtendCounter<I> {
    #[inline]
    fn extend_item(&mut self, _: I) {
        self.value += 1;
    }
}

impl<I> Default for ExtendCounter<I> {
    #[inline]
    fn default() -> Self {
        Self {
            value: 0,
            phantom: std::marker::PhantomData,
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
    pub fn new_rom(sha1: &str) -> Result<Self, hex::FromHexError> {
        parse_sha1(sha1).map(|sha1| Part::Rom { sha1 })
    }

    #[inline]
    pub fn new_disk(sha1: &str) -> Result<Self, hex::FromHexError> {
        parse_sha1(sha1).map(|sha1| Part::Disk { sha1 })
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
        xattr::get(path, CACHE_XATTR)
            .ok()
            .flatten()
            .and_then(|v| match v.split_first() {
                Some((b'r', sha1_hex)) => {
                    let mut sha1 = [0; 20];
                    hex::decode_to_slice(sha1_hex, &mut sha1)
                        .map(|()| Self::Rom { sha1 })
                        .ok()
                }
                Some((b'd', sha1_hex)) => {
                    let mut sha1 = [0; 20];
                    hex::decode_to_slice(sha1_hex, &mut sha1)
                        .map(|()| Self::Disk { sha1 })
                        .ok()
                }
                _ => None,
            })
    }

    #[inline]
    pub fn set_xattr(&self, path: &Path) {
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

    pub fn verify<'s>(
        &'s self,
        name: &'s str,
        path: PathBuf,
    ) -> Result<VerifySuccess<'s>, VerifyFailure<'s>> {
        match Part::from_cached_path(path.as_ref()) {
            Ok(ref disk_part) if self == disk_part => Ok(VerifySuccess { name, part: self }),
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

fn subdir_files(root: &Path, progress: &MultiProgress) -> Vec<PathBuf> {
    use indicatif::ProgressIterator;
    use walkdir::WalkDir;

    let pbar = progress.add(
        ProgressBar::new_spinner()
            .with_style(find_files_style())
            .with_message("locating files"),
    );

    let walkdir = WalkDir::new(root).into_iter().progress_with(pbar.clone());

    let results = if cfg!(unix) {
        use nohash::IntSet;
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

    progress.remove(&pbar);

    results
}

type ZipParts = Vec<usize>;

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
            (RomSource::File { .. }, RomSource::Url { .. }) => true,
            (RomSource::Url { .. }, RomSource::File { .. }) => false,
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

        let mut result = vec![(
            Part::from_reader(&mut r)?,
            RomSource::File {
                file: Arc::clone(&file),
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
                        file: Arc::clone(&file),
                        has_xattr: false,
                        zip_parts,
                    },
                )
            }));
        }

        Ok(result)
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
                None => hard_link(source, target)
                    .map(|()| Extracted::Linked {
                        has_xattr: *has_xattr,
                    })
                    .or_else(|_| {
                        Rate::from_copy(|| copy(source, target))
                            .map(|rate| Extracted::Copied { rate })
                            .map_err(Error::IO)
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
            .and_then(|mut w| Rate::from_copy(|| std::io::copy(&mut r, &mut w)))
            .map(|rate| Extracted::Copied { rate })
            .map_err(Error::IO),

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
    fn from_copy<F>(copy: F) -> Result<Option<Self>, std::io::Error>
    where
        F: FnOnce() -> Result<u64, std::io::Error>,
    {
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
            b => write!(f, "{:2} GiB/s", b / G),
        }
    }
}

pub type RomSources<'u> = DashMap<Part, RomSource<'u>>;

pub fn file_rom_sources<'r>(root: &Path, progress: &MultiProgress) -> RomSources<'r> {
    use indicatif::ParallelProgressIterator;
    use rayon::prelude::*;

    let files = subdir_files(root, progress);

    let pbar = progress.add(
        ProgressBar::new(files.len() as u64)
            .with_style(verify_style())
            .with_message("cataloging files"),
    );

    let results = files
        .into_par_iter()
        .progress_with(pbar.clone())
        .flat_map(|pb| {
            RomSource::from_path(pb)
                .unwrap_or_else(|_| Vec::new())
                .into_par_iter()
        })
        .collect();

    progress.remove(&pbar);

    results
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
