use clap::{Args, Parser, Subcommand};
use roxmltree::Document;
use rusqlite::Connection;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

mod dat;
mod dirs;
mod duplicates;
mod extra;
mod game;
mod http;
mod mame;
mod mess;
mod nointro;
mod redump;
mod split;

static MAME: &str = "mame";
static MESS: &str = "mess";
static EXTRA: &str = "extra";
static REDUMP: &str = "redump";
static NOINTRO: &str = "nointro";

static CACHE_MAME: &str = "mame.cbor";
static CACHE_MESS: &str = "mess.cbor";
static CACHE_EXTRA: &str = "extra.cbor";
static CACHE_REDUMP: &str = "redump.cbor";
static CACHE_NOINTRO: &str = "nointro.cbor";
static CACHE_MESS_SPLIT: &str = "mess-split.cbor";
static CACHE_REDUMP_SPLIT: &str = "redump-split.cbor";

// used to add context about which file caused a given error
#[derive(Debug)]
pub struct FileError<E> {
    file: PathBuf,
    error: E,
}

impl<E: std::error::Error> std::error::Error for FileError<E> {}

impl<E: std::error::Error> std::fmt::Display for FileError<E> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}: {}", self.file.display(), self.error)
    }
}

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    Xml(roxmltree::Error),
    SerdeXml(FileError<quick_xml::de::DeError>),
    Sql(rusqlite::Error),
    CborWrite(ciborium::ser::Error<std::io::Error>),
    TomlWrite(toml::ser::Error),
    Zip(zip::result::ZipError),
    Http(attohttpc::Error),
    HttpCode(attohttpc::StatusCode),
    NoSuchDatFile(String),
    NoSuchSoftwareList(String),
    NoSuchSoftware(String),
    MissingCache(&'static str),
    InvalidCache(&'static str),
    InvalidPath,
    InvalidSha1(FileError<game::Sha1ParseError>),
}

impl Error {
    #[inline]
    fn no_such_dat(s: &str) -> Self {
        Error::NoSuchDatFile(s.to_owned())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err)
    }
}

impl From<roxmltree::Error> for Error {
    fn from(err: roxmltree::Error) -> Self {
        Error::Xml(err)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Error::Sql(err)
    }
}

impl From<zip::result::ZipError> for Error {
    fn from(err: zip::result::ZipError) -> Self {
        Error::Zip(err)
    }
}

impl From<attohttpc::Error> for Error {
    #[inline]
    fn from(err: attohttpc::Error) -> Self {
        Error::Http(err)
    }
}

impl From<toml::ser::Error> for Error {
    #[inline]
    fn from(err: toml::ser::Error) -> Self {
        Error::TomlWrite(err)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IO(err) => Some(err),
            Error::Xml(err) => Some(err),
            err @ Error::SerdeXml(_) => err.source(),
            Error::Sql(err) => Some(err),
            Error::CborWrite(err) => Some(err),
            Error::TomlWrite(err) => Some(err),
            Error::Zip(err) => Some(err),
            Error::Http(err) => Some(err),
            err @ Error::InvalidSha1(_) => err.source(),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IO(err) => err.fmt(f),
            Error::Xml(err) => err.fmt(f),
            Error::SerdeXml(err) => err.fmt(f),
            Error::Sql(err) => err.fmt(f),
            Error::CborWrite(err) => err.fmt(f),
            Error::TomlWrite(err) => err.fmt(f),
            Error::Zip(err) => err.fmt(f),
            Error::Http(err) => err.fmt(f),
            Error::HttpCode(code) => match code.canonical_reason() {
                Some(reason) => write!(f, "HTTP error {} - {}", code.as_str(), reason),
                None => write!(f, "HTTP error {}", code.as_str()),
            },
            Error::NoSuchDatFile(s) => write!(f, "no such dat file \"{}\"", s),
            Error::NoSuchSoftwareList(s) => write!(f, "no such software list \"{}\"", s),
            Error::NoSuchSoftware(s) => write!(f, "no such software \"{}\"", s),
            Error::MissingCache(s) => write!(
                f,
                "missing cache files, please run \"emuman {} create\" to populate",
                s
            ),
            Error::InvalidCache(s) => write!(
                f,
                "outdated or invalid cache files, please run \"emuman {} create\" to repopulate",
                s
            ),
            Error::InvalidPath => write!(f, "invalid UTF-8 path"),
            Error::InvalidSha1(err) => err.fmt(f),
        }
    }
}

enum Resource {
    File(PathBuf),
    Url(String),
}

impl Resource {
    fn open(&self) -> Result<ResourceFile, Error> {
        match self {
            Resource::File(f) => File::open(f).map(ResourceFile::File).map_err(Error::IO),
            Resource::Url(u) => http::fetch_url_data(u.as_str())
                .map(|data| ResourceFile::Url(std::io::Cursor::new(data))),
        }
    }

    // separates resources by files and URLs
    fn partition(resources: Vec<Resource>) -> (Vec<PathBuf>, Vec<String>) {
        let mut files = Vec::default();
        let mut urls = Vec::default();

        for resource in resources {
            match resource {
                Resource::File(f) => files.push(f),
                Resource::Url(u) => urls.push(u),
            }
        }

        (files, urls)
    }
}

impl From<&std::ffi::OsStr> for Resource {
    #[inline]
    fn from(osstr: &std::ffi::OsStr) -> Self {
        match osstr.to_str() {
            Some(s) if url::Url::parse(s).is_ok() => Self::Url(s.to_string()),
            _ => Self::File(PathBuf::from(osstr)),
        }
    }
}

enum ResourceFile {
    File(std::fs::File),
    Url(std::io::Cursor<Box<[u8]>>),
}

impl std::io::Read for ResourceFile {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        match self {
            ResourceFile::File(f) => f.read(buf),
            ResourceFile::Url(f) => f.read(buf),
        }
    }
}

impl std::io::Seek for ResourceFile {
    #[inline]
    fn seek(&mut self, from: std::io::SeekFrom) -> Result<u64, std::io::Error> {
        match self {
            ResourceFile::File(f) => f.seek(from),
            ResourceFile::Url(f) => f.seek(from),
        }
    }
}

#[derive(Args)]
struct OptMameInit {
    /// SQLite database
    #[clap(long = "db", parse(from_os_str))]
    database: Option<PathBuf>,

    /// MAME's XML file or URL
    #[clap(parse(from_os_str))]
    xml: Option<Resource>,
}

impl OptMameInit {
    fn execute(self) -> Result<(), Error> {
        let xml_data = match self.xml {
            Some(resource) => {
                let mut f = resource.open()?;
                let mut data = String::new();
                if is_zip(&mut f)? {
                    zip::ZipArchive::new(f)?
                        .by_index(0)?
                        .read_to_string(&mut data)?;
                } else {
                    f.read_to_string(&mut data)?;
                }
                data
            }
            None => {
                let mut xml_data = String::new();
                std::io::stdin().read_to_string(&mut xml_data)?;
                xml_data
            }
        };

        let xml =
            Document::parse_with_options(&xml_data, roxmltree::ParsingOptions { allow_dtd: true })?;

        if let Some(db_file) = self.database {
            let mut db = open_db(&db_file)?;
            let trans = db.transaction()?;

            mame::create_tables(&trans)?;
            mame::clear_tables(&trans)?;
            mame::xml_to_db(&xml, &trans)?;
            trans.commit()?;

            println!("* Wrote \"{}\"", db_file.display());

            Ok(())
        } else {
            write_cache(CACHE_MAME, mame::xml_to_game_db(&xml))
        }
    }
}

#[derive(Args)]
struct OptMameList {
    /// sorting order, use "description", "year" or "creator"
    #[clap(short = 's', long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// display simple list with less information
    #[clap(short = 'S', long = "simple")]
    simple: bool,

    /// search term for querying specific machines
    search: Option<String>,
}

impl OptMameList {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<game::GameDb>(MAME, CACHE_MAME)?;
        db.list(self.search.as_deref(), self.sort, self.simple);
        Ok(())
    }
}

#[derive(Args)]
struct OptMameGames {
    /// display simple list with less information
    #[clap(short = 'S', long = "simple")]
    simple: bool,

    /// games to search for, by short name
    games: Vec<String>,
}

impl OptMameGames {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<game::GameDb>(MAME, CACHE_MAME)?;
        db.games(&self.games, self.simple);
        Ok(())
    }
}

#[derive(Args)]
struct OptMameParts {
    /// game's parts to search for
    game: String,
}

impl OptMameParts {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<game::GameDb>(MAME, CACHE_MAME)?;
        db.display_parts(&self.game)
    }
}

#[derive(Args)]
struct OptMameReport {
    /// sorting order, use "description", "year" or "creator"
    #[clap(short = 's', long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// ROMs directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// display simple report with less information
    #[clap(short = 'S', long = "simple")]
    simple: bool,

    /// search term for querying specific machines
    search: Option<String>,
}

impl OptMameReport {
    fn execute(self) -> Result<(), Error> {
        let machines: HashSet<String> = dirs::mame_roms(self.roms)
            .as_ref()
            .read_dir()?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect();

        let db = read_cache::<game::GameDb>(MAME, CACHE_MAME)?;
        db.report(&machines, self.search.as_deref(), self.sort, self.simple);

        Ok(())
    }
}

#[derive(Args)]
struct OptMameVerify {
    /// ROMs directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// verify all possible machines
    #[clap(long = "all")]
    all: bool,

    /// verify only working machines
    #[clap(long = "working")]
    working: bool,

    /// display only failures
    #[clap(long = "failures")]
    failures: bool,

    /// game to verify
    #[clap(short = 'g', long = "game")]
    machines: Vec<String>,
}

impl OptMameVerify {
    fn execute(self) -> Result<(), Error> {
        let mut db: game::GameDb = read_cache(MAME, CACHE_MAME)?;

        if self.working {
            db.retain_working();
        }

        let roms_dir = dirs::mame_roms(self.roms);

        let games: HashSet<String> = if self.all {
            db.all_games()
        } else if !self.machines.is_empty() {
            // only validate user-specified machines
            let machines = self.machines.iter().cloned().collect();
            db.validate_games(&machines)?;
            machines
        } else {
            // ignore stuff that's on disk but not valid machines
            roms_dir
                .as_ref()
                .read_dir()?
                .filter_map(|e| {
                    e.ok()
                        .and_then(|e| e.file_name().into_string().ok())
                        .filter(|s| db.is_game(s))
                })
                .collect()
        };

        verify(&db, roms_dir, &games, self.failures);

        Ok(())
    }
}

#[derive(Args)]
struct OptMameAdd {
    /// output directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// game to add
    #[clap(short = 'g', long = "game")]
    machines: Vec<String>,

    /// input file, directory, or URL
    #[clap(parse(from_os_str))]
    input: Vec<Resource>,
}

impl OptMameAdd {
    fn execute(self) -> Result<(), Error> {
        let db: game::GameDb = read_cache(MAME, CACHE_MAME)?;

        let roms_dir = dirs::mame_roms(self.roms);

        let (input, input_url) = Resource::partition(self.input);

        let mut roms = if self.machines.is_empty() {
            game::all_rom_sources(&input, &input_url)
        } else {
            game::get_rom_sources(&input, &input_url, db.required_parts(&self.machines)?)
        };

        if self.machines.is_empty() {
            add_and_verify(&mut roms, &roms_dir, db.games_iter())?;
        } else {
            add_and_verify(
                &mut roms,
                &roms_dir,
                self.machines.iter().filter_map(|game| db.game(game)),
            )?;
        }

        Ok(())
    }
}

#[derive(Subcommand)]
enum OptMame {
    /// initialize internal database
    #[clap(name = "init")]
    Init(OptMameInit),

    /// list all games
    #[clap(name = "list")]
    List(OptMameList),

    /// list a games's ROMs
    #[clap(name = "parts")]
    Parts(OptMameParts),

    /// list given games, in order
    #[clap(name = "games")]
    Games(OptMameGames),

    /// generate report of games in collection
    #[clap(name = "report")]
    Report(OptMameReport),

    /// verify ROMs in directory
    #[clap(name = "verify")]
    Verify(OptMameVerify),

    /// add ROMs to directory
    #[clap(name = "add")]
    Add(OptMameAdd),
}

impl OptMame {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptMame::Init(o) => o.execute(),
            OptMame::List(o) => o.execute(),
            OptMame::Parts(o) => o.execute(),
            OptMame::Games(o) => o.execute(),
            OptMame::Report(o) => o.execute(),
            OptMame::Verify(o) => o.execute(),
            OptMame::Add(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptMessInit {
    /// SQLite database
    #[clap(long = "db", parse(from_os_str))]
    database: Option<PathBuf>,

    /// XML files from hash database
    #[clap(parse(from_os_str))]
    xml: Vec<PathBuf>,
}

impl OptMessInit {
    fn execute(self) -> Result<(), Error> {
        use mess::MessDb;

        if let Some(db_file) = self.database {
            let mut db = open_db(&db_file)?;
            let trans = db.transaction()?;

            mess::create_tables(&trans)?;
            mess::clear_tables(&trans)?;
            self.xml
                .iter()
                .try_for_each(|p| mess::add_file(p, &trans))?;
            trans.commit()?;

            println!("* Wrote \"{}\"", db_file.display());
        } else {
            let mut db = MessDb::default();
            let mut split_db = split::SplitDb::default();

            for file in self.xml.iter() {
                let mut xml_data = String::new();

                File::open(file).and_then(|mut f| f.read_to_string(&mut xml_data))?;

                let tree = Document::parse(&xml_data)?;

                let (name, game_db) = mess::xml_to_game_db(&mut split_db, &tree);
                db.insert(name, game_db);
            }

            write_cache(CACHE_MESS, &db)?;
            write_cache(CACHE_MESS_SPLIT, &split_db)?;
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptMessList {
    /// software list to use
    software_list: Option<String>,

    /// sorting order, use "description", "year" or "publisher"
    #[clap(short = 's', long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// display simple list with less information
    #[clap(short = 'S', long = "simple")]
    simple: bool,

    /// search term for querying specific items
    search: Option<String>,
}

impl OptMessList {
    fn execute(self) -> Result<(), Error> {
        let mut db: mess::MessDb = read_cache(MESS, CACHE_MESS)?;

        match self.software_list.as_deref() {
            Some("any") => mess::list(&db, self.search.as_deref(), self.sort, self.simple),
            Some(software_list) => {
                let db = db
                    .remove(software_list)
                    .ok_or_else(|| Error::NoSuchSoftwareList(software_list.to_string()))?;
                db.list(self.search.as_deref(), self.sort, self.simple)
            }
            None => mess::list_all(&db),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptMessGames {
    /// display simple list with less information
    #[clap(short = 'S', long = "simple")]
    simple: bool,

    /// software list to use
    software_list: String,

    /// games to search for, by short name
    games: Vec<String>,
}

impl OptMessGames {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS).and_then(|mut db| {
            db.remove(&self.software_list)
                .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))
        })?;

        db.games(&self.games, self.simple);
        Ok(())
    }
}

#[derive(Args)]
struct OptMessParts {
    /// software list to use
    software_list: String,

    /// game's parts to search for
    game: String,
}

impl OptMessParts {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS).and_then(|mut db| {
            db.remove(&self.software_list)
                .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))
        })?;

        db.display_parts(&self.game)
    }
}

#[derive(Args)]
struct OptMessReport {
    /// sorting order, use "description", "year" or "creator"
    #[clap(short = 's', long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// ROMs directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// software list to use
    software_list: String,

    /// display simple report with less information
    #[clap(short = 'S', long = "simple")]
    simple: bool,

    /// search term for querying specific software
    search: Option<String>,
}

impl OptMessReport {
    fn execute(self) -> Result<(), Error> {
        use crate::game::GameRow;

        let mut mess_db: mess::MessDb = read_cache(MESS, CACHE_MESS)?;

        if self.software_list == "any" {
            let sort = self.sort;
            let roms_dir = dirs::mess_roms_all(self.roms);

            let mut results: Vec<(&str, GameRow)> = Vec::new();

            for (name, db) in mess_db.iter() {
                let roms = roms_dir.as_ref().join(name);
                if let Ok(dir) = roms.read_dir() {
                    let software: HashSet<String> = dir
                        .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
                        .collect();

                    results.extend(
                        db.report_results(&software, self.search.as_deref(), self.simple)
                            .into_iter()
                            .map(|row| (name.as_str(), row)),
                    );
                }
            }

            results.sort_by(|(_, a), (_, b)| a.compare(b, sort));

            mess::display_results(&results);
        } else {
            let db = mess_db
                .remove(&self.software_list)
                .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

            let software: HashSet<String> = dirs::mess_roms(self.roms, &self.software_list)
                .as_ref()
                .read_dir()?
                .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
                .collect();

            db.report(&software, self.search.as_deref(), self.sort, self.simple);
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptMessVerify {
    /// ROMs directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// verify all possible machines
    #[clap(long = "all")]
    all: bool,

    /// verify only working machines
    #[clap(long = "working")]
    working: bool,

    /// display only failures
    #[clap(long = "failures")]
    failures: bool,

    /// software list to use
    software_list: String,

    /// game to verify
    #[clap(short = 'g', long = "game")]
    software: Vec<String>,
}

impl OptMessVerify {
    fn execute(self) -> Result<(), Error> {
        let mut db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?
            .remove(&self.software_list)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

        let roms_dir = dirs::mess_roms(self.roms, &self.software_list);

        if self.working {
            db.retain_working();
        }

        let software: HashSet<String> = if self.all {
            db.all_games()
        } else if !self.software.is_empty() {
            let software = self.software.clone().into_iter().collect();
            db.validate_games(&software)?;
            software
        } else {
            roms_dir
                .as_ref()
                .read_dir()?
                .filter_map(|e| {
                    e.ok()
                        .and_then(|e| e.file_name().into_string().ok())
                        .filter(|s| db.is_game(s))
                })
                .collect()
        };

        verify(&db, &roms_dir, &software, self.failures);

        Ok(())
    }
}

#[derive(Args)]
struct OptMessVerifyAll {
    /// ROMs directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// verify all possible machines
    #[clap(long = "all")]
    all: bool,

    /// verify only working machines
    #[clap(long = "working")]
    working: bool,

    /// display only failures
    #[clap(long = "failures")]
    failures: bool,
}

impl OptMessVerifyAll {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?;

        let roms_dir = dirs::mess_roms_all(self.roms);

        for (software_list, mut db) in db.into_iter() {
            let roms_path = roms_dir.as_ref().join(&software_list);

            if self.working {
                db.retain_working();
            }

            let software: HashSet<String> = if self.all {
                db.all_games()
            } else {
                roms_path
                    .read_dir()
                    .map(|dir| {
                        dir.filter_map(|e| {
                            e.ok()
                                .and_then(|e| e.file_name().into_string().ok())
                                .filter(|s| db.is_game(s))
                        })
                        .collect()
                    })
                    .unwrap_or_default()
            };

            verify_all(&software_list, &db, &roms_path, &software, self.failures);
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptMessAdd {
    /// output directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// software list to use
    software_list: String,

    /// game to add
    #[clap(short = 'g', long = "game")]
    software: Vec<String>,

    /// input file, directory, or URL
    #[clap(parse(from_os_str))]
    input: Vec<Resource>,
}

impl OptMessAdd {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?
            .remove(&self.software_list)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

        let roms_dir = dirs::mess_roms(self.roms, &self.software_list);

        let (input, input_url) = Resource::partition(self.input);

        let mut roms = if self.software.is_empty() {
            game::all_rom_sources(&input, &input_url)
        } else {
            game::get_rom_sources(&input, &input_url, db.required_parts(&self.software)?)
        };

        if self.software.is_empty() {
            add_and_verify(&mut roms, &roms_dir, db.games_iter())
        } else {
            add_and_verify(
                &mut roms,
                &roms_dir,
                self.software.iter().filter_map(|game| db.game(game)),
            )
        }
    }
}

#[derive(Args)]
struct OptMessAddAll {
    /// output directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// input file, directory, or URL
    #[clap(parse(from_os_str))]
    input: Vec<Resource>,
}

impl OptMessAddAll {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?;

        let roms_dir = dirs::mess_roms_all(self.roms);

        let (input, input_url) = Resource::partition(self.input);

        let mut roms = game::all_rom_sources(&input, &input_url);

        db.into_iter().try_for_each(|(software, db)| {
            add_and_verify_all(
                &software,
                &mut roms,
                &roms_dir.as_ref().join(&software),
                db.games_iter(),
            )
        })
    }
}

#[derive(Args)]
struct OptMessSplit {
    /// target directory for split ROMs
    #[clap(short = 'r', long = "roms", parse(from_os_str), default_value = ".")]
    output: PathBuf,

    /// ROMs to split
    #[clap(parse(from_os_str))]
    roms: Vec<PathBuf>,
}

impl OptMessSplit {
    fn execute(self) -> Result<(), Error> {
        use rayon::prelude::*;

        let db = read_cache::<split::SplitDb>(MESS, CACHE_MESS_SPLIT)?;

        self.roms.par_iter().try_for_each(|rom| {
            let mut f = File::open(&rom)?;

            let roms: Vec<Vec<u8>> = if is_zip(&mut f)? {
                let mut zip = zip::ZipArchive::new(f)?;
                (0..zip.len())
                    .map(|index| {
                        let mut rom_data = Vec::new();
                        zip.by_index(index)?.read_to_end(&mut rom_data)?;
                        Ok(rom_data)
                    })
                    .collect::<Result<Vec<Vec<u8>>, Error>>()?
            } else {
                let mut rom_data = Vec::new();
                f.read_to_end(&mut rom_data)?;
                vec![rom_data]
            };

            for rom_data in roms.into_iter() {
                let data = mess::strip_ines_header(&rom_data);

                if let Some(exact_match) = db
                    .possible_matches(data.len() as u64)
                    .iter()
                    .find(|m| m.matches(data))
                {
                    exact_match.extract(&self.output, data)?;
                }
            }

            Ok(())
        })
    }
}

#[derive(Subcommand)]
#[clap(name = "sl")]
enum OptMess {
    /// initialize internal database
    #[clap(name = "init")]
    Init(OptMessInit),

    /// list all software in software list
    #[clap(name = "list")]
    List(OptMessList),

    /// list given games, in order
    #[clap(name = "games")]
    Games(OptMessGames),

    /// list a machine's ROMs
    #[clap(name = "parts")]
    Parts(OptMessParts),

    /// generate report of sets in collection
    #[clap(name = "report")]
    Report(OptMessReport),

    /// verify ROMs in directory
    #[clap(name = "verify")]
    Verify(OptMessVerify),

    /// verify all ROMs in all software lists in directory
    #[clap(name = "verify-all")]
    VerifyAll(OptMessVerifyAll),

    /// add ROMs to directory
    #[clap(name = "add")]
    Add(OptMessAdd),

    /// add all ROMs from all software lists to directory
    #[clap(name = "add-all")]
    AddAll(OptMessAddAll),

    /// split ROM into software list-compatible parts, if necessary
    #[clap(name = "split")]
    Split(OptMessSplit),
}

impl OptMess {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptMess::Init(o) => o.execute(),
            OptMess::List(o) => o.execute(),
            OptMess::Games(o) => o.execute(),
            OptMess::Parts(o) => o.execute(),
            OptMess::Report(o) => o.execute(),
            OptMess::Verify(o) => o.execute(),
            OptMess::VerifyAll(o) => o.execute(),
            OptMess::Add(o) => o.execute(),
            OptMess::AddAll(o) => o.execute(),
            OptMess::Split(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptExtraInit {
    /// extras .DAT file files
    #[clap(parse(from_os_str))]
    dats: Vec<PathBuf>,

    /// completely replace old dat files
    #[clap(long = "replace")]
    replace: bool,
}

impl OptExtraInit {
    fn execute(self) -> Result<(), Error> {
        let mut db = if self.replace {
            extra::ExtraDb::default()
        } else {
            read_cache(EXTRA, CACHE_EXTRA).unwrap_or_default()
        };

        for dats in self.dats.into_iter().map(dat::read_unflattened_dats) {
            for dat in dats? {
                db.insert(dat.name().to_owned(), dat);
            }
        }

        write_cache(CACHE_EXTRA, db)
    }
}

#[derive(Args)]
struct OptExtraDirs {
    // sort output by version
    #[clap(short = 'V')]
    sort_by_version: bool,
}

impl OptExtraDirs {
    fn execute(self) -> Result<(), Error> {
        display_dirs(
            dirs::extra_dirs(),
            read_cache(EXTRA, CACHE_EXTRA)?,
            self.sort_by_version,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraList {
    /// extras name
    name: Option<String>,
}

impl OptExtraList {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA)?;

        match self.name.as_deref() {
            Some(name) => db.get(name).ok_or_else(|| Error::no_such_dat(name))?.list(),
            None => dat::DatFile::list_all(db),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraVerify {
    /// extras directory
    #[clap(short = 'd', long = "dir", parse(from_os_str))]
    dir: Option<PathBuf>,

    /// extras category to verify
    extra: String,

    /// display only failures
    #[clap(long = "failures")]
    failures: bool,

    /// verify all possible entries
    #[clap(long = "all")]
    all: bool,
}

impl OptExtraVerify {
    fn execute(self) -> Result<(), Error> {
        let mut db: extra::ExtraDb = read_cache(EXTRA, CACHE_EXTRA)?;

        let datfile = db
            .remove(&self.extra)
            .ok_or_else(|| Error::no_such_dat(&self.extra))?;

        let mut table = init_dat_table();

        game::display_dat_results(
            &mut table,
            &datfile,
            datfile.verify(dirs::extra_dir(self.dir, &self.extra).as_ref(), self.all),
            self.failures,
        );

        display_dat_table(table, None);

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraVerifyAll {
    /// display only failures
    #[clap(long = "failures")]
    failures: bool,

    /// verify all possible entries
    #[clap(long = "all")]
    all: bool,
}

impl OptExtraVerifyAll {
    fn execute(self) -> Result<(), Error> {
        let db: extra::ExtraDb = read_cache(EXTRA, CACHE_EXTRA)?;

        let mut total = game::VerifyResultsSummary::default();

        let mut table = init_dat_table();

        for (name, dir) in dirs::extra_dirs() {
            if let Some(datfile) = db.get(&name) {
                total += game::display_dat_results(
                    &mut table,
                    datfile,
                    datfile.verify(&dir, self.all),
                    self.failures,
                );
            }
        }

        display_dat_table(table, Some(total));

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraAdd {
    /// output directory
    #[clap(short = 'd', long = "dir", parse(from_os_str))]
    dir: Option<PathBuf>,

    /// extra to add
    extra: String,

    /// input file, directory, or URL
    #[clap(parse(from_os_str))]
    input: Vec<Resource>,

    /// verify all possible machines
    #[clap(long = "all")]
    all: bool,
}

impl OptExtraAdd {
    fn execute(self) -> Result<(), Error> {
        let mut db: extra::ExtraDb = read_cache(EXTRA, CACHE_EXTRA)?;

        let datfile = db
            .remove(&self.extra)
            .ok_or_else(|| Error::no_such_dat(&self.extra))?;

        let (input, input_url) = Resource::partition(self.input);

        let mut roms = game::get_rom_sources(&input, &input_url, datfile.required_parts());

        let mut table = init_dat_table();

        game::display_dat_results(
            &mut table,
            &datfile,
            datfile.add_and_verify(
                &mut roms,
                dirs::extra_dir(self.dir, &self.extra).as_ref(),
                self.all,
                |p| eprintln!("{}", p),
            )?,
            true,
        );

        display_dat_table(table, None);

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraAddAll {
    /// verify all possible machines
    #[clap(long = "all")]
    all: bool,

    /// input file, directory, or URL
    #[clap(parse(from_os_str))]
    input: Vec<Resource>,
}

impl OptExtraAddAll {
    fn execute(self) -> Result<(), Error> {
        let mut db = read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA)?;

        let (input, input_url) = Resource::partition(self.input);

        let mut parts = game::all_rom_sources(&input, &input_url);

        let mut total = game::VerifyResultsSummary::default();

        let mut table = init_dat_table();

        for (name, dir) in dirs::extra_dirs() {
            if let Some(datfile) = db.remove(&name) {
                total += game::display_dat_results(
                    &mut table,
                    &datfile,
                    datfile.add_and_verify(&mut parts, &dir, self.all, |p| eprintln!("{}", p))?,
                    true,
                );
            }
        }
        display_dat_table(table, Some(total));

        Ok(())
    }
}

#[derive(Subcommand)]
#[clap(name = "extra")]
enum OptExtra {
    /// initialize internal database
    #[clap(name = "init")]
    Init(OptExtraInit),

    /// list defined directories
    #[clap(name = "dirs")]
    Dirs(OptExtraDirs),

    /// list all extras categories
    #[clap(name = "list")]
    List(OptExtraList),

    /// verify parts in directory
    #[clap(name = "verify")]
    Verify(OptExtraVerify),

    /// add files to directory
    #[clap(name = "add")]
    Add(OptExtraAdd),

    /// add files to all directories
    #[clap(name = "add-all")]
    AddAll(OptExtraAddAll),

    /// verify all files in directory
    #[clap(name = "verify-all")]
    VerifyAll(OptExtraVerifyAll),
}

impl OptExtra {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptExtra::Init(o) => o.execute(),
            OptExtra::Dirs(o) => o.execute(),
            OptExtra::List(o) => o.execute(),
            OptExtra::Verify(o) => o.execute(),
            OptExtra::Add(o) => o.execute(),
            OptExtra::AddAll(o) => o.execute(),
            OptExtra::VerifyAll(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptRedumpInit {
    /// SQLite database
    #[clap(long = "db", parse(from_os_str))]
    database: Option<PathBuf>,

    /// Redump XML or Zip file
    #[clap(parse(from_os_str))]
    xml: Vec<PathBuf>,
}

impl OptRedumpInit {
    fn execute(self) -> Result<(), Error> {
        if let Some(db_file) = self.database {
            let mut db = open_db(&db_file)?;
            let trans = db.transaction()?;

            redump::create_tables(&trans)?;
            redump::clear_tables(&trans)?;

            for file in self.xml {
                for dat in dat::read_datafiles(file)? {
                    redump::add_data_file_to_db(&trans, &dat)?;
                }
            }

            trans.commit()?;

            println!("* Wrote \"{}\"", db_file.display());
        } else {
            let mut redump_db = redump::RedumpDb::default();
            let mut split_db = split::SplitDb::default();

            for file in self.xml.into_iter() {
                for (file, data) in dat::read_dats_from_file(file)? {
                    let datafile: crate::dat::Datafile =
                        match quick_xml::de::from_reader(std::io::Cursor::new(data)) {
                            Ok(dat) => dat,
                            Err(error) => return Err(Error::SerdeXml(FileError { file, error })),
                        };

                    redump::populate_split_db(&mut split_db, &datafile);

                    let dat = crate::dat::DatFile::new_flattened(datafile)
                        .map_err(|error| Error::InvalidSha1(FileError { file, error }))?;

                    redump_db.insert(dat.name().to_owned(), dat);
                }
            }

            write_cache(CACHE_REDUMP, &redump_db)?;
            write_cache(CACHE_REDUMP_SPLIT, &split_db)?;
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpDirs {
    // sort output by version
    #[clap(short = 'V')]
    sort_by_version: bool,
}

impl OptRedumpDirs {
    fn execute(self) -> Result<(), Error> {
        display_dirs(
            dirs::redump_dirs(),
            read_cache(REDUMP, CACHE_REDUMP)?,
            self.sort_by_version,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpList {
    /// software list to use
    software_list: Option<String>,
}

impl OptRedumpList {
    fn execute(self) -> Result<(), Error> {
        let db: redump::RedumpDb = read_cache(REDUMP, CACHE_REDUMP)?;

        match self.software_list.as_deref() {
            Some(name) => db.get(name).ok_or_else(|| Error::no_such_dat(name))?.list(),
            None => dat::DatFile::list_all(db),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpParts {
    /// software list to use
    software_list: String,

    /// game's tracks to search for
    game: String,
}

impl OptRedumpParts {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(REDUMP, CACHE_REDUMP).and_then(|mut db| {
            db.remove(&self.software_list)
                .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))
        })?;

        db.display_parts(&self.game)
    }
}

#[derive(Args)]
struct OptRedumpVerify {
    /// root directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    root: Option<PathBuf>,

    /// software list to use
    software_list: String,

    /// display only failures
    #[clap(long = "failures")]
    failures: bool,

    /// verify all possible entries
    #[clap(long = "all")]
    all: bool,
}

impl OptRedumpVerify {
    fn execute(self) -> Result<(), Error> {
        let mut db: nointro::NointroDb = read_cache(REDUMP, CACHE_REDUMP)?;

        let datfile = db
            .remove(&self.software_list)
            .ok_or_else(|| Error::no_such_dat(&self.software_list))?;

        let mut table = init_dat_table();

        game::display_dat_results(
            &mut table,
            &datfile,
            datfile.verify(
                dirs::redump_roms(self.root, &self.software_list).as_ref(),
                self.all,
            ),
            self.failures,
        );

        display_dat_table(table, None);

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpAdd {
    /// output directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    output: Option<PathBuf>,

    /// software list to use
    software_list: String,

    /// input file, directory, or URL
    #[clap(parse(from_os_str))]
    input: Vec<Resource>,

    /// verify all possible machines
    #[clap(long = "all")]
    all: bool,
}

impl OptRedumpAdd {
    fn execute(self) -> Result<(), Error> {
        let mut db = read_cache::<redump::RedumpDb>(MESS, CACHE_REDUMP)?;

        let datfile = db
            .remove(&self.software_list)
            .ok_or_else(|| Error::no_such_dat(&self.software_list))?;

        let (input, input_url) = Resource::partition(self.input);

        let mut roms = game::get_rom_sources(&input, &input_url, datfile.required_parts());

        let mut table = init_dat_table();

        game::display_dat_results(
            &mut table,
            &datfile,
            datfile.add_and_verify(
                &mut roms,
                dirs::redump_roms(self.output, &self.software_list).as_ref(),
                self.all,
                |p| eprintln!("{}", p),
            )?,
            true,
        );
        display_dat_table(table, None);

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpSplit {
    /// directory to place output tracks
    #[clap(short = 'r', long = "roms", parse(from_os_str), default_value = ".")]
    root: PathBuf,

    /// input .bin file
    #[clap(parse(from_os_str))]
    bins: Vec<PathBuf>,
}

impl OptRedumpSplit {
    fn execute(self) -> Result<(), Error> {
        let db: split::SplitDb = read_cache(REDUMP, CACHE_REDUMP_SPLIT)?;

        self.bins.iter().try_for_each(|bin_path| {
            let matches = bin_path
                .metadata()
                .map(|m| db.possible_matches(m.len()))
                .unwrap_or(&[]);
            if !matches.is_empty() {
                let mut bin_data = Vec::new();
                File::open(bin_path).and_then(|mut f| f.read_to_end(&mut bin_data))?;
                if let Some(exact_match) = matches.iter().find(|m| m.matches(&bin_data)) {
                    exact_match.extract(&self.root, &bin_data)?;
                }
            }
            Ok(())
        })
    }
}

#[derive(Subcommand)]
#[clap(name = "redump")]
enum OptRedump {
    /// initialize internal database
    #[clap(name = "init")]
    Init(OptRedumpInit),

    /// list defined directories
    #[clap(name = "dirs")]
    Dirs(OptRedumpDirs),

    /// list all software in software list
    #[clap(name = "list")]
    List(OptRedumpList),

    /// list all tracks for a given game
    #[clap(name = "parts")]
    Parts(OptRedumpParts),

    /// verify files against Redump database
    #[clap(name = "verify")]
    Verify(OptRedumpVerify),

    /// add tracks to directory
    #[clap(name = "add")]
    Add(OptRedumpAdd),

    /// split .bin file into multiple tracks
    #[clap(name = "split")]
    Split(OptRedumpSplit),
}

impl OptRedump {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptRedump::Init(o) => o.execute(),
            OptRedump::Dirs(o) => o.execute(),
            OptRedump::List(o) => o.execute(),
            OptRedump::Parts(o) => o.execute(),
            OptRedump::Verify(o) => o.execute(),
            OptRedump::Add(o) => o.execute(),
            OptRedump::Split(o) => o.execute(),
        }
    }
}

#[derive(Subcommand)]
#[clap(name = "nointro")]
enum OptNointro {
    /// initialize internal database
    #[clap(name = "init")]
    Init(OptNointroInit),

    /// list defined directories
    #[clap(name = "dirs")]
    Dirs(OptNointroDirs),

    /// list categories or ROMs
    #[clap(name = "list")]
    List(OptNointroList),

    /// verify category's ROMs
    #[clap(name = "verify")]
    Verify(OptNointroVerify),

    /// verify all ROMs in all categories
    #[clap(name = "verify-all")]
    VerifyAll(OptNointroVerifyAll),

    /// add and verify category's ROMs
    #[clap(name = "add")]
    Add(OptNointroAdd),

    /// add ROMs to all categories
    #[clap(name = "add-all")]
    AddAll(OptNointroAddAll),
}

impl OptNointro {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptNointro::Init(o) => o.execute(),
            OptNointro::Dirs(o) => o.execute(),
            OptNointro::List(o) => o.execute(),
            OptNointro::Verify(o) => o.execute(),
            OptNointro::VerifyAll(o) => o.execute(),
            OptNointro::Add(o) => o.execute(),
            OptNointro::AddAll(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptNointroInit {
    /// No-Intro DAT or Zip file
    #[clap(parse(from_os_str))]
    dats: Vec<PathBuf>,

    /// completely replace old dat files
    #[clap(long = "replace")]
    replace: bool,
}

impl OptNointroInit {
    fn execute(self) -> Result<(), Error> {
        let mut db = if self.replace {
            nointro::NointroDb::default()
        } else {
            read_cache(NOINTRO, CACHE_NOINTRO).unwrap_or_default()
        };

        for dats in self.dats.into_iter().map(dat::read_dats) {
            for dat in dats? {
                db.insert(dat.name().to_owned(), dat);
            }
        }

        write_cache(CACHE_NOINTRO, db)
    }
}

#[derive(Args)]
struct OptNointroDirs {
    /// sort output by version
    #[clap(short = 'V')]
    sort_by_version: bool,
}

impl OptNointroDirs {
    fn execute(self) -> Result<(), Error> {
        display_dirs(
            dirs::nointro_dirs(),
            read_cache(NOINTRO, CACHE_NOINTRO)?,
            self.sort_by_version,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroList {
    /// category name
    name: Option<String>,
}

impl OptNointroList {
    fn execute(self) -> Result<(), Error> {
        let db: nointro::NointroDb = read_cache(NOINTRO, CACHE_NOINTRO)?;

        match self.name.as_deref() {
            Some(name) => db.get(name).ok_or_else(|| Error::no_such_dat(name))?.list(),
            None => dat::DatFile::list_all(db),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroVerify {
    /// ROMs directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// category name to verify
    name: String,

    /// display only failures
    #[clap(long = "failures")]
    failures: bool,

    /// verify all possible entries
    #[clap(long = "all")]
    all: bool,
}

impl OptNointroVerify {
    fn execute(self) -> Result<(), Error> {
        let mut db: nointro::NointroDb = read_cache(NOINTRO, CACHE_NOINTRO)?;

        let datfile = db
            .remove(&self.name)
            .ok_or_else(|| Error::no_such_dat(&self.name))?;

        let mut table = init_dat_table();
        game::display_dat_results(
            &mut table,
            &datfile,
            datfile.verify(dirs::nointro_roms(self.roms, &self.name).as_ref(), self.all),
            self.failures,
        );
        display_dat_table(table, None);

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroVerifyAll {
    /// display only failures
    #[clap(long = "failures")]
    failures: bool,

    /// verify all possible entries
    #[clap(long = "all")]
    all: bool,
}

impl OptNointroVerifyAll {
    fn execute(self) -> Result<(), Error> {
        let db: nointro::NointroDb = read_cache(NOINTRO, CACHE_NOINTRO)?;

        let mut total = game::VerifyResultsSummary::default();
        let mut table = init_dat_table();
        for (name, dir) in dirs::nointro_dirs() {
            if let Some(datfile) = db.get(&name) {
                total += game::display_dat_results(
                    &mut table,
                    datfile,
                    datfile.verify(&dir, self.all),
                    self.failures,
                );
            }
        }
        display_dat_table(table, Some(total));

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroAdd {
    /// output directory
    #[clap(short = 'r', long = "roms", parse(from_os_str))]
    roms: Option<PathBuf>,

    /// category name to add ROMs to
    name: String,

    /// input file, directory, or URL
    #[clap(parse(from_os_str))]
    input: Vec<Resource>,

    /// verify all possible machines
    #[clap(long = "all")]
    all: bool,
}

impl OptNointroAdd {
    fn execute(self) -> Result<(), Error> {
        let mut db: nointro::NointroDb = read_cache(NOINTRO, CACHE_NOINTRO)?;

        let datfile = db
            .remove(&self.name)
            .ok_or_else(|| Error::no_such_dat(&self.name))?;

        let (input, input_url) = Resource::partition(self.input);

        let mut roms = game::get_rom_sources(&input, &input_url, datfile.required_parts());

        let mut table = init_dat_table();
        game::display_dat_results(
            &mut table,
            &datfile,
            datfile.add_and_verify(
                &mut roms,
                dirs::nointro_roms(self.roms, &self.name).as_ref(),
                self.all,
                |p| eprintln!("{}", p),
            )?,
            true,
        );
        display_dat_table(table, None);

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroAddAll {
    /// display only failures
    #[clap(long = "failures")]
    failures: bool,

    /// input file, directory, or URL
    #[clap(parse(from_os_str))]
    input: Vec<Resource>,

    /// verify all possible machines
    #[clap(long = "all")]
    all: bool,
}

impl OptNointroAddAll {
    fn execute(self) -> Result<(), Error> {
        let mut db: nointro::NointroDb = read_cache(NOINTRO, CACHE_NOINTRO)?;

        let (input, input_url) = Resource::partition(self.input);

        let mut parts = game::all_rom_sources(&input, &input_url);

        let mut table = init_dat_table();
        let mut total = game::VerifyResultsSummary::default();
        for (name, dir) in dirs::extra_dirs() {
            if let Some(datfile) = db.remove(&name) {
                total += game::display_dat_results(
                    &mut table,
                    &datfile,
                    datfile.add_and_verify(&mut parts, &dir, self.all, |p| eprintln!("{}", p))?,
                    self.failures,
                );
            }
        }
        display_dat_table(table, Some(total));

        Ok(())
    }
}

#[derive(Args)]
struct OptIdentify {
    /// ROMs or CHDs to identify
    parts: Vec<PathBuf>,

    /// perform reverse lookup
    #[clap(short = 'l', long = "lookup")]
    lookup: bool,
}

impl OptIdentify {
    fn execute(self) -> Result<(), Error> {
        use crate::dat::DatFile;
        use crate::game::{GameDb, Part, RomSource};
        use prettytable::{cell, format, row, Table};
        use rayon::iter::{IntoParallelIterator, ParallelIterator};
        use std::collections::{BTreeSet, HashMap};

        let sources = self
            .parts
            .into_par_iter()
            .map(RomSource::from_path)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten();

        if self.lookup {
            let mut lookup: HashMap<&Part, BTreeSet<[&str; 4]>> = HashMap::default();

            let mame_db: GameDb = read_cache(MAME, CACHE_MAME).unwrap_or_default();
            let mess_db: BTreeMap<String, GameDb> =
                read_cache(MESS, CACHE_MESS).unwrap_or_default();

            let dat_parts: [(&str, BTreeMap<String, DatFile>); 3] = [
                ("extra", read_cache(EXTRA, CACHE_EXTRA).unwrap_or_default()),
                (
                    "nointro",
                    read_cache(NOINTRO, CACHE_NOINTRO).unwrap_or_default(),
                ),
                (
                    "redump",
                    read_cache(REDUMP, CACHE_REDUMP).unwrap_or_default(),
                ),
            ];

            for game in mame_db.games_iter() {
                for (rom, part) in game.parts.iter() {
                    lookup
                        .entry(part)
                        .or_default()
                        .insert(["mame", "", game.name.as_str(), rom]);
                }
            }

            // invert caches into a Part -> [identifiers] lookup table
            for (system, game_db) in mess_db.iter() {
                for game in game_db.games_iter() {
                    for (rom, part) in game.parts.iter() {
                        lookup.entry(part).or_default().insert([
                            "mess",
                            system,
                            game.name.as_str(),
                            rom,
                        ]);
                    }
                }
            }

            for (category, datfiles) in &dat_parts {
                for (system, datfile) in datfiles.iter() {
                    for (game, parts) in datfile.game_parts() {
                        for (rom, part) in parts.iter() {
                            lookup
                                .entry(part)
                                .or_default()
                                .insert([category, system, game, rom]);
                        }
                    }
                }
            }

            let mut table = Table::new();
            table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
            table.get_format().column_separator('\u{2502}');

            for (part, source) in sources {
                for [category, system, game, rom] in lookup.get(&part).into_iter().flatten() {
                    table.add_row(row![source, category, system, game, rom]);
                }
            }

            table.printstd();
        } else {
            for (part, source) in sources {
                println!("{}  {}", part.digest(), source);
            }
        }

        Ok(())
    }
}

#[derive(Subcommand)]
enum OptCache {
    /// add cache entries to files
    Add(OptCacheAdd),

    /// remove cache entries from files
    #[clap(name = "delete")]
    Delete(OptCacheDelete),

    /// verify existing cache entries
    #[clap(name = "verify")]
    Verify(OptCacheVerify),

    /// find duplicate files and link them together
    #[clap(name = "link-dupes")]
    LinkDupes(OptCacheLinkDupes),
}

impl OptCache {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptCache::Add(o) => o.execute(),
            OptCache::Delete(o) => o.execute(),
            OptCache::Verify(o) => o.execute(),
            OptCache::LinkDupes(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptCacheAdd {
    /// files or directories
    #[clap(parse(from_os_str))]
    paths: Vec<PathBuf>,
}

impl OptCacheAdd {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Part;
        use indicatif::{ParallelProgressIterator, ProgressBar};
        use rayon::prelude::*;

        let pb = ProgressBar::new_spinner().with_message("locating files");
        let files = {
            pb.wrap_iter(
                self.paths
                    .into_iter()
                    .flat_map(unique_sub_files)
                    .filter(|pb| matches!(Part::has_xattr(pb), Ok(false))),
            )
            .collect::<Vec<PathBuf>>()
        };
        pb.finish_and_clear();

        let pb = ProgressBar::new(files.len() as u64)
            .with_style(crate::game::verify_style())
            .with_message("adding cache entries");

        files
            .into_par_iter()
            .progress_with(pb.clone())
            .for_each(|file: PathBuf| match Part::from_path(&file) {
                Ok(part) => part.set_xattr(&file),
                Err(err) => pb.println(format!("{} : {}", file.display(), err)),
            });

        pb.finish_and_clear();

        Ok(())
    }
}

#[derive(Args)]
struct OptCacheDelete {
    /// files or directories
    #[clap(parse(from_os_str))]
    paths: Vec<PathBuf>,
}

impl OptCacheDelete {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Part;
        use indicatif::ProgressBar;

        let pb = ProgressBar::new_spinner().with_message("removing cache entries");

        for file in pb.wrap_iter(
            self.paths
                .into_iter()
                .flat_map(unique_sub_files)
                .filter(|pb| matches!(Part::has_xattr(pb), Ok(true))),
        ) {
            Part::remove_xattr(&file)?;
        }

        pb.finish_and_clear();

        Ok(())
    }
}

#[derive(Args)]
struct OptCacheVerify {
    /// files or directories
    #[clap(parse(from_os_str))]
    paths: Vec<PathBuf>,
}

impl OptCacheVerify {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Part;
        use indicatif::{ParallelProgressIterator, ProgressBar};
        use rayon::prelude::*;
        use std::collections::HashMap;

        let pb = ProgressBar::new_spinner().with_message("locating files");
        let files = {
            pb.wrap_iter(self.paths.into_iter().flat_map(unique_sub_files))
                .collect::<Vec<PathBuf>>()
        };
        pb.finish_and_clear();

        let pb = ProgressBar::new(files.len() as u64)
            .with_style(crate::game::verify_style())
            .with_message("reading cache entries");

        let cache = files
            .into_par_iter()
            .progress_with(pb.clone())
            .filter_map(|file| Part::get_xattr(&file).map(|part| (file, part)))
            .collect::<HashMap<PathBuf, Part>>();

        pb.finish_and_clear();

        let pb = ProgressBar::new(cache.len() as u64)
            .with_style(crate::game::verify_style())
            .with_message("verifying cache entries");

        cache
            .par_iter()
            .progress_with(pb.clone())
            .for_each(|(file, part)| match part.is_valid(&file) {
                Ok(true) => { /* do nothing*/ }
                Ok(false) => pb.println(format!("BAD : {}", file.display())),
                Err(err) => pb.println(format!("ERROR : {} : {}", file.display(), err)),
            });

        pb.finish_and_clear();

        Ok(())
    }
}

#[derive(Args)]
struct OptCacheLinkDupes {
    /// files or directories
    #[clap(parse(from_os_str))]
    paths: Vec<PathBuf>,
}

impl OptCacheLinkDupes {
    fn execute(self) -> Result<(), Error> {
        use crate::duplicates::{DuplicateFiles, Duplicates};
        use indicatif::ProgressBar;

        let mut db = DuplicateFiles::default();

        let pb = ProgressBar::new_spinner()
            .with_style(crate::game::find_files_style())
            .with_message("linking duplicate files");

        for file in pb.wrap_iter(self.paths.into_iter().flat_map(sub_files)) {
            use std::fs;

            match db.get_or_add(file) {
                Ok(None) => {}
                Ok(Some((duplicate, original))) => {
                    match fs::remove_file(&duplicate)
                        .and_then(|()| fs::hard_link(&original, &duplicate))
                    {
                        Ok(()) => pb.println(format!(
                            "{} \u{2192} {}",
                            original.display(),
                            duplicate.display()
                        )),
                        Err(err) => pb.println(format!("{}: {}", duplicate.display(), err)),
                    }
                }
                Err((source, err)) => pb.println(format!("{}: {}", source.display(), err)),
            }
        }

        pb.finish_and_clear();

        Ok(())
    }
}

/// Emulation Database Manager
#[derive(Parser)]
enum Opt {
    /// arcade software management
    #[clap(subcommand)]
    Mame(OptMame),

    /// console and portable software management
    #[clap(subcommand)]
    Sl(OptMess),

    /// extra files management, like snapshots
    #[clap(subcommand)]
    Extra(OptExtra),

    /// disc image software management
    #[clap(subcommand)]
    Redump(OptRedump),

    /// nointro game management
    #[clap(subcommand)]
    Nointro(OptNointro),

    /// identify ROM or CHD by hash
    Identify(OptIdentify),

    /// file cache management
    #[clap(subcommand)]
    Cache(OptCache),
}

impl Opt {
    fn execute(self) -> Result<(), Error> {
        match self {
            Opt::Mame(o) => o.execute(),
            Opt::Sl(o) => o.execute(),
            Opt::Extra(o) => o.execute(),
            Opt::Redump(o) => o.execute(),
            Opt::Nointro(o) => o.execute(),
            Opt::Identify(o) => o.execute(),
            Opt::Cache(o) => o.execute(),
        }
    }
}

fn main() {
    if let Err(err) = Opt::parse().execute() {
        eprintln!("* {}", err);
    }
}

fn is_zip<R>(mut reader: R) -> Result<bool, std::io::Error>
where
    R: Read + Seek,
{
    use std::io::SeekFrom;

    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    reader.seek(SeekFrom::Start(0))?;
    Ok(&buf == b"\x50\x4b\x03\x04")
}

fn open_db<P: AsRef<Path>>(db: P) -> Result<Connection, Error> {
    let db = Connection::open(db)?;
    db.pragma_update(None, "foreign_keys", &"ON")?;
    Ok(db)
}

fn write_cache<S>(db_file: &str, cache: S) -> Result<(), Error>
where
    S: Serialize,
{
    use directories::ProjectDirs;
    use std::fs::create_dir_all;
    use std::io::BufWriter;

    let dirs = ProjectDirs::from("", "", "EmuMan").expect("no valid home directory found");
    let dir = dirs.data_local_dir();
    create_dir_all(dir)?;
    let path = dir.join(db_file);
    let f = BufWriter::new(File::create(&path)?);
    ciborium::ser::into_writer(&cache, f).map_err(Error::CborWrite)?;
    println!("* Wrote \"{}\"", path.display());
    Ok(())
}

fn read_cache<D>(utility: &'static str, db_file: &str) -> Result<D, Error>
where
    D: DeserializeOwned,
{
    use directories::ProjectDirs;
    use std::io::BufReader;

    let dirs = ProjectDirs::from("", "", "EmuMan").expect("no valid home directory");
    let f = BufReader::new(
        File::open(dirs.data_local_dir().join(db_file))
            .map_err(|_| Error::MissingCache(utility))?,
    );
    ciborium::de::from_reader(f).map_err(|_| Error::InvalidCache(utility))
}

fn verify<P: AsRef<Path>>(
    db: &game::GameDb,
    root: P,
    games: &HashSet<String>,
    only_failures: bool,
) {
    let results = db.verify(root.as_ref(), games);

    let successes = results.iter().filter(|(_, v)| v.is_empty()).count();

    let display = if only_failures {
        game::display_bad_results
    } else {
        game::display_all_results
    };

    for (game, failures) in results.iter() {
        display(game, failures);
    }

    eprintln!("{} tested, {} OK", games.len(), successes);
}

fn verify_all(
    software_list: &str,
    db: &game::GameDb,
    root: &Path,
    games: &HashSet<String>,
    only_failures: bool,
) {
    let results = db.verify(root, games);

    let successes = results.iter().filter(|(_, v)| v.is_empty()).count();

    let display = if only_failures {
        game::display_bad_results
    } else {
        game::display_all_results
    };

    for (game, failures) in results.iter() {
        display(&format!("{software_list}/{game}"), failures);
    }

    eprintln!("{} tested, {} OK", games.len(), successes);
}

fn add_and_verify_games<'g, I, F, P>(
    mut display: F,
    roms: &mut game::RomSources,
    root: P,
    games: I,
) -> Result<(), Error>
where
    P: AsRef<Path>,
    F: FnMut(&str, &[game::VerifyFailure]),
    I: Iterator<Item = &'g game::Game>,
{
    use indicatif::{ProgressBar, ProgressStyle};

    let pb = match games.size_hint() {
        (_, Some(total)) => ProgressBar::new(total as u64)
            .with_style(ProgressStyle::default_bar().template("{wide_msg} {pos} / {len}")),
        (_, None) => ProgressBar::new_spinner(),
    }
    .with_message("adding and verifying");

    let results = pb
        .wrap_iter(games.map(|game| {
            game.add_and_verify(roms, root.as_ref(), |p| pb.println(p.to_string()))
                .map(|failures| (game.name.as_str(), failures))
        }))
        .collect::<Result<BTreeMap<_, _>, Error>>()?;

    pb.finish_and_clear();

    let successes = results.values().filter(|v| v.is_empty()).count();

    for (game, failures) in results.iter() {
        display(game, failures);
    }

    eprintln!("{} added, {} OK", results.len(), successes);

    Ok(())
}

#[inline]
fn add_and_verify<'g, I, P>(roms: &mut game::RomSources, root: P, games: I) -> Result<(), Error>
where
    P: AsRef<Path>,
    I: Iterator<Item = &'g game::Game>,
{
    add_and_verify_games(game::display_bad_results, roms, root, games)
}

#[inline]
fn add_and_verify_all<'g, I, P>(
    software_list: &str,
    roms: &mut game::RomSources,
    root: P,
    games: I,
) -> Result<(), Error>
where
    P: AsRef<Path>,
    I: Iterator<Item = &'g game::Game>,
{
    add_and_verify_games(
        |game, failures| game::display_bad_results(&format!("{software_list}/{game}"), failures),
        roms,
        root,
        games,
    )
}

fn display_dirs<D>(dirs: D, db: BTreeMap<String, dat::DatFile>, sort_by_version: bool)
where
    D: Iterator<Item = (String, PathBuf)>,
{
    use prettytable::{cell, format, row, Table};

    let mut results: Vec<[String; 3]> = dirs
        .filter_map(|(name, dir)| {
            db.get(&name).map(|dat| {
                [
                    dat.version().to_owned(),
                    dat.name().to_owned(),
                    dir.to_string_lossy().to_string(),
                ]
            })
        })
        .collect();

    if sort_by_version {
        results.sort_unstable_by(|x, y| x[0].cmp(&y[0]));
    }

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.get_format().column_separator('\u{2502}');
    for [version, name, dir] in results {
        table.add_row(row![r->version, name, dir]);
    }
    table.printstd();
}

fn init_dat_table() -> prettytable::Table {
    use prettytable::{cell, format, row, Table};

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.get_format().column_separator('\u{2502}');
    table.set_titles(row![r->"Tested", r->"OK", ""]);
    table
}

fn display_dat_table(mut table: prettytable::Table, summary: Option<game::VerifyResultsSummary>) {
    if let Some(summary) = summary {
        use prettytable::{cell, row};

        table.add_row(row![r->summary.total, r->summary.successes, "Total"]);
    }
    table.printstd();
}

fn sub_files(root: PathBuf) -> Box<dyn Iterator<Item = PathBuf>> {
    if root.is_file() {
        Box::new(std::iter::once(root))
    } else if root.is_dir() {
        Box::new(
            walkdir::WalkDir::new(root)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
                .map(|e| e.into_path()),
        )
    } else {
        Box::new(std::iter::empty())
    }
}

struct UniqueSubFiles<I> {
    iter: I,
    seen: HashSet<crate::game::FileId>,
}

impl<I: Iterator<Item = PathBuf>> Iterator for UniqueSubFiles<I> {
    type Item = PathBuf;

    fn next(&mut self) -> Option<PathBuf> {
        loop {
            let next = self.iter.next()?;
            if let Ok(file_id) = crate::game::FileId::new(&next) {
                if self.seen.insert(file_id) {
                    break Some(next);
                }
            }
        }
    }
}

#[inline]
fn unique_sub_files(root: PathBuf) -> impl Iterator<Item = PathBuf> {
    UniqueSubFiles {
        iter: sub_files(root),
        seen: HashSet::default(),
    }
}
