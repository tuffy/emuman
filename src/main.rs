use roxmltree::Document;
use rusqlite::Connection;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use structopt::StructOpt;

mod extra;
mod game;
mod mame;
mod mess;
mod redump;
mod split;

static MAME: &str = "mame";
static MESS: &str = "mess";
static EXTRA: &str = "extra";
static REDUMP: &str = "redump";

static CACHE_MAME: &str = "mame.cbor";
static CACHE_MESS: &str = "mess.cbor";
static CACHE_EXTRA: &str = "extra.cbor";
static CACHE_REDUMP: &str = "redump.cbor";
static CACHE_MESS_SPLIT: &str = "mess-split.cbor";
static CACHE_REDUMP_SPLIT: &str = "redump-split.cbor";

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    XML(roxmltree::Error),
    SQL(rusqlite::Error),
    CBOR(serde_cbor::Error),
    Zip(zip::result::ZipError),
    NoSuchSoftwareList(String),
    NoSuchSoftware(String),
    MissingCache(&'static str),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err)
    }
}

impl From<roxmltree::Error> for Error {
    fn from(err: roxmltree::Error) -> Self {
        Error::XML(err)
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Error::SQL(err)
    }
}

impl From<serde_cbor::Error> for Error {
    fn from(err: serde_cbor::Error) -> Self {
        Error::CBOR(err)
    }
}

impl From<zip::result::ZipError> for Error {
    fn from(err: zip::result::ZipError) -> Self {
        Error::Zip(err)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IO(err) => Some(err),
            Error::XML(err) => Some(err),
            Error::SQL(err) => Some(err),
            Error::CBOR(err) => Some(err),
            Error::Zip(err) => Some(err),
            Error::NoSuchSoftwareList(_) | Error::NoSuchSoftware(_) | Error::MissingCache(_) => {
                None
            }
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IO(err) => err.fmt(f),
            Error::XML(err) => err.fmt(f),
            Error::SQL(err) => err.fmt(f),
            Error::CBOR(err) => err.fmt(f),
            Error::Zip(err) => err.fmt(f),
            Error::NoSuchSoftwareList(s) => write!(f, "no such software list \"{}\"", s),
            Error::NoSuchSoftware(s) => write!(f, "no such software \"{}\"", s),
            Error::MissingCache(s) => write!(
                f,
                "missing cache files, please run \"emuman {} create\" to populate",
                s
            ),
        }
    }
}

#[derive(StructOpt)]
struct OptMameCreate {
    /// SQLite database
    #[structopt(long = "db", parse(from_os_str))]
    database: Option<PathBuf>,

    /// MAME's XML file
    #[structopt(parse(from_os_str))]
    xml: Option<PathBuf>,
}

impl OptMameCreate {
    fn execute(self) -> Result<(), Error> {
        let xml_data = if let Some(path) = self.xml {
            read_raw_or_zip(path)?
        } else {
            use std::io::stdin;

            let mut xml_data = String::new();
            stdin().read_to_string(&mut xml_data)?;
            xml_data
        };

        let xml = Document::parse(&xml_data)?;

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

#[derive(StructOpt)]
struct OptMameList {
    /// sorting order, use "description", "year" or "creator"
    #[structopt(short = "s", long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// display simple list with less information
    #[structopt(short = "S", long = "simple")]
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

#[derive(StructOpt)]
struct OptMameGames {
    /// display simple list with less information
    #[structopt(short = "S", long = "simple")]
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

#[derive(StructOpt)]
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

#[derive(StructOpt)]
struct OptMameReport {
    /// sorting order, use "description", "year" or "creator"
    #[structopt(short = "s", long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// ROMs directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// display simple report with less information
    #[structopt(short = "S", long = "simple")]
    simple: bool,

    /// search term for querying specific machines
    search: Option<String>,
}

impl OptMameReport {
    fn execute(self) -> Result<(), Error> {
        let machines: HashSet<String> = self
            .roms
            .read_dir()?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect();

        let db = read_cache::<game::GameDb>(MAME, CACHE_MAME)?;
        db.report(&machines, self.search.as_deref(), self.sort, self.simple);
        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMameVerify {
    /// ROMs directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// verify all possible machines
    #[structopt(long = "all")]
    all: bool,

    /// verify only working machines
    #[structopt(long = "working")]
    working: bool,

    /// display only failures
    #[structopt(long = "failures")]
    failures: bool,

    /// machine to verify
    machines: Vec<String>,
}

impl OptMameVerify {
    fn execute(self) -> Result<(), Error> {
        let mut db: game::GameDb = read_cache(MAME, CACHE_MAME)?;

        if self.working {
            db.retain_working();
        }

        let games: HashSet<String> = if self.all {
            db.all_games()
        } else if !self.machines.is_empty() {
            // only validate user-specified machines
            let machines = self.machines.iter().cloned().collect();
            db.validate_games(&machines)?;
            machines
        } else {
            // ignore stuff that's on disk but not valid machines
            self.roms
                .read_dir()?
                .filter_map(|e| {
                    e.ok()
                        .and_then(|e| e.file_name().into_string().ok())
                        .filter(|s| db.is_game(s))
                })
                .collect()
        };

        verify(&db, &self.roms, &games, self.failures);

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMameAdd {
    /// input directory
    #[structopt(short = "i", long = "input", parse(from_os_str), default_value = ".")]
    input: PathBuf,

    /// output directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// don't actually add ROMs
    #[structopt(long = "dry-run")]
    dry_run: bool,

    /// machine to add
    machines: Vec<String>,
}

impl OptMameAdd {
    fn execute(mut self) -> Result<(), Error> {
        let db: game::GameDb = read_cache(MAME, CACHE_MAME)?;

        let mut roms = if self.machines.is_empty() {
            self.machines = db.all_games();
            game::all_rom_sources(&self.input)
        } else {
            game::get_rom_sources(&self.input, db.required_parts(&self.machines)?)
        };

        let copy = if self.dry_run {
            game::extract_dry_run
        } else {
            game::extract
        };

        self.machines
            .iter()
            .try_for_each(|game| db.games[game].add(&mut roms, &self.roms, copy))
    }
}

#[derive(StructOpt)]
struct OptMameRename {
    /// output directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// don't actually rename ROMs
    #[structopt(long = "dry-run")]
    dry_run: bool,

    /// machine to rename
    machines: Vec<String>,
}

impl OptMameRename {
    fn execute(mut self) -> Result<(), Error> {
        let db: game::GameDb = read_cache(MAME, CACHE_MAME)?;

        if self.machines.is_empty() {
            self.machines = db.all_games();
        } else {
            db.validate_games(&self.machines)?;
        }

        let file_move = if self.dry_run {
            game::file_move_dry_run
        } else {
            game::file_move
        };

        self.machines
            .iter()
            .try_for_each(|game| db.games[game].rename(&self.roms, file_move))
    }
}

#[derive(StructOpt)]
#[structopt(name = "mame")]
enum OptMame {
    /// create internal database
    #[structopt(name = "create")]
    Create(OptMameCreate),

    /// list all machines
    #[structopt(name = "list")]
    List(OptMameList),

    /// list a machine's ROMs
    #[structopt(name = "parts")]
    Parts(OptMameParts),

    /// list given games, in order
    #[structopt(name = "games")]
    Games(OptMameGames),

    /// generate report of sets in collection
    #[structopt(name = "report")]
    Report(OptMameReport),

    /// verify ROMs in directory
    #[structopt(name = "verify")]
    Verify(OptMameVerify),

    /// add ROMs to directory
    #[structopt(name = "add")]
    Add(OptMameAdd),

    /// rename ROMs in game directories, if necessary
    #[structopt(name = "rename")]
    Rename(OptMameRename),
}

impl OptMame {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptMame::Create(o) => o.execute(),
            OptMame::List(o) => o.execute(),
            OptMame::Parts(o) => o.execute(),
            OptMame::Games(o) => o.execute(),
            OptMame::Report(o) => o.execute(),
            OptMame::Verify(o) => o.execute(),
            OptMame::Add(o) => o.execute(),
            OptMame::Rename(o) => o.execute(),
        }
    }
}

#[derive(StructOpt)]
struct OptMessCreate {
    /// SQLite database
    #[structopt(long = "db", parse(from_os_str))]
    database: Option<PathBuf>,

    /// XML files from hash database
    #[structopt(parse(from_os_str))]
    xml: Vec<PathBuf>,
}

impl OptMessCreate {
    fn execute(self) -> Result<(), Error> {
        use mess::MessDb;

        if let Some(db_file) = self.database {
            let mut db = open_db(&db_file)?;
            let trans = db.transaction()?;

            mess::create_tables(&trans)?;
            mess::clear_tables(&trans)?;
            self.xml
                .iter()
                .try_for_each(|p| mess::add_file(&p, &trans))?;
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

#[derive(StructOpt)]
struct OptMessList {
    /// software list to use
    software_list: Option<String>,

    /// sorting order, use "description", "year" or "publisher"
    #[structopt(short = "s", long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// display simple list with less information
    #[structopt(short = "S", long = "simple")]
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

#[derive(StructOpt)]
struct OptMessGames {
    /// display simple list with less information
    #[structopt(short = "S", long = "simple")]
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

#[derive(StructOpt)]
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

#[derive(StructOpt)]
struct OptMessReport {
    /// sorting order, use "description", "year" or "creator"
    #[structopt(short = "s", long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// ROMs directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// software list to use
    software_list: String,

    /// display simple report with less information
    #[structopt(short = "S", long = "simple")]
    simple: bool,

    /// search term for querying specific software
    search: Option<String>,
}

impl OptMessReport {
    fn execute(self) -> Result<(), Error> {
        use crate::game::GameRow;

        let mut mess_db: mess::MessDb = read_cache(MESS, CACHE_MESS)?;

        if self.software_list == "any" {
            let mut results: Vec<(&str, GameRow)> = Vec::new();
            for (name, db) in mess_db.iter() {
                let roms = self.roms.join(name);
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
            results.sort_by(|(_, a), (_, b)| a.compare(b, self.sort));
            mess::display_results(&results);
        } else {
            let db = mess_db
                .remove(&self.software_list)
                .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

            let software: HashSet<String> = self
                .roms
                .read_dir()?
                .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
                .collect();

            db.report(&software, self.search.as_deref(), self.sort, self.simple);
        }

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMessVerify {
    /// ROMs directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// verify all possible machines
    #[structopt(long = "all")]
    all: bool,

    /// verify only working machines
    #[structopt(long = "working")]
    working: bool,

    /// display only failures
    #[structopt(long = "failures")]
    failures: bool,

    /// software list to use
    software_list: String,

    /// machine to verify
    software: Vec<String>,
}

impl OptMessVerify {
    fn execute(self) -> Result<(), Error> {
        let mut db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?
            .remove(&self.software_list)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

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
            self.roms
                .read_dir()?
                .filter_map(|e| {
                    e.ok()
                        .and_then(|e| e.file_name().into_string().ok())
                        .filter(|s| db.is_game(s))
                })
                .collect()
        };

        verify(&db, &self.roms, &software, self.failures);

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMessVerifyAll {
    /// ROMs directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// verify all possible machines
    #[structopt(long = "all")]
    all: bool,

    /// verify only working machines
    #[structopt(long = "working")]
    working: bool,

    /// display only failures
    #[structopt(long = "failures")]
    failures: bool,
}

impl OptMessVerifyAll {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?;

        for (software_list, mut db) in db.into_iter() {
            let roms_path = self.roms.join(software_list);

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

            verify(&db, &roms_path, &software, self.failures);
        }

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMessAdd {
    /// input directory
    #[structopt(short = "i", long = "input", parse(from_os_str), default_value = ".")]
    input: PathBuf,

    /// output directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// don't actually add ROMs
    #[structopt(long = "dry-run")]
    dry_run: bool,

    /// software list to use
    software_list: String,

    /// software to add
    software: Vec<String>,
}

impl OptMessAdd {
    fn execute(mut self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?
            .remove(&self.software_list)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

        let mut roms = if self.software.is_empty() {
            self.software = db.all_games();
            game::all_rom_sources(&self.input)
        } else {
            game::get_rom_sources(&self.input, db.required_parts(&self.software)?)
        };

        let copy = if self.dry_run {
            game::extract_dry_run
        } else {
            game::extract
        };

        self.software
            .iter()
            .try_for_each(|game| db.games[game].add(&mut roms, &self.roms, copy))
    }
}

#[derive(StructOpt)]
struct OptMessAddAll {
    /// input directory
    #[structopt(short = "i", long = "input", parse(from_os_str), default_value = ".")]
    input: PathBuf,

    /// output directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// don't actually add ROMs
    #[structopt(long = "dry-run")]
    dry_run: bool,
}

impl OptMessAddAll {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?;

        let mut roms = game::all_rom_sources(&self.input);

        let copy = if self.dry_run {
            game::extract_dry_run
        } else {
            game::extract
        };

        db.into_iter().try_for_each(|(software, db)| {
            let roms_path = self.roms.join(software);
            db.games_iter()
                .try_for_each(|game| game.add(&mut roms, &roms_path, copy))
        })
    }
}

#[derive(StructOpt)]
struct OptMessRename {
    /// output directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    roms: PathBuf,

    /// don't actually rename ROMs
    #[structopt(long = "dry-run")]
    dry_run: bool,

    /// software list to use
    software_list: String,

    /// software to rename
    software: Vec<String>,
}

impl OptMessRename {
    fn execute(mut self) -> Result<(), Error> {
        let db = read_cache::<mess::MessDb>(MESS, CACHE_MESS)?
            .remove(&self.software_list)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

        if self.software.is_empty() {
            self.software = db.all_games();
        }

        let file_move = if self.dry_run {
            game::file_move_dry_run
        } else {
            game::file_move
        };

        self.software
            .iter()
            .try_for_each(|game| db.games[game].rename(&self.roms, file_move))
    }
}

#[derive(StructOpt)]
struct OptMessSplit {
    /// target directory for split ROMs
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    output: PathBuf,

    /// ROMs to split
    #[structopt(parse(from_os_str))]
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
                    .find(|m| m.matches(&data))
                {
                    exact_match.extract(&self.output, &data)?;
                }
            }

            Ok(())
        })
    }
}

#[derive(StructOpt)]
#[structopt(name = "mess")]
enum OptMess {
    /// create internal database
    #[structopt(name = "create")]
    Create(OptMessCreate),

    /// list all software in software list
    #[structopt(name = "list")]
    List(OptMessList),

    /// list given games, in order
    #[structopt(name = "games")]
    Games(OptMessGames),

    /// list a machine's ROMs
    #[structopt(name = "parts")]
    Parts(OptMessParts),

    /// generate report of sets in collection
    #[structopt(name = "report")]
    Report(OptMessReport),

    /// verify ROMs in directory
    #[structopt(name = "verify")]
    Verify(OptMessVerify),

    /// verify all ROMs in all software lists in directory
    #[structopt(name = "verify-all")]
    VerifyAll(OptMessVerifyAll),

    /// add ROMs to directory
    #[structopt(name = "add")]
    Add(OptMessAdd),

    /// add all ROMs from all software lists to directory
    #[structopt(name = "add-all")]
    AddAll(OptMessAddAll),

    /// rename ROMs in directory, if necessary
    #[structopt(name = "rename")]
    Rename(OptMessRename),

    /// split ROM into MESS-compatible parts, if necessary
    #[structopt(name = "split")]
    Split(OptMessSplit),
}

impl OptMess {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptMess::Create(o) => o.execute(),
            OptMess::List(o) => o.execute(),
            OptMess::Games(o) => o.execute(),
            OptMess::Parts(o) => o.execute(),
            OptMess::Report(o) => o.execute(),
            OptMess::Verify(o) => o.execute(),
            OptMess::VerifyAll(o) => o.execute(),
            OptMess::Add(o) => o.execute(),
            OptMess::AddAll(o) => o.execute(),
            OptMess::Rename(o) => o.execute(),
            OptMess::Split(o) => o.execute(),
        }
    }
}

#[derive(StructOpt)]
struct OptExtraCreate {
    /// extras .DAT file files
    #[structopt(parse(from_os_str))]
    dats: Vec<PathBuf>,

    /// append new .DAT file to existing set
    #[structopt(short = "a", long = "append")]
    append: bool,
}

impl OptExtraCreate {
    fn execute(self) -> Result<(), Error> {
        let new_games = self
            .dats
            .into_iter()
            .map(|dat| read_dat_or_zip(dat))
            .collect::<Result<Vec<Vec<(String, game::GameDb)>>, Error>>()?;

        let mut db = if self.append {
            read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA)?
        } else {
            extra::ExtraDb::default()
        };

        for (name, game_db) in new_games.into_iter().flatten() {
            db.insert(name, game_db);
        }

        write_cache(CACHE_EXTRA, db)
    }
}

#[derive(StructOpt)]
struct OptExtraList {}

impl OptExtraList {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA)?;
        mess::list_all(&db);
        Ok(())
    }
}

#[derive(StructOpt)]
struct OptExtraParts {
    /// sections's parts to search for
    section: String,

    /// section's name to search for
    name: String,
}

impl OptExtraParts {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA).and_then(|mut db| {
            db.remove(&self.name)
                .ok_or_else(|| Error::NoSuchSoftwareList(self.name.clone()))
        })?;

        db.display_parts(&self.section)
    }
}

#[derive(StructOpt)]
struct OptExtraVerify {
    /// extras directory
    #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
    dir: PathBuf,

    /// extras category to verify
    extra: String,

    /// machine to verify
    software: Vec<String>,

    /// display only failures
    #[structopt(long = "failures")]
    failures: bool,
}

impl OptExtraVerify {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA)?
            .remove(&self.extra)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.extra.clone()))?;

        let software: HashSet<String> = if !self.software.is_empty() {
            let software = self.software.iter().cloned().collect();
            db.validate_games(&software)?;
            software
        } else {
            self.dir
                .read_dir()?
                .filter_map(|e| {
                    e.ok()
                        .and_then(|e| e.file_name().into_string().ok())
                        .filter(|s| db.is_game(s))
                })
                .collect()
        };

        verify(&db, &self.dir, &software, self.failures);

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptExtraVerifyAll {
    /// extras directory
    #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
    dir: PathBuf,

    /// verify all possible machines
    #[structopt(long = "all")]
    all: bool,

    /// display only failures
    #[structopt(long = "failures")]
    failures: bool,
}

impl OptExtraVerifyAll {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA)?;

        for (extra_list, db) in db.into_iter() {
            let extras_path = self.dir.join(extra_list);

            let software: HashSet<String> = if self.all {
                db.all_games()
            } else {
                extras_path
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

            verify(&db, &extras_path, &software, self.failures);
        }

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptExtraAdd {
    /// input directory
    #[structopt(short = "i", long = "input", parse(from_os_str), default_value = ".")]
    input: PathBuf,

    /// output directory
    #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
    dir: PathBuf,

    /// don't actually add files
    #[structopt(long = "dry-run")]
    dry_run: bool,

    /// extra to add
    extra: String,

    /// machine to add
    software: Vec<String>,
}

impl OptExtraAdd {
    fn execute(mut self) -> Result<(), Error> {
        let db = read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA)?
            .remove(&self.extra)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.extra.clone()))?;

        let mut parts = if self.software.is_empty() {
            self.software = db.all_games();
            game::all_rom_sources(&self.input)
        } else {
            game::get_rom_sources(&self.input, db.required_parts(&self.software)?)
        };

        let copy = if self.dry_run {
            game::extract_dry_run
        } else {
            game::extract
        };

        self.software
            .iter()
            .try_for_each(|game| db.games[game].add(&mut parts, &self.dir, copy))
    }
}

#[derive(StructOpt)]
struct OptExtraAddAll {
    /// input directory
    #[structopt(short = "i", long = "input", parse(from_os_str), default_value = ".")]
    input: PathBuf,

    /// output directory
    #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
    dir: PathBuf,

    /// don't actually add files
    #[structopt(long = "dry-run")]
    dry_run: bool,
}

impl OptExtraAddAll {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA)?;

        let mut parts = game::all_rom_sources(&self.input);

        let copy = if self.dry_run {
            game::extract_dry_run
        } else {
            game::extract
        };

        db.into_iter().try_for_each(|(extra, db)| {
            let extras_path = self.dir.join(extra);
            db.games_iter()
                .try_for_each(|game| game.add(&mut parts, &extras_path, copy))
        })
    }
}

#[derive(StructOpt)]
#[structopt(name = "extra")]
enum OptExtra {
    /// create internal database
    #[structopt(name = "create")]
    Create(OptExtraCreate),

    /// list all extras categories
    #[structopt(name = "list")]
    List(OptExtraList),

    /// list an extra's parts
    #[structopt(name = "parts")]
    Parts(OptExtraParts),

    /// verify parts in directory
    #[structopt(name = "verify")]
    Verify(OptExtraVerify),

    /// add files to directory
    #[structopt(name = "add")]
    Add(OptExtraAdd),

    /// add all files to directory
    #[structopt(name = "add-all")]
    AddAll(OptExtraAddAll),

    /// verify all files in directory
    #[structopt(name = "verify-all")]
    VerifyAll(OptExtraVerifyAll),
}

impl OptExtra {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptExtra::Create(o) => o.execute(),
            OptExtra::List(o) => o.execute(),
            OptExtra::Parts(o) => o.execute(),
            OptExtra::Verify(o) => o.execute(),
            OptExtra::Add(o) => o.execute(),
            OptExtra::AddAll(o) => o.execute(),
            OptExtra::VerifyAll(o) => o.execute(),
        }
    }
}

#[derive(StructOpt)]
struct OptRedumpCreate {
    /// SQLite database
    #[structopt(long = "db", parse(from_os_str))]
    database: Option<PathBuf>,

    /// redump XML file
    #[structopt(parse(from_os_str))]
    xml: Vec<PathBuf>,
}

impl OptRedumpCreate {
    fn execute(self) -> Result<(), Error> {
        if let Some(db_file) = self.database {
            let mut db = open_db(&db_file)?;
            let trans = db.transaction()?;

            redump::create_tables(&trans)?;
            redump::clear_tables(&trans)?;
            self.xml
                .iter()
                .try_for_each(|p| redump::add_file(&p, &trans))?;
            trans.commit()?;

            println!("* Wrote \"{}\"", db_file.display());
        } else {
            let mut redump_db = redump::RedumpDb::default();
            let mut split_db = split::SplitDb::default();

            for file in self.xml.iter() {
                let xml_data = read_raw_or_zip(file)?;
                let tree = Document::parse(&xml_data)?;
                let (name, game_db) = redump::add_xml_file(&mut split_db, &tree);
                redump_db.insert(name, game_db);
            }

            write_cache(CACHE_REDUMP, &redump_db)?;
            write_cache(CACHE_REDUMP_SPLIT, &split_db)?;
        }

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptRedumpList {
    /// software list to use
    software_list: Option<String>,

    /// search term for querying specific items
    search: Option<String>,
}

impl OptRedumpList {
    fn execute(self) -> Result<(), Error> {
        let mut db: redump::RedumpDb = read_cache(REDUMP, CACHE_REDUMP)?;

        if let Some(software_list) = self.software_list {
            let db = db
                .remove(&software_list)
                .ok_or_else(|| Error::NoSuchSoftwareList(software_list))?;
            redump::list(&db, self.search.as_deref())
        } else {
            redump::list_all(&db)
        }

        Ok(())
    }
}

#[derive(StructOpt)]
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

#[derive(StructOpt)]
struct OptRedumpVerify {
    /// root directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    root: PathBuf,

    /// verify all possible software
    #[structopt(long = "all")]
    all: bool,

    /// display only failures
    #[structopt(long = "failures")]
    failures: bool,

    /// software list to use
    software_list: String,

    /// software to verify
    software: Vec<String>,
}

impl OptRedumpVerify {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache::<redump::RedumpDb>(MESS, CACHE_REDUMP)?
            .remove(&self.software_list)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

        let games: HashSet<String> = if self.all {
            db.all_games()
        } else if !self.software.is_empty() {
            // only validate user-specified games
            let games = self.software.clone().into_iter().collect();
            db.validate_games(&games)?;
            games
        } else {
            // ignore stuff that's on disk but not valid games
            self.root
                .read_dir()?
                .filter_map(|e| {
                    e.ok()
                        .and_then(|e| e.file_name().into_string().ok())
                        .filter(|s| db.is_game(s))
                })
                .collect()
        };

        verify(&db, &self.root, &games, self.failures);

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptRedumpAdd {
    /// input directory
    #[structopt(short = "i", long = "input", parse(from_os_str), default_value = ".")]
    input: PathBuf,

    /// output directory
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    output: PathBuf,

    /// don't actually add tracks
    #[structopt(long = "dry-run")]
    dry_run: bool,

    /// software list to use
    software_list: String,

    /// software to add
    software: Vec<String>,
}

impl OptRedumpAdd {
    fn execute(mut self) -> Result<(), Error> {
        let db = read_cache::<redump::RedumpDb>(MESS, CACHE_REDUMP)?
            .remove(&self.software_list)
            .ok_or_else(|| Error::NoSuchSoftwareList(self.software_list.clone()))?;

        let mut roms = if self.software.is_empty() {
            self.software = db.all_games();
            game::all_rom_sources(&self.input)
        } else {
            game::get_rom_sources(&self.input, db.required_parts(&self.software)?)
        };

        let copy = if self.dry_run {
            game::extract_dry_run
        } else {
            game::extract
        };

        self.software
            .iter()
            .try_for_each(|game| db.games[game].add(&mut roms, &self.output, copy))
    }
}

#[derive(StructOpt)]
struct OptRedumpSplit {
    /// directory to place output tracks
    #[structopt(short = "r", long = "roms", parse(from_os_str), default_value = ".")]
    root: PathBuf,

    /// input .bin file
    #[structopt(parse(from_os_str))]
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

#[derive(StructOpt)]
#[structopt(name = "redump")]
enum OptRedump {
    /// create internal database
    #[structopt(name = "create")]
    Create(OptRedumpCreate),

    /// list all software in software list
    #[structopt(name = "list")]
    List(OptRedumpList),

    /// list all tracks for a given game
    #[structopt(name = "parts")]
    Parts(OptRedumpParts),

    /// verify files against Redump database
    #[structopt(name = "verify")]
    Verify(OptRedumpVerify),

    /// add tracks to directory
    #[structopt(name = "add")]
    Add(OptRedumpAdd),

    /// split .bin file into multiple tracks
    #[structopt(name = "split")]
    Split(OptRedumpSplit),
}

impl OptRedump {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptRedump::Create(o) => o.execute(),
            OptRedump::List(o) => o.execute(),
            OptRedump::Parts(o) => o.execute(),
            OptRedump::Verify(o) => o.execute(),
            OptRedump::Add(o) => o.execute(),
            OptRedump::Split(o) => o.execute(),
        }
    }
}

#[derive(StructOpt)]
struct OptIdentify {
    /// ROMs or CHDs to identify
    parts: Vec<PathBuf>,

    /// perform reverse lookup
    #[structopt(short = "l", long = "lookup")]
    lookup: bool,
}

impl OptIdentify {
    fn execute(self) -> Result<(), Error> {
        use crate::game::{Part, RomSource};
        use prettytable::Table;
        use std::borrow::Cow;
        use std::collections::{BTreeSet, HashMap};

        if self.lookup {
            let mut lookup: HashMap<Part, BTreeSet<[Cow<'static, str>; 4]>> = HashMap::default();

            for (_, game) in read_cache::<game::GameDb>(MAME, CACHE_MAME)
                .unwrap_or_default()
                .games
            {
                for (name, part) in game.parts {
                    lookup.entry(part).or_default().insert([
                        "mame".into(),
                        "".into(),
                        game.name.clone().into(),
                        name.clone().into(),
                    ]);
                }
            }

            for (system_name, system) in
                read_cache::<mess::MessDb>(MESS, CACHE_MESS).unwrap_or_default()
            {
                for (_, game) in system.games {
                    for (name, part) in game.parts {
                        lookup.entry(part).or_default().insert([
                            "mess".into(),
                            system_name.clone().into(),
                            game.name.clone().into(),
                            name.clone().into(),
                        ]);
                    }
                }
            }

            for (system_name, system) in
                read_cache::<extra::ExtraDb>(EXTRA, CACHE_EXTRA).unwrap_or_default()
            {
                for (_, game) in system.games {
                    for (name, part) in game.parts {
                        lookup.entry(part).or_default().insert([
                            "extra".into(),
                            system_name.clone().into(),
                            game.name.clone().into(),
                            name.clone().into(),
                        ]);
                    }
                }
            }

            for path in self.parts.into_iter() {
                println!("{}:", path.display());
                for (part, _) in RomSource::from_path(path)? {
                    match lookup.get(&part) {
                        Some(sources) => {
                            use prettytable::{cell, format, row};
                            let mut table = Table::new();
                            table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                            table.get_format().column_separator('\u{2502}');

                            for [category, system, game, rom] in sources {
                                table.add_row(row![category, system, game, rom]);
                            }

                            table.printstd();
                        }
                        None => {
                            println!("unknown");
                        }
                    }
                }
            }
        } else {
            for path in self.parts.into_iter() {
                for (part, source) in RomSource::from_path(path)? {
                    println!("{}  {}", part.digest(), source);
                }
            }
        }

        Ok(())
    }
}

/// Emulation Database Manager
#[derive(StructOpt)]
enum Opt {
    /// arcade software management
    #[structopt(name = "mame")]
    Mame(OptMame),

    /// console and portable software management
    #[structopt(name = "mess")]
    Mess(OptMess),

    /// extra files management, like snapshots
    #[structopt(name = "extra")]
    Extra(OptExtra),

    /// disc image software management
    #[structopt(name = "redump")]
    Redump(OptRedump),

    /// identify ROM or CHD by hash
    #[structopt(name = "identify")]
    Identify(OptIdentify),
}

impl Opt {
    fn execute(self) -> Result<(), Error> {
        match self {
            Opt::Mame(o) => o.execute(),
            Opt::Mess(o) => o.execute(),
            Opt::Extra(o) => o.execute(),
            Opt::Redump(o) => o.execute(),
            Opt::Identify(o) => o.execute(),
        }
    }
}

fn main() {
    if let Err(err) = Opt::from_args().execute() {
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

// attempts to read the first file in a .zip file,
// or the whole raw file if it is not a .zip
fn read_raw_or_zip<P: AsRef<Path>>(file: P) -> Result<String, Error> {
    let mut f = File::open(file)?;
    let mut data = String::new();
    if is_zip(&mut f)? {
        use zip::ZipArchive;

        ZipArchive::new(f)?.by_index(0)?.read_to_string(&mut data)?;
    } else {
        f.read_to_string(&mut data)?;
    }
    Ok(data)
}

// attempts to read all .dat files in a .zip file
// or the whole raw file if it is not a .zip
fn read_dat_or_zip<P: AsRef<Path>>(file: P) -> Result<Vec<(String, game::GameDb)>, Error> {
    let mut f = File::open(file)?;
    if is_zip(&mut f)? {
        use zip::ZipArchive;

        let mut zip = ZipArchive::new(f)?;

        let dats = zip
            .file_names()
            .filter(|s| s.ends_with(".dat"))
            .map(|s| s.to_owned())
            .collect::<Vec<String>>();

        dats.into_iter()
            .map(|name| {
                let mut data = String::new();
                zip.by_name(&name)?.read_to_string(&mut data)?;
                let tree = Document::parse(&data)?;
                Ok(extra::dat_to_game_db(&tree))
            })
            .collect()
    } else {
        let mut data = String::new();
        f.read_to_string(&mut data)?;
        let tree = Document::parse(&data)?;
        Ok(vec![extra::dat_to_game_db(&tree)])
    }
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
    serde_cbor::to_writer(f, &cache).map_err(Error::CBOR)?;
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
    serde_cbor::from_reader(f).map_err(Error::CBOR)
}

fn verify(db: &game::GameDb, root: &Path, games: &HashSet<String>, only_failures: bool) {
    let results = db.verify(root, games);

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
