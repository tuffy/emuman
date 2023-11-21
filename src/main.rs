#![forbid(unsafe_code)]

use base64::Engine;
use clap::{Args, Parser, Subcommand};
use indicatif::{MultiProgress, ProgressBar};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

mod dat;
mod dirs;
mod duplicates;
mod game;
mod http;
mod mame;
mod mess;
mod split;

static MAME: &str = "mame";
static MESS: &str = "mess";
static EXTRA: &str = "extra";
static REDUMP: &str = "redump";
static NOINTRO: &str = "nointro";

static DB_MAME: &str = "mame.cbor";
static DB_MESS_SPLIT: &str = "mess-split.cbor";
static DB_REDUMP_SPLIT: &str = "redump-split.cbor";

static DIR_SL: &str = "sl";
static DIR_EXTRA: &str = "extra";
static DIR_NOINTRO: &str = "nointro";
static DIR_REDUMP: &str = "redump";

pub fn terminal_height() -> usize {
    use terminal_size::{terminal_size, Height};

    const PAGE_SIZE: usize = 25;

    terminal_size()
        .and_then(|(_, Height(h))| usize::try_from(h).ok())
        .and_then(|size| size.checked_sub(3))
        .map(|size| size.clamp(2, PAGE_SIZE))
        .unwrap_or(PAGE_SIZE)
}

// used to add context about which file caused a given error
#[derive(Debug)]
pub struct ResourceError<E> {
    file: Resource,
    error: E,
}

impl<E: std::error::Error> std::error::Error for ResourceError<E> {}

impl<E: std::error::Error> std::fmt::Display for ResourceError<E> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}: {}", self.file, self.error)
    }
}

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    Xml(quick_xml::de::DeError),
    XmlFile(ResourceError<quick_xml::de::DeError>),
    CborWrite(ciborium::ser::Error<std::io::Error>),
    TomlWrite(toml::ser::Error),
    Zip(zip::result::ZipError),
    Http(attohttpc::Error),
    HttpCode(attohttpc::StatusCode),
    Inquire(inquire::error::InquireError),
    NoSuchDatFile(String),
    NoDatFiles,
    NoDatFilesFound,
    EmptyDatFile,
    NoSuchSoftwareList(String),
    NoSoftwareLists,
    NoSuchSoftware(String),
    MissingCache(&'static str),
    InvalidCache(&'static str),
    InvalidPath,
    InvalidSha1(ResourceError<hex::FromHexError>),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err)
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

impl From<inquire::error::InquireError> for Error {
    #[inline]
    fn from(err: inquire::error::InquireError) -> Self {
        Error::Inquire(err)
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::IO(err) => err.fmt(f),
            Error::Xml(err) => err.fmt(f),
            Error::XmlFile(err) => err.fmt(f),
            Error::CborWrite(err) => err.fmt(f),
            Error::TomlWrite(err) => err.fmt(f),
            Error::Zip(err) => err.fmt(f),
            Error::Http(err) => err.fmt(f),
            Error::HttpCode(code) => match code.canonical_reason() {
                Some(reason) => write!(f, "HTTP error {} - {}", code.as_str(), reason),
                None => write!(f, "HTTP error {}", code.as_str()),
            },
            Error::Inquire(err) => err.fmt(f),
            Error::NoSuchDatFile(s) => write!(f, "no such DAT file \"{}\"", s),
            Error::NoDatFiles => write!(f, "no DAT files have been initialized"),
            Error::NoDatFilesFound => write!(f, "no DAT files found in resource"),
            Error::EmptyDatFile => write!(f, "DAT file contains no games"),
            Error::NoSuchSoftwareList(s) => write!(f, "no such software list \"{}\"", s),
            Error::NoSuchSoftware(s) => write!(f, "no such software \"{}\"", s),
            Error::NoSoftwareLists => write!(f, "no software lists initialized"),
            Error::MissingCache(s) => write!(
                f,
                "missing cache files, please run \"emuman {} init\" to populate",
                s
            ),
            Error::InvalidCache(s) => write!(
                f,
                "outdated or invalid cache files, please run \"emuman {} init\" to repopulate",
                s
            ),
            Error::InvalidPath => write!(f, "invalid UTF-8 path"),
            Error::InvalidSha1(err) => err.fmt(f),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Resource {
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

    fn rom_sources(&self, progress: &MultiProgress) -> game::RomSources {
        match self {
            Self::File(f) => game::file_rom_sources(f, progress),
            Self::Url(url) => game::url_rom_sources(url, progress),
        }
    }
}

impl std::fmt::Display for Resource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Resource::File(pb) => write!(f, "{}", pb.display()),
            Resource::Url(s) => write!(f, "{}", s),
        }
    }
}

impl From<String> for Resource {
    #[inline]
    fn from(s: String) -> Self {
        if url::Url::parse(&s).is_ok() {
            Self::Url(s)
        } else {
            Self::File(s.into())
        }
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

impl From<std::ffi::OsString> for Resource {
    #[inline]
    fn from(osstr: std::ffi::OsString) -> Self {
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
    /// MAME's XML file or URL
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

        quick_xml::de::from_str(&xml_data)
            .map_err(Error::Xml)
            .and_then(|mame: mame::Mame| write_game_db(DB_MAME, mame.into_game_db()))
    }
}

#[derive(Args)]
struct OptMameDir;

impl OptMameDir {
    fn execute(self) -> Result<(), Error> {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::Table;

        let mut table = Table::new();
        table
            .set_header(vec!["Version", "Directory"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        table.add_row(vec![
            read_game_db::<game::GameDb>(MAME, DB_MAME)?.description(),
            &dirs::mame_roms(None).as_ref().to_string_lossy(),
        ]);

        println!("{table}");

        Ok(())
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
        let db = read_game_db::<game::GameDb>(MAME, DB_MAME)?;
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
        let db = read_game_db::<game::GameDb>(MAME, DB_MAME)?;
        match self.games.as_slice() {
            [] => db.games(&select_game_names(&db)?, self.simple),
            games => db.games(games, self.simple),
        }
        Ok(())
    }
}

#[derive(Args)]
struct OptMameParts {
    /// game's parts to search for
    game: Option<String>,
}

impl OptMameParts {
    fn execute(self) -> Result<(), Error> {
        let db = read_game_db::<game::GameDb>(MAME, DB_MAME)?;

        match self.game {
            Some(game) => db.display_parts(&game),
            None => db.display_parts(select_game_name(&db)?),
        }
    }
}

#[derive(Args)]
struct OptMameReport {
    /// sorting order, use "description", "year" or "creator"
    #[clap(short = 's', long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// ROMs directory
    #[clap(short = 'r', long = "roms")]
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

        let db = read_game_db::<game::GameDb>(MAME, DB_MAME)?;
        db.report(&machines, self.search.as_deref(), self.sort, self.simple);

        Ok(())
    }
}

#[derive(Args)]
struct OptMameVerify {
    /// ROMs directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// game to verify
    #[clap(short = 'g', long = "game")]
    machines: Vec<String>,
}

impl OptMameVerify {
    fn execute(self) -> Result<(), Error> {
        let db: game::GameDb = read_game_db(MAME, DB_MAME)?;

        let roms_dir = dirs::mame_roms(self.roms);

        match self.machines.as_slice() {
            [] => verify(&db, roms_dir, db.games_iter()),
            machines => verify(
                &db,
                roms_dir,
                db.valid_games::<_, Vec<_>>(machines)?.into_iter(),
            ),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptMameRepair {
    /// output directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// game to add
    #[clap(short = 'g', long = "game")]
    machines: Vec<String>,

    /// input file, directory, or URL
    input: Vec<Resource>,
}

impl OptMameRepair {
    fn execute(self) -> Result<(), Error> {
        let db: game::GameDb = read_game_db(MAME, DB_MAME)?;

        let roms_dir = dirs::mame_roms(self.roms);

        let mut roms = rom_sources(&self.input);

        match self.machines.as_slice() {
            [] => add_and_verify(&mut roms, roms_dir, db.games_iter()),
            machines => add_and_verify(
                &mut roms,
                roms_dir,
                db.valid_games::<_, Vec<_>>(machines)?.into_iter(),
            ),
        }
    }
}

#[derive(Subcommand)]
enum OptMame {
    /// initialize internal database
    Init(OptMameInit),

    /// list defined directory
    Dir(OptMameDir),

    /// list all games
    List(OptMameList),

    /// list a games's ROMs
    Parts(OptMameParts),

    /// list given games, in order
    Games(OptMameGames),

    /// generate report of games in collection
    Report(OptMameReport),

    /// verify ROMs in directory
    Verify(OptMameVerify),

    /// add and repair ROMs in directory
    #[clap(alias = "add")]
    Repair(OptMameRepair),
}

impl OptMame {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptMame::Init(o) => o.execute(),
            OptMame::Dir(o) => o.execute(),
            OptMame::List(o) => o.execute(),
            OptMame::Parts(o) => o.execute(),
            OptMame::Games(o) => o.execute(),
            OptMame::Report(o) => o.execute(),
            OptMame::Verify(o) => o.execute(),
            OptMame::Repair(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptMessInit {
    /// XML files from hash database
    xml: Vec<PathBuf>,
}

impl OptMessInit {
    fn execute(self) -> Result<(), Error> {
        let mut split_db = split::SplitDb::new();

        for file in self.xml.into_iter() {
            let sl: mess::Softwarelist =
                quick_xml::de::from_reader(File::open(&file).map(std::io::BufReader::new)?)
                    .map_err(|error| {
                        Error::XmlFile(ResourceError {
                            error,
                            file: Resource::File(file),
                        })
                    })?;

            sl.populate_split_db(&mut split_db);
            write_named_db(DIR_SL, &sl.name().to_owned(), sl.into_game_db())?;
        }

        write_game_db(DB_MESS_SPLIT, &split_db)?;

        Ok(())
    }
}

#[derive(Args)]
struct OptMessDirs {
    search: Option<String>,
}

impl OptMessDirs {
    fn execute(self) -> Result<(), Error> {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::Table;

        let mut table = Table::new();
        table
            .set_header(vec!["Software", "Directory"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        table.add_rows(
            read_collected_dbs::<BTreeMap<String, game::GameDb>, _>(DIR_SL)
                .into_iter()
                .filter(|(_, db)| match &self.search {
                    Some(search) => db.description().contains(search),
                    None => true,
                })
                .map(|(name, db)| {
                    vec![
                        db.description().to_owned(),
                        dirs::mess_roms(None, &name)
                            .as_ref()
                            .to_string_lossy()
                            .to_string(),
                    ]
                }),
        );

        println!("{table}");

        Ok(())
    }
}

#[derive(Args)]
struct OptMessList {
    /// software list to use
    #[clap(short = 'L', long = "software")]
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
        match self.software_list.as_deref() {
            Some("any") => mess::list(
                &read_collected_dbs(DIR_SL),
                self.search.as_deref(),
                self.sort,
                self.simple,
            ),
            Some(software_list) => read_named_db::<game::GameDb>(MESS, DIR_SL, software_list)?
                .list(self.search.as_deref(), self.sort, self.simple),
            None => mess::list_all(&read_collected_dbs(DIR_SL)),
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
    #[clap(short = 'L', long = "software")]
    software_list: Option<String>,

    /// games to search for, by short name
    games: Vec<String>,
}

impl OptMessGames {
    fn execute(self) -> Result<(), Error> {
        let software_list = match self.software_list {
            Some(software_list) => read_named_db(MESS, DIR_SL, &software_list)?,
            None => select_software_list()?,
        };

        match self.games.as_slice() {
            [] => software_list.games(&select_game_names(&software_list)?, self.simple),
            games => software_list.games(games, self.simple),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptMessParts {
    /// software list to use
    #[clap(short = 'L', long = "software")]
    software_list: Option<String>,

    /// game's parts to search for
    game: Option<String>,
}

impl OptMessParts {
    fn execute(self) -> Result<(), Error> {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::Table;

        let mut software_list = match self.software_list {
            Some(software_list) => read_named_db(MESS, DIR_SL, &software_list)?,
            None => select_software_list()?,
        };

        let game = match self.game {
            Some(game) => software_list
                .remove_game(&game)
                .ok_or_else(|| Error::NoSuchSoftware(game.to_string()))?,
            None => select_software_list_game(software_list)?,
        };

        let mut table = Table::new();
        table
            .set_header(vec!["Part", "SHA1 Hash"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        game.display_parts(&mut table);
        println!("{table}");
        Ok(())
    }
}

#[derive(Args)]
struct OptMessReport {
    /// sorting order, use "description", "year" or "creator"
    #[clap(short = 's', long = "sort", default_value = "description")]
    sort: game::GameColumn,

    /// ROMs directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// software list to use
    #[clap(short = 'L', long = "software")]
    software_list: Option<String>,

    /// display simple report with less information
    #[clap(short = 'S', long = "simple")]
    simple: bool,

    /// search term for querying specific software
    search: Option<String>,
}

impl OptMessReport {
    fn execute(self) -> Result<(), Error> {
        let (db, software_list) = match self.software_list {
            Some(software_list) => (
                read_named_db::<game::GameDb>(MESS, DIR_SL, &software_list)?,
                software_list,
            ),
            None => select_software_list_and_name()?,
        };

        let software: HashSet<String> = dirs::mess_roms(self.roms, &software_list)
            .as_ref()
            .read_dir()?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect();

        db.report(&software, self.search.as_deref(), self.sort, self.simple);

        Ok(())
    }
}

#[derive(Args)]
struct OptMessVerify {
    /// ROMs directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// software list to use
    #[clap(short = 'L', long = "software")]
    software_list: Option<String>,

    /// game to verify
    #[clap(short = 'g', long = "game")]
    software: Vec<String>,
}

impl OptMessVerify {
    fn execute(self) -> Result<(), Error> {
        let (db, software_list) = match self.software_list {
            Some(software_list) => (
                read_named_db::<game::GameDb>(MESS, DIR_SL, &software_list)?,
                software_list,
            ),
            None => select_software_list_and_name()?,
        };

        let roms_dir = dirs::mess_roms(self.roms, &software_list);

        match self.software.as_slice() {
            [] => verify(&db, roms_dir, db.games_iter()),
            machines => verify(
                &db,
                roms_dir,
                db.valid_games::<_, Vec<_>>(machines)?.into_iter(),
            ),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptMessVerifyAll {
    /// ROMs directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// show all systems in output table
    #[clap(short = 'A', long = "all")]
    show_all: bool,
}

impl OptMessVerifyAll {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Never;

        process_all_mess(
            "verifying software lists",
            self.roms,
            |parts, path, _| -> Result<_, Never> { Ok(parts.verify_failures(path)) },
            self.show_all,
        )
        .unwrap();

        Ok(())
    }
}

#[derive(Args)]
struct OptMessRepair {
    /// output directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// software list to use
    #[clap(short = 'L', long = "software")]
    software_list: Option<String>,

    /// game to add
    #[clap(short = 'g', long = "game")]
    software: Vec<String>,

    /// input file, directory, or URL
    input: Vec<Resource>,
}

impl OptMessRepair {
    fn execute(self) -> Result<(), Error> {
        let (db, software_list) = match self.software_list {
            Some(software_list) => (
                read_named_db::<game::GameDb>(MESS, DIR_SL, &software_list)?,
                software_list,
            ),
            None => select_software_list_and_name()?,
        };

        let roms_dir = dirs::mess_roms(self.roms, &software_list);

        let mut roms = rom_sources(&self.input);

        match self.software.as_slice() {
            [] => add_and_verify(&mut roms, &roms_dir, db.games_iter()),
            software => add_and_verify(
                &mut roms,
                roms_dir,
                db.valid_games::<_, Vec<_>>(software)?.into_iter(),
            ),
        }
    }
}

#[derive(Args)]
struct OptMessRepairAll {
    /// output directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// input file, directory, or URL
    input: Vec<Resource>,

    /// show all systems in output table
    #[clap(short = 'A', long = "all")]
    show_all: bool,
}

impl OptMessRepairAll {
    fn execute(self) -> Result<(), Error> {
        let rom_sources = rom_sources(&self.input);

        process_all_mess(
            "adding and verifying software lists",
            self.roms,
            |parts, path, mbar| {
                parts.add_and_verify_failures(&rom_sources, path, |repaired| {
                    mbar.println(repaired.to_string()).unwrap();
                    repaired.into_fixed_pathbuf()
                })
            },
            self.show_all,
        )
    }
}

#[derive(Args)]
struct OptMessSplit {
    /// target directory for split ROMs
    #[clap(short = 'r', long = "roms", default_value = ".")]
    output: PathBuf,

    /// ROMs to split
    roms: Vec<PathBuf>,
}

impl OptMessSplit {
    fn execute(self) -> Result<(), Error> {
        use rayon::prelude::*;

        let db = read_game_db::<split::SplitDb>(MESS, DB_MESS_SPLIT)?;

        self.roms.par_iter().try_for_each(|rom| {
            let mut f = File::open(rom)?;

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
    Init(OptMessInit),

    /// list defined directories
    Dirs(OptMessDirs),

    /// list all software in software list
    List(OptMessList),

    /// list given games, in order
    Games(OptMessGames),

    /// list a machine's ROMs
    Parts(OptMessParts),

    /// generate report of sets in collection
    Report(OptMessReport),

    /// verify ROMs in directory
    Verify(OptMessVerify),

    /// verify all ROMs in all software lists in directory
    VerifyAll(OptMessVerifyAll),

    /// add and repair ROMs in directory
    #[clap(alias = "add")]
    Repair(OptMessRepair),

    /// add and repair all software list ROMs in all directories
    #[clap(alias = "add-all")]
    RepairAll(OptMessRepairAll),

    /// split ROM into software list-compatible parts, if necessary
    Split(OptMessSplit),
}

impl OptMess {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptMess::Init(o) => o.execute(),
            OptMess::Dirs(o) => o.execute(),
            OptMess::List(o) => o.execute(),
            OptMess::Games(o) => o.execute(),
            OptMess::Parts(o) => o.execute(),
            OptMess::Report(o) => o.execute(),
            OptMess::Verify(o) => o.execute(),
            OptMess::VerifyAll(o) => o.execute(),
            OptMess::Repair(o) => o.execute(),
            OptMess::RepairAll(o) => o.execute(),
            OptMess::Split(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptExtraInit {
    /// extras .DAT file files
    dats: Vec<Resource>,

    /// completely replace old dat files
    #[clap(long = "replace")]
    replace: bool,
}

impl OptExtraInit {
    fn execute(self) -> Result<(), Error> {
        if self.replace {
            clear_named_dbs(DIR_EXTRA)?;
        }

        for datfile in dat::fetch_and_parse::<_, Vec<_>>(self.dats, |file, datfile| {
            dat::DatFile::new_unflattened(datfile)
                .map_err(|error| Error::InvalidSha1(ResourceError { file, error }))
        })? {
            write_named_db(DIR_EXTRA, datfile.name(), &datfile)?;
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraDestroy {
    /// extra names
    extras: Vec<String>,
}

impl OptExtraDestroy {
    fn execute(self) -> Result<(), Error> {
        match self.extras.as_slice() {
            [] => {
                let mut dats = read_named_dbs(DIR_EXTRA)
                    .into_iter()
                    .flatten()
                    .map(|(_, d)| d)
                    .collect::<Vec<dat::DatFile>>();

                dats.sort_unstable_by(|x, y| x.name().cmp(y.name()));

                inquire::MultiSelect::new("select extras DAT(s) to destroy", dats)
                    .with_page_size(terminal_height())
                    .prompt()
                    .map_err(Error::Inquire)
                    .and_then(|extras| {
                        extras
                            .into_iter()
                            .try_for_each(|e| destroy_named_db(DIR_EXTRA, e.name()))
                    })
            }
            extras => extras
                .iter()
                .try_for_each(|e| destroy_named_db(DIR_EXTRA, e)),
        }
    }
}

#[derive(Args)]
struct OptExtraDirs {
    // sort output by version
    #[clap(short = 'V')]
    sort_by_version: bool,

    search: Option<String>,
}

impl OptExtraDirs {
    fn execute(self) -> Result<(), Error> {
        display_dirs(
            dirs::extra_dirs(),
            read_collected_dbs(DIR_EXTRA),
            self.search,
            self.sort_by_version,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraSizes {
    /// sort output by total size
    #[clap(short = 'S')]
    sort_by_size: bool,

    search: Option<String>,
}

impl OptExtraSizes {
    fn execute(self) -> Result<(), Error> {
        display_dir_sizes(
            dirs::extra_dirs(),
            read_collected_dbs(DIR_EXTRA),
            self.search,
            self.sort_by_size,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraList {
    /// extras name
    name: Option<String>,

    search: Option<String>,
}

impl OptExtraList {
    fn execute(self) -> Result<(), Error> {
        match self.name.as_deref() {
            Some(name) => {
                read_named_db::<dat::DatFile>(EXTRA, DIR_EXTRA, name)?.list(self.search.as_deref())
            }
            None => dat::DatFile::list_all(read_collected_dbs::<BTreeMap<_, _>, _>(DIR_EXTRA)),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraVerify {
    /// extras directory
    #[clap(short = 'd', long = "dir")]
    dir: Option<PathBuf>,

    /// extras category to verify
    #[clap(short = 'E', long = "extra")]
    extra: Option<String>,
}

impl OptExtraVerify {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Never;

        let dir = self.dir;

        let extra = match self.extra {
            Some(name) => name,
            None if dir.is_none() => dirs::select_extra_name()?,
            None => dirs::select_any_extra_name()?,
        };

        process_dat(read_named_db(EXTRA, DIR_EXTRA, &extra)?, |datfile, pbar| {
            Ok::<_, Never>(datfile.verify(dirs::extra_dir(dir, &extra).as_ref(), pbar))
        })
        .unwrap();

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraVerifyAll {
    /// show all systems in output table
    #[clap(short = 'A', long = "all")]
    show_all: bool,
}

impl OptExtraVerifyAll {
    fn execute(self) -> Result<(), Error> {
        use game::Never;

        process_all_dat(
            "verifying all MAME extras",
            dirs::extra_dirs(),
            |name| read_named_db(EXTRA, DIR_EXTRA, name),
            |datfile, dir, pbar| Ok::<_, Never>(datfile.verify(dir, pbar)),
            self.show_all,
        )
        .unwrap();

        Ok(())
    }
}

#[derive(Args)]
struct OptExtraRepair {
    /// output directory
    #[clap(short = 'd', long = "dir")]
    dir: Option<PathBuf>,

    /// extras category to add files to
    #[clap(short = 'E', long = "extra")]
    extra: Option<String>,

    /// input file, directory, or URL
    input: Vec<Resource>,
}

impl OptExtraRepair {
    fn execute(self) -> Result<(), Error> {
        let dir = self.dir;
        let extra = match self.extra {
            Some(extra) => extra,
            None if dir.is_none() => dirs::select_extra_name()?,
            None => dirs::select_any_extra_name()?,
        };
        let datfile: dat::DatFile = read_named_db::<dat::DatFile>(EXTRA, DIR_EXTRA, &extra)?;
        let mut rom_sources = rom_sources(&self.input);

        process_dat(datfile, |datfile, pbar| {
            datfile.add_and_verify(
                &mut rom_sources,
                dirs::extra_dir(dir, &extra).as_ref(),
                pbar,
            )
        })
    }
}

#[derive(Args)]
struct OptExtraRepairAll {
    /// input file, directory, or URL
    input: Vec<Resource>,

    /// show all systems in output table
    #[clap(short = 'A', long = "all")]
    show_all: bool,
}

impl OptExtraRepairAll {
    fn execute(self) -> Result<(), Error> {
        let mut parts = rom_sources(&self.input);

        process_all_dat(
            "adding and verifying all MAME extras",
            dirs::extra_dirs(),
            |name| read_named_db(EXTRA, DIR_EXTRA, name),
            |datfile, dir, pbar| datfile.add_and_verify(&mut parts, dir, pbar),
            self.show_all,
        )
    }
}

#[derive(Args)]
struct OptExtraParts {
    /// DAT name to find parts for
    #[clap(short = 'D', long = "dat")]
    name: Option<String>,

    /// game's parts to search for
    game: Option<String>,
}

impl OptExtraParts {
    fn execute(self) -> Result<(), Error> {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::Table;

        let mut datfile = match self.name {
            Some(name) => read_named_db::<dat::DatFile>(EXTRA, DIR_EXTRA, &name),
            None => {
                let mut dats = read_named_dbs(DIR_EXTRA)
                    .into_iter()
                    .flatten()
                    .map(|(_, d)| d)
                    .collect::<Vec<dat::DatFile>>();

                dats.sort_unstable_by(|x, y| x.name().cmp(y.name()));

                inquire::Select::new("select DAT", dats)
                    .with_page_size(terminal_height())
                    .prompt()
                    .map_err(Error::Inquire)
            }
        }?;

        let game = match self.game {
            Some(game) => datfile
                .remove_game(&game)
                .ok_or_else(|| Error::NoSuchSoftware(game.to_string()))?,
            None => select_datfile_game(datfile)?,
        };

        let mut table = Table::new();
        table
            .set_header(vec!["Part", "SHA1 Hash"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        for (name, part) in game.into_iter().collect::<BTreeMap<_, _>>() {
            table.add_row(vec![name, part.digest().to_string()]);
        }
        println!("{table}");

        Ok(())
    }
}

#[derive(Subcommand)]
#[clap(name = "extra")]
enum OptExtra {
    /// initialize internal database
    Init(OptExtraInit),

    /// remove extras from internal database
    Destroy(OptExtraDestroy),

    /// list defined directories
    Dirs(OptExtraDirs),

    /// display total sizes of defined directories
    Sizes(OptExtraSizes),

    /// list all extras categories
    List(OptExtraList),

    /// verify parts in directory
    Verify(OptExtraVerify),

    /// add and repair files to directory
    #[clap(alias = "add")]
    Repair(OptExtraRepair),

    /// add and repair files in all directories
    #[clap(alias = "add-all")]
    RepairAll(OptExtraRepairAll),

    /// verify all files in directory
    VerifyAll(OptExtraVerifyAll),

    /// display extra's parts
    Parts(OptExtraParts),
}

impl OptExtra {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptExtra::Init(o) => o.execute(),
            OptExtra::Destroy(o) => o.execute(),
            OptExtra::Dirs(o) => o.execute(),
            OptExtra::Sizes(o) => o.execute(),
            OptExtra::List(o) => o.execute(),
            OptExtra::Verify(o) => o.execute(),
            OptExtra::Repair(o) => o.execute(),
            OptExtra::RepairAll(o) => o.execute(),
            OptExtra::VerifyAll(o) => o.execute(),
            OptExtra::Parts(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptRedumpInit {
    /// Redump XML or Zip file
    xml: Vec<Resource>,

    /// interactively edit DAT contents before importing
    #[clap(long = "edit")]
    edit: bool,
}

impl OptRedumpInit {
    fn execute(self) -> Result<(), Error> {
        let mut split_db = split::SplitDb::new();

        for datfile in dat::fetch_and_parse::<_, Vec<_>>(self.xml, |file, datfile| {
            (if self.edit {
                let old_dat = read_named_db(REDUMP, DIR_REDUMP, datfile.name()).ok();
                dat::edit_file(datfile, old_dat)
            } else {
                Ok(datfile)
            })
            .map(|datfile| {
                split_db.populate(&datfile);
                datfile
            })
            .and_then(|datfile| {
                dat::DatFile::new_flattened(datfile)
                    .map_err(|error| Error::InvalidSha1(ResourceError { file, error }))
            })
        })? {
            write_named_db(DIR_REDUMP, datfile.name(), &datfile)?;
        }

        write_game_db(DB_REDUMP_SPLIT, &split_db)?;

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpDestroy {
    /// DAT file names
    dats: Vec<String>,
}

impl OptRedumpDestroy {
    fn execute(self) -> Result<(), Error> {
        match self.dats.as_slice() {
            [] => {
                let mut dats = read_named_dbs(DIR_REDUMP)
                    .into_iter()
                    .flatten()
                    .map(|(_, d)| d)
                    .collect::<Vec<dat::DatFile>>();

                dats.sort_unstable_by(|x, y| x.name().cmp(y.name()));

                inquire::MultiSelect::new("select Redump DAT(s) to destroy", dats)
                    .with_page_size(terminal_height())
                    .prompt()
                    .map_err(Error::Inquire)
                    .and_then(|dats| {
                        dats.into_iter()
                            .try_for_each(|d| destroy_named_db(DIR_REDUMP, d.name()))
                    })
            }
            dats => dats
                .iter()
                .try_for_each(|d| destroy_named_db(DIR_REDUMP, d)),
        }
    }
}

#[derive(Args)]
struct OptRedumpDirs {
    // sort output by version
    #[clap(short = 'V')]
    sort_by_version: bool,

    search: Option<String>,
}

impl OptRedumpDirs {
    fn execute(self) -> Result<(), Error> {
        display_dirs(
            dirs::redump_dirs(),
            read_collected_dbs(DIR_REDUMP),
            self.search,
            self.sort_by_version,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpSizes {
    /// sort output by total size
    #[clap(short = 'S')]
    sort_by_size: bool,

    search: Option<String>,
}

impl OptRedumpSizes {
    fn execute(self) -> Result<(), Error> {
        display_dir_sizes(
            dirs::redump_dirs(),
            read_collected_dbs(DIR_REDUMP),
            self.search,
            self.sort_by_size,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpList {
    /// software list to use
    software_list: Option<String>,

    search: Option<String>,
}

impl OptRedumpList {
    fn execute(self) -> Result<(), Error> {
        match self.software_list.as_deref() {
            Some(name) => read_named_db::<dat::DatFile>(REDUMP, DIR_REDUMP, name)?
                .list(self.search.as_deref()),
            None => dat::DatFile::list_all(read_collected_dbs::<BTreeMap<_, _>, _>(DIR_REDUMP)),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpVerify {
    /// root directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// DAT name to verify disk images for
    #[clap(short = 'D', long = "dat")]
    name: Option<String>,
}

impl OptRedumpVerify {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Never;

        let roms = self.roms;

        let name = match self.name {
            Some(name) => name,
            None if roms.is_none() => dirs::select_redump_name()?,
            None => dirs::select_any_redump_name()?,
        };

        process_dat(
            read_named_db(REDUMP, DIR_REDUMP, &name)?,
            |datfile, pbar| {
                Ok::<_, Never>(datfile.verify(dirs::redump_roms(roms, &name).as_ref(), pbar))
            },
        )
        .unwrap();

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpVerifyAll {
    /// show all systems in output table
    #[clap(short = 'A', long = "all")]
    show_all: bool,
}

impl OptRedumpVerifyAll {
    fn execute(self) -> Result<(), Error> {
        use game::Never;

        process_all_dat(
            "verifying all Redump files",
            dirs::redump_dirs(),
            |name| read_named_db(REDUMP, DIR_REDUMP, name),
            |datfile, dir, pbar| Ok::<_, Never>(datfile.verify(dir, pbar)),
            self.show_all,
        )
        .unwrap();

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpRepair {
    /// output directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// DAT name to add disk images for
    #[clap(short = 'D', long = "dat")]
    name: Option<String>,

    /// input file, directory, or URL
    input: Vec<Resource>,
}

impl OptRedumpRepair {
    fn execute(self) -> Result<(), Error> {
        let roms = self.roms;
        let name = match self.name {
            Some(name) => name,
            None if roms.is_none() => dirs::select_redump_name()?,
            None => dirs::select_any_redump_name()?,
        };
        let datfile: dat::DatFile = read_named_db::<dat::DatFile>(REDUMP, DIR_REDUMP, &name)?;
        let mut rom_sources = rom_sources(&self.input);

        process_dat(datfile, |datfile, pbar| {
            datfile.add_and_verify(
                &mut rom_sources,
                dirs::redump_roms(roms, &name).as_ref(),
                pbar,
            )
        })
    }
}

#[derive(Args)]
struct OptRedumpRepairAll {
    /// input file, directory, or URL
    input: Vec<Resource>,

    /// show all systems in output table
    #[clap(short = 'A', long = "all")]
    show_all: bool,
}

impl OptRedumpRepairAll {
    fn execute(self) -> Result<(), Error> {
        let mut parts = rom_sources(&self.input);

        process_all_dat(
            "adding and verifying all Redump files",
            dirs::redump_dirs(),
            |name| read_named_db(REDUMP, DIR_REDUMP, name),
            |datfile, dir, pbar| datfile.add_and_verify(&mut parts, dir, pbar),
            self.show_all,
        )
    }
}

#[derive(Args)]
struct OptRedumpParts {
    /// DAT name to find parts for
    #[clap(short = 'D', long = "dat")]
    name: Option<String>,

    /// game's parts to search for
    game: Option<String>,
}

impl OptRedumpParts {
    fn execute(self) -> Result<(), Error> {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::Table;

        let mut datfile = match self.name {
            Some(name) => read_named_db::<dat::DatFile>(REDUMP, DIR_REDUMP, &name),
            None => {
                let mut dats = read_named_dbs(DIR_REDUMP)
                    .into_iter()
                    .flatten()
                    .map(|(_, d)| d)
                    .collect::<Vec<dat::DatFile>>();

                dats.sort_unstable_by(|x, y| x.name().cmp(y.name()));
                inquire::Select::new("select DAT", dats)
                    .with_page_size(terminal_height())
                    .prompt()
                    .map_err(Error::Inquire)
            }
        }?;

        let game = match self.game {
            Some(game) => datfile
                .remove_game(&game)
                .ok_or_else(|| Error::NoSuchSoftware(game.to_string()))?,
            None => select_datfile_game(datfile)?,
        };

        let mut table = Table::new();
        table
            .set_header(vec!["Part", "SHA1 Hash"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        for (name, part) in game.into_iter().collect::<BTreeMap<_, _>>() {
            table.add_row(vec![name, part.digest().to_string()]);
        }
        println!("{table}");

        Ok(())
    }
}

#[derive(Args)]
struct OptRedumpSplit {
    /// directory to place output tracks
    #[clap(short = 'r', long = "roms", default_value = ".")]
    root: PathBuf,

    /// input .bin file
    bins: Vec<PathBuf>,
}

impl OptRedumpSplit {
    fn execute(self) -> Result<(), Error> {
        let db: split::SplitDb = read_game_db(REDUMP, DB_REDUMP_SPLIT)?;

        self.bins.iter().try_for_each(|bin_path| {
            match bin_path.metadata().map(|m| db.possible_matches(m.len())) {
                Err(_) | Ok([]) => Ok(()),
                Ok(matches) => {
                    let mut bin_data = Vec::new();
                    File::open(bin_path).and_then(|mut f| f.read_to_end(&mut bin_data))?;
                    if let Some(exact_match) = matches.iter().find(|m| m.matches(&bin_data)) {
                        exact_match.extract(&self.root, &bin_data)?;
                    }
                    Ok(())
                }
            }
        })
    }
}

#[derive(Subcommand)]
#[clap(name = "redump")]
enum OptRedump {
    /// initialize internal database
    Init(OptRedumpInit),

    /// remove dat file from internal database
    Destroy(OptRedumpDestroy),

    /// list defined directories
    Dirs(OptRedumpDirs),

    /// display total sizes of defined directories
    Sizes(OptRedumpSizes),

    /// list all software in software list
    List(OptRedumpList),

    /// verify files against Redump database
    Verify(OptRedumpVerify),

    /// verify all ROMs in all categories
    VerifyAll(OptRedumpVerifyAll),

    /// add and repair tracks in directory
    #[clap(alias = "add")]
    Repair(OptRedumpRepair),

    /// and and repair tracks in all categories
    #[clap(alias = "add-all")]
    RepairAll(OptRedumpRepairAll),

    /// split .bin file into multiple tracks
    Split(OptRedumpSplit),

    /// display game's parts
    Parts(OptRedumpParts),
}

impl OptRedump {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptRedump::Init(o) => o.execute(),
            OptRedump::Destroy(o) => o.execute(),
            OptRedump::Dirs(o) => o.execute(),
            OptRedump::Sizes(o) => o.execute(),
            OptRedump::List(o) => o.execute(),
            OptRedump::Verify(o) => o.execute(),
            OptRedump::VerifyAll(o) => o.execute(),
            OptRedump::Repair(o) => o.execute(),
            OptRedump::RepairAll(o) => o.execute(),
            OptRedump::Split(o) => o.execute(),
            OptRedump::Parts(o) => o.execute(),
        }
    }
}

#[derive(Subcommand)]
#[clap(name = "nointro")]
enum OptNointro {
    /// initialize internal database
    Init(OptNointroInit),

    /// remove dat file from internal database
    Destroy(OptNointroDestroy),

    /// list defined directories
    Dirs(OptNointroDirs),

    /// display total sizes of defined directories
    Sizes(OptNointroSizes),

    /// list categories or ROMs
    List(OptNointroList),

    /// verify category's ROMs
    Verify(OptNointroVerify),

    /// verify all ROMs in all categories
    VerifyAll(OptNointroVerifyAll),

    /// add and verify category's ROMs
    #[clap(alias = "add")]
    Repair(OptNointroRepair),

    /// add ROMs to all categories
    #[clap(alias = "add-all")]
    RepairAll(OptNointroRepairAll),

    /// display game's parts
    Parts(OptNointroParts),
}

impl OptNointro {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptNointro::Init(o) => o.execute(),
            OptNointro::Destroy(o) => o.execute(),
            OptNointro::Dirs(o) => o.execute(),
            OptNointro::Sizes(o) => o.execute(),
            OptNointro::List(o) => o.execute(),
            OptNointro::Verify(o) => o.execute(),
            OptNointro::VerifyAll(o) => o.execute(),
            OptNointro::Repair(o) => o.execute(),
            OptNointro::RepairAll(o) => o.execute(),
            OptNointro::Parts(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptNointroInit {
    /// No-Intro DAT or Zip file
    dats: Vec<Resource>,

    /// completely replace old DAT files
    #[clap(long = "replace")]
    replace: bool,

    /// interactively edit DAT contents before importing
    #[clap(long = "edit")]
    edit: bool,
}

impl OptNointroInit {
    fn execute(self) -> Result<(), Error> {
        if self.replace {
            clear_named_dbs(DIR_NOINTRO)?;
        }

        for datfile in dat::fetch_and_parse::<_, Vec<_>>(self.dats, |file, datfile| {
            (if self.edit {
                let old_dat = read_named_db(NOINTRO, DIR_NOINTRO, datfile.name()).ok();
                dat::edit_file(datfile, old_dat)
            } else {
                Ok(datfile)
            })
            .and_then(|datfile| {
                dat::DatFile::new_flattened(datfile)
                    .map_err(|error| Error::InvalidSha1(ResourceError { file, error }))
            })
        })? {
            write_named_db(DIR_NOINTRO, datfile.name(), &datfile)?;
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroDestroy {
    /// DAT file names
    dats: Vec<String>,
}

impl OptNointroDestroy {
    fn execute(self) -> Result<(), Error> {
        match self.dats.as_slice() {
            [] => {
                let mut dats = read_named_dbs(DIR_NOINTRO)
                    .into_iter()
                    .flatten()
                    .map(|(_, d)| d)
                    .collect::<Vec<dat::DatFile>>();

                dats.sort_unstable_by(|x, y| x.name().cmp(y.name()));

                inquire::MultiSelect::new("select No-Intro DAT(s) to destroy", dats)
                    .with_page_size(terminal_height())
                    .prompt()
                    .map_err(Error::Inquire)
                    .and_then(|dats| {
                        dats.into_iter()
                            .try_for_each(|d| destroy_named_db(DIR_NOINTRO, d.name()))
                    })
            }
            dats => dats
                .iter()
                .try_for_each(|d| destroy_named_db(DIR_NOINTRO, d)),
        }
    }
}

#[derive(Args)]
struct OptNointroDirs {
    /// sort output by version
    #[clap(short = 'V')]
    sort_by_version: bool,

    search: Option<String>,
}

impl OptNointroDirs {
    fn execute(self) -> Result<(), Error> {
        display_dirs(
            dirs::nointro_dirs(),
            read_collected_dbs(DIR_NOINTRO),
            self.search,
            self.sort_by_version,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroSizes {
    /// sort output by total size
    #[clap(short = 'S')]
    sort_by_size: bool,

    search: Option<String>,
}

impl OptNointroSizes {
    fn execute(self) -> Result<(), Error> {
        display_dir_sizes(
            dirs::nointro_dirs(),
            read_collected_dbs(DIR_NOINTRO),
            self.search,
            self.sort_by_size,
        );

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroList {
    /// category name
    name: Option<String>,

    search: Option<String>,
}

impl OptNointroList {
    fn execute(self) -> Result<(), Error> {
        match self.name.as_deref() {
            Some(name) => read_named_db::<dat::DatFile>(NOINTRO, DIR_NOINTRO, name)?
                .list(self.search.as_deref()),
            None => dat::DatFile::list_all(read_collected_dbs::<BTreeMap<_, _>, _>(DIR_NOINTRO)),
        }

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroVerify {
    /// ROMs directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// DAT name to verify ROMs for
    #[clap(short = 'D', long = "dat")]
    name: Option<String>,
}

impl OptNointroVerify {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Never;

        let roms = self.roms;

        let name = match self.name {
            Some(name) => name,
            None if roms.is_none() => dirs::select_nointro_name()?,
            None => dirs::select_any_nointro_name()?,
        };

        process_dat(
            read_named_db(NOINTRO, DIR_NOINTRO, &name)?,
            |datfile, pbar| {
                Ok::<_, Never>(datfile.verify(dirs::nointro_roms(roms, &name).as_ref(), pbar))
            },
        )
        .unwrap();

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroVerifyAll {
    /// show all systems in output table
    #[clap(short = 'A', long = "all")]
    show_all: bool,
}

impl OptNointroVerifyAll {
    fn execute(self) -> Result<(), Error> {
        use game::Never;

        process_all_dat(
            "verifying all No-Intro files",
            dirs::nointro_dirs(),
            |name| read_named_db(NOINTRO, DIR_NOINTRO, name),
            |datfile, dir, pbar| Ok::<_, Never>(datfile.verify(dir, pbar)),
            self.show_all,
        )
        .unwrap();

        Ok(())
    }
}

#[derive(Args)]
struct OptNointroRepair {
    /// output directory
    #[clap(short = 'r', long = "roms")]
    roms: Option<PathBuf>,

    /// DAT name to add ROMs to
    #[clap(short = 'D', long = "dat")]
    name: Option<String>,

    /// input file, directory, or URL
    input: Vec<Resource>,
}

impl OptNointroRepair {
    fn execute(self) -> Result<(), Error> {
        let roms = self.roms;
        let name = match self.name {
            Some(name) => name,
            None if roms.is_none() => dirs::select_nointro_name()?,
            None => dirs::select_any_nointro_name()?,
        };
        let datfile: dat::DatFile = read_named_db::<dat::DatFile>(NOINTRO, DIR_NOINTRO, &name)?;
        let mut rom_sources = rom_sources(&self.input);

        process_dat(datfile, |datfile, pbar| {
            datfile.add_and_verify(
                &mut rom_sources,
                dirs::nointro_roms(roms, &name).as_ref(),
                pbar,
            )
        })
    }
}

#[derive(Args)]
struct OptNointroRepairAll {
    /// input file, directory, or URL
    input: Vec<Resource>,

    /// show all systems in output table
    #[clap(short = 'A', long = "all")]
    show_all: bool,
}

impl OptNointroRepairAll {
    fn execute(self) -> Result<(), Error> {
        let mut parts = rom_sources(&self.input);

        process_all_dat(
            "adding and verifying No-Intro files",
            dirs::nointro_dirs(),
            |name| read_named_db(NOINTRO, DIR_NOINTRO, name),
            |datfile, dir, pbar| datfile.add_and_verify(&mut parts, dir, pbar),
            self.show_all,
        )
    }
}

#[derive(Args)]
struct OptNointroParts {
    /// DAT name to find parts for
    #[clap(short = 'D', long = "dat")]
    name: Option<String>,

    /// game's parts to search for
    game: Option<String>,
}

impl OptNointroParts {
    fn execute(self) -> Result<(), Error> {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::Table;

        let mut datfile = match self.name {
            Some(name) => read_named_db::<dat::DatFile>(NOINTRO, DIR_NOINTRO, &name),
            None => {
                let mut dats = read_named_dbs(DIR_NOINTRO)
                    .into_iter()
                    .flatten()
                    .map(|(_, d)| d)
                    .collect::<Vec<dat::DatFile>>();

                dats.sort_unstable_by(|x, y| x.name().cmp(y.name()));

                inquire::Select::new("select DAT", dats)
                    .with_page_size(terminal_height())
                    .prompt()
                    .map_err(Error::Inquire)
            }
        }?;

        let game = match self.game {
            Some(game) => datfile
                .remove_game(&game)
                .ok_or_else(|| Error::NoSuchSoftware(game.to_string()))?,
            None => select_datfile_game(datfile)?,
        };

        let mut table = Table::new();
        table
            .set_header(vec!["Part", "SHA1 Hash"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        for (name, part) in game.into_iter().collect::<BTreeMap<_, _>>() {
            table.add_row(vec![name, part.digest().to_string()]);
        }
        println!("{table}");

        Ok(())
    }
}

#[derive(Subcommand)]
#[clap(name = "nointro")]
enum OptDat {
    /// list ROMs defined in DAT
    List(OptDatList),

    /// verify ROMs defined in DAT
    Verify(OptDatVerify),

    /// add and verify ROMs defined in DAT
    #[clap(alias = "add")]
    Repair(OptDatRepair),

    /// display game's parts in DAT
    Parts(OptDatParts),
}

impl OptDat {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptDat::List(o) => o.execute(),
            OptDat::Verify(o) => o.execute(),
            OptDat::Repair(o) => o.execute(),
            OptDat::Parts(o) => o.execute(),
        }
    }
}

#[derive(Args)]
struct OptDatList {
    dat: Resource,

    search: Option<String>,
}

impl OptDatList {
    fn execute(self) -> Result<(), Error> {
        dat::fetch_and_parse_single(self.dat, |file, datfile| {
            dat::DatFile::new_flattened(datfile)
                .map_err(|error| Error::InvalidSha1(ResourceError { file, error }))
        })?
        .list(self.search.as_deref());

        Ok(())
    }
}

#[derive(Args)]
struct OptDatVerify {
    dat: Resource,

    roms: PathBuf,

    /// interactively edit DAT contents before verifying
    #[clap(long = "edit")]
    edit: bool,
}

impl OptDatVerify {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Never;

        process_dat(
            dat::fetch_and_parse_single(self.dat, |file, datfile| {
                (if self.edit {
                    dat::edit_file(datfile, None)
                } else {
                    Ok(datfile)
                })
                .and_then(|datfile| {
                    dat::DatFile::new_flattened(datfile)
                        .map_err(|error| Error::InvalidSha1(ResourceError { file, error }))
                })
            })?,
            |datfile, pbar| Ok::<_, Never>(datfile.verify(&self.roms, pbar)),
        )
        .unwrap();

        Ok(())
    }
}

#[derive(Args)]
struct OptDatRepair {
    dat: Resource,

    roms: PathBuf,

    /// input file, directory, or URL
    input: Vec<Resource>,

    /// interactively edit DAT contents before verifying
    #[clap(long = "edit")]
    edit: bool,
}

impl OptDatRepair {
    fn execute(self) -> Result<(), Error> {
        let mut rom_sources = rom_sources(&self.input);

        process_dat(
            dat::fetch_and_parse_single(self.dat, |file, datfile| {
                (if self.edit {
                    dat::edit_file(datfile, None)
                } else {
                    Ok(datfile)
                })
                .and_then(|datfile| {
                    dat::DatFile::new_flattened(datfile)
                        .map_err(|error| Error::InvalidSha1(ResourceError { file, error }))
                })
            })?,
            |datfile, pbar| datfile.add_and_verify(&mut rom_sources, &self.roms, pbar),
        )
    }
}

#[derive(Args)]
struct OptDatParts {
    dat: Resource,

    /// game's parts to search for
    game: Option<String>,
}

impl OptDatParts {
    fn execute(self) -> Result<(), Error> {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::Table;

        let mut datfile = dat::fetch_and_parse_single(self.dat, |file, datfile| {
            dat::DatFile::new_flattened(datfile)
                .map_err(|error| Error::InvalidSha1(ResourceError { file, error }))
        })?;

        let game = match self.game {
            Some(game) => datfile
                .remove_game(&game)
                .ok_or_else(|| Error::NoSuchSoftware(game.to_string()))?,
            None => select_datfile_game(datfile)?,
        };

        let mut table = Table::new();
        table
            .set_header(vec!["Part", "SHA1 Hash"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        for (name, part) in game.into_iter().collect::<BTreeMap<_, _>>() {
            table.add_row(vec![name, part.digest().to_string()]);
        }
        println!("{table}");

        Ok(())
    }
}

#[derive(Args)]
struct OptIdentify {
    /// ROMs or CHDs to identify
    resources: Vec<Resource>,

    /// perform reverse lookup
    #[clap(short = 'l', long = "lookup")]
    lookup: bool,
}

impl OptIdentify {
    fn execute(self) -> Result<(), Error> {
        use crate::dat::DatFile;
        use crate::game::{GameDb, Part};
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;
        use comfy_table::Table;
        use indicatif::{ProgressDrawTarget, ProgressIterator};
        use std::collections::{BTreeSet, HashMap};

        let mbar = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(2));
        let pbar1 = mbar.add(
            ProgressBar::new(self.resources.len().try_into().unwrap())
                .with_style(game::verify_style()),
        );
        pbar1.set_message("retrieving ROMs");

        if self.lookup {
            use iter_group::IntoGroup;

            let mame_db: GameDb = read_game_db(MAME, DB_MAME).unwrap_or_default();
            let mess_db: BTreeMap<String, GameDb> = read_collected_dbs(DIR_SL);

            let dat_parts: [(&str, BTreeMap<String, DatFile>); 3] = [
                ("extra", read_collected_dbs(DIR_EXTRA)),
                ("nointro", read_collected_dbs(DIR_NOINTRO)),
                ("redump", read_collected_dbs(DIR_REDUMP)),
            ];

            let lookup = mame_db
                .games_iter()
                .flat_map(|game| {
                    game.parts
                        .iter()
                        .map(move |(rom, part)| (part, ["mame", "", game.name.as_str(), rom]))
                })
                .chain(mess_db.iter().flat_map(|(system, game_db)| {
                    game_db.games_iter().flat_map(move |game| {
                        game.parts
                            .iter()
                            .map(move |(rom, part)| (part, ["sl", system, game.name.as_str(), rom]))
                    })
                }))
                .chain(dat_parts.iter().flat_map(|(category, datfiles)| {
                    datfiles.iter().flat_map(move |(system, datfile)| {
                        datfile.game_parts().flat_map(move |(game, parts)| {
                            parts.iter().map(move |(rom, part)| {
                                (part, [category, system.as_str(), game, rom])
                            })
                        })
                    })
                }))
                .filter(|(part, _)| !part.is_placeholder())
                .group::<HashMap<&Part, BTreeSet<[&str; 4]>>>();

            let mut table = Table::new();
            table
                .set_header(vec!["Source", "Category", "System", "Game", "Part"])
                .load_preset(UTF8_FULL_CONDENSED)
                .apply_modifier(UTF8_ROUND_CORNERS);

            for resource in self.resources.into_iter().progress_with(pbar1) {
                for (part, source) in resource.rom_sources(&mbar) {
                    for [category, system, game, rom] in lookup.get(&part).into_iter().flatten() {
                        table.add_row(vec![
                            source.to_string().as_str(),
                            category,
                            system,
                            game,
                            rom,
                        ]);
                    }
                }
            }

            mbar.clear().unwrap();

            println!("{table}");
        } else {
            for resource in self.resources.into_iter().progress_with(pbar1) {
                for (part, source) in resource.rom_sources(&mbar) {
                    mbar.println(format!("{}  {}", part.digest(), source))
                        .unwrap();
                }
            }

            mbar.clear().unwrap();
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
    paths: Vec<PathBuf>,
}

impl OptCacheAdd {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Part;
        use indicatif::ParallelProgressIterator;
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
    paths: Vec<PathBuf>,
}

impl OptCacheDelete {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Part;

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
    paths: Vec<PathBuf>,
}

impl OptCacheVerify {
    fn execute(self) -> Result<(), Error> {
        use crate::game::Part;
        use indicatif::ParallelProgressIterator;
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
            .for_each(|(file, part)| match part.is_valid(file) {
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
    paths: Vec<PathBuf>,
}

impl OptCacheLinkDupes {
    fn execute(self) -> Result<(), Error> {
        use crate::duplicates::{DuplicateFiles, Duplicates};

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
                        .and_then(|()| fs::hard_link(original, &duplicate))
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

    /// raw DAT management
    #[clap(subcommand)]
    Dat(OptDat),

    /// identify ROM or CHD by hash
    Identify(OptIdentify),

    /// file cache management
    #[clap(subcommand)]
    Cache(OptCache),
}

impl Opt {
    fn execute(self) -> Result<(), Error> {
        promote_dbs()?;

        match self {
            Opt::Mame(o) => o.execute(),
            Opt::Sl(o) => o.execute(),
            Opt::Extra(o) => o.execute(),
            Opt::Redump(o) => o.execute(),
            Opt::Nointro(o) => o.execute(),
            Opt::Dat(o) => o.execute(),
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

fn write_game_db<S>(db_file: &'static str, db: S) -> Result<(), Error>
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
    let f = BufWriter::new(File::create(path)?);
    ciborium::ser::into_writer(&db, f).map_err(Error::CborWrite)?;
    Ok(())
}

fn read_game_db<D>(utility: &'static str, db_file: &'static str) -> Result<D, Error>
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

fn named_db_dir(db_dir: &'static str) -> PathBuf {
    directories::ProjectDirs::from("", "", "EmuMan")
        .expect("no valid home directory found")
        .data_local_dir()
        .join(db_dir)
}

const BASE64_ENGINE: base64::engine::GeneralPurpose = base64::engine::general_purpose::URL_SAFE;

// names might contain slashes, so we'll encode them
// into base64 to ensure they stay in the directory we put them in
fn named_db_path(db_dir: &'static str, name: &str) -> PathBuf {
    named_db_dir(db_dir).join(BASE64_ENGINE.encode(name))
}

// extracts database name from existing path, if any
fn path_db_name(path: &Path) -> Option<String> {
    String::from_utf8(BASE64_ENGINE.decode(path.file_name()?.to_str()?).ok()?).ok()
}

fn write_named_db<S: Serialize>(db_dir: &'static str, name: &str, cache: S) -> Result<(), Error> {
    use std::fs::create_dir_all;
    use std::io::BufWriter;

    let path = named_db_path(db_dir, name);

    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }

    ciborium::ser::into_writer(&cache, BufWriter::new(File::create(&path)?))
        .map_err(Error::CborWrite)?;

    Ok(())
}

fn read_named_db<D: DeserializeOwned>(
    utility: &'static str,
    db_dir: &'static str,
    name: &str,
) -> Result<D, Error> {
    ciborium::de::from_reader(
        File::open(named_db_path(db_dir, name))
            .map(std::io::BufReader::new)
            .map_err(|_| Error::MissingCache(utility))?,
    )
    .map_err(|_| Error::InvalidCache(utility))
}

fn clear_named_dbs(db_dir: &'static str) -> Result<(), Error> {
    let files: Vec<_> = std::fs::read_dir(named_db_dir(db_dir))
        .map(|dir| dir.filter_map(|e| e.map(|e| e.path()).ok()).collect())
        .unwrap_or_default();

    files
        .into_iter()
        .try_for_each(std::fs::remove_file)
        .map_err(Error::IO)
}

fn destroy_named_db(db_dir: &'static str, name: &str) -> Result<(), Error> {
    let path = named_db_path(db_dir, name);
    if path.is_file() {
        std::fs::remove_file(path).map_err(Error::IO)
    } else {
        Err(Error::NoSuchDatFile(name.to_owned()))
    }
}

fn read_named_dbs<D>(db_dir: &'static str) -> Option<impl Iterator<Item = (String, D)>>
where
    D: DeserializeOwned,
{
    #[inline]
    fn read_game_db<D: DeserializeOwned>(path: &Path) -> Option<(String, D)> {
        Some((
            path_db_name(path)?,
            File::open(path)
                .ok()
                .map(std::io::BufReader::new)
                .and_then(|f| ciborium::de::from_reader(f).ok())?,
        ))
    }

    match std::fs::read_dir(named_db_dir(db_dir)) {
        Ok(dir) => Some(dir.filter_map(|entry| {
            entry
                .ok()
                .map(|entry| entry.path())
                .and_then(|path| read_game_db(&path))
        })),
        Err(_) => None,
    }
}

fn read_db_names(db_dir: &'static str) -> Option<impl Iterator<Item = String>> {
    match std::fs::read_dir(named_db_dir(db_dir)) {
        Ok(dir) => Some(dir.filter_map(|entry| {
            entry
                .ok()
                .map(|entry| entry.path())
                .and_then(|path| path_db_name(&path))
        })),
        Err(_) => None,
    }
}

fn read_collected_dbs<C, D>(db_dir: &'static str) -> C
where
    C: std::iter::FromIterator<(String, D)>,
    D: DeserializeOwned,
{
    read_named_dbs(db_dir).into_iter().flatten().collect()
}

fn select_game_name(db: &game::GameDb) -> Result<&str, Error> {
    struct DbEntry<'s> {
        name: &'s str,
        description: &'s str,
    }

    impl<'s> fmt::Display for DbEntry<'s> {
        #[inline]
        fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
            self.description.fmt(f)
        }
    }

    let mut games = db
        .games_iter()
        .map(|g| DbEntry {
            name: &g.name,
            description: &g.description,
        })
        .collect::<Vec<_>>();
    games.sort_unstable_by_key(|g| g.description);

    inquire::Select::new("select game", games)
        .with_page_size(terminal_height())
        .prompt()
        .map(|DbEntry { name, .. }| name)
        .map_err(Error::Inquire)
}

fn select_game_names(db: &game::GameDb) -> Result<Vec<&str>, Error> {
    struct DbEntry<'s> {
        name: &'s str,
        description: &'s str,
    }

    impl<'s> fmt::Display for DbEntry<'s> {
        #[inline]
        fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
            self.description.fmt(f)
        }
    }

    let mut games = db
        .games_iter()
        .map(|g| DbEntry {
            name: &g.name,
            description: &g.description,
        })
        .collect::<Vec<_>>();
    games.sort_unstable_by_key(|g| g.description);

    inquire::MultiSelect::new("select game", games)
        .with_page_size(terminal_height())
        .prompt()
        .map(|entries| {
            entries
                .into_iter()
                .map(|DbEntry { name, .. }| name)
                .collect()
        })
        .map_err(Error::Inquire)
}

fn select_software_list_and_name() -> Result<(game::GameDb, String), Error> {
    struct DbEntry {
        shortname: String,
        db: game::GameDb,
    }

    impl fmt::Display for DbEntry {
        #[inline]
        fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
            self.db.description().fmt(f)
        }
    }

    let software_lists: mess::MessDb = read_collected_dbs(DIR_SL);

    if software_lists.is_empty() {
        Err(Error::NoSoftwareLists)
    } else {
        inquire::Select::new(
            "select software list",
            software_lists
                .into_iter()
                .map(|(shortname, db)| DbEntry { shortname, db })
                .collect(),
        )
        .with_page_size(terminal_height())
        .prompt()
        .map(|DbEntry { db, shortname }| (db, shortname))
        .map_err(Error::Inquire)
    }
}

#[inline]
fn select_software_list() -> Result<game::GameDb, Error> {
    select_software_list_and_name().map(|(db, _)| db)
}

fn select_software_list_game(db: game::GameDb) -> Result<game::Game, Error> {
    struct GameEntry {
        game: game::Game,
    }

    impl fmt::Display for GameEntry {
        #[inline]
        fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
            self.game.description.fmt(f)
        }
    }

    let mut games = db
        .into_games()
        .map(|game| GameEntry { game })
        .collect::<Vec<_>>();
    games.sort_unstable_by(|x, y| x.game.description.cmp(&y.game.description));

    inquire::Select::new("select game", games)
        .with_page_size(terminal_height())
        .prompt()
        .map(|GameEntry { game }| game)
        .map_err(Error::Inquire)
}

fn select_datfile_game(dat: dat::DatFile) -> Result<game::GameParts, Error> {
    struct GameEntry {
        name: String,
        game: game::GameParts,
    }

    impl fmt::Display for GameEntry {
        #[inline]
        fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
            self.name.fmt(f)
        }
    }

    let mut games = dat
        .into_game_parts()
        .map(|(name, game)| GameEntry { name, game })
        .collect::<Vec<_>>();

    match games.len() {
        0 => Err(Error::EmptyDatFile),
        1 => Ok(games.pop().unwrap().game),
        _ => {
            games.sort_unstable_by(|x, y| x.name.cmp(&y.name));

            inquire::Select::new("select game", games)
                .with_page_size(terminal_height())
                .prompt()
                .map(|GameEntry { game, .. }| game)
                .map_err(Error::Inquire)
        }
    }
}

// takes older config formats and converts them to the new style
// so that one doesn't have to re-init everything
fn promote_dbs() -> Result<(), Error> {
    fn promote<D, S>(
        config_name: &'static str,
        old_file: &'static str,
        new_dir: &'static str,
    ) -> Result<(), Error>
    where
        D: DeserializeOwned + IntoIterator<Item = (String, S)>,
        S: Serialize,
    {
        let old_config_name = named_db_dir(old_file);

        if !named_db_dir(new_dir).is_dir() && old_config_name.is_file() {
            if let Ok(old_config) = read_game_db::<D>("", old_file) {
                eprintln!("updating \"{config_name}\" config to new format");

                old_config
                    .into_iter()
                    .try_for_each(|(name, system)| write_named_db(new_dir, &name, system))
                    .and_then(|()| std::fs::remove_file(old_config_name).map_err(Error::IO))?;
            }
        }

        Ok(())
    }

    promote::<BTreeMap<String, game::GameDb>, game::GameDb>("sl", "mess.cbor", DIR_SL)?;
    promote::<BTreeMap<String, dat::DatFile>, dat::DatFile>("extra", "extra.cbor", DIR_EXTRA)?;
    promote::<BTreeMap<String, dat::DatFile>, dat::DatFile>("redump", "redump.cbor", DIR_REDUMP)?;
    promote::<BTreeMap<String, dat::DatFile>, dat::DatFile>(
        "nointro",
        "nointro.cbor",
        DIR_NOINTRO,
    )?;

    Ok(())
}

fn process_games<'g, I, P, E>(
    message: &'static str,
    root: P,
    games: I,
    handle_game: impl Fn(&'g game::Game, &Path, &ProgressBar) -> Result<Vec<game::VerifyFailure<'g>>, E>
        + Sync,
) -> Result<(), E>
where
    P: AsRef<Path> + Sync,
    I: ExactSizeIterator<Item = &'g game::Game> + Send,
    E: Send,
{
    use indicatif::ParallelProgressIterator;
    use rayon::prelude::*;

    let total = games.len();

    let pbar = ProgressBar::new(total.try_into().unwrap())
        .with_style(game::verify_style())
        .with_message(message);

    let results = games
        .par_bridge()
        .progress_with(pbar.clone())
        .map(|game| handle_game(game, root.as_ref(), &pbar))
        .collect::<Result<Vec<_>, _>>()?;

    pbar.finish_and_clear();

    let successes = results.iter().filter(|v| v.is_empty()).count();

    let mut failures = results.into_iter().flatten().collect::<Vec<_>>();
    failures.sort_unstable_by(|x, y| x.path().cmp(y.path()));
    failures.dedup_by(|x, y| x.path() == y.path());

    for failure in failures {
        println!("{failure}");
    }

    eprintln!("{total} tested, {successes} OK");

    Ok(())
}

fn verify<'g, I, P>(db: &'g game::GameDb, root: P, games: I)
where
    P: AsRef<Path> + Sync,
    I: ExactSizeIterator<Item = &'g game::Game>,
    I: Send,
{
    process_games("verifying games", root, games, |game, root, _| {
        Ok::<_, game::Never>(db.verify(root, game))
    })
    .unwrap()
}

#[inline]
fn add_and_verify<'g, I, P>(roms: &mut game::RomSources, root: P, games: I) -> Result<(), Error>
where
    P: AsRef<Path> + Sync,
    I: ExactSizeIterator<Item = &'g game::Game>,
    I: Send,
{
    process_games(
        "adding and verifying games",
        root,
        games,
        |game, root, pbar| {
            game.add_and_verify(roms, root.as_ref(), |r| {
                pbar.println(format!("{r}"));
                r.into_fixed_pathbuf()
            })
        },
    )
}

fn process_all_mess<E>(
    message: &'static str,
    roms: Option<PathBuf>,
    handle_parts: impl for<'g> Fn(
            &'g game::GameParts,
            &Path,
            &MultiProgress,
        ) -> Result<Vec<game::VerifyFailure<'g>>, E>
        + Sync,
    show_all: bool,
) -> Result<(), E>
where
    E: Send,
{
    use crate::game::verify_style;
    use indicatif::{ParallelProgressIterator, ProgressDrawTarget, ProgressIterator};

    let roms_dir = dirs::mess_roms_all(roms);
    let mut total = game::VerifyResultsSummary::default();
    let mut table = init_dat_table();
    let dbs = read_collected_dbs::<BTreeMap<_, _>, game::GameDb>(DIR_SL);

    let mbar = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(2));
    let pbar1 =
        mbar.add(ProgressBar::new(dbs.len().try_into().unwrap()).with_style(verify_style()));
    pbar1.set_message(message);

    for (software_list, db) in dbs.into_iter().progress_with(pbar1.clone()) {
        use crate::game::{Game, VerifyFailure};
        use rayon::prelude::*;

        let pbar2 = mbar.insert_after(
            &pbar1,
            ProgressBar::new(db.len().try_into().unwrap()).with_style(verify_style()),
        );
        pbar2.set_message(software_list.clone());

        let db_root = roms_dir.as_ref().join(&software_list);

        let mut results = db
            .games_map()
            .par_iter()
            .progress_with(pbar2.clone())
            .map(|(_, Game { name, parts, .. })| {
                Ok((
                    name.as_str(),
                    handle_parts(parts, &db_root.join(name), &mbar)?,
                ))
            })
            .collect::<Result<BTreeMap<&str, Vec<VerifyFailure>>, E>>()?;

        results
            .values_mut()
            .for_each(|v| v.sort_unstable_by(|x, y| x.path().cmp(y.path())));

        let db_total = game::VerifyResultsSummary {
            successes: results.values().filter(|v| v.is_empty()).count(),
            total: db.len(),
        };

        for (_, failures) in results {
            for failure in failures {
                mbar.println(format!("{failure}")).unwrap();
            }
        }

        if show_all || (db_total.successes != db_total.total) {
            table.add_row(db_total.row(&software_list));
        }
        total += db_total;
        mbar.remove(&pbar2);
    }

    mbar.clear().unwrap();
    display_dat_table(table, Some(total));

    Ok(())
}

fn process_dat<E>(
    datfile: dat::DatFile,
    process: impl for<'d> FnOnce(
        &'d dat::DatFile,
        &indicatif::ProgressBar,
    ) -> Result<dat::VerifyResults<'d>, E>,
) -> Result<(), E> {
    let mut table = init_dat_table();
    let pbar = datfile.progress_bar();
    let dat::VerifyResults { failures, summary } = process(&datfile, &pbar)?;
    pbar.finish_and_clear();
    for failure in failures {
        println!("{failure}");
    }
    table.add_row(summary.row(datfile.name()));
    display_dat_table(table, None);

    Ok(())
}

fn process_all_dat<I, E>(
    message: &'static str,
    dirs: I,
    read_named_db: impl Fn(&str) -> Result<dat::DatFile, Error>,
    mut process_dat: impl for<'d> FnMut(
        &'d dat::DatFile,
        &Path,
        &indicatif::ProgressBar,
    ) -> Result<dat::VerifyResults<'d>, E>,
    show_all: bool,
) -> Result<(), E>
where
    I: ExactSizeIterator<Item = (String, PathBuf)>,
{
    use game::verify_style;
    use indicatif::{ProgressDrawTarget, ProgressIterator};

    let mbar = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(2));
    let pbar1 =
        mbar.add(ProgressBar::new(dirs.len().try_into().unwrap()).with_style(verify_style()));
    pbar1.set_message(message);

    let mut table = init_dat_table();
    let mut total = game::VerifyResultsSummary::default();
    for (name, dir) in dirs.progress_with(pbar1.clone()) {
        if let Ok(datfile) = read_named_db(&name) {
            let pbar2 = mbar.insert_after(&pbar1, datfile.progress_bar());
            let dat::VerifyResults { failures, summary } = process_dat(&datfile, &dir, &pbar2)?;
            pbar2.finish_and_clear();
            for failure in failures {
                mbar.println(format!("{}", failure)).unwrap();
            }
            if show_all || (summary.successes != summary.total) {
                table.add_row(summary.row(datfile.name()));
            }
            total += summary;
            mbar.remove(&pbar2);
        }
    }
    display_dat_table(table, Some(total));

    Ok(())
}

fn display_dirs<D>(
    dirs: D,
    db: BTreeMap<String, dat::DatFile>,
    search: Option<String>,
    sort_by_version: bool,
) where
    D: Iterator<Item = (String, PathBuf)>,
{
    use comfy_table::modifiers::UTF8_ROUND_CORNERS;
    use comfy_table::presets::UTF8_FULL_CONDENSED;
    use comfy_table::Table;

    let mut results: Vec<[String; 3]> = dirs
        .filter_map(|(name, dir)| {
            db.get(&name)
                .filter(|dat| match &search {
                    Some(search) => dat.version().contains(search) || dat.name().contains(search),
                    None => true,
                })
                .map(|dat| {
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
    table
        .set_header(vec!["Version", "DAT Name", "Directory"])
        .load_preset(UTF8_FULL_CONDENSED)
        .apply_modifier(UTF8_ROUND_CORNERS);

    for [version, name, dir] in results {
        table.add_row(vec![version, name, dir]);
    }
    println!("{table}");
}

fn display_dir_sizes<D>(
    dirs: D,
    db: BTreeMap<String, dat::DatFile>,
    search: Option<String>,
    sort_by_size: bool,
) where
    D: Iterator<Item = (String, PathBuf)>,
{
    use crate::game::FileSize;
    use comfy_table::modifiers::UTF8_ROUND_CORNERS;
    use comfy_table::presets::UTF8_FULL_CONDENSED;
    use comfy_table::{Cell, CellAlignment, Table};

    struct Size(u64);

    impl fmt::Display for Size {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            const K: f64 = (1 << 10) as f64;
            const M: f64 = (1 << 20) as f64;
            const G: f64 = (1 << 30) as f64;
            const T: f64 = (1u64 << 40) as f64;

            match self.0 {
                b if b < (1 << 10) => write!(f, "{:.2} B", b),
                b if b < (1 << 20) => write!(f, "{:.2} KiB", b as f64 / K),
                b if b < (1 << 30) => write!(f, "{:.2} MiB", b as f64 / M),
                b if b < (1 << 40) => write!(f, "{:.2} GiB", b as f64 / G),
                b => write!(f, "{:.2} TiB", b as f64 / T),
            }
        }
    }

    let mut results: Vec<(FileSize, String, PathBuf)> = dirs
        .filter_map(|(name, dir)| {
            db.get(&name)
                .filter(|dat| match &search {
                    Some(search) => dat.version().contains(search) || dat.name().contains(search),
                    None => true,
                })
                .map(|dat| (dat.size(&dir), dat.name().to_owned(), dir))
        })
        .collect();

    if sort_by_size {
        results.sort_unstable_by(|x, y| x.0.cmp(&y.0));
    }

    if !results.is_empty() {
        let total = results.iter().map(|(size, _, _)| *size).sum();
        results.push((total, "Total".to_owned(), PathBuf::default()));
    }

    let mut table = Table::new();
    table
        .set_header(vec!["Size", "Real Size", "DAT Name", "Directory"])
        .load_preset(UTF8_FULL_CONDENSED)
        .apply_modifier(UTF8_ROUND_CORNERS);

    for (FileSize { len, real }, name, dir) in results {
        table.add_row(vec![
            Cell::new(Size(len)).set_alignment(CellAlignment::Right),
            Cell::new(Size(real)).set_alignment(CellAlignment::Right),
            Cell::new(name),
            Cell::new(dir.to_string_lossy()),
        ]);
    }
    println!("{table}");
}

fn init_dat_table() -> comfy_table::Table {
    use comfy_table::modifiers::UTF8_ROUND_CORNERS;
    use comfy_table::presets::UTF8_FULL_CONDENSED;
    use comfy_table::{Cell, CellAlignment};

    let mut table = comfy_table::Table::new();
    table
        .set_header(vec![
            Cell::new("Tested").set_alignment(CellAlignment::Right),
            Cell::new("OK").set_alignment(CellAlignment::Right),
            Cell::new(""),
        ])
        .load_preset(UTF8_FULL_CONDENSED)
        .apply_modifier(UTF8_ROUND_CORNERS);

    table
}

fn display_dat_table(mut table: comfy_table::Table, summary: Option<game::VerifyResultsSummary>) {
    if let Some(summary) = summary {
        table.add_row(summary.row("Total"));
    }
    println!("{table}");
}

fn rom_sources(sources: &[Resource]) -> game::RomSources {
    use indicatif::{ParallelProgressIterator, ProgressDrawTarget};
    use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

    fn merge_sources<'u>(
        mut base: game::RomSources<'u>,
        mut extend: game::RomSources<'u>,
    ) -> game::RomSources<'u> {
        use dashmap::mapref::entry::Entry;

        if extend.len() > base.len() {
            std::mem::swap(&mut base, &mut extend);
        }

        for (part, source) in extend {
            match base.entry(part) {
                Entry::Occupied(mut o) if source.more_local_than(o.get()) => {
                    o.insert(source);
                }
                Entry::Occupied(_) => {}
                Entry::Vacant(v) => {
                    v.insert(source);
                }
            }
        }

        base
    }

    let mbar = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(2));
    let pbar1 = mbar
        .add(ProgressBar::new(sources.len().try_into().unwrap()).with_style(game::verify_style()));
    pbar1.set_message("retrieving ROMs");

    let results = sources
        .par_iter()
        .progress_with(pbar1)
        .map(|r| r.rom_sources(&mbar))
        .collect::<Vec<_>>()
        .into_iter()
        .fold(game::empty_rom_sources(), |acc, r| merge_sources(acc, r));

    mbar.clear().unwrap();

    results
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
