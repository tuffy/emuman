use rayon::prelude::*;
use roxmltree::Document;
use rusqlite::Connection;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

mod mame;
mod mess;
mod redump;
mod report;
mod rom;

static CACHE_MAME_ADD: &str = "mame-add.db";
static CACHE_MAME_VERIFY: &str = "mame-verify.db";
static CACHE_MAME_REPORT: &str = "mame-report.db";
static CACHE_MESS_ADD: &str = "mess-add.db";
static CACHE_MESS_VERIFY: &str = "mess-verify.db";
static CACHE_MESS_REPORT: &str = "mess-report.db";
static CACHE_MESS_SPLIT: &str = "mess-split.db";
static CACHE_REDUMP_VERIFY: &str = "redump-verify.db";
static CACHE_REDUMP_SPLIT: &str = "redump-split.db";

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    XML(roxmltree::Error),
    SQL(rusqlite::Error),
    CBOR(serde_cbor::Error),
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

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::IO(err) => err.description(),
            Error::XML(err) => err.description(),
            Error::SQL(err) => err.description(),
            Error::CBOR(err) => err.description(),
        }
    }

    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IO(err) => Some(err),
            Error::XML(err) => Some(err),
            Error::SQL(err) => Some(err),
            Error::CBOR(err) => Some(err),
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
        let mut xml_data = String::new();

        if let Some(path) = self.xml {
            File::open(path).and_then(|mut xml| xml.read_to_string(&mut xml_data))?;
        } else {
            use std::io::stdin;

            stdin().read_to_string(&mut xml_data)?;
        }

        let xml = Document::parse(&xml_data)?;

        if let Some(db_file) = self.database {
            let mut db = open_db(&db_file)?;
            let trans = db.transaction()?;

            mame::create_tables(&trans)?;
            mame::clear_tables(&trans)?;
            mame::xml_to_db(&xml, &trans)?;
            trans.commit()?;

            println!("* Wrote \"{}\"", db_file.display());
        }

        write_cache(CACHE_MAME_VERIFY, mame::VerifyDb::from_xml(&xml))?;
        write_cache(CACHE_MAME_REPORT, mame::ReportDb::from_xml(&xml))?;
        write_cache(CACHE_MAME_ADD, mame::AddDb::from_xml(&xml))?;

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMameAdd {
    /// input directory
    #[structopt(short = "i", long = "input", parse(from_os_str), default_value = ".")]
    input: PathBuf,

    /// output directory
    #[structopt(short = "o", long = "output", parse(from_os_str), default_value = ".")]
    output: PathBuf,

    /// don't actually add ROMs
    #[structopt(long = "dry-run")]
    dry_run: bool,

    /// machine to add
    machines: Vec<String>,
}

impl OptMameAdd {
    fn execute(self) -> Result<(), Error> {
        use walkdir::WalkDir;

        let roms: Vec<PathBuf> = WalkDir::new(&self.input)
            .into_iter()
            .filter_map(|e| {
                e.ok()
                    .filter(|e| e.file_type().is_file())
                    .map(|e| e.into_path())
            })
            .collect();

        let mut db: mame::AddDb = read_cache(CACHE_MAME_ADD)?;
        db.retain_machines(&self.machines.clone().into_iter().collect());

        roms.par_iter()
            .try_for_each(|rom| mame::copy(&db, &self.output, rom, self.dry_run))
            .map_err(Error::IO)
    }
}

#[derive(StructOpt)]
struct OptMameVerify {
    /// root directory
    #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
    root: PathBuf,

    /// machine to verify
    machines: Vec<String>,
}

impl OptMameVerify {
    fn execute(self) -> Result<(), Error> {
        use std::sync::Mutex;

        let machines: HashSet<String> = if !self.machines.is_empty() {
            self.machines.clone().into_iter().collect()
        } else {
            self.root
                .read_dir()?
                .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
                .collect()
        };

        let romdb: mame::VerifyDb = read_cache(CACHE_MAME_VERIFY)?;

        let (devices, games): (Vec<String>, Vec<String>) =
            machines.into_iter().partition(|m| romdb.is_device(m));
        let devices: Mutex<HashSet<String>> = Mutex::new(devices.into_iter().collect());

        games.par_iter().for_each(|game| {
            match mame::verify(&romdb, &self.root, &game) {
                Ok(VerifyResult::NoMachine) => {}
                Ok(VerifyResult::Ok(verified)) => {
                    devices.lock().unwrap().retain(|d| !verified.contains(d));
                    println!("{} : OK", game);
                }
                Ok(VerifyResult::Bad(mismatches)) => {
                    use std::io::{stdout, Write};

                    // ensure results are generated as a unit
                    let stdout = stdout();
                    let mut handle = stdout.lock();
                    writeln!(&mut handle, "{} : BAD", game).unwrap();
                    for mismatch in mismatches {
                        writeln!(&mut handle, "  {}", mismatch).unwrap();
                    }
                }
                Err(err) => println!("{} : ERROR : {}", game, err),
            }
        });

        for device in devices.into_inner().unwrap().into_iter() {
            println!("{} : UNUSED DEVICE", device);
        }

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMameList {
    /// sorting order, use "description", "year" or "publisher"
    #[structopt(short = "s", long = "sort", default_value = "description")]
    sort: report::SortBy,

    /// display simple list with less information
    #[structopt(short = "S", long = "simple")]
    simple: bool,

    /// search term for querying specific machines
    search: Option<String>,
}

impl OptMameList {
    fn execute(self) -> Result<(), Error> {
        mame::list(
            &read_cache(CACHE_MAME_REPORT)?,
            self.search.as_ref().map(|s| s.deref()),
            self.sort,
            self.simple,
        );
        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMameReport {
    /// sorting order, use "description", "year" or "manufacturer"
    #[structopt(short = "s", long = "sort", default_value = "description")]
    sort: report::SortBy,

    /// root directory
    #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
    root: PathBuf,

    /// display simple report with less information
    #[structopt(short = "S", long = "simple")]
    simple: bool,

    /// search term for querying specific machines
    search: Option<String>,
}

impl OptMameReport {
    fn execute(self) -> Result<(), Error> {
        let machines: HashSet<String> = self
            .root
            .read_dir()?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect();

        mame::report(
            &read_cache(CACHE_MAME_REPORT)?,
            &machines,
            self.search.as_ref().map(|s| s.deref()),
            self.sort,
            self.simple,
        );
        Ok(())
    }
}

#[derive(StructOpt)]
#[structopt(name = "mame")]
enum OptMame {
    /// create internal database
    #[structopt(name = "create")]
    Create(OptMameCreate),

    /// add ROMs to directory
    #[structopt(name = "add")]
    Add(OptMameAdd),

    /// verify ROMs in directory
    #[structopt(name = "verify")]
    Verify(OptMameVerify),

    /// list all machines
    #[structopt(name = "list")]
    List(OptMameList),

    /// generate report of sets in collection
    #[structopt(name = "report")]
    Report(OptMameReport),
}

impl OptMame {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptMame::Create(o) => o.execute(),
            OptMame::Add(o) => o.execute(),
            OptMame::Verify(o) => o.execute(),
            OptMame::List(o) => o.execute(),
            OptMame::Report(o) => o.execute(),
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
        }

        let mut verify = mess::VerifyDb::default();
        let mut report = mess::ReportDb::default();
        let mut add = mess::AddDb::default();
        let mut split = mess::SplitDb::default();

        for file in self.xml.iter() {
            let mut xml_data = String::new();

            File::open(file).and_then(|mut f| f.read_to_string(&mut xml_data))?;

            let tree = Document::parse(&xml_data)?;
            let root = tree.root_element();

            verify.add_xml(&root);
            report.add_xml(&root);
            add.add_xml(&root);
            split.add_xml(&root);
        }

        report.sort_software();

        write_cache(CACHE_MESS_VERIFY, &verify)?;
        write_cache(CACHE_MESS_REPORT, &report)?;
        write_cache(CACHE_MESS_ADD, &add)?;
        write_cache(CACHE_MESS_SPLIT, &split)?;

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMessAdd {
    /// input directory
    #[structopt(short = "i", long = "input", parse(from_os_str), default_value = ".")]
    input: PathBuf,

    /// output directory
    #[structopt(short = "o", long = "output", parse(from_os_str), default_value = ".")]
    output: PathBuf,

    /// don't actually add ROMs
    #[structopt(long = "dry-run")]
    dry_run: bool,

    /// software list to use
    software_list: String,

    /// software to add
    software: Vec<String>,
}

impl OptMessAdd {
    fn execute(self) -> Result<(), Error> {
        use walkdir::WalkDir;

        let roms: Vec<PathBuf> = WalkDir::new(&self.input)
            .into_iter()
            .filter_map(|e| {
                e.ok()
                    .filter(|e| e.file_type().is_file())
                    .map(|e| e.into_path())
            })
            .collect();

        let software: HashSet<String> = if !self.software.is_empty() {
            self.software.clone().into_iter().collect()
        } else {
            read_cache::<mess::VerifyDb>(CACHE_MESS_VERIFY)?.into_all_software(&self.software_list)
        };

        if let Some(mut db) =
            read_cache::<mess::AddDb>(CACHE_MESS_ADD)?.into_software_list(&self.software_list)
        {
            db.retain_software(&software);

            roms.par_iter()
                .try_for_each(|rom| mess::copy(&db, &self.output, rom, self.dry_run))
                .map_err(Error::IO)
        } else {
            Ok(())
        }
    }
}

#[derive(StructOpt)]
struct OptMessVerify {
    /// root directory
    #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
    root: PathBuf,

    /// software list to use
    software_list: String,

    /// machine to verify
    software: Vec<String>,
}

impl OptMessVerify {
    fn execute(self) -> Result<(), Error> {
        let software: HashSet<String> = if !self.software.is_empty() {
            self.software.clone().into_iter().collect()
        } else {
            self.root
                .read_dir()?
                .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
                .collect()
        };

        let db = match read_cache::<mess::VerifyDb>(CACHE_MESS_VERIFY)?
            .into_software_list(&self.software_list)
        {
            Some(db) => db,
            None => return Ok(()), // no software to check
        };

        software.par_iter().for_each(|software| {
            match mess::verify(&db, &self.root, &software) {
                Ok(VerifyResult::NoMachine) => {}
                Ok(VerifyResult::Ok(_)) => println!("{} : OK", software),
                Ok(VerifyResult::Bad(mismatches)) => {
                    use std::io::{stdout, Write};

                    // ensure results are generated as a unit
                    let stdout = stdout();
                    let mut handle = stdout.lock();
                    writeln!(&mut handle, "{} : BAD", software).unwrap();
                    for mismatch in mismatches {
                        writeln!(&mut handle, "  {}", mismatch).unwrap();
                    }
                }
                Err(err) => println!("{} : ERROR : {}", software, err),
            }
        });

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMessList {
    /// software list to use
    software_list: Option<String>,

    /// sorting order, use "description", "year" or "publisher"
    #[structopt(short = "s", long = "sort", default_value = "description")]
    sort: report::SortBy,

    /// display simple list with less information
    #[structopt(short = "S", long = "simple")]
    simple: bool,

    /// search term for querying specific items
    search: Option<String>,
}

impl OptMessList {
    fn execute(self) -> Result<(), Error> {
        let db: mess::ReportDb = read_cache(CACHE_MESS_REPORT)?;

        if let Some(software_list) = self.software_list {
            mess::list(
                &db,
                &software_list,
                self.search.as_ref().map(|s| s.deref()),
                self.sort,
                self.simple,
            )
        } else {
            mess::list_all(&db)
        }

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMessReport {
    /// sorting order, use "description", "year" or "publisher"
    #[structopt(short = "s", long = "sort", default_value = "description")]
    sort: report::SortBy,

    /// root directory
    #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
    root: PathBuf,

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
        let db: mess::ReportDb = read_cache(CACHE_MESS_REPORT)?;

        let software: HashSet<String> = self
            .root
            .read_dir()?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect();

        mess::report(
            &db,
            &self.software_list,
            &software,
            self.search.as_ref().map(|s| s.deref()),
            self.sort,
            self.simple,
        );
        Ok(())
    }
}

#[derive(StructOpt)]
struct OptMessSplit {
    /// directory to place split ROM pieces
    #[structopt(short = "o", long = "output", parse(from_os_str), default_value = ".")]
    root: PathBuf,

    /// delete input ROM once split
    #[structopt(long = "delete")]
    delete: bool,

    /// software list
    software_list: String,

    /// ROMs to split
    #[structopt(parse(from_os_str))]
    roms: Vec<PathBuf>,
}

impl OptMessSplit {
    fn execute(self) -> Result<(), Error> {
        let db: mess::SplitDb = read_cache(CACHE_MESS_SPLIT)?;

        if let Some(db) = db.into_software_list(&self.software_list) {
            self.roms.par_iter().try_for_each(|rom| {
                let mut rom_data = Vec::new();
                File::open(&rom).and_then(|mut f| f.read_to_end(&mut rom_data))?;

                if mess::is_ines_format(&rom_data) {
                    mess::remove_ines_header(&mut rom_data);
                }

                if let Some(exact_match) = db
                    .get(&(rom_data.len() as u64))
                    .and_then(|matches| matches.iter().find(|m| m.matches(&rom_data)))
                {
                    exact_match.extract(&self.root, &rom_data)?;
                    if self.delete {
                        use std::fs::remove_file;

                        remove_file(&rom)?;
                    }
                }
                Ok(())
            })
        } else {
            Ok(())
        }
    }
}

#[derive(StructOpt)]
#[structopt(name = "mess")]
enum OptMess {
    /// create internal database
    #[structopt(name = "create")]
    Create(OptMessCreate),

    /// add ROMs to directory
    #[structopt(name = "add")]
    Add(OptMessAdd),

    /// verify ROMs in directory
    #[structopt(name = "verify")]
    Verify(OptMessVerify),

    /// list all software in software list
    #[structopt(name = "list")]
    List(OptMessList),

    /// generate report of sets in collection
    #[structopt(name = "report")]
    Report(OptMessReport),

    /// split ROM into MESS-compatible parts, if necessary
    #[structopt(name = "split")]
    Split(OptMessSplit),
}

impl OptMess {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptMess::Create(o) => o.execute(),
            OptMess::Add(o) => o.execute(),
            OptMess::Verify(o) => o.execute(),
            OptMess::List(o) => o.execute(),
            OptMess::Report(o) => o.execute(),
            OptMess::Split(o) => o.execute(),
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
        }

        let mut verify = redump::VerifyDb::default();
        let mut split = redump::SplitDb::default();

        for file in self.xml.iter() {
            let mut xml_data = String::new();

            File::open(file).and_then(|mut f| f.read_to_string(&mut xml_data))?;

            let tree = Document::parse(&xml_data)?;
            let root = tree.root_element();

            verify.add_xml(&root);
            split.add_xml(&root);
        }

        write_cache(CACHE_REDUMP_VERIFY, &verify)?;
        write_cache(CACHE_REDUMP_SPLIT, &split)?;

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptRedumpVerify {
    /// software to verify
    #[structopt(parse(from_os_str))]
    software: Vec<PathBuf>,
}

impl OptRedumpVerify {
    fn execute(self) -> Result<(), Error> {
        let db = read_cache(CACHE_REDUMP_VERIFY)?;

        self.software
            .par_iter()
            .for_each(|path| match redump::verify(&db, &path) {
                Ok(result) => println!("{} : {}", result, path.display()),
                Err(err) => eprintln!("* {} : {}", path.display(), err),
            });

        Ok(())
    }
}

#[derive(StructOpt)]
struct OptRedumpSplit {
    /// directory to place output tracks
    #[structopt(short = "o", long = "output", parse(from_os_str), default_value = ".")]
    root: PathBuf,

    /// delete input .bin once split
    #[structopt(long = "delete")]
    delete: bool,

    /// input .bin file
    #[structopt(parse(from_os_str))]
    bins: Vec<PathBuf>,
}

impl OptRedumpSplit {
    fn execute(self) -> Result<(), Error> {
        let db: redump::SplitDb = read_cache(CACHE_REDUMP_SPLIT)?;

        self.bins.iter().try_for_each(|bin_path| {
            let mut bin_data = Vec::new();
            File::open(bin_path).and_then(|mut f| f.read_to_end(&mut bin_data))?;

            if let Some(exact_match) = db
                .possible_matches(bin_data.len() as u64)
                .and_then(|matches| matches.iter().find(|m| m.matches(&bin_data)))
            {
                exact_match.extract(&self.root, &bin_data)?;
                if self.delete {
                    use std::fs::remove_file;

                    remove_file(bin_path)?;
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

    /// verify files against Redump database
    #[structopt(name = "verify")]
    Verify(OptRedumpVerify),

    /// split .bin file into multiple tracks
    #[structopt(name = "split")]
    Split(OptRedumpSplit),
}

impl OptRedump {
    fn execute(self) -> Result<(), Error> {
        match self {
            OptRedump::Create(o) => o.execute(),
            OptRedump::Verify(o) => o.execute(),
            OptRedump::Split(o) => o.execute(),
        }
    }
}

/// Emulation Database Manager
#[derive(StructOpt)]
enum Opt {
    /// MAME management
    #[structopt(name = "mame")]
    Mame(OptMame),

    /// MESS management
    #[structopt(name = "mess")]
    Mess(OptMess),

    /// Redump management
    #[structopt(name = "redump")]
    Redump(OptRedump),
}

impl Opt {
    fn execute(self) -> Result<(), Error> {
        match self {
            Opt::Mame(o) => o.execute(),
            Opt::Mess(o) => o.execute(),
            Opt::Redump(o) => o.execute(),
        }
    }
}

fn main() {
    if let Err(err) = Opt::from_args().execute() {
        eprintln!("* {}", err);
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

fn read_cache<D>(db_file: &str) -> Result<D, Error>
where
    D: DeserializeOwned,
{
    use directories::ProjectDirs;
    use std::io::BufReader;

    let dirs = ProjectDirs::from("", "", "EmuMan").expect("no valid home directory");
    let f = BufReader::new(File::open(dirs.data_local_dir().join(db_file))?);
    serde_cbor::from_reader(f).map_err(Error::CBOR)
}

pub enum VerifyMismatch {
    Missing(PathBuf),
    Extra(PathBuf),
    Bad(PathBuf),
}

impl fmt::Display for VerifyMismatch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VerifyMismatch::Missing(pb) => write!(f, "MISSING : {}", pb.display()),
            VerifyMismatch::Extra(pb) => write!(f, "EXTRA : {}", pb.display()),
            VerifyMismatch::Bad(pb) => write!(f, "BAD : {}", pb.display()),
        }
    }
}

pub enum VerifyResult {
    Ok(HashSet<String>),
    Bad(Vec<VerifyMismatch>),
    NoMachine,
}

impl VerifyResult {
    fn nothing_to_check(machine: &str) -> Self {
        let mut set = HashSet::new();
        set.insert(machine.to_string());
        VerifyResult::Ok(set)
    }
}

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
