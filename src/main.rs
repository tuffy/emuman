use rayon::prelude::*;
use roxmltree::Document;
use rusqlite::Connection;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::exit;
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

macro_rules! exit_with_error {
    ($error:expr) => {{
        eprintln!("{}", $error);
        exit(1)
    }};
}

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
#[structopt(name = "mame")]
enum OptMame {
    /// create internal database
    #[structopt(name = "create")]
    Create {
        /// SQLite database
        #[structopt(long = "db", parse(from_os_str))]
        database: Option<PathBuf>,

        /// MAME's XML file
        #[structopt(parse(from_os_str))]
        xml: Option<PathBuf>,
    },

    /// add ROMs to directory
    #[structopt(name = "add")]
    Add {
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
        machine: Vec<String>,
    },

    /// verify ROMs in directory
    #[structopt(name = "verify")]
    Verify {
        /// root directory
        #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
        root: PathBuf,

        /// machine to verify
        machine: Vec<String>,
    },

    /// generate report of sets in collection
    #[structopt(name = "report")]
    Report {
        /// sorting order, use "description", "year" or "manufacturer"
        #[structopt(short = "s", long = "sort")]
        sort: Option<String>,

        /// root directory
        #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
        root: PathBuf,

        /// machines to report on
        machine: Vec<String>,
    },
}

#[derive(StructOpt)]
#[structopt(name = "mess")]
enum OptMess {
    /// create internal database
    #[structopt(name = "create")]
    Create {
        /// SQLite database
        #[structopt(long = "db", parse(from_os_str))]
        database: Option<PathBuf>,

        /// XML files from hash database
        #[structopt(parse(from_os_str))]
        xml: Vec<PathBuf>,
    },

    /// add ROMs to directory
    #[structopt(name = "add")]
    Add {
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
    },

    /// verify ROMs in directory
    #[structopt(name = "verify")]
    Verify {
        /// root directory
        #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
        root: PathBuf,

        /// software list to use
        software_list: String,

        /// machine to verify
        software: Vec<String>,
    },

    /// generate report of sets in collection
    #[structopt(name = "report")]
    Report {
        /// sorting order, use "description", "year" or "publisher"
        #[structopt(short = "s", long = "sort")]
        sort: Option<String>,

        /// root directory
        #[structopt(short = "d", long = "dir", parse(from_os_str), default_value = ".")]
        root: PathBuf,

        /// software list to use
        software_list: Option<String>,

        /// software to generate report on
        software: Vec<String>,
    },

    /// split ROM into MESS-compatible parts, if necessary
    #[structopt(name = "split")]
    Split {
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
        rom: Vec<PathBuf>,
    },
}

#[derive(StructOpt)]
#[structopt(name = "redump")]
enum OptRedump {
    /// create internal database
    #[structopt(name = "create")]
    Create {
        /// SQLite database
        #[structopt(long = "db", parse(from_os_str))]
        database: Option<PathBuf>,

        /// redump XML file
        #[structopt(parse(from_os_str))]
        xml: Vec<PathBuf>,
    },

    /// verify files against Redump database
    #[structopt(name = "verify")]
    Verify {
        /// software to verify
        #[structopt(parse(from_os_str))]
        software: Vec<PathBuf>,
    },

    /// split .bin file into multiple tracks
    #[structopt(name = "split")]
    Split {
        /// directory to place output tracks
        #[structopt(short = "o", long = "output", parse(from_os_str), default_value = ".")]
        root: PathBuf,

        /// delete input .bin once split
        #[structopt(long = "delete")]
        delete: bool,

        /// input .bin file
        #[structopt(parse(from_os_str))]
        bin: PathBuf,
    },
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

fn main() -> Result<(), Error> {
    match Opt::from_args() {
        Opt::Mame(OptMame::Create { xml, database }) => {
            if let Some(path) = xml {
                mame_create(File::open(path)?, database.as_ref().map(|t| t.deref()))?
            } else {
                use std::io::stdin;

                mame_create(stdin(), database.as_ref().map(|t| t.deref()))?
            }
        }
        Opt::Mame(OptMame::Add {
            input,
            output,
            machine,
            dry_run,
        }) => mame_add(&input, &output, &machine, dry_run)?,
        Opt::Mame(OptMame::Verify { root, machine }) => mame_verify(&root, &machine)?,
        Opt::Mame(OptMame::Report {
            root,
            machine,
            sort,
        }) => mame_report(&root, &machine, sort.as_ref().map(|t| t.deref()))?,
        Opt::Mess(OptMess::Create { xml, database }) => {
            mess_create(&xml, database.as_ref().map(|t| t.deref()))?
        }
        Opt::Mess(OptMess::Add {
            input,
            output,
            software_list,
            software,
            dry_run,
        }) => mess_add(&input, &output, &software_list, &software, dry_run)?,
        Opt::Mess(OptMess::Verify {
            root,
            software_list,
            software,
        }) => mess_verify(&root, &software_list, &software)?,
        Opt::Mess(OptMess::Report {
            root,
            software_list,
            software,
            sort,
        }) => mess_report(
            &root,
            software_list.as_ref().map(|t| t.deref()),
            &software,
            sort.as_ref().map(|t| t.deref()),
        )?,
        Opt::Mess(OptMess::Split {
            root,
            software_list,
            rom,
            delete,
        }) => mess_split(&root, &software_list, &rom, delete)?,
        Opt::Redump(OptRedump::Create { xml, database }) => {
            redump_create(&xml, database.as_ref().map(|t| t.deref()))?
        }
        Opt::Redump(OptRedump::Verify { software }) => redump_verify(&software)?,
        Opt::Redump(OptRedump::Split { root, bin, delete }) => redump_split(&root, &bin, delete)?,
    }

    Ok(())
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

fn mame_create<R: Read>(mut xml: R, sqlite_db: Option<&Path>) -> Result<(), Error> {
    let mut xml_data = String::new();

    xml.read_to_string(&mut xml_data)?;

    let xml = Document::parse(&xml_data)?;

    if let Some(db_file) = sqlite_db {
        let mut db = open_db(db_file)?;
        let trans = db.transaction()?;

        mame::create_tables(&trans)
            .and_then(|()| mame::clear_tables(&trans))
            .and_then(|()| mame::xml_to_db(&xml, &trans))
            .and_then(|()| trans.commit().map_err(Error::SQL))?;

        println!("* Wrote \"{}\"", db_file.display());
    }

    write_cache(CACHE_MAME_VERIFY, mame::VerifyDb::from_xml(&xml))?;
    write_cache(CACHE_MAME_REPORT, mame::ReportDb::from_xml(&xml))?;
    write_cache(CACHE_MAME_ADD, mame::AddDb::from_xml(&xml))?;

    Ok(())
}

fn mame_add<S>(input: &Path, output: &Path, machines: &[S], dry_run: bool) -> Result<(), Error>
where
    S: AsRef<str>,
{
    use std::collections::HashSet;
    use walkdir::WalkDir;

    let machines: HashSet<String> = machines.iter().map(|s| s.as_ref().to_string()).collect();

    let roms: Vec<PathBuf> = WalkDir::new(input)
        .into_iter()
        .filter_map(|e| {
            e.ok()
                .filter(|e| e.file_type().is_file())
                .map(|e| e.into_path())
        })
        .collect();

    let mut db: mame::AddDb = read_cache(CACHE_MAME_ADD)?;
    db.retain_machines(&machines);

    roms.par_iter()
        .try_for_each(|rom| mame::copy(&db, output, rom, dry_run))
        .map_err(Error::IO)
}

fn mame_verify<S>(root: &Path, machines: &[S]) -> Result<(), Error>
where
    S: AsRef<str>,
{
    use std::collections::HashSet;

    let machines: HashSet<String> = if !machines.is_empty() {
        machines.iter().map(|s| s.as_ref().to_string()).collect()
    } else {
        root.read_dir()
            .unwrap_or_else(|err| exit_with_error!(err))
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect()
    };

    let romdb = read_cache(CACHE_MAME_VERIFY)?;

    machines.par_iter().for_each(|machine| {
        match mame::verify(&romdb, &root, &machine) {
            Ok(VerifyResult::NoMachine) => {}
            Ok(VerifyResult::Ok) => println!("{} : OK", machine),
            Ok(VerifyResult::Bad(mismatches)) => {
                use std::io::{stdout, Write};

                // ensure results are generated as a unit
                let stdout = stdout();
                let mut handle = stdout.lock();
                writeln!(&mut handle, "{} : BAD", machine).unwrap();
                for mismatch in mismatches {
                    writeln!(&mut handle, "  {}", mismatch).unwrap();
                }
            }
            Err(err) => println!("{} : ERROR : {}", machine, err),
        }
    });

    Ok(())
}

fn mame_report<S>(root: &Path, machines: &[S], sort: Option<&str>) -> Result<(), Error>
where
    S: AsRef<str>,
{
    use std::collections::HashSet;

    let machines: HashSet<String> = if !machines.is_empty() {
        machines.iter().map(|s| s.as_ref().to_string()).collect()
    } else {
        root.read_dir()?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect()
    };

    let sort = match sort {
        Some("manufacturer") => report::SortBy::Manufacturer,
        Some("year") => report::SortBy::Year,
        _ => report::SortBy::Description,
    };

    mame::report(&read_cache(CACHE_MAME_REPORT)?, &machines, sort);
    Ok(())
}

fn mess_create<P>(xml: &[P], sqlite_db: Option<&Path>) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let files: Vec<PathBuf> = xml.iter().map(|f| f.as_ref().into()).collect();

    if let Some(db_file) = sqlite_db {
        let mut db = open_db(db_file)?;
        let trans = db.transaction()?;

        mess::create_tables(&trans)
            .and_then(|()| mess::clear_tables(&trans))
            .and_then(|()| files.iter().try_for_each(|p| mess::add_file(&p, &trans)))
            .and_then(|()| trans.commit().map_err(Error::SQL))?;

        println!("* Wrote \"{}\"", db_file.display());
    }

    let mut verify = mess::VerifyDb::default();
    let mut report = mess::ReportDb::default();
    let mut add = mess::AddDb::default();
    let mut split = mess::SplitDb::default();

    for file in files.iter() {
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

fn mess_add<S>(
    input: &Path,
    output: &Path,
    software_list: &str,
    software: &[S],
    dry_run: bool,
) -> Result<(), Error>
where
    S: AsRef<str>,
{
    use std::collections::HashSet;
    use walkdir::WalkDir;

    let roms: Vec<PathBuf> = WalkDir::new(input)
        .into_iter()
        .filter_map(|e| {
            e.ok()
                .filter(|e| e.file_type().is_file())
                .map(|e| e.into_path())
        })
        .collect();

    let software: HashSet<String> = if !software.is_empty() {
        software.iter().map(|s| s.as_ref().to_string()).collect()
    } else {
        read_cache::<mess::VerifyDb>(CACHE_MESS_VERIFY)?.into_all_software(software_list)
    };

    if let Some(mut db) =
        read_cache::<mess::AddDb>(CACHE_MESS_ADD)?.into_software_list(&software_list)
    {
        db.retain_software(&software);

        roms.par_iter()
            .try_for_each(|rom| mess::copy(&db, &output, rom, dry_run))
            .map_err(Error::IO)
    } else {
        Ok(())
    }
}

fn mess_verify<S>(root: &Path, software_list: &str, software: &[S]) -> Result<(), Error>
where
    S: AsRef<str>,
{
    use std::collections::HashSet;

    let software: HashSet<String> = if !software.is_empty() {
        software.iter().map(|s| s.as_ref().to_owned()).collect()
    } else {
        root.read_dir()?
            .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
            .collect()
    };

    let db =
        match read_cache::<mess::VerifyDb>(CACHE_MESS_VERIFY)?.into_software_list(software_list) {
            Some(db) => db,
            None => return Ok(()), // no software to check
        };

    software.par_iter().for_each(|software| {
        match mess::verify(&db, &root, &software) {
            Ok(VerifyResult::NoMachine) => {}
            Ok(VerifyResult::Ok) => println!("{} : OK", software),
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

fn mess_report<S>(
    root: &Path,
    software_list: Option<&str>,
    software: &[S],
    sort: Option<&str>,
) -> Result<(), Error>
where
    S: AsRef<str>,
{
    use std::collections::HashSet;

    let db: mess::ReportDb = read_cache(CACHE_MESS_REPORT)?;

    if let Some(software_list) = software_list {
        let software: HashSet<String> = if !software.is_empty() {
            software.iter().map(|s| s.as_ref().to_owned()).collect()
        } else {
            root.read_dir()?
                .filter_map(|e| e.ok().and_then(|e| e.file_name().into_string().ok()))
                .collect()
        };

        let sort = match sort {
            Some("publisher") => report::SortBy::Manufacturer,
            Some("year") => report::SortBy::Year,
            _ => report::SortBy::Description,
        };

        mess::report(&db, software_list, &software, sort);
        Ok(())
    } else {
        mess::report_all(&db);
        Ok(())
    }
}

fn mess_split<P>(root: &Path, software_list: &str, roms: &[P], delete: bool) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let db: mess::SplitDb = read_cache(CACHE_MESS_SPLIT)?;
    let roms: Vec<PathBuf> = roms.iter().map(|p| p.as_ref().to_path_buf()).collect();

    if let Some(db) = db.into_software_list(software_list) {
        roms.into_par_iter().try_for_each(|rom| {
            let mut rom_data = Vec::new();
            File::open(&rom).and_then(|mut f| f.read_to_end(&mut rom_data))?;

            if mess::is_ines_format(&rom_data) {
                mess::remove_ines_header(&mut rom_data);
            }

            if let Some(exact_match) = db
                .get(&(rom_data.len() as u64))
                .and_then(|matches| matches.iter().find(|m| m.matches(&rom_data)))
            {
                exact_match.extract(&root, &rom_data)?;
                if delete {
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

fn redump_create<P>(xml: &[P], sqlite_db: Option<&Path>) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let files: Vec<PathBuf> = xml.iter().map(|p| p.as_ref().to_path_buf()).collect();

    if let Some(db_file) = sqlite_db {
        let mut db = open_db(db_file)?;
        let trans = db.transaction()?;

        redump::create_tables(&trans)
            .and_then(|()| redump::clear_tables(&trans))
            .and_then(|()| files.iter().try_for_each(|p| redump::add_file(&p, &trans)))
            .and_then(|()| trans.commit().map_err(Error::SQL))?;

        println!("* Wrote \"{}\"", db_file.display());
    }

    let mut verify = redump::VerifyDb::default();
    let mut split = redump::SplitDb::default();

    for file in files.iter() {
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

fn redump_verify<P>(software: &[P]) -> Result<(), Error>
where
    P: AsRef<Path>,
{
    let files: Vec<PathBuf> = software.iter().map(|p| p.as_ref().to_path_buf()).collect();

    let db = read_cache(CACHE_REDUMP_VERIFY)?;

    files
        .par_iter()
        .for_each(|path| match redump::verify(&db, &path) {
            Ok(result) => println!("{} : {}", result, path.display()),
            Err(err) => eprintln!("* {} : {}", path.display(), err),
        });

    Ok(())
}

fn redump_split(root: &Path, bin_path: &Path, delete: bool) -> Result<(), Error> {
    let bin_size = bin_path.metadata().map(|m| m.len())?;

    let db: redump::SplitDb = read_cache(CACHE_REDUMP_SPLIT)?;

    if let Some(matches) = db.possible_matches(bin_size) {
        let mut bin_data = Vec::new();
        File::open(bin_path).and_then(|mut f| f.read_to_end(&mut bin_data))?;
        if let Some(exact_match) = matches.iter().find(|m| m.matches(&bin_data)) {
            exact_match.extract(&root, &bin_data)?;
            if delete {
                use std::fs::remove_file;

                remove_file(bin_path)?;
            }
        }
    }

    Ok(())
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
    Ok,
    Bad(Vec<VerifyMismatch>),
    NoMachine,
}
