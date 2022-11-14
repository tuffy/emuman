use super::{Error, PAGE_SIZE};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const DIR_CONFIG_FILE: &str = "dirs.toml";

#[derive(Default, Serialize, Deserialize)]
struct DirectoryConfig {
    mame: Option<String>,
    mess: Option<String>,
    extra: BTreeMap<String, String>,
    redump: BTreeMap<String, String>,
    nointro: BTreeMap<String, String>,
}

#[derive(Copy, Clone)]
enum Set {
    Changed,
    Unchanged,
}

impl DirectoryConfig {
    fn new() -> Option<Self> {
        use std::io::Read;

        let mut toml = Vec::new();

        std::fs::File::open(Self::location())
            .and_then(|mut f| f.read_to_end(&mut toml))
            .ok()?;

        toml::from_slice(&toml).ok()
    }

    fn save(self) -> Result<(), Error> {
        use std::io::Write;

        let data = toml::to_string_pretty(&self)?;

        std::fs::File::create(Self::location())
            .and_then(|mut w| w.write_all(data.as_bytes()))
            .map_err(Error::IO)
    }

    fn location() -> PathBuf {
        directories::ProjectDirs::from("", "", "EmuMan")
            .expect("no valid home directory")
            .data_local_dir()
            .join(DIR_CONFIG_FILE)
    }

    #[inline]
    fn get<F>(f: F) -> Option<PathBuf>
    where
        F: FnOnce(DirectoryConfig) -> Option<String>,
    {
        f(Self::new()?).map(PathBuf::from)
    }

    fn set<F>(f: F, value: PathBuf) -> Result<Set, Error>
    where
        F: FnOnce(&mut DirectoryConfig, String) -> Set,
    {
        let value = value
            .into_os_string()
            .into_string()
            .map_err(|_| Error::InvalidPath)?;

        let mut config = Self::new().unwrap_or_default();
        match f(&mut config, value) {
            set @ Set::Unchanged => Ok(set),
            set => config.save().map(|()| set),
        }
    }
}

#[inline]
pub fn default() -> PathBuf {
    PathBuf::from(".")
}

enum RomSource {
    UserProvided(PathBuf),
    FromConfig(PathBuf),
    Default(PathBuf),
}

impl RomSource {
    fn new<F>(user: Option<PathBuf>, from_config: F) -> Self
    where
        F: FnOnce() -> Option<PathBuf>,
    {
        match user {
            Some(source) => Self::UserProvided(source),
            None => match from_config() {
                Some(source) => Self::FromConfig(source),
                None => Self::Default(default()),
            },
        }
    }
}

impl AsRef<Path> for RomSource {
    #[inline]
    fn as_ref(&self) -> &Path {
        match self {
            RomSource::UserProvided(pb) => pb.as_path(),
            RomSource::FromConfig(pb) => pb.as_path(),
            RomSource::Default(pb) => pb.as_path(),
        }
    }
}

pub struct MameRoms(RomSource);

impl MameRoms {
    #[inline]
    fn new(roms: Option<PathBuf>) -> Self {
        Self(RomSource::new(roms, || DirectoryConfig::get(|d| d.mame)))
    }
}

impl AsRef<Path> for MameRoms {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl Drop for MameRoms {
    fn drop(&mut self) {
        if let RomSource::UserProvided(roms) = &self.0 {
            match roms.canonicalize().map_err(Error::IO).and_then(|pb| {
                DirectoryConfig::set(
                    |d, s| {
                        if d.mame.as_ref() != Some(&s) {
                            d.mame = Some(s);
                            Set::Changed
                        } else {
                            Set::Unchanged
                        }
                    },
                    pb,
                )
            }) {
                Ok(Set::Changed) => eprintln!(
                    "* default MAME ROMs directory updated to : \"{}\"",
                    roms.display()
                ),
                Ok(Set::Unchanged) => {}
                Err(err) => eprintln!("* {}", err),
            }
        }
    }
}

#[inline]
pub fn mame_roms(roms: Option<PathBuf>) -> MameRoms {
    MameRoms::new(roms)
}

pub struct MessRoms<'s> {
    roms: RomSource,
    software_list: Option<&'s str>,
}

impl<'s> MessRoms<'s> {
    fn new(roms: Option<PathBuf>, software_list: Option<&'s str>) -> Self {
        Self {
            roms: RomSource::new(roms, || match software_list {
                None => DirectoryConfig::get(|d| d.mess),
                Some(list) => DirectoryConfig::get(|d| d.mess).map(|d| d.join(list)),
            }),
            software_list,
        }
    }
}

impl<'s> AsRef<Path> for MessRoms<'s> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.roms.as_ref()
    }
}

impl<'s> Drop for MessRoms<'s> {
    fn drop(&mut self) {
        if let RomSource::UserProvided(provided) = &self.roms {
            if let Some(roms) = if self.software_list.is_some() {
                provided.parent()
            } else {
                Some(provided.as_path())
            } {
                match roms.canonicalize().map_err(Error::IO).and_then(|pb| {
                    DirectoryConfig::set(
                        |d, s| {
                            if d.mess.as_ref() != Some(&s) {
                                d.mess = Some(s);
                                Set::Changed
                            } else {
                                Set::Unchanged
                            }
                        },
                        pb,
                    )
                }) {
                    Ok(Set::Changed) => eprintln!(
                        "* default software list ROMs directory updated to : \"{}\"",
                        roms.display()
                    ),
                    Ok(Set::Unchanged) => {}
                    Err(err) => eprintln!("* {}", err),
                }
            }
        }
    }
}

#[inline]
pub fn mess_roms_all(root: Option<PathBuf>) -> MessRoms<'static> {
    MessRoms::new(root, None)
}

#[inline]
pub fn mess_roms(roms: Option<PathBuf>, software_list: &str) -> MessRoms<'_> {
    MessRoms::new(roms, Some(software_list))
}

pub struct ExtraParts<'e> {
    extras: RomSource,
    extra: &'e str,
}

impl<'e> ExtraParts<'e> {
    fn new(extras: Option<PathBuf>, extra: &'e str) -> Self {
        Self {
            extras: RomSource::new(extras, || {
                DirectoryConfig::get(|mut d| d.extra.remove(extra))
            }),
            extra,
        }
    }
}

impl<'e> AsRef<Path> for ExtraParts<'e> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.extras.as_ref()
    }
}

impl<'e> Drop for ExtraParts<'e> {
    fn drop(&mut self) {
        if let RomSource::UserProvided(extras) = &self.extras {
            match extras.canonicalize().map_err(Error::IO).and_then(|pb| {
                DirectoryConfig::set(
                    |d, s| match d.extra.insert(self.extra.to_owned(), s.clone()) {
                        Some(old_value) if s == old_value => Set::Unchanged,
                        _ => Set::Changed,
                    },
                    pb,
                )
            }) {
                Ok(Set::Changed) => eprintln!(
                    "* default \"{}\" directory updated to : \"{}\"",
                    self.extra,
                    extras.display()
                ),
                Ok(Set::Unchanged) => {}
                Err(err) => eprintln!("* {}", err),
            }
        }
    }
}

#[inline]
pub fn extra_dirs() -> Box<dyn ExactSizeIterator<Item = (String, PathBuf)>> {
    match DirectoryConfig::new() {
        Some(DirectoryConfig { extra, .. }) => {
            Box::new(extra.into_iter().map(|(k, v)| (k, PathBuf::from(v))))
        }
        None => Box::new(std::iter::empty()),
    }
}

#[inline]
pub fn extra_dir(dir: Option<PathBuf>, extra: &str) -> ExtraParts<'_> {
    ExtraParts::new(dir, extra)
}

pub fn extra_dir_names() -> Option<Vec<String>> {
    DirectoryConfig::new()
        .map(|DirectoryConfig { extra, .. }| extra.into_iter().map(|(k, _)| k).collect::<Vec<_>>())
        .filter(|v| !v.is_empty())
}

pub fn select_extra_name() -> Result<String, Error> {
    select_by_name("select extras category", extra_dir_names)
}

pub struct NointroRoms<'s> {
    roms: RomSource,
    name: &'s str,
}

impl<'s> NointroRoms<'s> {
    fn new(roms: Option<PathBuf>, name: &'s str) -> Self {
        Self {
            roms: RomSource::new(roms, || {
                DirectoryConfig::get(|mut d| d.nointro.remove(name))
            }),
            name,
        }
    }
}

impl<'s> AsRef<Path> for NointroRoms<'s> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.roms.as_ref()
    }
}

impl<'s> Drop for NointroRoms<'s> {
    fn drop(&mut self) {
        if let RomSource::UserProvided(roms) = &self.roms {
            match roms.canonicalize().map_err(Error::IO).and_then(|pb| {
                DirectoryConfig::set(
                    |d, s| match d.nointro.insert(self.name.to_owned(), s.clone()) {
                        Some(old_value) if s == old_value => Set::Unchanged,
                        _ => Set::Changed,
                    },
                    pb,
                )
            }) {
                Ok(Set::Changed) => eprintln!(
                    "* default \"{}\" directory updated to : \"{}\"",
                    self.name,
                    roms.display()
                ),
                Ok(Set::Unchanged) => {}
                Err(err) => eprintln!("* {}", err),
            }
        }
    }
}

#[inline]
pub fn nointro_roms(roms: Option<PathBuf>, name: &str) -> NointroRoms<'_> {
    NointroRoms::new(roms, name)
}

pub fn nointro_dirs() -> Box<dyn ExactSizeIterator<Item = (String, PathBuf)>> {
    match DirectoryConfig::new() {
        Some(DirectoryConfig { nointro, .. }) => {
            Box::new(nointro.into_iter().map(|(k, v)| (k, PathBuf::from(v))))
        }
        None => Box::new(std::iter::empty()),
    }
}

pub fn nointro_dir_names() -> Option<Vec<String>> {
    DirectoryConfig::new()
        .map(|DirectoryConfig { nointro, .. }| {
            nointro.into_iter().map(|(k, _)| k).collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
}

pub fn select_nointro_name() -> Result<String, Error> {
    select_by_name("select DAT", nointro_dir_names)
}

pub struct RedumpRoms<'r> {
    roms: RomSource,
    name: &'r str,
}

impl<'r> RedumpRoms<'r> {
    fn new(roms: Option<PathBuf>, name: &'r str) -> Self {
        Self {
            roms: RomSource::new(roms, || DirectoryConfig::get(|mut d| d.redump.remove(name))),
            name,
        }
    }
}

impl<'r> AsRef<Path> for RedumpRoms<'r> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.roms.as_ref()
    }
}

impl<'r> Drop for RedumpRoms<'r> {
    fn drop(&mut self) {
        if let RomSource::UserProvided(roms) = &self.roms {
            match roms.canonicalize().map_err(Error::IO).and_then(|pb| {
                DirectoryConfig::set(
                    |d, s| match d.redump.insert(self.name.to_owned(), s.clone()) {
                        Some(old_value) if s == old_value => Set::Unchanged,
                        _ => Set::Changed,
                    },
                    pb,
                )
            }) {
                Ok(Set::Changed) => eprintln!(
                    "* default \"{}\" directory updated to : \"{}\"",
                    self.name,
                    roms.display()
                ),
                Ok(Set::Unchanged) => {}
                Err(err) => eprintln!("* {}", err),
            }
        }
    }
}

#[inline]
pub fn redump_roms(roms: Option<PathBuf>, name: &str) -> RedumpRoms<'_> {
    RedumpRoms::new(roms, name)
}

pub fn redump_dirs() -> Box<dyn ExactSizeIterator<Item = (String, PathBuf)>> {
    match DirectoryConfig::new() {
        Some(DirectoryConfig { redump, .. }) => {
            Box::new(redump.into_iter().map(|(k, v)| (k, PathBuf::from(v))))
        }
        None => Box::new(std::iter::empty()),
    }
}

pub fn redump_dir_names() -> Option<Vec<String>> {
    DirectoryConfig::new()
        .map(|DirectoryConfig { redump, .. }| {
            redump.into_iter().map(|(k, _)| k).collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
}

pub fn select_redump_name() -> Result<String, Error> {
    select_by_name("select DAT", redump_dir_names)
}

fn select_by_name<N>(prompt: &'static str, names: N) -> Result<String, Error>
where
    N: FnOnce() -> Option<Vec<String>>,
{
    names().ok_or(Error::NoDatFiles).and_then(|names| {
        inquire::Select::new(prompt, names)
            .with_page_size(PAGE_SIZE)
            .prompt()
            .map_err(Error::Inquire)
    })
}
