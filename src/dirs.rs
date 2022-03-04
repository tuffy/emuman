use super::Error;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const DIR_CONFIG_FILE: &str = "dirs.toml";

#[derive(Default, Serialize, Deserialize)]
struct DirectoryConfig {
    mame: Option<String>,
    mess: Option<String>,
    extra: Option<String>,
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

pub struct MameRoms {
    provided: Option<PathBuf>,
    actual: PathBuf,
}

impl MameRoms {
    fn new(roms: Option<PathBuf>) -> Self {
        Self {
            actual: roms
                .as_ref()
                .cloned()
                .or_else(|| DirectoryConfig::get(|d| d.mame))
                .unwrap_or_else(default),
            provided: roms,
        }
    }
}

impl AsRef<Path> for MameRoms {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.actual.as_path()
    }
}

impl Drop for MameRoms {
    fn drop(&mut self) {
        if let Some(roms) = &self.provided {
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
    provided: Option<PathBuf>,
    actual: PathBuf,
    software_list: Option<&'s str>,
}

impl<'s> MessRoms<'s> {
    fn new(roms: Option<PathBuf>, software_list: Option<&'s str>) -> Self {
        Self {
            actual: roms
                .as_ref()
                .cloned()
                .or_else(|| match software_list {
                    None => DirectoryConfig::get(|d| d.mess),
                    Some(list) => DirectoryConfig::get(|d| d.mess).map(|d| d.join(list)),
                })
                .unwrap_or_else(default),
            provided: roms,
            software_list,
        }
    }
}

impl<'s> AsRef<Path> for MessRoms<'s> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.actual.as_path()
    }
}

impl<'s> Drop for MessRoms<'s> {
    fn drop(&mut self) {
        if let Some(roms) = if self.software_list.is_some() {
            self.provided.as_ref().and_then(|d| d.parent())
        } else {
            self.provided.as_deref()
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
                    "* default MESS ROMs directory updated to : \"{}\"",
                    roms.display()
                ),
                Ok(Set::Unchanged) => {}
                Err(err) => eprintln!("* {}", err),
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
    provided: Option<PathBuf>,
    actual: PathBuf,
    extra: Option<&'e str>,
}

impl<'e> ExtraParts<'e> {
    fn new(roms: Option<PathBuf>, extra: Option<&'e str>) -> Self {
        Self {
            actual: roms
                .as_ref()
                .cloned()
                .or_else(|| match extra {
                    None => DirectoryConfig::get(|d| d.extra),
                    Some(list) => DirectoryConfig::get(|d| d.extra).map(|d| d.join(list)),
                })
                .unwrap_or_else(default),
            provided: roms,
            extra,
        }
    }
}

impl<'e> AsRef<Path> for ExtraParts<'e> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.actual.as_path()
    }
}

impl<'e> Drop for ExtraParts<'e> {
    fn drop(&mut self) {
        if let Some(roms) = if self.extra.is_some() {
            self.provided.as_ref().and_then(|d| d.parent())
        } else {
            self.provided.as_deref()
        } {
            match roms.canonicalize().map_err(Error::IO).and_then(|pb| {
                DirectoryConfig::set(
                    |d, s| {
                        if d.extra.as_ref() != Some(&s) {
                            d.extra = Some(s);
                            Set::Changed
                        } else {
                            Set::Unchanged
                        }
                    },
                    pb,
                )
            }) {
                Ok(Set::Changed) => eprintln!(
                    "* default MAME extras directory updated to : \"{}\"",
                    roms.display()
                ),
                Ok(Set::Unchanged) => {}
                Err(err) => eprintln!("* {}", err),
            }
        }
    }
}

#[inline]
pub fn extra_dir_all(root: Option<PathBuf>) -> ExtraParts<'static> {
    ExtraParts::new(root, None)
}

#[inline]
pub fn extra_dir(dir: Option<PathBuf>, extra: &str) -> ExtraParts<'_> {
    ExtraParts::new(dir, Some(extra))
}

pub struct NointroRoms<'s> {
    provided: Option<PathBuf>,
    actual: PathBuf,
    name: &'s str,
}

impl<'s> NointroRoms<'s> {
    fn new(roms: Option<PathBuf>, name: &'s str) -> Self {
        Self {
            actual: roms
                .as_ref()
                .cloned()
                .or_else(|| DirectoryConfig::get(|d| d.nointro.get(name).cloned()))
                .unwrap_or_else(default),
            provided: roms,
            name,
        }
    }
}

impl<'s> AsRef<Path> for NointroRoms<'s> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.actual.as_path()
    }
}

impl<'s> Drop for NointroRoms<'s> {
    fn drop(&mut self) {
        if let Some(roms) = &self.provided {
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

pub struct RedumpRoms<'r> {
    provided: Option<PathBuf>,
    actual: PathBuf,
    name: &'r str,
}

impl<'r> RedumpRoms<'r> {
    fn new(roms: Option<PathBuf>, name: &'r str) -> Self {
        Self {
            actual: roms
                .as_ref()
                .cloned()
                .or_else(|| DirectoryConfig::get(|d| d.redump.get(name).cloned()))
                .unwrap_or_else(default),
            provided: roms,
            name,
        }
    }
}

impl<'r> AsRef<Path> for RedumpRoms<'r> {
    #[inline]
    fn as_ref(&self) -> &Path {
        self.actual.as_path()
    }
}

impl<'r> Drop for RedumpRoms<'r> {
    fn drop(&mut self) {
        if let Some(roms) = &self.provided {
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
