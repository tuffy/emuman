use super::{Error, FileError};
use crate::game::{ExtractedPart, GameParts, Part, RomSources, Sha1ParseError, VerifyFailure};
use fxhash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct Datafile {
    header: Header,
    game: Option<Vec<Game>>,
}

#[derive(Debug, Deserialize)]
pub struct Header {
    name: String,
    version: String,
}

#[derive(Debug, Deserialize)]
pub struct Game {
    name: String,
    rom: Vec<Rom>,
}

impl Game {
    #[inline]
    fn to_parts(self) -> Result<(String, GameParts), Sha1ParseError> {
        Ok((
            self.name,
            self.rom
                .into_iter()
                .filter_map(|rom| rom.to_part())
                .collect::<Result<GameParts, _>>()?,
        ))
    }
}

#[derive(Debug, Deserialize)]
pub struct Rom {
    name: String,
    sha1: Option<String>,
}

impl Rom {
    #[inline]
    fn to_part(self) -> Option<Result<(String, Part), Sha1ParseError>> {
        match self.sha1 {
            Some(sha1) => match Part::new_rom(&sha1) {
                Ok(part) => Some(Ok((self.name, part))),
                Err(err) => Some(Err(err)),
            },
            None => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DatFile {
    Tree {
        name: String,
        version: String,
        games: BTreeMap<String, GameParts>,
    },
    Flat {
        name: String,
        version: String,
        games: GameParts,
    },
}

impl DatFile {
    pub fn name(&self) -> &str {
        match self {
            DatFile::Tree { name, .. } => name.as_str(),
            DatFile::Flat { name, .. } => name.as_str(),
        }
    }

    pub fn version(&self) -> &str {
        match self {
            DatFile::Tree { version, .. } => version.as_str(),
            DatFile::Flat { version, .. } => version.as_str(),
        }
    }

    pub fn games(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        match self {
            DatFile::Tree { games, .. } => Box::new(games.keys().map(|k| k.as_str())),
            DatFile::Flat { games, .. } => Box::new(games.keys().map(|k| k.as_str())),
        }
    }

    pub fn verify(&self, root: &Path) -> Vec<VerifyFailure> {
        // FIXME - group failures by game?
        match self {
            DatFile::Tree { games, .. } => games
                .iter()
                .map(|(name, game)| game.verify(&root.join(name)))
                .flatten()
                .collect(),
            DatFile::Flat { games, .. } => games.verify(root),
        }
    }

    pub fn add_and_verify<F>(
        &self,
        roms: &mut RomSources,
        root: &Path,
        mut progress: F,
    ) -> Result<Vec<VerifyFailure>, Error>
    where
        F: FnMut(ExtractedPart<'_>),
    {
        match self {
            DatFile::Tree { games, .. } => {
                let mut failures = Vec::new();
                for (name, game) in games.iter() {
                    failures
                        .extend(game.add_and_verify_root(roms, &root.join(name), |p| progress(p))?);
                }
                Ok(failures)
            }
            DatFile::Flat { games, .. } => games.add_and_verify_root(roms, root, progress),
        }
    }

    pub fn required_parts(&self) -> FxHashSet<Part> {
        match self {
            DatFile::Tree { games, .. } => games
                .values()
                .flat_map(|game| game.values().cloned())
                .collect(),
            DatFile::Flat { games, .. } => games.values().cloned().collect(),
        }
    }
}

impl TryFrom<Datafile> for DatFile {
    type Error = Sha1ParseError;

    fn try_from(datafile: Datafile) -> Result<Self, Sha1ParseError> {
        if datafile
            .game
            .iter()
            .flatten()
            .all(|game| game.rom.len() == 1)
        {
            Ok(DatFile::Flat {
                name: datafile.header.name,
                version: datafile.header.version,
                games: datafile
                    .game
                    .into_iter()
                    .flatten()
                    .flat_map(|game| game.rom.into_iter())
                    .filter_map(|rom| rom.to_part())
                    .collect::<Result<GameParts, _>>()?,
            })
        } else {
            Ok(DatFile::Tree {
                name: datafile.header.name,
                version: datafile.header.version,
                games: datafile
                    .game
                    .into_iter()
                    .flatten()
                    .map(|game| game.to_parts())
                    .collect::<Result<BTreeMap<String, GameParts>, _>>()?,
            })
        }
    }
}

#[inline]
fn parse_dat(file: PathBuf, data: Box<[u8]>) -> Result<DatFile, Error> {
    let datafile: Datafile = match quick_xml::de::from_reader(std::io::Cursor::new(data)) {
        Ok(dat) => dat,
        Err(error) => return Err(Error::SerdeXml(FileError { file, error })),
    };

    datafile
        .try_into()
        .map_err(|error| Error::InvalidSha1(FileError { file, error }))
}

fn read_dats_from_file(file: PathBuf) -> Result<Vec<(PathBuf, Box<[u8]>)>, Error> {
    use super::is_zip;
    use std::io::Read;

    let mut f = std::fs::File::open(&file)?;

    match is_zip(&mut f) {
        Ok(true) => {
            let mut zip = zip::ZipArchive::new(f)?;

            let dats = zip
                .file_names()
                .filter(|s| s.ends_with(".dat"))
                .map(|s| s.to_owned())
                .collect::<Vec<String>>();

            dats.into_iter()
                .map(|name| {
                    let mut data = Vec::new();
                    zip.by_name(&name)?.read_to_end(&mut data)?;
                    Ok((PathBuf::from(name), data.into_boxed_slice()))
                })
                .collect()
        }
        Ok(false) => {
            let mut data = Vec::new();
            f.read_to_end(&mut data)?;
            Ok(vec![(file, data.into_boxed_slice())])
        }
        Err(err) => Err(Error::IO(err)),
    }
}

#[inline]
pub fn read_dats(file: PathBuf) -> Result<Vec<DatFile>, Error> {
    read_dats_from_file(file).and_then(|v| {
        v.into_iter()
            .map(|(file, data)| parse_dat(file, data))
            .collect()
    })
}
