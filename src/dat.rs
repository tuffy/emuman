use super::{Error, FileError};
use crate::game::{ExtractedPart, GameParts, Part, RomSources, Sha1ParseError, VerifyFailure};
use fxhash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct Datafile {
    pub header: Header,
    pub game: Option<Vec<Game>>,
}

#[derive(Debug, Deserialize)]
pub struct Header {
    pub name: String,
    pub description: String,
    pub version: String,
    pub author: String,
    pub homepage: String,
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct Game {
    pub name: String,
    pub description: String,
    pub rom: Vec<Rom>,
}

impl Game {
    #[inline]
    fn into_parts(self) -> Result<(String, GameParts), Sha1ParseError> {
        Ok((
            self.name,
            self.rom
                .into_iter()
                .filter_map(|rom| rom.into_part())
                .collect::<Result<GameParts, _>>()?,
        ))
    }
}

#[derive(Debug, Deserialize)]
pub struct Rom {
    pub name: String,
    pub size: Option<u64>,
    pub crc: Option<String>,
    pub md5: Option<String>,
    pub sha1: Option<String>,
}

impl Rom {
    #[inline]
    fn into_part(self) -> Option<Result<(String, Part), Sha1ParseError>> {
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
pub struct DatFile {
    name: String,
    version: String,
    // games with a single ROM
    flat: GameParts,
    // games with multiple ROMs
    tree: BTreeMap<String, GameParts>,
}

impl DatFile {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn version(&self) -> &str {
        self.version.as_str()
    }

    pub fn games(&self) -> impl Iterator<Item = &str> {
        self.flat.keys().chain(self.tree.keys()).map(|s| s.as_str())
    }

    pub fn game_parts(&self) -> impl Iterator<Item = (&str, &GameParts)> {
        std::iter::once(("", &self.flat))
            .chain(self.tree.iter().map(|(game, parts)| (game.as_str(), parts)))
    }

    pub fn verify(&self, root: &Path) -> Vec<VerifyFailure> {
        let mut failures = self.flat.verify(root);
        for (name, game) in self.tree.iter() {
            failures.extend(game.verify(&root.join(name)));
        }
        failures
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
        let mut failures = Vec::new();
        for (name, game) in self.tree.iter() {
            failures.extend(game.add_and_verify_root(roms, &root.join(name), |p| progress(p))?);
        }
        failures.extend(self.flat.add_and_verify_root(roms, root, progress)?);
        Ok(failures)
    }

    pub fn required_parts(&self) -> FxHashSet<Part> {
        self.flat
            .values()
            .chain(self.tree.values().flat_map(|game| game.values()))
            .cloned()
            .collect()
    }
}

impl TryFrom<Datafile> for DatFile {
    type Error = Sha1ParseError;

    fn try_from(datafile: Datafile) -> Result<Self, Sha1ParseError> {
        let mut flat = GameParts::default();
        let mut tree = BTreeMap::default();

        for mut game in datafile.game.into_iter().flatten() {
            if game.rom.len() == 1 {
                if let Some(p) = game.rom.pop().and_then(|r| r.into_part()) {
                    let (name, part) = p?;
                    flat.insert(name, part);
                }
            } else {
                let (name, parts) = game.into_parts()?;
                tree.insert(name, parts);
            }
        }

        Ok(Self {
            name: datafile.header.name,
            version: datafile.header.version,
            flat,
            tree,
        })
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

#[inline]
fn parse_datafile(file: PathBuf, data: Box<[u8]>) -> Result<Datafile, Error> {
    quick_xml::de::from_reader(std::io::Cursor::new(data))
        .map_err(|error| Error::SerdeXml(FileError { file, error }))
}

pub fn read_dats_from_file(file: PathBuf) -> Result<Vec<(PathBuf, Box<[u8]>)>, Error> {
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

#[inline]
pub fn read_datafiles(file: PathBuf) -> Result<Vec<Datafile>, Error> {
    read_dats_from_file(file).and_then(|v| {
        v.into_iter()
            .map(|(file, data)| parse_datafile(file, data))
            .collect()
    })
}
