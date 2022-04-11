use super::{Error, FileError};
use crate::game::{GameParts, Part, RomSources, VerifyFailure};
use fxhash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct Datafile {
    pub header: Header,
    pub game: Option<Vec<Game>>,
    pub machine: Option<Vec<Game>>,
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
    pub rom: Option<Vec<Rom>>,
    pub disk: Option<Vec<Disk>>,
}

impl Game {
    #[inline]
    fn into_parts(self) -> Result<(String, GameParts), hex::FromHexError> {
        Ok((
            self.name,
            self.rom
                .into_iter()
                .flatten()
                .filter_map(|rom| rom.into_part())
                .chain(
                    self.disk
                        .into_iter()
                        .flatten()
                        .filter_map(|disk| disk.into_part()),
                )
                .collect::<Result<GameParts, _>>()?,
        ))
    }

    // if the game has exactly one ROM with a defined SHA1 field,
    // or it has exactly one disk with a defined SHA1 field,
    // flatten it into a single (rom_name, part) tuple,
    // otherwise return a (game_name, GameParts) tuple
    // of all the game parts it contains
    fn try_flatten(self) -> Result<Result<(String, Part), (String, GameParts)>, hex::FromHexError> {
        match &self {
            Game {
                rom: Some(roms),
                disk: None,
                ..
            } => match &roms[..] {
                [Rom {
                    name,
                    sha1: Some(sha1),
                    ..
                }] => Part::new_rom(sha1).map(|part| Ok((name.clone(), part))),
                _ => self.into_parts().map(Err),
            },
            Game {
                rom: None,
                disk: Some(disks),
                ..
            } => match &disks[..] {
                [Disk {
                    name,
                    sha1: Some(sha1),
                    ..
                }] => Part::new_disk(sha1).map(|part| Ok((name.clone() + ".chd", part))),
                _ => self.into_parts().map(Err),
            },
            _ => self.into_parts().map(Err),
        }
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
    fn into_part(self) -> Option<Result<(String, Part), hex::FromHexError>> {
        match self.sha1 {
            Some(sha1) => match Part::new_rom(&sha1) {
                Ok(part) => Some(Ok((self.name, part))),
                Err(err) => Some(Err(err)),
            },
            None => None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Disk {
    pub name: String,
    pub sha1: Option<String>,
}

impl Disk {
    #[inline]
    fn into_part(self) -> Option<Result<(String, Part), hex::FromHexError>> {
        match self.sha1 {
            Some(sha1) => match Part::new_disk(&sha1) {
                Ok(part) => Some(Ok((self.name + ".chd", part))),
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
    pub fn new_flattened(datafile: Datafile) -> Result<Self, hex::FromHexError> {
        let mut flat = GameParts::default();
        let mut tree = BTreeMap::default();

        for game in datafile
            .game
            .into_iter()
            .flatten()
            .chain(datafile.machine.into_iter().flatten())
        {
            match game.try_flatten()? {
                Ok((name, part)) => {
                    flat.insert(name, part);
                }
                Err((name, parts)) => {
                    tree.insert(name, parts);
                }
            }
        }

        Ok(Self {
            name: datafile.header.name,
            version: datafile.header.version,
            flat,
            tree,
        })
    }

    pub fn new_unflattened(datafile: Datafile) -> Result<Self, hex::FromHexError> {
        let mut tree = BTreeMap::default();

        for game in datafile
            .game
            .into_iter()
            .flatten()
            .chain(datafile.machine.into_iter().flatten())
        {
            let (name, parts) = game.into_parts()?;
            tree.insert(name, parts);
        }

        Ok(Self {
            name: datafile.header.name,
            version: datafile.header.version,
            flat: GameParts::default(),
            tree,
        })
    }

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

    pub fn list_all<I, T>(iter: I)
    where
        I: IntoIterator<Item = (T, Self)>,
    {
        use prettytable::{cell, format, row, Table};
        let mut table = Table::new();

        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

        for (_, datfile) in iter {
            table.add_row(row![datfile.name(), datfile.version()]);
        }

        table.printstd();
    }

    pub fn list(&self) {
        use prettytable::{cell, format, row, Table};

        let mut games = self.games().collect::<Vec<_>>();
        games.sort_unstable();

        let mut table = Table::new();

        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

        for game in games {
            table.add_row(row![game]);
        }

        table.printstd();
    }

    pub fn verify(&self, root: &Path, all: bool) -> BTreeMap<&str, Vec<VerifyFailure>> {
        let progress_bar =
            indicatif::ProgressBar::new(self.flat.len() as u64 + self.tree.len() as u64)
                .with_style(crate::game::verify_style())
                .with_message(format!("verifying : {} ({})", self.name, self.version));

        let mut failures = BTreeMap::default();

        let (flat_successes, flat_failures) = self
            .flat
            .verify_with_progress::<Vec<_>, Vec<_>, _>(root, || progress_bar.inc(1));

        failures.extend(
            flat_successes
                .into_iter()
                .map(|success| (success.name, Vec::new())),
        );

        if all {
            failures.extend(
                flat_failures
                    .into_iter()
                    .filter_map(|failure| match failure {
                        VerifyFailure::Missing { name, .. } => Some((name, vec![failure])),
                        VerifyFailure::Bad { name, .. } => Some((name, vec![failure])),
                        _ => None,
                    }),
            );

            failures.extend(
                progress_bar
                    .wrap_iter(self.tree.iter())
                    .map(|(name, game)| (name.as_str(), game.verify_failures(&root.join(name)))),
            );
        } else {
            failures.extend(
                flat_failures
                    .into_iter()
                    .filter_map(|failure| match failure {
                        VerifyFailure::Bad { name, .. } => Some((name, vec![failure])),
                        _ => None,
                    }),
            );

            for (name, game) in progress_bar.wrap_iter(self.tree.iter()) {
                let game_root = root.join(name);
                if game_root.is_dir() {
                    failures.insert(name, game.verify_failures(&game_root));
                }
            }
        }

        progress_bar.finish_and_clear();

        failures
    }

    pub fn add_and_verify(
        &self,
        roms: &mut RomSources,
        root: &Path,
        all: bool,
    ) -> Result<BTreeMap<&str, Vec<VerifyFailure>>, Error> {
        let progress_bar =
            indicatif::ProgressBar::new(self.flat.len() as u64 + self.tree.len() as u64)
                .with_style(crate::game::verify_style())
                .with_message(format!(
                    "adding and verifying : {} ({})",
                    self.name, self.version
                ));

        let mut failures: BTreeMap<&str, Vec<_>> = BTreeMap::default();

        let (flat_successes, flat_failures): (Vec<_>, Vec<_>) =
            self.flat.add_and_verify_with_progress(
                roms,
                root,
                || progress_bar.inc(1),
                |r| progress_bar.println(r.to_string()),
            )?;

        failures.extend(
            flat_successes
                .into_iter()
                .map(|success| (success.name, Vec::new())),
        );

        if all {
            failures.extend(
                flat_failures
                    .into_iter()
                    .filter_map(|failure| match failure {
                        VerifyFailure::Missing { name, .. } => Some((name, vec![failure])),
                        VerifyFailure::Bad { name, .. } => Some((name, vec![failure])),
                        _ => None,
                    }),
            );

            for (name, game) in progress_bar.wrap_iter(self.tree.iter()) {
                failures.insert(
                    name,
                    game.add_and_verify_failures(roms, &root.join(name), |r| {
                        progress_bar.println(r.to_string())
                    })?,
                );
            }
        } else {
            failures.extend(
                flat_failures
                    .into_iter()
                    .filter_map(|failure| match failure {
                        VerifyFailure::Bad { name, .. } => Some((name, vec![failure])),
                        _ => None,
                    }),
            );

            for (name, game) in progress_bar.wrap_iter(self.tree.iter()) {
                let (
                    crate::game::ExtendExists {
                        exists: has_successes,
                        ..
                    },
                    game_failures,
                ): (_, Vec<_>) = game.add_and_verify(roms, &root.join(name), |r| {
                    progress_bar.println(r.to_string())
                })?;

                if has_successes
                    || !game_failures
                        .iter()
                        .all(|f| matches!(f, VerifyFailure::Missing { .. }))
                {
                    failures.insert(name, game_failures);
                }
            }
        }

        progress_bar.finish_and_clear();

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

#[inline]
fn parse_dat(file: PathBuf, data: Box<[u8]>, flatten: bool) -> Result<DatFile, Error> {
    let datafile: Datafile = match quick_xml::de::from_reader(std::io::Cursor::new(data)) {
        Ok(dat) => dat,
        Err(error) => return Err(Error::XmlFile(FileError { file, error })),
    };

    (if flatten {
        DatFile::new_flattened(datafile)
    } else {
        DatFile::new_unflattened(datafile)
    })
    .map_err(|error| Error::InvalidSha1(FileError { file, error }))
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
            .map(|(file, data)| parse_dat(file, data, true))
            .collect()
    })
}

#[inline]
pub fn read_unflattened_dats(file: PathBuf) -> Result<Vec<DatFile>, Error> {
    read_dats_from_file(file).and_then(|v| {
        v.into_iter()
            .map(|(file, data)| parse_dat(file, data, false))
            .collect()
    })
}
