use super::{Error, FileError};
use crate::game::{GameParts, Part, RomSources, VerifyFailure};
use comfy_table::Table;
use fxhash::FxHashSet;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct Datafile {
    header: Header,
    game: Option<Vec<Game>>,
    machine: Option<Vec<Game>>,
}

impl Datafile {
    #[inline]
    pub fn games(&self) -> impl Iterator<Item = &Game> {
        self.game.iter().flatten()
    }
}

#[derive(Debug, Deserialize)]
pub struct Header {
    name: String,
    version: String,
}

#[derive(Debug, Deserialize)]
pub struct Game {
    name: String,
    rom: Option<Vec<Rom>>,
    disk: Option<Vec<Disk>>,
}

impl Game {
    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn roms(&self) -> impl Iterator<Item = &Rom> {
        self.rom.iter().flatten()
    }

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
    name: String,
    size: Option<u64>,
    sha1: Option<String>,
}

impl Rom {
    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn size(&self) -> Option<u64> {
        self.size
    }

    #[inline]
    pub fn sha1(&self) -> Option<&str> {
        self.sha1.as_deref()
    }

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
    name: String,
    sha1: Option<String>,
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

impl std::fmt::Display for DatFile {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.name.fmt(f)
    }
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

    // un-flattens the DAT into (game_name, parts) tuples
    pub fn into_game_parts(self) -> impl Iterator<Item = (String, GameParts)> {
        self.flat
            .into_iter()
            .map(|(game, part)| (game.clone(), std::iter::once((game, part)).collect()))
            .chain(self.tree.into_iter())
    }

    pub fn game_parts(&self) -> impl Iterator<Item = (&str, &GameParts)> {
        std::iter::once(("", &self.flat))
            .chain(self.tree.iter().map(|(game, parts)| (game.as_str(), parts)))
    }

    pub fn remove_game(&mut self, name: &str) -> Option<GameParts> {
        self.flat
            .remove(name)
            .map(|part| std::iter::once((name.to_string(), part)).collect())
            .or_else(|| self.tree.remove(name))
    }

    pub fn list_all<I, T>(iter: I)
    where
        I: IntoIterator<Item = (T, Self)>,
    {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;

        let mut table = Table::new();
        table
            .set_header(vec!["Version", "DAT Name"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        for (_, datfile) in iter {
            table.add_row(vec![datfile.version(), datfile.name()]);
        }

        println!("{table}");
    }

    pub fn list(&self) {
        use comfy_table::modifiers::UTF8_ROUND_CORNERS;
        use comfy_table::presets::UTF8_FULL_CONDENSED;

        let mut games = self.games().collect::<Vec<_>>();
        games.sort_unstable();

        let mut table = Table::new();
        table
            .set_header(vec!["Game"])
            .load_preset(UTF8_FULL_CONDENSED)
            .apply_modifier(UTF8_ROUND_CORNERS);

        for game in games {
            table.add_row(vec![game]);
        }

        println!("{table}");
    }

    pub fn verify(&self, root: &Path, all: bool) -> BTreeMap<Option<&str>, Vec<VerifyFailure>> {
        use std::collections::HashSet;

        let progress_bar =
            indicatif::ProgressBar::new(self.flat.len() as u64 + self.tree.len() as u64)
                .with_style(crate::game::verify_style())
                .with_message(format!("verifying : {} ({})", self.name, self.version));

        let mut directories = std::fs::read_dir(root)
            .map(|d| {
                d.filter_map(|e| e.ok())
                    .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                    .map(|e| e.path())
                    .collect::<HashSet<PathBuf>>()
            })
            .unwrap_or_default();

        let mut failures = BTreeMap::default();

        let (flat_successes, flat_failures) = self
            .flat
            .verify_with_progress::<Vec<_>, Vec<_>, _>(root, || progress_bar.inc(1));

        failures.extend(
            flat_successes
                .into_iter()
                .map(|success| (Some(success.name), Vec::new())),
        );

        if all {
            for failure in flat_failures {
                failures
                    .entry(match failure {
                        VerifyFailure::Missing { name, .. } | VerifyFailure::Bad { name, .. } => {
                            Some(name)
                        }
                        _ => None,
                    })
                    .or_default()
                    .push(failure);
            }

            failures.extend(
                progress_bar
                    .wrap_iter(self.tree.iter())
                    .map(|(name, game)| {
                        let game_root = root.join(name);
                        directories.remove(&game_root);
                        (Some(name.as_str()), game.verify_failures(&game_root))
                    }),
            );
        } else {
            for failure in flat_failures {
                match failure {
                    VerifyFailure::Missing { .. } => { /* do nothing*/ }
                    VerifyFailure::Bad { name, .. } => {
                        failures.entry(Some(name)).or_default().push(failure)
                    }
                    _ => failures.entry(None).or_default().push(failure),
                }
            }

            for (name, game) in progress_bar.wrap_iter(self.tree.iter()) {
                let game_root = root.join(name);
                directories.remove(&game_root);
                if game_root.is_dir() {
                    failures.insert(Some(name), game.verify_failures(&game_root));
                }
            }
        }

        progress_bar.finish_and_clear();

        failures
            .entry(None)
            .or_default()
            .extend(directories.into_iter().map(VerifyFailure::extra_dir));

        failures
    }

    pub fn add_and_verify(
        &self,
        roms: &mut RomSources,
        root: &Path,
        all: bool,
    ) -> Result<BTreeMap<Option<&str>, Vec<VerifyFailure>>, Error> {
        use std::collections::HashSet;

        let progress_bar =
            indicatif::ProgressBar::new(self.flat.len() as u64 + self.tree.len() as u64)
                .with_style(crate::game::verify_style())
                .with_message(format!(
                    "adding and verifying : {} ({})",
                    self.name, self.version
                ));

        let mut directories = std::fs::read_dir(root)
            .map(|d| {
                d.filter_map(|e| e.ok())
                    .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
                    .map(|e| e.path())
                    .collect::<HashSet<PathBuf>>()
            })
            .unwrap_or_default();

        let mut failures: BTreeMap<Option<&str>, Vec<_>> = BTreeMap::default();

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
                .map(|success| (Some(success.name), Vec::new())),
        );

        if all {
            for failure in flat_failures {
                failures
                    .entry(match failure {
                        VerifyFailure::Missing { name, .. } | VerifyFailure::Bad { name, .. } => {
                            Some(name)
                        }
                        _ => None,
                    })
                    .or_default()
                    .push(failure);
            }

            for (name, game) in progress_bar.wrap_iter(self.tree.iter()) {
                let game_root = root.join(name);

                directories.remove(&game_root);

                failures.insert(
                    Some(name),
                    game.add_and_verify_failures(roms, &game_root, |r| {
                        progress_bar.println(r.to_string())
                    })?,
                );
            }
        } else {
            for failure in flat_failures {
                match failure {
                    VerifyFailure::Missing { .. } => { /* do nothing*/ }
                    VerifyFailure::Bad { name, .. } => {
                        failures.entry(Some(name)).or_default().push(failure)
                    }
                    _ => failures.entry(None).or_default().push(failure),
                }
            }

            for (name, game) in progress_bar.wrap_iter(self.tree.iter()) {
                let game_root = root.join(name);

                directories.remove(&game_root);

                let (
                    crate::game::ExtendExists {
                        exists: has_successes,
                        ..
                    },
                    game_failures,
                ): (_, Vec<_>) =
                    game.add_and_verify(roms, &game_root, |r| progress_bar.println(r.to_string()))?;

                if has_successes
                    || !game_failures
                        .iter()
                        .all(|f| matches!(f, VerifyFailure::Missing { .. }))
                {
                    failures.insert(Some(name), game_failures);
                }
            }
        }

        progress_bar.finish_and_clear();

        failures
            .entry(None)
            .or_default()
            .extend(directories.into_iter().map(VerifyFailure::extra_dir));

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
