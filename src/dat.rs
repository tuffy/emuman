use super::{Error, ResourceError};
use crate::game::{GameParts, Part, RomSources, VerifyFailure};
use crate::Resource;
use comfy_table::Table;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

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

    fn process<E>(
        &self,
        root: &Path,
        increment_progress: impl Fn() + Send + Sync,
        handle_failure: impl Fn(VerifyFailure) -> Result<Result<(), VerifyFailure>, E> + Send + Sync,
    ) -> Result<VerifyResults, E>
    where
        E: Send,
    {
        use crate::game::{ExtendCounter, ExtendSink, GameDir};
        use dashmap::DashMap;
        use rayon::prelude::*;
        use std::sync::Mutex;

        let GameDir {
            files,
            dirs,
            failures,
        }: GameDir<DashMap<_, _>, DashMap<_, _>, Vec<_>> = GameDir::open(root);

        // first, handle loose files not in subdirectories
        let (
            ExtendCounter {
                value: successes, ..
            },
            failures,
        ): (_, Vec<_>) = self.flat.process(
            files,
            failures,
            |name| root.join(name),
            &increment_progress,
            &handle_failure,
        )?;

        let successes = Mutex::new(successes);
        let failures = Mutex::new(failures);

        // then handle everything with a subdirectory
        self.tree.par_iter().try_for_each(|(name, parts)| {
            let (_, game_failures): (ExtendSink<_>, Vec<_>) = parts.process_parts(
                &dirs
                    .remove(name)
                    .map(|(_, v)| v)
                    .unwrap_or_else(|| root.join(name)),
                &increment_progress,
                &handle_failure,
            )?;

            if game_failures.is_empty() {
                *successes.lock().unwrap() += 1;
            } else {
                failures.lock().unwrap().extend(game_failures);
            }

            Ok(())
        })?;

        let successes = successes.into_inner().unwrap();
        let mut failures = failures.into_inner().unwrap();

        // mark any leftover directories as extras
        failures.extend(dirs.into_iter().map(|(_, v)| VerifyFailure::extra_dir(v)));

        failures.sort_unstable_by(|x, y| x.path().cmp(y.path()));

        Ok(VerifyResults {
            failures,
            summary: crate::game::VerifyResultsSummary {
                successes,
                total: self.flat.len() + self.tree.len(),
            },
        })
    }

    pub fn progress_bar(&self) -> indicatif::ProgressBar {
        indicatif::ProgressBar::new(
            (self.flat.len() + self.tree.values().map(|g| g.len()).sum::<usize>())
                .try_into()
                .unwrap(),
        )
        .with_style(crate::game::verify_style())
        .with_message(format!("{} ({})", self.name, self.version))
    }

    pub fn verify(&self, root: &Path, progress_bar: &indicatif::ProgressBar) -> VerifyResults {
        use crate::game::Never;

        let results = self
            .process(
                root,
                || progress_bar.inc(1),
                |failure| Ok::<_, Never>(Err(failure)),
            )
            .unwrap();

        results
    }

    pub fn add_and_verify(
        &self,
        roms: &mut RomSources,
        root: &Path,
        progress_bar: &indicatif::ProgressBar,
    ) -> Result<VerifyResults, Error> {
        self.process(
            root,
            || progress_bar.inc(1),
            |failure| match failure.try_fix(roms) {
                Ok(Ok(fix)) => {
                    progress_bar.println(fix.to_string());
                    Ok(Ok(()))
                }
                Ok(Err(f)) => Ok(Err(f)),
                Err(e) => Err(e),
            },
        )
    }
}

pub struct VerifyResults<'v> {
    pub failures: Vec<VerifyFailure<'v>>,
    pub summary: crate::game::VerifyResultsSummary,
}

#[inline]
fn parse_dat(file: Resource, data: Box<[u8]>, flatten: bool) -> Result<DatFile, Error> {
    let datafile: Datafile = match quick_xml::de::from_reader(std::io::Cursor::new(data)) {
        Ok(dat) => dat,
        Err(error) => return Err(Error::XmlFile(ResourceError { file, error })),
    };

    (if flatten {
        DatFile::new_flattened(datafile)
    } else {
        DatFile::new_unflattened(datafile)
    })
    .map_err(|error| Error::InvalidSha1(ResourceError { file, error }))
}

pub fn read_dats_from_file(file: Resource) -> Result<Vec<(Resource, Box<[u8]>)>, Error> {
    use super::is_zip;
    use std::io::Read;

    let mut f = file.open()?;

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
                    Ok((file.clone(), data.into_boxed_slice()))
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
pub fn read_dats(file: Resource) -> Result<Vec<DatFile>, Error> {
    read_dats_from_file(file).and_then(|v| {
        v.into_iter()
            .map(|(file, data)| parse_dat(file, data, true))
            .collect()
    })
}

#[inline]
pub fn read_unflattened_dats(file: Resource) -> Result<Vec<DatFile>, Error> {
    read_dats_from_file(file).and_then(|v| {
        v.into_iter()
            .map(|(file, data)| parse_dat(file, data, false))
            .collect()
    })
}
