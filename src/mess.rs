use super::{
    game::{Game, GameColumn, GameDb, GameParts, GameRow, Part as GamePart, Status},
    split::{SplitDb, SplitGame, SplitPart},
};
use crate::game::parse_int;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
pub struct Softwarelist {
    name: String,
    description: String,
    software: Option<Vec<Software>>,
}

impl Softwarelist {
    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn into_game_db(self) -> GameDb {
        GameDb::new(
            self.description,
            self.software
                .into_iter()
                .flatten()
                .map(|game| game.into_game())
                .map(|game| (game.name.clone(), game))
                .collect(),
        )
    }

    #[inline]
    pub fn populate_split_db(&self, db: &mut SplitDb) {
        db.extend(
            self.software
                .iter()
                .flatten()
                .filter_map(|software| software.to_split_db()),
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct Software {
    name: String,
    description: String,
    year: String,
    publisher: String,
    supported: Option<String>,
    part: Option<Vec<Part>>,
}

impl Software {
    fn into_game(self) -> Game {
        Game {
            name: self.name,
            description: self.description,
            creator: self.publisher,
            year: self.year,
            status: match self.supported.as_deref() {
                Some("partial") => Status::Partial,
                Some("no") => Status::NotWorking,
                _ => Status::Working,
            },
            is_device: false,
            devices: Vec::default(),
            parts: self
                .part
                .into_iter()
                .flatten()
                .map(|part| part.into_parts())
                .reduce(|mut acc, item| {
                    acc.extend(item.into_iter());
                    acc
                })
                .unwrap_or_default(),
        }
    }

    fn to_split_db(&self) -> Option<(u64, SplitGame)> {
        let rom_sizes = self
            .part
            .iter()
            .flatten()
            .next()
            .and_then(|part| part.dataarea.iter().flatten().next())
            .and_then(|dataarea| {
                dataarea
                    .rom
                    .iter()
                    .flatten()
                    .map(|rom| rom.to_size())
                    .collect::<Option<Vec<RomSize>>>()
            })
            .filter(|sizes| sizes.len() > 1)?;

        let total: u64 = rom_sizes.iter().map(|s| s.size).sum();

        let mut offset = 0;
        let mut game = SplitGame::new(self.name.clone());

        for RomSize { name, size, sha1 } in rom_sizes {
            game.push_track(SplitPart::new(name, offset, offset + size as usize, sha1));
            offset += size as usize;
        }

        Some((total, game))
    }
}

#[derive(Debug, Deserialize)]
pub struct Part {
    dataarea: Option<Vec<Dataarea>>,
    diskarea: Option<Vec<Diskarea>>,
}

impl Part {
    fn into_parts(self) -> GameParts {
        self.dataarea
            .into_iter()
            .flatten()
            .flat_map(|dataarea| dataarea.into_parts())
            .chain(
                self.diskarea
                    .into_iter()
                    .flatten()
                    .flat_map(|diskarea| diskarea.into_parts()),
            )
            .collect()
    }
}

#[derive(Debug, Deserialize)]
pub struct Dataarea {
    rom: Option<Vec<Rom>>,
}

impl Dataarea {
    fn into_parts(self) -> impl Iterator<Item = (String, GamePart)> {
        self.rom
            .into_iter()
            .flatten()
            .flat_map(|rom| rom.into_part())
    }
}

#[derive(Debug, Deserialize)]
pub struct Rom {
    name: Option<String>,
    size: Option<String>,
    sha1: Option<String>,
}

impl Rom {
    #[inline]
    fn into_part(self) -> Option<(String, GamePart)> {
        Some((self.name?, GamePart::new_rom(&self.sha1?).ok()?))
    }

    #[inline]
    fn to_size(&self) -> Option<RomSize> {
        Some(RomSize {
            name: self.name.as_deref()?,
            size: parse_int(self.size.as_deref()?).ok()?,
            sha1: self.sha1.as_deref()?,
        })
    }
}

struct RomSize<'s> {
    name: &'s str,
    size: u64,
    sha1: &'s str,
}

#[derive(Debug, Deserialize)]
pub struct Diskarea {
    disk: Option<Vec<Disk>>,
}

impl Diskarea {
    fn into_parts(self) -> impl Iterator<Item = (String, GamePart)> {
        self.disk
            .into_iter()
            .flatten()
            .flat_map(|disk| disk.into_part())
    }
}

#[derive(Debug, Deserialize)]
pub struct Disk {
    name: String,
    sha1: Option<String>,
}

impl Disk {
    #[inline]
    fn into_part(self) -> Option<(String, GamePart)> {
        Some((self.name + ".chd", GamePart::new_disk(&self.sha1?).ok()?))
    }
}

pub type MessDb = BTreeMap<String, GameDb>;

pub fn list(db: &MessDb, search: Option<&str>, sort: GameColumn, simple: bool) {
    let mut results: Vec<(&str, GameRow)> = db
        .iter()
        .flat_map(|(name, game_db)| {
            game_db
                .list_results(search, simple)
                .into_iter()
                .map(move |row| (name.as_str(), row))
        })
        .collect();

    results.sort_by(|(_, a), (_, b)| a.compare(b, sort));

    display_results(&results);
}

pub fn display_results(results: &[(&str, GameRow)]) {
    use prettytable::{format, row, Table};

    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.get_format().column_separator('\u{2502}');

    for (db_name, game) in results {
        let description = game.description;
        let creator = game.creator;
        let year = game.year;
        let name = game.name;

        table.add_row(match game.status {
            Status::Working => row![description, creator, year, db_name, name],
            Status::Partial => row![FY => description, creator, year, db_name, name],
            Status::NotWorking => row![FR => description, creator, year, db_name, name],
        });
    }

    table.printstd();
}

pub fn list_all(db: &MessDb) {
    use prettytable::{format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for (name, game_db) in db.iter() {
        table.add_row(row![game_db.description(), name]);
    }

    table.printstd();
}

pub fn strip_ines_header(data: &[u8]) -> &[u8] {
    if (data.len() >= 4) && (&data[0..4] == b"NES\x1a") {
        &data[16..]
    } else {
        data
    }
}
