use super::{
    game::{Game, GameColumn, GameDb, GameParts, GameRow, Part as GamePart, Status},
    split::{SplitDb, SplitGame, SplitPart},
    Error,
};
use crate::game::parse_int;
use rusqlite::{named_params, Transaction};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

const CREATE_SOFTWARE_LIST: &str = "CREATE TABLE IF NOT EXISTS SoftwareList (
    softwarelist_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(80) NOT NULL,
    description VARCHAR(80)
)";

const CREATE_SOFTWARE: &str = "CREATE TABLE IF NOT EXISTS Software (
    software_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    softwarelist_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    cloneof VARCHAR(80),
    supported CHAR(3) NOT NULL,
    description VARCHAR(80),
    year CHAR(4),
    publisher VARCHAR(80),
    FOREIGN KEY (softwarelist_id) REFERENCES SoftwareList (softwarelist_id) ON DELETE CASCADE
)";

const CREATE_SOFTWARE_INDEX: &str = "CREATE INDEX IF NOT EXISTS Software_name ON Software (name)";

const CREATE_INFO: &str = "CREATE TABLE IF NOT EXISTS Info (
    software_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value VARCHAR(80),
    FOREIGN KEY (software_id) REFERENCES Software (software_id) ON DELETE CASCADE
)";

const CREATE_SHARED_FEATURE: &str = "CREATE TABLE IF NOT EXISTS SharedFeature (
    software_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value VARCHAR(80),
    FOREIGN KEY (software_id) REFERENCES Software (software_id) ON DELETE CASCADE
)";

const CREATE_PART: &str = "CREATE TABLE IF NOT EXISTS Part (
    part_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    software_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    interface VARCHAR(80) NOT NULL,
    FOREIGN KEY (software_id) REFERENCES Software (software_id) ON DELETE CASCADE
)";

const CREATE_FEATURE: &str = "CREATE TABLE IF NOT EXISTS Feature (
    part_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value VARCHAR(80),
    FOREIGN KEY (part_id) REFERENCES Part (part_id) ON DELETE CASCADE
)";

const CREATE_DATA_AREA: &str = "CREATE TABLE IF NOT EXISTS DataArea (
    data_area_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    part_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    size VARCHAR(10),
    width CHAR(2),
    endianness CHAR(6),
    FOREIGN KEY (part_id) REFERENCES Part (part_id) ON DELETE CASCADE
)";

const CREATE_ROM: &str = "CREATE TABLE IF NOT EXISTS ROM (
    data_area_id INTEGER NOT NULL,
    name VARCHAR(80),
    size INTEGER,
    crc CHAR(8),
    sha1 CHAR(40),
    offset INTEGER,
    value VARCHAR(4),
    status CHAR(7),
    loadflag VARCHAR(20),
    FOREIGN KEY (data_area_id) REFERENCES DataArea (data_area_id) ON DELETE CASCADE
)";

const CREATE_ROM_INDEX: &str = "CREATE INDEX IF NOT EXISTS ROM_sha1 ON ROM (sha1)";

const CREATE_DISK_AREA: &str = "CREATE TABLE IF NOT EXISTS DiskArea (
    disk_area_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    part_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    FOREIGN KEY (part_id) REFERENCES Part (part_id) ON DELETE CASCADE
)";

const CREATE_DISK: &str = "CREATE TABLE IF NOT EXISTS Disk (
    disk_area_id INTEGER NOT NULL,
    name VARCHAR(80),
    sha1 CHAR(40),
    status CHAR(7) NOT NULL,
    writeable CHAR(2) NOT NULL,
    FOREIGN KEY (disk_area_id) REFERENCES DiskArea(disk_area_id) ON DELETE CASCADE
)";

const CREATE_DISK_INDEX: &str = "CREATE INDEX IF NOT EXISTS Disk_sha1 ON Disk (sha1)";

const CREATE_DIPSWITCH: &str = "CREATE TABLE IF NOT EXISTS Dipswitch (
    dipswitch_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    part_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    tag VARCHAR(10) NOT NULL,
    mask INTEGER NOT NULL,
    FOREIGN KEY (part_id) REFERENCES Part (part_id) ON DELETE CASCADE
)";

const CREATE_DIPVALUE: &str = "CREATE TABLE IF NOT EXISTS Dipvalue (
    dipswitch_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value INTEGER NOT NULL,
    switch_default CHAR(3) NOT NULL,
    FOREIGN KEY (dipswitch_id) REFERENCES Dipswitch (dipswitch_id) ON DELETE CASCADE
)";

pub fn create_tables(db: &Transaction) -> Result<(), Error> {
    use rusqlite::params;

    db.execute(CREATE_SOFTWARE_LIST, params![])?;
    db.execute(CREATE_SOFTWARE, params![])?;
    db.execute(CREATE_SOFTWARE_INDEX, params![])?;
    db.execute(CREATE_INFO, params![])?;
    db.execute(CREATE_SHARED_FEATURE, params![])?;
    db.execute(CREATE_PART, params![])?;
    db.execute(CREATE_FEATURE, params![])?;
    db.execute(CREATE_DATA_AREA, params![])?;
    db.execute(CREATE_ROM, params![])?;
    db.execute(CREATE_ROM_INDEX, params![])?;
    db.execute(CREATE_DISK_AREA, params![])?;
    db.execute(CREATE_DISK, params![])?;
    db.execute(CREATE_DISK_INDEX, params![])?;
    db.execute(CREATE_DIPSWITCH, params![])?;
    db.execute(CREATE_DIPVALUE, params![])?;
    Ok(())
}

pub fn clear_tables(db: &Transaction) -> Result<(), Error> {
    use rusqlite::params;

    db.execute("DELETE FROM SoftwareList", params![])
        .map(|_| ())
        .map_err(Error::Sql)
}

#[derive(Debug, Deserialize)]
pub struct Softwarelist {
    pub name: String,
    pub description: String,
    pub software: Option<Vec<Software>>,
}

impl Softwarelist {
    fn into_db(self, db: &Transaction) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO SoftwareList
            (name, description) VALUES (:name, :description)",
        )
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":name": self.name,
                ":description": self.description,
            })
        })?;

        let softwarelist_id = db.last_insert_rowid();

        self.software
            .into_iter()
            .flatten()
            .try_for_each(|s| s.into_db(db, softwarelist_id))
    }

    pub fn into_game_db(self) -> GameDb {
        GameDb {
            description: self.description,
            date: None,
            games: self
                .software
                .into_iter()
                .flatten()
                .map(|game| game.into_game())
                .map(|game| (game.name.clone(), game))
                .collect(),
        }
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
    pub name: String,
    pub description: String,
    pub year: String,
    pub publisher: String,
    pub cloneof: Option<String>,
    pub supported: Option<String>,
    pub info: Option<Vec<Info>>,
    pub part: Option<Vec<Part>>,
}

impl Software {
    fn into_db(self, db: &Transaction, softwarelist_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO Software
            (softwarelist_id, name, cloneof, supported, description, year, publisher) VALUES
            (:softwarelist_id, :name, :cloneof, :supported, :description, :year, :publisher)",
        )
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":softwarelist_id": softwarelist_id,
                ":name": self.name,
                ":cloneof": self.cloneof,
                ":supported": self.supported.as_deref().unwrap_or("yes"),
                ":description": self.description,
                ":year": self.year,
                ":publisher": self.publisher,
            })
        })?;

        let software_id = db.last_insert_rowid();

        self.info
            .into_iter()
            .flatten()
            .try_for_each(|info| info.into_db(db, software_id))?;

        self.part
            .into_iter()
            .flatten()
            .try_for_each(|part| part.into_db(db, software_id))
    }

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
        let mut game = SplitGame {
            name: self.name.clone(),
            ..SplitGame::default()
        };

        for RomSize { name, size, sha1 } in rom_sizes {
            game.tracks.push(SplitPart::new(name, offset, offset + size as usize, sha1));
            offset += size as usize;
        }

        Some((total, game))
    }
}

#[derive(Debug, Deserialize)]
pub struct Info {
    pub name: String,
    pub value: String,
}

impl Info {
    fn into_db(self, db: &Transaction, software_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO Info (software_id, name, value) VALUES
            (:software_id, :name, :value)",
        )
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":software_id": software_id,
                ":name": self.name,
                ":value": self.value
            })
        })
        .map(|_| ())
    }
}

#[derive(Debug, Deserialize)]
pub struct Part {
    pub name: String,
    pub interface: Option<String>,
    pub feature: Option<Vec<Feature>>,
    pub dataarea: Option<Vec<Dataarea>>,
    pub diskarea: Option<Vec<Diskarea>>,
    pub dipswitch: Option<Vec<Dipswitch>>,
}

impl Part {
    fn into_db(self, db: &Transaction, software_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO Part (software_id, name, interface) VALUES
            (:software_id, :name, :interface)",
        )
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":software_id": software_id,
                ":name": self.name,
                ":interface": self.interface,
            })
        })?;

        let part_id = db.last_insert_rowid();

        self.feature
            .into_iter()
            .flatten()
            .try_for_each(|feature| feature.into_db(db, part_id))?;

        self.dataarea
            .into_iter()
            .flatten()
            .try_for_each(|dataarea| dataarea.into_db(db, part_id))?;

        self.diskarea
            .into_iter()
            .flatten()
            .try_for_each(|diskarea| diskarea.into_db(db, part_id))?;

        self.dipswitch
            .into_iter()
            .flatten()
            .try_for_each(|dipswitch| dipswitch.into_db(db, part_id))
    }

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
pub struct Feature {
    pub name: String,
    pub value: String,
}

impl Feature {
    fn into_db(self, db: &Transaction, part_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO Feature (part_id, name, value) VALUES
            (:part_id, :name, :value)",
        )
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":part_id": part_id,
                ":name": self.name,
                ":value": self.value,
            })
        })
        .map(|_| ())
    }
}

#[derive(Debug, Deserialize)]
pub struct Dataarea {
    pub name: String,
    pub size: Option<String>,
    pub width: Option<String>,
    pub endianness: Option<String>,
    pub rom: Option<Vec<Rom>>,
}

impl Dataarea {
    fn into_db(self, db: &Transaction, part_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO DataArea (part_id, name, size, width, endianness)
            VALUES (:part_id, :name, :size, :width, :endianness)",
        )
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":part_id": part_id,
                ":name": self.name,
                ":size": self.size.as_ref().map(|s| parse_int(s).map(|i| i as i64).unwrap_or_default()),
                ":width": self.width,
                ":endianness": self.endianness,
            })
        })?;

        let data_area_id = db.last_insert_rowid();

        self.rom
            .into_iter()
            .flatten()
            .try_for_each(|rom| rom.into_db(db, data_area_id))
    }

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
    crc: Option<String>,
    sha1: Option<String>,
    offset: Option<String>,
    value: Option<String>,
    status: Option<String>,
    loadflag: Option<String>,
}

impl Rom {
    fn into_db(self, db: &Transaction, data_area_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO ROM
            (data_area_id, name, size, crc, sha1, offset, value, status, loadflag)
            VALUES
            (:data_area_id, :name, :size, :crc, :sha1,
             :offset, :value, :status, :loadflag)")
        .and_then(|mut stmt| stmt.execute(
            named_params! {
                ":data_area_id": data_area_id,
                ":name": self.name,
                ":size": self.size.as_ref().map(|s| parse_int(s).map(|i| i as i64).expect("invalid ROM size integer")),
                ":crc": self.crc,
                ":sha1": self.sha1,
                ":offset": self.offset.as_ref().map(|s| parse_int(s).map(|i| i as i64).expect("invalid ROM offset integer")),
                ":value": self.value,
                ":status": self.status,
                ":loadflag": self.loadflag,
            }
        ))
        .map(|_| ())
    }

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
    pub name: String,
    pub disk: Option<Vec<Disk>>,
}

impl Diskarea {
    fn into_db(self, db: &Transaction, part_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached("INSERT INTO DiskArea (part_id, name) VALUES (:part_id, :name)")
            .and_then(|mut stmt| {
                stmt.execute(named_params! {
                    ":part_id": part_id,
                    ":name": self.name,
                })
            })?;

        let disk_area_id = db.last_insert_rowid();

        self.disk
            .into_iter()
            .flatten()
            .try_for_each(|disk| disk.into_db(db, disk_area_id))
    }

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
    status: Option<String>,
    writeable: Option<String>,
}

impl Disk {
    fn into_db(self, db: &Transaction, disk_area_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO Disk (disk_area_id, name, sha1, status, writeable)
            VALUES (:disk_area_id, :name, :sha1, :status, :writeable)",
        )
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":disk_area_id": disk_area_id,
                ":name": self.name,
                ":sha1": self.sha1,
                ":status": self.status.as_deref().unwrap_or("good"),
                ":writeable": self.writeable.as_deref().unwrap_or("no")
            })
        })
        .map(|_| ())
    }

    #[inline]
    fn into_part(self) -> Option<(String, GamePart)> {
        Some((self.name, GamePart::new_disk(&self.sha1?).ok()?))
    }
}

#[derive(Debug, Deserialize)]
pub struct Dipswitch {
    name: String,
    tag: String,
    mask: Option<String>,
    dipvalue: Vec<Dipvalue>,
}

impl Dipswitch {
    fn into_db(self, db: &Transaction, part_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO Dipswitch (part_id, name, tag, mask) VALUES
            (:part_id, :name, :tag, :mask)")
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":part_id": part_id,
                ":name": self.name,
                ":tag": self.tag,
                ":mask": self.mask.as_ref().map(|s| parse_int(s).map(|i| i as i64).expect("invalid dipswitch mask integer"))
            })
        })?;

        let dipswitch_id = db.last_insert_rowid();

        self.dipvalue
            .into_iter()
            .try_for_each(|value| value.into_db(db, dipswitch_id))
    }
}

#[derive(Debug, Deserialize)]
pub struct Dipvalue {
    name: String,
    value: Option<String>,
    default: Option<String>,
}

impl Dipvalue {
    fn into_db(self, db: &Transaction, dipswitch_id: i64) -> Result<(), rusqlite::Error> {
        db.prepare_cached(
            "INSERT INTO Dipvalue (dipswitch_id, name, value, switch_default) VALUES
            (:dipswitch_id, :name, :value, :default)")
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":dipswitch_id": dipswitch_id,
                ":name": self.name,
                ":value": self.value.as_ref().map(|s| parse_int(s).map(|i| i as i64).expect("invalid dipswitch value")),
                ":default": self.default.as_deref().unwrap_or("no")
            })
        })
        .map(|_| ())
    }
}

pub fn add_file(file: PathBuf, db: &Transaction) -> Result<(), Error> {
    quick_xml::de::from_reader(std::fs::File::open(&file).map(std::io::BufReader::new)?)
        .map_err(|error: quick_xml::de::DeError| Error::SerdeXml(crate::FileError { error, file }))
        .and_then(|sl: Softwarelist| sl.into_db(db).map_err(crate::Error::Sql))
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
    use prettytable::{cell, format, row, Table};

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
    use prettytable::{cell, format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for (name, game_db) in db.iter() {
        table.add_row(row![game_db.description, name]);
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
