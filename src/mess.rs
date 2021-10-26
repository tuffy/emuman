use super::{
    game::{Game, GameColumn, GameDb, GameRow, Part, Status},
    split::{SplitDb, SplitGame, SplitPart},
    Error,
};
use crate::game::parse_int;
use roxmltree::{Document, Node};
use rusqlite::{named_params, Transaction};
use std::collections::BTreeMap;
use std::path::Path;

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

pub fn add_file(xml_path: &Path, db: &Transaction) -> Result<(), Error> {
    use std::fs::File;
    use std::io::Read;

    let mut xml_file = String::new();
    File::open(xml_path).and_then(|mut f| f.read_to_string(&mut xml_file))?;

    let tree = Document::parse(&xml_file)?;
    add_software_list(db, &tree.root_element()).map_err(Error::Sql)
}

fn add_software_list(db: &Transaction, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO SoftwareList
        (name, description) VALUES (:name, :description)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":name": node.attribute("name"),
            ":description": node.attribute("description")
        })
    })?;

    let softwarelist_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "software")
        .try_for_each(|c| add_software(db, softwarelist_id, &c))
}

fn add_software(
    db: &Transaction,
    softwarelist_id: i64,
    node: &Node,
) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Software
        (softwarelist_id, name, cloneof, supported) VALUES
        (:softwarelist_id, :name, :cloneof, :supported)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":softwarelist_id": softwarelist_id,
            ":name": node.attribute("name"),
            ":cloneof": node.attribute("cloneof"),
            ":supported": node.attribute("supported").unwrap_or("yes")
        })
    })?;

    let software_id = db.last_insert_rowid();

    for child in node.children() {
        match child.tag_name().name() {
            "description" => add_description(db, software_id, &child)?,
            "year" => add_year(db, software_id, &child)?,
            "publisher" => add_publisher(db, software_id, &child)?,
            "info" => add_info(db, software_id, &child)?,
            "sharedfeat" => add_shared_feature(db, software_id, &child)?,
            "part" => add_part(db, software_id, &child)?,
            _ => { /*ignore unsupported nodes*/ }
        }
    }

    Ok(())
}

fn add_description(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "UPDATE Software SET description = :description WHERE
         (software_id = :software_id)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":software_id": software_id,
            ":description": node.text()
        })
    })
    .map(|_| ())
}

fn add_year(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached("UPDATE Software SET year = :year WHERE (software_id = :software_id)")
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":software_id": software_id,
                ":year": node.text()
            })
        })
        .map(|_| ())
}

fn add_publisher(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "UPDATE Software SET publisher = :publisher WHERE
         (software_id = :software_id)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":software_id": software_id,
            ":publisher": node.text()
        })
    })
    .map(|_| ())
}

fn add_info(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Info (software_id, name, value) VALUES
        (:software_id, :name, :value)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":software_id": software_id,
            ":name": node.attribute("name"),
            ":value": node.attribute("value")
        })
    })
    .map(|_| ())
}

fn add_shared_feature(
    db: &Transaction,
    software_id: i64,
    node: &Node,
) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO SharedFeature (software_id, name, value) VALUES
        (:software_id, :name, :value)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":software_id": software_id,
            ":name": node.attribute("name"),
            ":value": node.attribute("value")
        })
    })
    .map(|_| ())
}

fn add_part(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Part (software_id, name, interface) VALUES
        (:software_id, :name, :interface)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":software_id": software_id,
            ":name": node.attribute("name"),
            ":interface": node.attribute("interface")
        })
    })?;

    let part_id = db.last_insert_rowid();

    for child in node.children() {
        match child.tag_name().name() {
            "feature" => add_feature(db, part_id, &child)?,
            "dataarea" => add_data_area(db, part_id, &child)?,
            "diskarea" => add_disk_area(db, part_id, &child)?,
            "dipswitch" => add_dipswitch(db, part_id, &child)?,
            _ => { /*ignore unsupported nodes*/ }
        }
    }

    Ok(())
}

fn add_feature(db: &Transaction, part_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Feature (part_id, name, value) VALUES
        (:part_id, :name, :value)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":part_id": part_id,
            ":name": node.attribute("name"),
            ":value": node.attribute("value")
        })
    })
    .map(|_| ())
}

fn add_data_area(db: &Transaction, part_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO DataArea (part_id, name, size, width, endianness)
        VALUES (:part_id, :name, :size, :width, :endianness)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":part_id": part_id,
            ":name": node.attribute("name"),
            ":size": node.attribute("size").map(|s| parse_int(s).map(|i| i as i64).expect("invalid data area size")),
            ":width": node.attribute("width"),
            ":endianness": node.attribute("endianness")
        })
    })?;

    let data_area_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "rom")
        .try_for_each(|c| add_rom(db, data_area_id, &c))
}

fn add_rom(db: &Transaction, data_area_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO ROM
        (data_area_id, name, size, crc, sha1, offset, value, status, loadflag)
        VALUES
        (:data_area_id, :name, :size, :crc, :sha1,
         :offset, :value, :status, :loadflag)")
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":data_area_id": data_area_id,
            ":name": node.attribute("name"),
            ":size": node.attribute("size").map(|s| parse_int(s).map(|i| i as i64).expect("invalid ROM size integer")),
            ":crc": node.attribute("crc"),
            ":sha1": node.attribute("sha1"),
            ":offset": node.attribute("offset").map(|s| parse_int(s).map(|i| i as i64).expect("invalid ROM offset integer")),
            ":value": node.attribute("value"),
            ":status": node.attribute("status"),
            ":loadflag": node.attribute("loadflag")
        }
    ))
    .map(|_| ())
}

fn add_disk_area(db: &Transaction, part_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached("INSERT INTO DiskArea (part_id, name) VALUES (:part_id, :name)")
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":part_id": part_id,
                ":name": node.attribute("name")
            })
        })?;

    let disk_area_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "disk")
        .try_for_each(|c| add_disk(db, disk_area_id, &c))
}

fn add_disk(db: &Transaction, disk_area_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Disk (disk_area_id, name, sha1, status, writeable)
        VALUES (:disk_area_id, :name, :sha1, :status, :writeable)",
    )
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":disk_area_id": disk_area_id,
            ":name": node.attribute("name"),
            ":sha1": node.attribute("sha1"),
            ":status": node.attribute("status").unwrap_or("good"),
            ":writeable": node.attribute("writeable").unwrap_or("no")
        })
    })
    .map(|_| ())
}

fn add_dipswitch(db: &Transaction, part_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Dipswitch (part_id, name, tag, mask) VALUES
        (:part_id, :name, :tag, :mask)")
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":part_id": part_id,
            ":name": node.attribute("name"),
            ":tag": node.attribute("tag"),
            ":mask": node.attribute("mask").map(|s| parse_int(s).map(|i| i as i64).expect("invalid dipswitch mask integer"))
        })
    })?;

    let dipswitch_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "dipvalue")
        .try_for_each(|c| add_dipvalue(db, dipswitch_id, &c))
}

fn add_dipvalue(db: &Transaction, dipswitch_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Dipvalue (dipswitch_id, name, value, switch_default) VALUES
        (:dipswitch_id, :name, :value, :default)")
    .and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":dipswitch_id": dipswitch_id,
            ":name": node.attribute("name"),
            ":value": node.attribute("value").map(|s| parse_int(s).map(|i| i as i64).expect("invalid dipswitch value")),
            ":default": node.attribute("default").unwrap_or("no")
        })
    })
    .map(|_| ())
}

pub type MessDb = BTreeMap<String, GameDb>;

pub fn xml_to_game_db(split_db: &mut SplitDb, tree: &Document) -> (String, GameDb) {
    let root = tree.root_element();
    let mut db = GameDb::default();
    let software_list_name = root.attribute("name").unwrap().to_string();
    if let Some(description) = root.attribute("description") {
        db.description = description.to_string();
    }

    for software in root
        .children()
        .filter(|c| c.tag_name().name() == "software")
    {
        db.games.insert(
            software.attribute("name").unwrap().to_string(),
            xml_to_game(&software),
        );

        let (size, game) = xml_to_split(&software);
        if game.tracks.len() > 1 {
            split_db
                .games
                .entry(size)
                .or_insert_with(Vec::new)
                .push(game);
        }
    }

    (software_list_name, db)
}

fn xml_to_game(node: &Node) -> Game {
    fn node_to_rom(node: &Node) -> Option<(String, Part)> {
        (node.tag_name().name() == "rom")
            .then(|| {
                node.attribute("sha1").map(|sha1| {
                    (
                        node.attribute("name").unwrap().to_string(),
                        Part::new_rom(sha1, parse_int(node.attribute("size").unwrap()).unwrap()),
                    )
                })
            })
            .flatten()
    }

    fn node_to_disk(node: &Node) -> Option<(String, Part)> {
        (node.tag_name().name() == "disk")
            .then(|| {
                node.attribute("sha1").map(|sha1| {
                    (
                        Part::name_to_chd(node.attribute("name").unwrap()),
                        Part::new_disk(sha1),
                    )
                })
            })
            .flatten()
    }

    let mut game = Game {
        name: node.attribute("name").unwrap().to_string(),
        status: match node.attribute("supported") {
            Some("partial") => Status::Partial,
            Some("no") => Status::NotWorking,
            _ => Status::Working,
        },
        ..Game::default()
    };

    for child in node.children() {
        match child.tag_name().name() {
            "description" => {
                game.description = child
                    .text()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::default)
            }
            "year" => {
                game.year = child
                    .text()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::default)
            }
            "publisher" => {
                game.creator = child
                    .text()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::default)
            }
            "part" => {
                for child in child.children() {
                    match child.tag_name().name() {
                        "dataarea" => {
                            game.parts
                                .extend(child.children().filter_map(|n| node_to_rom(&n)));
                        }
                        "diskarea" => {
                            game.parts
                                .extend(child.children().filter_map(|n| node_to_disk(&n)));
                        }
                        _ => { /* ignore other child types */ }
                    }
                }
            }
            _ => { /*ignore other child types*/ }
        }
    }

    game
}

fn xml_to_split(node: &Node) -> (u64, SplitGame) {
    let mut offset = 0;
    let mut game = SplitGame {
        name: node.attribute("name").unwrap().to_string(),
        ..SplitGame::default()
    };

    for rom in node
        .children()
        .filter(|part| part.tag_name().name() == "part")
        .flat_map(|parts| {
            parts
                .children()
                .filter(|dataarea| dataarea.tag_name().name() == "dataarea")
                .flat_map(|dataarea| {
                    dataarea
                        .children()
                        .filter(|rom| rom.tag_name().name() == "rom")
                })
        })
    {
        if let Some(sha1) = rom.attribute("sha1") {
            let size = parse_int(rom.attribute("size").unwrap()).unwrap() as usize;
            game.tracks.push(SplitPart::new(
                rom.attribute("name").unwrap(),
                offset,
                offset + size as usize,
                sha1,
            ));
            offset += size;
        }
    }

    (offset as u64, game)
}

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
