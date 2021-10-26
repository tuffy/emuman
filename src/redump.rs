use super::{
    game::{Game, GameColumn, GameDb, Part},
    split::{SplitDb, SplitGame, SplitPart},
    Error,
};
use roxmltree::{Document, Node};
use rusqlite::{named_params, Transaction};
use std::collections::BTreeMap;
use std::path::Path;

const CREATE_DATAFILE: &str = "CREATE TABLE IF NOT EXISTS Datafile (
    datafile_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(80) NOT NULL,
    description VARCHAR(80) NOT NULL,
    version VARCHAR(80) NOT NULL,
    date VARCHAR(80) NOT NULL,
    author VARCHAR(80) NOT NULL,
    homepage VARCHAR(80) NOT NULL,
    url VARCHAR(80) NOT NULL
)";

const INSERT_DATAFILE: &str = "INSERT INTO Datafile
    (name, description, version, date, author, homepage, url) VALUES
    (:name, :description, :version, :date, :author, :homepage, :url)";

const CREATE_GAME: &str = "CREATE TABLE IF NOT EXISTS Game (
    game_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    datafile_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    category VARCHAR(80) NOT NULL,
    description VARCHAR(80) NOT NULL,
    FOREIGN KEY (datafile_id) REFERENCES Datafile (datafile_id) ON DELETE CASCADE
)";

const INSERT_GAME: &str = "INSERT INTO Game
    (datafile_id, name, category, description) VALUES
    (:datafile_id, :name, :category, :description)";

const CREATE_ROM: &str = "CREATE TABLE IF NOT EXISTS ROM (
    rom_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    game_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    size INTEGER NOT NULL,
    crc CHAR(8) NOT NULL,
    md5 CHAR(32) NOT NULL,
    sha1 CHAR(40) NOT NULL,
    FOREIGN KEY (game_id) REFERENCES Game (game_id) ON DELETE CASCADE
)";

const CREATE_ROM_INDEX: &str = "CREATE INDEX IF NOT EXISTS ROM_sha1 ON ROM (sha1)";

const INSERT_ROM: &str = "INSERT INTO ROM
    (game_id, name, size, crc, md5, sha1) VALUES
    (:game_id, :name, :size, :crc, :md5, :sha1)";

pub fn create_tables(db: &Transaction) -> Result<(), Error> {
    use rusqlite::params;

    db.execute(CREATE_DATAFILE, params![]).map_err(Error::Sql)?;
    db.execute(CREATE_GAME, params![]).map_err(Error::Sql)?;
    db.execute(CREATE_ROM, params![]).map_err(Error::Sql)?;
    db.execute(CREATE_ROM_INDEX, params![])
        .map_err(Error::Sql)?;
    Ok(())
}

pub fn clear_tables(db: &Transaction) -> Result<(), Error> {
    use rusqlite::params;

    db.execute("DELETE FROM Datafile", params![])
        .map(|_| ())
        .map_err(Error::Sql)
}

pub fn add_file(xml_path: &Path, db: &Transaction) -> Result<(), Error> {
    use std::fs::File;
    use std::io::Read;

    let mut xml_file = String::new();

    File::open(xml_path)
        .and_then(|mut f| f.read_to_string(&mut xml_file))
        .map_err(Error::IO)?;

    let tree = Document::parse(&xml_file).map_err(Error::Xml)?;

    add_data_file(db, &tree.root_element()).map_err(Error::Sql)
}

#[inline]
fn child_text<'a>(node: &'a Node, child_name: &str) -> Option<&'a str> {
    node.children()
        .find(|c| c.tag_name().name() == child_name)
        .and_then(|c| c.text())
}

fn add_data_file(db: &Transaction, node: &Node) -> Result<(), rusqlite::Error> {
    let header = node.children().find(|c| c.tag_name().name() == "header");

    db.prepare_cached(INSERT_DATAFILE).and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":name":
                header.as_ref().and_then(|node| child_text(node, "name")),
            ":description":
                header.as_ref().and_then(|node| child_text(node, "description")),
            ":version":
                header.as_ref().and_then(|node| child_text(node, "version")),
            ":date":
                header.as_ref().and_then(|node| child_text(node, "date")),
            ":author":
                header.as_ref().and_then(|node| child_text(node, "author")),
            ":homepage":
                header.as_ref().and_then(|node| child_text(node, "homepage")),
            ":url":
                header.as_ref().and_then(|node| child_text(node, "url")),
        })
    })?;

    let data_file_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "game")
        .try_for_each(|c| add_game(db, data_file_id, &c))
}

fn add_game(db: &Transaction, data_file_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(INSERT_GAME).and_then(|mut stmt| {
        stmt.execute(named_params! {
        ":datafile_id": data_file_id,
        ":name": node.attribute("name"),
        ":category": child_text(node, "category"),
        ":description": child_text(node, "description")})
    })?;

    let game_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "rom")
        .try_for_each(|c| add_rom(db, game_id, &c))
}

fn add_rom(db: &Transaction, game_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    use std::str::FromStr;

    db.prepare_cached(INSERT_ROM)
    .and_then(|mut stmt|
        stmt.execute(named_params! {
            ":game_id": game_id,
            ":name": node.attribute("name"),
            ":size": node.attribute("size").map(|s| i64::from_str(s).expect("invalid rom size integer")),
            ":crc": node.attribute("crc"),
            ":md5": node.attribute("md5"),
            ":sha1": node.attribute("sha1")}))
    .map(|_| ())
}

pub type RedumpDb = BTreeMap<String, GameDb>;

pub fn add_xml_file(split_db: &mut SplitDb, tree: &Document) -> (String, GameDb) {
    let root = tree.root_element();
    let header = root
        .children()
        .find(|c| c.tag_name().name() == "header")
        .unwrap();
    let name = header
        .children()
        .find(|c| c.tag_name().name() == "name")
        .and_then(|s| s.text().map(|s| s.to_string()))
        .unwrap_or_default();
    let description = header
        .children()
        .find(|c| c.tag_name().name() == "description")
        .and_then(|s| s.text().map(|s| s.to_string()))
        .unwrap_or_default();
    let mut game_db = GameDb {
        description,
        ..GameDb::default()
    };

    for game in root.children().filter(|c| c.tag_name().name() == "game") {
        game_db.games.insert(
            game.attribute("name").unwrap().to_string(),
            xml_to_game(&game),
        );

        let (total_size, split) = xml_to_split(&game);
        if split.tracks.len() > 1 {
            split_db
                .games
                .entry(total_size)
                .or_insert_with(Vec::new)
                .push(split);
        }
    }

    (name, game_db)
}

fn xml_to_game(node: &Node) -> Game {
    let mut game = Game {
        name: node.attribute("name").unwrap().to_string(),
        description: node
            .children()
            .find(|c| c.tag_name().name() == "description")
            .and_then(|c| c.text().map(|s| s.to_string()))
            .unwrap_or_default(),
        ..Game::default()
    };

    for rom in node.children().filter(|c| c.tag_name().name() == "rom") {
        game.parts.insert(
            rom.attribute("name").unwrap().to_string(),
            Part::new_rom(rom.attribute("sha1").unwrap()),
        );
    }

    game
}

fn xml_to_split(node: &Node) -> (u64, SplitGame) {
    let mut offset = 0;
    let mut game = SplitGame {
        name: node.attribute("name").unwrap().to_string(),
        ..SplitGame::default()
    };

    for rom in node.children().filter(|c| c.tag_name().name() == "rom") {
        use std::str::FromStr;

        let name = rom.attribute("name").unwrap();
        if name.ends_with(".bin") {
            let size = usize::from_str(rom.attribute("size").unwrap()).unwrap();
            game.tracks.push(SplitPart::new(
                name,
                offset,
                offset + size,
                rom.attribute("sha1").unwrap(),
            ));
            offset += size;
        }
    }

    (offset as u64, game)
}

pub fn list_all(db: &RedumpDb) {
    use prettytable::{cell, format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for (name, game_db) in db.iter() {
        table.add_row(row![game_db.description, name]);
    }

    table.printstd();
}

pub fn list(db: &GameDb, search: Option<&str>) {
    use super::game::GameRow;
    use prettytable::{cell, format, row, Table};

    let mut results: Vec<GameRow> = if let Some(search) = search {
        db.games
            .values()
            .map(|g| g.report(false))
            .filter(|g| g.matches(search))
            .collect()
    } else {
        db.games.values().map(|g| g.report(false)).collect()
    };

    results.sort_by(|a, b| a.compare(b, GameColumn::Description));

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    for game in results {
        table.add_row(row![game.description]);
    }

    table.printstd();
}
