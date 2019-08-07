use super::rom::RomId;
use super::Error;
use roxmltree::{Document, Node};
use rusqlite::{named_params, Transaction};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io;
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

    db.execute(CREATE_DATAFILE, params![]).map_err(Error::SQL)?;
    db.execute(CREATE_GAME, params![]).map_err(Error::SQL)?;
    db.execute(CREATE_ROM, params![]).map_err(Error::SQL)?;
    db.execute(CREATE_ROM_INDEX, params![])
        .map_err(Error::SQL)?;
    Ok(())
}

pub fn clear_tables(db: &Transaction) -> Result<(), Error> {
    use rusqlite::params;

    db.execute("DELETE FROM Datafile", params![])
        .map(|_| ())
        .map_err(Error::SQL)
}

pub fn add_file(xml_path: &Path, db: &Transaction) -> Result<(), Error> {
    use std::fs::File;
    use std::io::Read;

    let mut xml_file = String::new();

    File::open(xml_path)
        .and_then(|mut f| f.read_to_string(&mut xml_file))
        .map_err(Error::IO)?;

    let tree = Document::parse(&xml_file).map_err(Error::XML)?;

    add_data_file(db, &tree.root_element()).map_err(Error::SQL)
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
        stmt.execute_named(named_params! {
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
        stmt.execute_named(named_params! {
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
        stmt.execute_named(named_params! {
            ":game_id": game_id,
            ":name": node.attribute("name"),
            ":size": node.attribute("size").map(|s| i64::from_str(s).expect("invalid rom size integer")),
            ":crc": node.attribute("crc"),
            ":md5": node.attribute("md5"),
            ":sha1": node.attribute("sha1")}))
    .map(|_| ())
}

#[derive(Default, Serialize, Deserialize)]
pub struct VerifyDb {
    roms: HashMap<String, Vec<RomId>>,
}

impl VerifyDb {
    pub fn add_xml(&mut self, node: &Node) {
        for game in node.children().filter(|c| c.tag_name().name() == "game") {
            for child in game.children() {
                if let Some(rom_id) = RomId::from_node(&child) {
                    self.roms
                        .entry(child.attribute("name").unwrap().to_string())
                        .or_insert_with(Vec::new)
                        .push(rom_id);
                }
            }
        }
    }
}

pub enum VerifyResult {
    Ok,
    Bad,
    NotFound,
}

impl fmt::Display for VerifyResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VerifyResult::Ok => write!(f, "OK"),
            VerifyResult::Bad => write!(f, "BAD"),
            VerifyResult::NotFound => write!(f, "NOT FOUND"),
        }
    }
}

pub fn verify(db: &VerifyDb, rom: &Path) -> Result<VerifyResult, io::Error> {
    let name = match rom.file_name().and_then(|f| f.to_str()) {
        Some(n) => n,
        None => return Ok(VerifyResult::NotFound),
    };

    if let Some(matches) = db.roms.get(name) {
        let rom_id = RomId::from_path(rom)?;

        if matches.iter().any(|match_id| &rom_id == match_id) {
            Ok(VerifyResult::Ok)
        } else {
            Ok(VerifyResult::Bad)
        }
    } else {
        Ok(VerifyResult::NotFound)
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct SplitDb(HashMap<u64, Vec<SplitGame>>);

impl SplitDb {
    pub fn add_xml(&mut self, node: &Node) {
        for c in node.children().filter(|c| c.tag_name().name() == "game") {
            let (k, v) = SplitGame::from_xml(&c);
            self.0.entry(k).or_insert_with(Vec::new).push(v);
        }
    }

    #[inline]
    pub fn possible_matches(&self, bin_size: u64) -> Option<&[SplitGame]> {
        self.0.get(&bin_size).map(|v| v.as_slice())
    }
}

#[derive(Serialize, Deserialize)]
pub struct SplitGame(Vec<SplitTrack>);

impl SplitGame {
    fn from_xml(node: &Node) -> (u64, Self) {
        let mut offset = 0;
        let mut tracks = Vec::new();

        for child in node.children().filter(|c| {
            c.attribute("name")
                .map(|n| n.ends_with(".bin"))
                .unwrap_or(false)
        }) {
            if let Some(rom_id) = RomId::from_node(&child) {
                tracks.push(SplitTrack {
                    name: child.attribute("name").unwrap().to_string(),
                    start: offset,
                    end: offset + rom_id.size as usize,
                    sha1: rom_id.sha1,
                });
                offset += rom_id.size as usize;
            }
        }

        (offset as u64, SplitGame(tracks))
    }

    pub fn matches(&self, data: &[u8]) -> bool {
        use rayon::prelude::*;
        self.0.par_iter().all(|t| t.matches(data))
    }

    pub fn extract(&self, root: &Path, data: &[u8]) -> Result<(), io::Error> {
        use rayon::prelude::*;
        self.0.par_iter().try_for_each(|t| t.extract(root, data))
    }
}

#[derive(Serialize, Deserialize)]
pub struct SplitTrack {
    name: String,
    start: usize,
    end: usize,
    sha1: String,
}

impl SplitTrack {
    fn matches(&self, data: &[u8]) -> bool {
        use sha1::Sha1;

        Sha1::from(&data[self.start..self.end]).hexdigest() == self.sha1
    }

    fn extract(&self, root: &Path, data: &[u8]) -> Result<(), io::Error> {
        use std::fs::File;
        use std::io::Write;

        let path = root.join(&self.name);
        match File::create(&path).and_then(|mut f| f.write_all(&data[self.start..self.end])) {
            Ok(()) => {
                println!("* {}", path.display());
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}
