use super::{
    split::{SplitDb, SplitGame, SplitPart},
    Error,
};
use crate::dat::{DatFile, Datafile};
use rusqlite::{named_params, Transaction};
use std::collections::BTreeMap;

const CREATE_DATAFILE: &str = "CREATE TABLE IF NOT EXISTS Datafile (
    datafile_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(80) NOT NULL,
    description VARCHAR(80) NOT NULL,
    version VARCHAR(80) NOT NULL,
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

pub fn add_data_file_to_db(db: &Transaction, file: &Datafile) -> Result<(), rusqlite::Error> {
    db.prepare_cached(INSERT_DATAFILE).and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":name": file.header.name,
            ":description": file.header.description,
            ":version": file.header.version,
            ":author": file.header.author,
            ":homepage": file.header.homepage,
            ":url": file.header.url,
        })
    })?;

    let data_file_id = db.last_insert_rowid();

    file.game
        .iter()
        .flatten()
        .try_for_each(|game| add_game(db, data_file_id, game))
}

fn add_game(
    db: &Transaction,
    data_file_id: i64,
    game: &crate::dat::Game,
) -> Result<(), rusqlite::Error> {
    db.prepare_cached(INSERT_GAME).and_then(|mut stmt| {
        stmt.execute(named_params! {
        ":datafile_id": data_file_id,
        ":name": game.name,
        ":description": game.description})
    })?;

    let game_id = db.last_insert_rowid();

    game.rom
        .iter()
        .try_for_each(|rom| add_rom(db, game_id, rom))
}

fn add_rom(db: &Transaction, game_id: i64, rom: &crate::dat::Rom) -> Result<(), rusqlite::Error> {
    db.prepare_cached(INSERT_ROM)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
            ":game_id": game_id,
            ":name": rom.name,
            ":size": rom.size,
            ":crc": rom.crc,
            ":md5": rom.md5,
            ":sha1": rom.sha1})
        })
        .map(|_| ())
}

pub type RedumpDb = BTreeMap<String, DatFile>;

pub fn populate_split_db(split_db: &mut SplitDb, datafile: &Datafile) {
    for game in datafile.game.iter().flatten() {
        let (total_size, split) = game_to_split(game);
        if split.tracks.len() > 1 {
            split_db
                .games
                .entry(total_size)
                .or_insert_with(Vec::new)
                .push(split);
        }
    }
}

fn game_to_split(game: &crate::dat::Game) -> (u64, SplitGame) {
    let mut offset = 0;
    let mut split_game = SplitGame {
        name: game.name.clone(),
        ..SplitGame::default()
    };

    for rom in &game.rom {
        if rom.name.ends_with(".bin") {
            let size = rom.size.unwrap() as usize;
            split_game.tracks.push(SplitPart::new(
                &rom.name,
                offset,
                offset + size,
                rom.sha1.as_ref().unwrap(),
            ));
            offset += size;
        }
    }

    (offset as u64, split_game)
}

pub fn list_all(db: &RedumpDb) {
    use prettytable::{cell, format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for (_, datfile) in db.iter() {
        table.add_row(row![datfile.name(), datfile.version()]);
    }

    table.printstd();
}

pub fn list(datfile: &DatFile) {
    use prettytable::{cell, format, row, Table};

    let games = datfile.games().collect::<Vec<_>>();

    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for game in games {
        table.add_row(row![game]);
    }

    table.printstd();
}
