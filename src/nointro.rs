use super::game::{Game, Part};
use roxmltree::Document;
use std::collections::BTreeMap;

pub type NointroDb = BTreeMap<String, Game>;

// treat entire .DAT file as a single game and each ROM
// as a part of that game, in order to keep the ROMs as sibling
// files instead of dumping them all into their own subdirectories

pub fn dat_to_nointro_db(tree: &Document) -> (String, Game) {
    let mut name = String::new();
    let mut game = Game::default();

    let root = tree.root_element();

    if let Some(node) = root.children().find(|c| c.tag_name().name() == "header") {
        for child in node.children() {
            match child.tag_name().name() {
                "description" => {
                    game.description = child
                        .text()
                        .map(|s| s.to_string())
                        .unwrap_or_else(String::default)
                }
                // overload year field as version
                "version" => game.year = child.text().map(|s| s.to_string()).unwrap_or_default(),
                "name" => {
                    name = child.text().map(|s| s.to_string()).unwrap_or_default();
                }
                _ => {}
            }
        }
    }

    for node in root.children().filter(|c| c.tag_name().name() == "game") {
        for rom in node.children().filter(|c| c.tag_name().name() == "rom") {
            if let Some(sha1) = rom.attribute("sha1") {
                game.parts.insert(
                    rom.attribute("name").unwrap().to_string(),
                    Part::new_rom(sha1).unwrap(),
                );
            }
        }
    }

    (name, game)
}

pub fn list_all(db: &NointroDb) {
    use prettytable::{cell, format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for (name, game_db) in db.iter() {
        table.add_row(row![name, game_db.year]);
    }

    table.printstd();
}

pub fn list(game: &Game) {
    let mut roms = game
        .parts
        .keys()
        .map(|key| key.as_str())
        .collect::<Vec<_>>();
    roms.sort_unstable();

    use prettytable::{cell, format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for rom in roms {
        table.add_row(row![rom]);
    }

    table.printstd();
}
