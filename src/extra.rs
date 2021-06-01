use super::game::{Game, GameDb, Part};
use roxmltree::Document;
use std::collections::BTreeMap;

pub type ExtraDb = BTreeMap<String, GameDb>;

pub fn dat_to_game_db(tree: &Document) -> (String, GameDb) {
    let mut name = String::new();
    let mut game_db = GameDb::default();

    let root = tree.root_element();

    if let Some(node) = root.children().find(|c| c.tag_name().name() == "header") {
        for child in node.children() {
            match child.tag_name().name() {
                "description" => {
                    game_db.description = child
                        .text()
                        .map(|s| s.to_string())
                        .unwrap_or_else(String::default)
                }
                "date" => game_db.date = child.text().map(|s| s.to_string()),
                "name" => {
                    name = child.text().map(|s| s.to_string()).unwrap_or_default();
                }
                _ => {}
            }
        }
    }

    for node in root.children().filter(|c| c.tag_name().name() == "machine") {
        let mut game = Game {
            name: node.attribute("name").unwrap().to_string(),
            ..Game::default()
        };

        for child in node.children() {
            match child.tag_name().name() {
                "rom" => {
                    if let Some(sha1) = child.attribute("sha1") {
                        game.parts.insert(
                            child.attribute("name").unwrap().to_string(),
                            Part::new_rom(sha1),
                        );
                    }
                }
                "description" => {
                    game.description = child.text().map(|s| s.to_string()).unwrap_or_default();
                }
                _ => { /*ignore other children*/ }
            }
        }

        game_db.games.insert(game.name.clone(), game);
    }

    (name, game_db)
}
