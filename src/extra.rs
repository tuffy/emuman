use super::game::{Game, Part};
use roxmltree::Document;

pub fn dat_to_game(tree: &Document) -> Game {
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
                "date" => {
                    game.year = child
                        .text()
                        .map(|s| s.to_string())
                        .unwrap_or_else(String::default)
                }
                _ => {}
            }
        }
    }

    if let Some(node) = root.children().find(|c| c.tag_name().name() == "machine") {
        game.name = node.attribute("name").unwrap().to_string();

        for child in node.children() {
            if child.tag_name().name() == "rom" {
                if let Some(sha1) = child.attribute("sha1") {
                    game.parts.insert(
                        child.attribute("name").unwrap().to_string(),
                        Part::new_rom(sha1),
                    );
                }
            }
        }
    }

    game
}
