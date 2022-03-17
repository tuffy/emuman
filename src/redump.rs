use super::split::{SplitDb, SplitGame, SplitPart};
use crate::dat::{DatFile, Datafile};
use std::collections::BTreeMap;

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

    for rom in game.rom.iter().flatten() {
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
