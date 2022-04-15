use crate::dat::Datafile;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct SplitDb {
    games: HashMap<u64, Vec<SplitGame>>,
}

impl SplitDb {
    #[inline]
    pub fn new() -> Self {
        Self {
            games: HashMap::new(),
        }
    }

    pub fn populate(&mut self, datafile: &Datafile) {
        fn game_to_split(game: &crate::dat::Game) -> (u64, SplitGame) {
            let mut offset = 0;
            let mut split_game = SplitGame::new(game.name().to_owned());

            for rom in game.roms() {
                if rom.name().ends_with(".bin") {
                    let size = rom.size().unwrap() as usize;
                    split_game.push_track(SplitPart::new(
                        rom.name(),
                        offset,
                        offset + size,
                        rom.sha1().unwrap(),
                    ));
                    offset += size;
                }
            }

            (offset as u64, split_game)
        }

        for game in datafile.games() {
            let (total_size, split) = game_to_split(game);
            if split.tracks.len() > 1 {
                self.games
                    .entry(total_size)
                    .or_insert_with(Vec::new)
                    .push(split);
            }
        }
    }

    #[inline]
    pub fn possible_matches(&self, total_size: u64) -> &[SplitGame] {
        self.games
            .get(&total_size)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}
impl Extend<(u64, SplitGame)> for SplitDb {
    #[inline]
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (u64, SplitGame)>,
    {
        for (size, game) in iter {
            self.games.entry(size).or_default().push(game);
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct SplitGame {
    name: String,
    tracks: Vec<SplitPart>,
}

impl SplitGame {
    #[inline]
    pub fn new(name: String) -> Self {
        Self {
            name,
            tracks: Vec::new(),
        }
    }

    #[inline]
    pub fn push_track(&mut self, track: SplitPart) {
        self.tracks.push(track)
    }

    #[inline]
    pub fn matches(&self, data: &[u8]) -> bool {
        use rayon::prelude::*;
        self.tracks.par_iter().all(|t| t.matches(data))
    }

    pub fn extract(&self, root: &Path, data: &[u8]) -> Result<(), io::Error> {
        use rayon::prelude::*;

        let game_root = root.join(&self.name);
        if !game_root.is_dir() {
            use std::fs::create_dir;

            create_dir(&game_root)?;
        }
        self.tracks
            .par_iter()
            .try_for_each(|t| t.extract(&game_root, data))
    }
}

#[derive(Serialize, Deserialize)]
pub struct SplitPart {
    name: String,
    start: usize,
    end: usize,
    sha1: [u8; 20],
}

impl SplitPart {
    pub fn new(name: &str, start: usize, end: usize, sha1: &str) -> Self {
        use crate::game::parse_sha1;

        SplitPart {
            name: name.to_string(),
            start,
            end,
            sha1: parse_sha1(sha1).unwrap(),
        }
    }

    fn matches(&self, data: &[u8]) -> bool {
        use sha1_smol::Sha1;

        Sha1::from(&data[self.start..self.end]).digest().bytes() == self.sha1
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
