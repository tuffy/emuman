use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::Path;

#[derive(Default, Serialize, Deserialize)]
pub struct SplitDb {
    pub games: HashMap<u64, Vec<SplitGame>>,
}

impl SplitDb {
    #[inline]
    pub fn possible_matches(&self, total_size: u64) -> &[SplitGame] {
        self.games
            .get(&total_size)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct SplitGame {
    pub name: String,
    pub tracks: Vec<SplitPart>,
}

impl SplitGame {
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
    pub name: String,
    pub start: usize,
    pub end: usize,
    pub sha1: String,
}

impl SplitPart {
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
