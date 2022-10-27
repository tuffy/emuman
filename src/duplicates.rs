use crate::game::{FileId, Part};
use nohash::IntMap;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

pub trait Duplicates {
    fn get_or_add(
        &mut self,
        source: PathBuf,
    ) -> Result<Option<(PathBuf, &Path)>, (PathBuf, std::io::Error)>;
}

#[derive(Default)]
pub struct DuplicateFiles {
    seen: HashSet<FileId>,
    parts: IntMap<u64, DuplicateParts>,
}

impl Duplicates for DuplicateFiles {
    fn get_or_add(
        &mut self,
        source: PathBuf,
    ) -> Result<Option<(PathBuf, &Path)>, (PathBuf, std::io::Error)> {
        let file_id = match FileId::new(&source) {
            Ok(id) => id,
            Err(err) => return Err((source, err)),
        };

        if self.seen.contains(&file_id) {
            Ok(None)
        } else {
            // don't attempt to link files together unless they're
            // on the same device
            match self
                .parts
                .entry(file_id.dev)
                .or_default()
                .get_or_add(source)
            {
                ok @ Ok(Some(_)) => ok,
                not_found @ Ok(None) => {
                    self.seen.insert(file_id);
                    not_found
                }
                err @ Err(_) => err,
            }
        }
    }
}

#[derive(Default)]
pub struct DuplicateParts(HashMap<Part, PathBuf>);

impl Duplicates for DuplicateParts {
    fn get_or_add(
        &mut self,
        source: PathBuf,
    ) -> Result<Option<(PathBuf, &Path)>, (PathBuf, std::io::Error)> {
        use std::collections::hash_map::Entry;

        let part = match Part::get_xattr(&source) {
            Some(part) => part,
            None => match Part::from_path(&source) {
                Ok(part) => part,
                Err(err) => return Err((source, err)),
            },
        };

        match self.0.entry(part) {
            Entry::Vacant(v) => {
                v.insert(source);
                Ok(None)
            }
            Entry::Occupied(o) => Ok(Some((source, o.into_mut().as_path()))),
        }
    }
}
