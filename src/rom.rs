use core::num::ParseIntError;
use roxmltree::Node;
use serde_derive::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::path::Path;

#[inline]
pub fn parse_int(s: &str) -> Result<u64, ParseIntError> {
    use std::str::FromStr;

    // MAME's use of integer values is a horror show
    let s = s.trim();

    u64::from_str(s)
        .or_else(|_| u64::from_str_radix(s, 16))
        .or_else(|e| {
            if s.starts_with("0x") {
                u64::from_str_radix(&s[2..], 16)
            } else {
                dbg!(s);
                Err(e)
            }
        })
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RomId {
    pub size: u64,
    pub sha1: String,
}

impl RomId {
    pub fn from_path(path: &Path) -> Result<Self, io::Error> {
        let mut f = File::open(path)?;
        let size = f.metadata().map(|m| m.len())?;
        let sha1 = calculate_sha1(&mut f)?;
        Ok(RomId { size, sha1 })
    }

    pub fn from_node(node: &Node) -> Option<Self> {
        if node.tag_name().name() == "rom" {
            Some(RomId {
                sha1: node.attribute("sha1").map(|s| s.to_string())?,
                size: node.attribute("size").map(|s| parse_int(s).unwrap())?,
            })
        } else {
            None
        }
    }
}

pub fn node_to_disk(node: &Node) -> Option<String> {
    if node.tag_name().name() == "disk" {
        node.attribute("sha1").map(|s| s.to_string())
    } else {
        None
    }
}

pub fn disk_to_chd(disk: &str) -> String {
    let mut d = disk.to_string();
    d.push_str(".chd");
    d
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SoftwareRom {
    pub game: String,
    pub rom: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SoftwareDisk {
    pub game: String,
    pub disk: String,
}

fn calculate_sha1(f: &mut io::Read) -> Result<String, io::Error> {
    use sha1::Sha1;

    let mut sha1 = Sha1::new();
    let mut buf = [0; 4096];
    loop {
        match f.read(&mut buf) {
            Ok(0) => return Ok(sha1.hexdigest()),
            Ok(bytes) => sha1.update(&buf[0..bytes]),
            Err(err) => return Err(err),
        }
    }
}

pub fn copy(source: &Path, target: &Path, dry_run: bool) -> Result<(), std::io::Error> {
    if target.exists() {
        Ok(())
    } else {
        let target_dir = target.parent().expect("target has no valid parent");
        if !dry_run {
            use std::fs::{copy, create_dir_all, hard_link};

            if !target_dir.is_dir() {
                create_dir_all(target_dir)?;
            }

            hard_link(source, target).or_else(|_| copy(source, target).map(|_| ()))?;
        }
        println!("{} -> {}", source.display(), target.display());
        Ok(())
    }
}

#[inline]
pub fn is_chd(chd_path: &Path) -> bool {
    match chd_sha1(chd_path) {
        Ok(Some(_)) => true,
        _ => false,
    }
}

pub fn chd_sha1(chd_path: &Path) -> Result<Option<String>, std::io::Error> {
    use bitstream_io::{BigEndian, BitReader};

    let mut r = BitReader::endian(File::open(chd_path)?, BigEndian);
    let mut tag = [0; 8];
    let mut sha1 = [0; 20];

    r.read_bytes(&mut tag)?;
    if &tag != b"MComprHD" {
        return Ok(None);
    }
    r.skip(32)?; // unused length
    let version: u32 = r.read(32)?;

    match version {
        3 => {
            r.skip(32 + 32 + 32 + 64 + 64 + 8 * 16 + 8 * 16 + 32)?;
        }
        4 => {
            r.skip(32 + 32 + 32 + 64 + 64 + 32)?;
        }
        5 => {
            r.skip(32 * 4 + 64 + 64 + 64 + 32 + 32 + 8 * 20)?;
        }
        _ => return Ok(None),
    }
    r.read_bytes(&mut sha1)?;
    Ok(Some(sha1.iter().map(|b| format!("{:02x}", b)).collect()))
}
