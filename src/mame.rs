use super::game::{Game, GameDb, Part, Status};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Mame {
    #[serde(rename = "@build")]
    build: Option<String>,
    machine: Vec<Machine>,
}

impl Mame {
    #[inline]
    pub fn into_game_db(self) -> GameDb {
        GameDb::new(
            self.build.unwrap_or_default(),
            self.machine
                .into_iter()
                .map(|machine| (machine.name.clone(), machine.into_game()))
                .collect(),
        )
    }
}

#[derive(Debug, Deserialize)]
pub struct Machine {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@isdevice")]
    isdevice: Option<String>,
    description: String,
    year: Option<String>,
    manufacturer: Option<String>,
    rom: Option<Vec<Rom>>,
    disk: Option<Vec<Disk>>,
    device_ref: Option<Vec<DeviceRef>>,
    driver: Option<Driver>,
}

impl Machine {
    #[inline]
    fn into_game(self) -> Game {
        Game {
            name: self.name,
            description: self.description,
            creator: self.manufacturer.unwrap_or_default(),
            year: self.year.unwrap_or_default(),
            status: self.driver.map(|d| d.status()).unwrap_or(Status::Working),
            is_device: matches!(self.isdevice.as_deref(), Some("yes")),
            parts: self
                .rom
                .into_iter()
                .flatten()
                .flat_map(Rom::into_part)
                .chain(self.disk.into_iter().flatten().flat_map(Disk::into_part))
                .collect(),
            devices: self
                .device_ref
                .into_iter()
                .flatten()
                .map(|device_ref| device_ref.name)
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Driver {
    #[serde(rename = "@status")]
    status: String,
}

impl Driver {
    fn status(&self) -> Status {
        match self.status.as_str() {
            "good" => Status::Working,
            "imperfect" => Status::Partial,
            "preliminary" => Status::NotWorking,
            _ => Status::Working,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Rom {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@sha1")]
    sha1: Option<String>,
}

impl Rom {
    #[inline]
    fn into_part(self) -> Option<(String, Part)> {
        Some((self.name, Part::new_rom(self.sha1.as_deref()?).ok()?))
    }
}

#[derive(Debug, Deserialize)]
struct Disk {
    #[serde(rename = "@name")]
    name: String,
    #[serde(rename = "@sha1")]
    sha1: Option<String>,
}

impl Disk {
    #[inline]
    fn into_part(self) -> Option<(String, Part)> {
        Some((
            self.name + ".chd",
            Part::new_disk(self.sha1.as_deref()?).ok()?,
        ))
    }
}

#[derive(Debug, Deserialize)]
struct DeviceRef {
    #[serde(rename = "@name")]
    name: String,
}
