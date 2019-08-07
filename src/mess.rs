use super::report::{ReportRow, SortBy, SummaryReportRow};
use super::rom::{
    chd_sha1, disk_to_chd, node_to_disk, parse_int, RomId, SoftwareDisk, SoftwareRom,
};
use super::{Error, VerifyMismatch, VerifyResult};
use roxmltree::{Document, Node};
use rusqlite::{named_params, Transaction};
use serde_derive::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

const CREATE_SOFTWARE_LIST: &str = "CREATE TABLE IF NOT EXISTS SoftwareList (
    softwarelist_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(80) NOT NULL,
    description VARCHAR(80)
)";

const CREATE_SOFTWARE: &str = "CREATE TABLE IF NOT EXISTS Software (
    software_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    softwarelist_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    cloneof VARCHAR(80),
    supported CHAR(3) NOT NULL,
    description VARCHAR(80),
    year CHAR(4),
    publisher VARCHAR(80),
    FOREIGN KEY (softwarelist_id) REFERENCES SoftwareList (softwarelist_id) ON DELETE CASCADE
)";

const CREATE_SOFTWARE_INDEX: &str = "CREATE INDEX IF NOT EXISTS Software_name ON Software (name)";

const CREATE_INFO: &str = "CREATE TABLE IF NOT EXISTS Info (
    software_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value VARCHAR(80),
    FOREIGN KEY (software_id) REFERENCES Software (software_id) ON DELETE CASCADE
)";

const CREATE_SHARED_FEATURE: &str = "CREATE TABLE IF NOT EXISTS SharedFeature (
    software_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value VARCHAR(80),
    FOREIGN KEY (software_id) REFERENCES Software (software_id) ON DELETE CASCADE
)";

const CREATE_PART: &str = "CREATE TABLE IF NOT EXISTS Part (
    part_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    software_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    interface VARCHAR(80) NOT NULL,
    FOREIGN KEY (software_id) REFERENCES Software (software_id) ON DELETE CASCADE
)";

const CREATE_FEATURE: &str = "CREATE TABLE IF NOT EXISTS Feature (
    part_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value VARCHAR(80),
    FOREIGN KEY (part_id) REFERENCES Part (part_id) ON DELETE CASCADE
)";

const CREATE_DATA_AREA: &str = "CREATE TABLE IF NOT EXISTS DataArea (
    data_area_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    part_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    size VARCHAR(10),
    width CHAR(2),
    endianness CHAR(6),
    FOREIGN KEY (part_id) REFERENCES Part (part_id) ON DELETE CASCADE
)";

const CREATE_ROM: &str = "CREATE TABLE IF NOT EXISTS ROM (
    data_area_id INTEGER NOT NULL,
    name VARCHAR(80),
    size INTEGER,
    crc CHAR(8),
    sha1 CHAR(40),
    offset INTEGER,
    value VARCHAR(4),
    status CHAR(7),
    loadflag VARCHAR(20),
    FOREIGN KEY (data_area_id) REFERENCES DataArea (data_area_id) ON DELETE CASCADE
)";

const CREATE_ROM_INDEX: &str = "CREATE INDEX IF NOT EXISTS ROM_sha1 ON ROM (sha1)";

const CREATE_DISK_AREA: &str = "CREATE TABLE IF NOT EXISTS DiskArea (
    disk_area_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    part_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    FOREIGN KEY (part_id) REFERENCES Part (part_id) ON DELETE CASCADE
)";

const CREATE_DISK: &str = "CREATE TABLE IF NOT EXISTS Disk (
    disk_area_id INTEGER NOT NULL,
    name VARCHAR(80),
    sha1 CHAR(40),
    status CHAR(7) NOT NULL,
    writeable CHAR(2) NOT NULL,
    FOREIGN KEY (disk_area_id) REFERENCES DiskArea(disk_area_id) ON DELETE CASCADE
)";

const CREATE_DISK_INDEX: &str = "CREATE INDEX IF NOT EXISTS Disk_sha1 ON Disk (sha1)";

const CREATE_DIPSWITCH: &str = "CREATE TABLE IF NOT EXISTS Dipswitch (
    dipswitch_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    part_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    tag VARCHAR(10) NOT NULL,
    mask INTEGER NOT NULL,
    FOREIGN KEY (part_id) REFERENCES Part (part_id) ON DELETE CASCADE
)";

const CREATE_DIPVALUE: &str = "CREATE TABLE IF NOT EXISTS Dipvalue (
    dipswitch_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value INTEGER NOT NULL,
    switch_default CHAR(3) NOT NULL,
    FOREIGN KEY (dipswitch_id) REFERENCES Dipswitch (dipswitch_id) ON DELETE CASCADE
)";

pub fn create_tables(db: &Transaction) -> Result<(), Error> {
    use rusqlite::params;

    db.execute(CREATE_SOFTWARE_LIST, params![])?;
    db.execute(CREATE_SOFTWARE, params![])?;
    db.execute(CREATE_SOFTWARE_INDEX, params![])?;
    db.execute(CREATE_INFO, params![])?;
    db.execute(CREATE_SHARED_FEATURE, params![])?;
    db.execute(CREATE_PART, params![])?;
    db.execute(CREATE_FEATURE, params![])?;
    db.execute(CREATE_DATA_AREA, params![])?;
    db.execute(CREATE_ROM, params![])?;
    db.execute(CREATE_ROM_INDEX, params![])?;
    db.execute(CREATE_DISK_AREA, params![])?;
    db.execute(CREATE_DISK, params![])?;
    db.execute(CREATE_DISK_INDEX, params![])?;
    db.execute(CREATE_DIPSWITCH, params![])?;
    db.execute(CREATE_DIPVALUE, params![])?;
    Ok(())
}

pub fn clear_tables(db: &Transaction) -> Result<(), Error> {
    use rusqlite::params;

    db.execute("DELETE FROM SoftwareList", params![])
        .map(|_| ())
        .map_err(Error::SQL)
}

pub fn add_file(xml_path: &Path, db: &Transaction) -> Result<(), Error> {
    use std::fs::File;
    use std::io::Read;

    let mut xml_file = String::new();
    File::open(xml_path).and_then(|mut f| f.read_to_string(&mut xml_file))?;

    let tree = Document::parse(&xml_file)?;
    add_software_list(db, &tree.root_element()).map_err(Error::SQL)
}

fn add_software_list(db: &Transaction, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO SoftwareList
        (name, description) VALUES (:name, :description)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":name": node.attribute("name"),
            ":description": node.attribute("description")
        })
    })?;

    let softwarelist_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "software")
        .try_for_each(|c| add_software(db, softwarelist_id, &c))
}

fn add_software(
    db: &Transaction,
    softwarelist_id: i64,
    node: &Node,
) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Software
        (softwarelist_id, name, cloneof, supported) VALUES
        (:softwarelist_id, :name, :cloneof, :supported)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":softwarelist_id": softwarelist_id,
            ":name": node.attribute("name"),
            ":cloneof": node.attribute("cloneof"),
            ":supported": node.attribute("supported").unwrap_or("yes")
        })
    })?;

    let software_id = db.last_insert_rowid();

    for child in node.children() {
        match child.tag_name().name() {
            "description" => add_description(db, software_id, &child)?,
            "year" => add_year(db, software_id, &child)?,
            "publisher" => add_publisher(db, software_id, &child)?,
            "info" => add_info(db, software_id, &child)?,
            "sharedfeat" => add_shared_feature(db, software_id, &child)?,
            "part" => add_part(db, software_id, &child)?,
            _ => { /*ignore unsupported nodes*/ }
        }
    }

    Ok(())
}

fn add_description(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "UPDATE Software SET description = :description WHERE
         (software_id = :software_id)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":software_id": software_id,
            ":description": node.text()
        })
    })
    .map(|_| ())
}

fn add_year(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached("UPDATE Software SET year = :year WHERE (software_id = :software_id)")
        .and_then(|mut stmt| {
            stmt.execute_named(named_params! {
                ":software_id": software_id,
                ":year": node.text()
            })
        })
        .map(|_| ())
}

fn add_publisher(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "UPDATE Software SET publisher = :publisher WHERE
         (software_id = :software_id)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":software_id": software_id,
            ":publisher": node.text()
        })
    })
    .map(|_| ())
}

fn add_info(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Info (software_id, name, value) VALUES
        (:software_id, :name, :value)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":software_id": software_id,
            ":name": node.attribute("name"),
            ":value": node.attribute("value")
        })
    })
    .map(|_| ())
}

fn add_shared_feature(
    db: &Transaction,
    software_id: i64,
    node: &Node,
) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO SharedFeature (software_id, name, value) VALUES
        (:software_id, :name, :value)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":software_id": software_id,
            ":name": node.attribute("name"),
            ":value": node.attribute("value")
        })
    })
    .map(|_| ())
}

fn add_part(db: &Transaction, software_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Part (software_id, name, interface) VALUES
        (:software_id, :name, :interface)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":software_id": software_id,
            ":name": node.attribute("name"),
            ":interface": node.attribute("interface")
        })
    })?;

    let part_id = db.last_insert_rowid();

    for child in node.children() {
        match child.tag_name().name() {
            "feature" => add_feature(db, part_id, &child)?,
            "dataarea" => add_data_area(db, part_id, &child)?,
            "diskarea" => add_disk_area(db, part_id, &child)?,
            "dipswitch" => add_dipswitch(db, part_id, &child)?,
            _ => { /*ignore unsupported nodes*/ }
        }
    }

    Ok(())
}

fn add_feature(db: &Transaction, part_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Feature (part_id, name, value) VALUES
        (:part_id, :name, :value)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":part_id": part_id,
            ":name": node.attribute("name"),
            ":value": node.attribute("value")
        })
    })
    .map(|_| ())
}

fn add_data_area(db: &Transaction, part_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO DataArea (part_id, name, size, width, endianness)
        VALUES (:part_id, :name, :size, :width, :endianness)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":part_id": part_id,
            ":name": node.attribute("name"),
            ":size": node.attribute("size").map(|s| parse_int(s).expect("invalid data area size")),
            ":width": node.attribute("width"),
            ":endianness": node.attribute("endianness")
        })
    })?;

    let data_area_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "rom")
        .try_for_each(|c| add_rom(db, data_area_id, &c))
}

fn add_rom(db: &Transaction, data_area_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO ROM
        (data_area_id, name, size, crc, sha1, offset, value, status, loadflag)
        VALUES
        (:data_area_id, :name, :size, :crc, :sha1,
         :offset, :value, :status, :loadflag)")
    .and_then(|mut stmt| stmt.execute_named(
        named_params! {
            ":data_area_id": data_area_id,
            ":name": node.attribute("name"),
            ":size": node.attribute("size").map(|s| parse_int(s).expect("invalid ROM size integer")),
            ":crc": node.attribute("crc"),
            ":sha1": node.attribute("sha1"),
            ":offset": node.attribute("offset").map(|s| parse_int(s).expect("invalid ROM offset integer")),
            ":value": node.attribute("value"),
            ":status": node.attribute("status"),
            ":loadflag": node.attribute("loadflag")
        }
    ))
    .map(|_| ())
}

fn add_disk_area(db: &Transaction, part_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached("INSERT INTO DiskArea (part_id, name) VALUES (:part_id, :name)")
        .and_then(|mut stmt| {
            stmt.execute_named(named_params! {
                ":part_id": part_id,
                ":name": node.attribute("name")
            })
        })?;

    let disk_area_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "disk")
        .try_for_each(|c| add_disk(db, disk_area_id, &c))
}

fn add_disk(db: &Transaction, disk_area_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Disk (disk_area_id, name, sha1, status, writeable)
        VALUES (:disk_area_id, :name, :sha1, :status, :writeable)",
    )
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":disk_area_id": disk_area_id,
            ":name": node.attribute("name"),
            ":sha1": node.attribute("sha1"),
            ":status": node.attribute("status").unwrap_or("good"),
            ":writeable": node.attribute("writeable").unwrap_or("no")
        })
    })
    .map(|_| ())
}

fn add_dipswitch(db: &Transaction, part_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Dipswitch (part_id, name, tag, mask) VALUES
        (:part_id, :name, :tag, :mask)")
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":part_id": part_id,
            ":name": node.attribute("name"),
            ":tag": node.attribute("tag"),
            ":mask": node.attribute("mask").map(|s| parse_int(s).expect("invalid dipswitch mask integer"))
        })
    })?;

    let dipswitch_id = db.last_insert_rowid();

    node.children()
        .filter(|c| c.tag_name().name() == "dipvalue")
        .try_for_each(|c| add_dipvalue(db, dipswitch_id, &c))
}

fn add_dipvalue(db: &Transaction, dipswitch_id: i64, node: &Node) -> Result<(), rusqlite::Error> {
    db.prepare_cached(
        "INSERT INTO Dipvalue (dipswitch_id, name, value, switch_default) VALUES
        (:dipswitch_id, :name, :value, :default)")
    .and_then(|mut stmt| {
        stmt.execute_named(named_params! {
            ":dipswitch_id": dipswitch_id,
            ":name": node.attribute("name"),
            ":value": node.attribute("value").map(|s| parse_int(s).expect("invalid dipswitch value")),
            ":default": node.attribute("default").unwrap_or("no")
        })
    })
    .map(|_| ())
}

#[derive(Serialize, Deserialize, Default)]
pub struct AddDb(HashMap<String, SoftwareListAddDb>);

impl AddDb {
    pub fn add_xml(&mut self, node: &Node) {
        self.0.insert(
            node.attribute("name").unwrap().to_string(),
            SoftwareListAddDb::from_xml(node),
        );
    }

    #[inline]
    pub fn into_software_list(mut self, software_list: &str) -> Option<SoftwareListAddDb> {
        self.0.remove(software_list)
    }
}

#[derive(Serialize, Deserialize)]
pub struct SoftwareListAddDb {
    roms: HashMap<RomId, Vec<SoftwareRom>>,
    disks: HashMap<String, Vec<SoftwareDisk>>,
}

impl SoftwareListAddDb {
    pub fn from_xml(node: &Node) -> Self {
        let mut roms = HashMap::new();
        let mut disks = HashMap::new();

        for software in node
            .children()
            .filter(|c| c.tag_name().name() == "software")
        {
            let software_name = software.attribute("name").unwrap();

            for part in software
                .children()
                .filter(|c| c.tag_name().name() == "part")
            {
                for child in part.children() {
                    match child.tag_name().name() {
                        "dataarea" => {
                            for c in child.children() {
                                if let Some(romid) = RomId::from_node(&c) {
                                    roms.entry(romid)
                                        .or_insert_with(Vec::new)
                                        .push(SoftwareRom {
                                            game: software_name.to_string(),
                                            rom: c.attribute("name").unwrap().to_string(),
                                        });
                                }
                            }
                        }
                        "diskarea" => {
                            for c in child.children() {
                                if let Some(sha1) = node_to_disk(&c) {
                                    disks
                                        .entry(sha1)
                                        .or_insert_with(Vec::new)
                                        .push(SoftwareDisk {
                                            game: software_name.to_string(),
                                            disk: disk_to_chd(c.attribute("name").unwrap()),
                                        })
                                }
                            }
                        }
                        _ => { /*ignore other entries*/ }
                    }
                }
            }
        }

        SoftwareListAddDb { roms, disks }
    }

    pub fn retain_software(&mut self, software: &HashSet<String>) {
        self.roms
            .values_mut()
            .for_each(|v| v.retain(|r| software.contains(&r.game)));
        self.roms.retain(|_, v| !v.is_empty());
        self.disks
            .values_mut()
            .for_each(|v| v.retain(|r| software.contains(&r.game)));
        self.disks.retain(|_, v| !v.is_empty());
    }
}

pub fn copy(
    db: &SoftwareListAddDb,
    root: &Path,
    rom: &Path,
    dry_run: bool,
) -> Result<(), io::Error> {
    use super::rom::copy;

    if let Ok(Some(disk_sha1)) = chd_sha1(rom) {
        if let Some(matches) = db.disks.get(&disk_sha1) {
            for SoftwareDisk {
                game: software_name,
                disk: disk_name,
            } in matches
            {
                let mut target = root.join(software_name);
                target.push(disk_name);
                copy(rom, &target, dry_run)?;
            }
        }
    } else {
        let rom_id = RomId::from_path(rom)?;
        if let Some(matches) = db.roms.get(&rom_id) {
            for SoftwareRom {
                game: software_name,
                rom: rom_name,
            } in matches
            {
                let mut target = root.join(software_name);
                target.push(rom_name);
                copy(rom, &target, dry_run)?;
            }
        }
    }

    Ok(())
}

fn node_to_reportrow(node: &Node) -> Option<ReportRow> {
    if node.tag_name().name() == "software" {
        Some(ReportRow {
            name: node.attribute("name").unwrap().to_string(),
            description: node
                .children()
                .find(|c| c.tag_name().name() == "description")
                .and_then(|c| c.text())
                .unwrap_or("")
                .to_string(),
            manufacturer: node
                .children()
                .find(|c| c.tag_name().name() == "publisher")
                .and_then(|c| c.text())
                .unwrap_or("")
                .to_string(),
            year: node
                .children()
                .find(|c| c.tag_name().name() == "year")
                .and_then(|c| c.text())
                .unwrap_or("")
                .to_string(),
        })
    } else {
        None
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct ReportDb {
    all: Vec<SummaryReportRow>,
    software_list: HashMap<String, HashMap<String, ReportRow>>,
}

impl ReportDb {
    pub fn add_xml(&mut self, node: &Node) {
        let name = node.attribute("name").unwrap();

        self.all.push(SummaryReportRow {
            name: name.to_string(),
            description: node.attribute("description").unwrap().to_string(),
        });

        self.software_list.insert(
            name.to_string(),
            node.children()
                .filter_map(|c| {
                    node_to_reportrow(&c).map(|r| (c.attribute("name").unwrap().to_string(), r))
                })
                .collect(),
        );
    }

    pub fn sort_software(&mut self) {
        self.all.sort_by(|x, y| x.description.cmp(&y.description));
    }
}

pub fn report_all(db: &ReportDb) {
    use prettytable::{cell, format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for row in db.all.iter() {
        table.add_row(row![row.description, row.name]);
    }

    table.printstd();
}

pub fn report(db: &ReportDb, software_list: &str, software: &HashSet<String>, sort: SortBy) {
    use prettytable::{cell, format, row, Table};

    let mut results: Vec<&ReportRow> = software
        .iter()
        .filter_map(|software| {
            db.software_list
                .get(software_list)
                .and_then(|soft_db| soft_db.get(software))
        })
        .collect();
    results.sort_unstable_by(|x, y| x.sort_by(y, sort));

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for machine in results {
        table.add_row(row![
            machine.description,
            machine.manufacturer,
            machine.year,
            machine.name
        ]);
    }
    table.printstd();
}

#[derive(Serialize, Deserialize, Default)]
pub struct VerifyDb(HashMap<String, SoftwareListDb>);

impl VerifyDb {
    #[inline]
    pub fn add_xml(&mut self, node: &Node) {
        self.0.insert(
            node.attribute("name").unwrap().to_string(),
            SoftwareListDb::from_xml(&node),
        );
    }

    #[inline]
    pub fn into_software_list(mut self, software_list: &str) -> Option<SoftwareListDb> {
        self.0.remove(software_list)
    }

    pub fn into_all_software(mut self, software_list: &str) -> HashSet<String> {
        self.0
            .remove(software_list)
            .map(|l| l.into_all_software())
            .unwrap_or_else(HashSet::new)
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct SoftwareListDb {
    software: HashSet<String>,
    roms: HashMap<String, HashMap<String, RomId>>,
}

impl SoftwareListDb {
    fn from_xml(node: &Node) -> Self {
        let mut db = SoftwareListDb::default();

        node.children()
            .filter(|c| c.tag_name().name() == "software")
            .for_each(|c| db.add_software(&c));

        db
    }

    fn add_software(&mut self, software: &Node) {
        let name = software.attribute("name").unwrap();

        self.software.insert(name.to_string());
        self.roms.insert(
            name.to_string(),
            software
                .children()
                .filter(|part| part.tag_name().name() == "part")
                .flat_map(|parts| {
                    parts
                        .children()
                        .filter(|dataarea| dataarea.tag_name().name() == "dataarea")
                        .flat_map(|dataarea| {
                            dataarea.children().filter_map(|rom| {
                                RomId::from_node(&rom)
                                    .map(|r| (rom.attribute("name").unwrap().to_string(), r))
                            })
                        })
                })
                .collect(),
        );
    }

    #[inline]
    fn into_all_software(self) -> HashSet<String> {
        self.software
    }
}

pub fn verify(db: &SoftwareListDb, root: &Path, software: &str) -> Result<VerifyResult, Error> {
    use super::mame::verify_rom;

    if !db.software.contains(software) {
        return Ok(VerifyResult::NoMachine);
    }

    let software_roms = match db.roms.get(software) {
        Some(m) => m,
        None => return Ok(VerifyResult::Ok), // no ROMs to check
    };

    let software_root = root.join(software);
    let mut mismatches = Vec::new();

    // first check the software's ROMs
    let mut files_on_disk: HashMap<String, PathBuf> = software_root
        .read_dir()
        .map(|entries| {
            entries
                .filter_map(|re| {
                    re.ok()
                        .map(|e| (e.file_name().to_string_lossy().into(), e.path()))
                })
                .collect()
        })
        .unwrap_or_else(|_| HashMap::new());

    for (rom_name, rom_id) in software_roms {
        if let Some(mismatch) = verify_rom(&software_root, rom_name, rom_id, &mut files_on_disk)? {
            mismatches.push(mismatch);
        }
    }

    mismatches.extend(
        files_on_disk
            .into_iter()
            .map(|(_, pb)| VerifyMismatch::Extra(pb)),
    );

    Ok(if mismatches.is_empty() {
        VerifyResult::Ok
    } else {
        VerifyResult::Bad(mismatches)
    })
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct SplitDb(HashMap<String, HashMap<u64, Vec<SplitSoftware>>>);

impl SplitDb {
    #[inline]
    pub fn add_xml(&mut self, node: &Node) {
        self.0.insert(
            node.attribute("name").unwrap().to_string(),
            SplitDb::add_software(node),
        );
    }

    fn add_software(node: &Node) -> HashMap<u64, Vec<SplitSoftware>> {
        let mut software_list = HashMap::default();

        for (total_size, software) in node.children().filter_map(|c| SplitSoftware::from_node(&c)) {
            if software.has_multiple_roms() {
                software_list
                    .entry(total_size)
                    .or_insert_with(Vec::new)
                    .push(software);
            }
        }

        software_list
    }

    #[inline]
    pub fn into_software_list(
        mut self,
        software_list: &str,
    ) -> Option<HashMap<u64, Vec<SplitSoftware>>> {
        self.0.remove(software_list)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SplitSoftware {
    name: String,
    roms: Vec<SplitRom>,
}

impl SplitSoftware {
    #[inline]
    fn has_multiple_roms(&self) -> bool {
        self.roms.len() > 1
    }

    fn from_node(software: &Node) -> Option<(u64, Self)> {
        if software.tag_name().name() == "software" {
            let name = software.attribute("name").unwrap().to_string();
            let mut roms = Vec::new();
            let mut offset = 0;

            for rom in software
                .children()
                .filter(|part| part.tag_name().name() == "part")
                .flat_map(|parts| {
                    parts
                        .children()
                        .filter(|dataarea| dataarea.tag_name().name() == "dataarea")
                        .flat_map(|dataarea| {
                            dataarea
                                .children()
                                .filter(|rom| rom.tag_name().name() == "rom")
                        })
                })
            {
                if let Some(sha1) = rom.attribute("sha1") {
                    let size = parse_int(rom.attribute("size").unwrap()).unwrap() as usize;
                    roms.push(SplitRom {
                        name: rom.attribute("name").unwrap().to_string(),
                        start: offset,
                        end: offset + size as usize,
                        sha1: sha1.to_string(),
                    });
                    offset += size;
                }
            }

            Some((offset as u64, SplitSoftware { name, roms }))
        } else {
            None
        }
    }

    #[inline]
    pub fn matches(&self, data: &[u8]) -> bool {
        self.roms.iter().all(|t| t.matches(data))
    }

    #[inline]
    pub fn extract(&self, root: &Path, data: &[u8]) -> Result<(), io::Error> {
        use std::fs::create_dir_all;

        let game_root = root.join(&self.name);
        create_dir_all(&game_root)?;
        self.roms
            .iter()
            .try_for_each(|t| t.extract(&game_root, data))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SplitRom {
    name: String,
    start: usize,
    end: usize,
    sha1: String,
}

impl SplitRom {
    #[inline]
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

#[inline]
pub fn is_ines_format(data: &[u8]) -> bool {
    (data.len() >= 4) && (&data[0..4] == b"NES\x1a")
}

#[inline]
pub fn remove_ines_header(data: &mut Vec<u8>) {
    let _ = data.drain(0..16);
}
