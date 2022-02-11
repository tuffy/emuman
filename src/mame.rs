use super::{
    game::{Game, GameDb, Part, Status},
    Error,
};
use roxmltree::{Document, Node};
use rusqlite::{named_params, params, Transaction};
use std::str::FromStr;

const CREATE_MACHINE: &str = "CREATE TABLE IF NOT EXISTS Machine (
    machine_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(10) NOT NULL UNIQUE,
    source_file VARCHAR(20) NOT NULL,
    is_bios CHAR(3),
    is_device CHAR(3),
    is_mechanical CHAR(3),
    runnable CHAR(3),
    clone_of VARCHAR(10),
    rom_of VARCHAR(10),
    sample_of VARCHAR(10),
    description VARCHAR(80),
    year VARCHAR(4),
    manufacturer VARCHAR(80),
    channels INTEGER,
    service CHAR(3),
    tilt CHAR(3),
    players INTEGER,
    coins INTEGER,
    status VARCHAR(20),
    emulation VARCHAR(20),
    savestate VARCHAR(20)
)";

const CREATE_MACHINE_INDEX: &str = "CREATE INDEX IF NOT EXISTS Machine_name ON Machine (name)";

const ADD_MACHINE: &str = "INSERT INTO Machine
    (name, source_file, is_bios, is_device, is_mechanical,
     runnable, clone_of, rom_of, sample_of) VALUES
    (:name, :source_file, :is_bios, :is_device, :is_mechanical,
     :runnable, :clone_of, :rom_of, :sample_of)";

const ADD_DESCRIPTION: &str = "UPDATE Machine SET description = :description
    WHERE (machine_id = :machine_id)";

const ADD_YEAR: &str = "UPDATE Machine SET year = :year
    WHERE (machine_id = :machine_id)";

const ADD_MANUFACTURER: &str = "UPDATE Machine SET manufacturer = :manufacturer
    WHERE (machine_id = :machine_id)";

const CREATE_BIOS_SET: &str = "CREATE TABLE IF NOT EXISTS BIOS_Set (
    machine_id INTEGER NOT NULL,
    name VARCHAR(20) NOT NULL,
    description VARCHAR(80) NOT NULL,
    set_default CHAR(3) NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_BIOS_SET: &str = "INSERT INTO BIOS_Set
    (machine_id, name, description, set_default) VALUES
    (:machine_id, :name, :description, :default)";

const CREATE_ROM: &str = "CREATE TABLE IF NOT EXISTS ROM (
    machine_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    bios VARCHAR(20),
    size INTEGER,
    crc CHAR(8),
    sha1 CHAR(40),
    merge VARCHAR(80),
    region VARCHAR(80),
    offset VARCHAR(20),
    status VARCHAR(8) NOT NULL,
    optional CHAR(3) NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const CREATE_ROM_INDEX: &str = "CREATE INDEX IF NOT EXISTS ROM_sha1 ON ROM (sha1)";

const ADD_ROM: &str = "INSERT INTO ROM
    (machine_id, name, bios, size, crc, sha1,
     merge, region, offset, status, optional) VALUES
    (:machine_id, :name, :bios, :size, :crc, :sha1,
     :merge, :region, :offset, :status, :optional)";

const CREATE_DISK: &str = "CREATE TABLE IF NOT EXISTS Disk (
    machine_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    sha1 CHAR(40),
    merge VARCHAR(80),
    region VARCHAR(80),
    disk_index INTEGER,
    writable CHAR(3) NOT NULL,
    status CHAR(8) NOT NULL,
    optional CHAR(3) NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const CREATE_DISK_INDEX: &str = "CREATE INDEX IF NOT EXISTS Disk_sha1 ON Disk (sha1)";

const ADD_DISK: &str = "INSERT INTO Disk
    (machine_id, name, sha1, merge, region, disk_index,
     writable, status, optional) VALUES
    (:machine_id, :name, :sha1, :merge, :region, :index,
     :writable, :status, :optional)";

const CREATE_DEVICE_REF: &str = "CREATE TABLE IF NOT EXISTS Device_Ref (
    machine_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_DEVICE_REF: &str = "INSERT INTO Device_Ref
    (machine_id, name) VALUES (:machine_id, :name)";

const CREATE_SAMPLE: &str = "CREATE TABLE IF NOT EXISTS Sample (
    machine_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_SAMPLE: &str = "INSERT INTO Sample
    (machine_id, name) VALUES (:machine_id, :name)";

const CREATE_CHIP: &str = "CREATE TABLE IF NOT EXISTS Chip (
    machine_id INTEGER NOT NULL,
    name VARCHAR(80),
    tag VARCHAR(80),
    clock INTEGER,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_CHIP: &str = "INSERT INTO Chip
    (machine_id, name, tag, clock) VALUES
    (:machine_id, :name, :tag, :clock)";

const CREATE_DISPLAY: &str = "CREATE TABLE IF NOT EXISTS Display (
    machine_id INTEGER NOT NULL,
    tag VARCHAR(80),
    type CHAR(8) NOT NULL,
    rotate INTEGER NOT NULL,
    flipx CHAR(3) NOT NULL,
    width INTEGER,
    height INTEGER,
    refresh REAL,
    pixclock INTEGER,
    htotal INTEGER,
    hbend INTEGER,
    hbstart INTEGER,
    vtotal INTEGER,
    vbend INTEGER,
    vbstart INTEGER,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_DISPLAY: &str = "INSERT INTO Display
    (machine_id, tag, type, rotate, flipx, width, height, refresh,
     pixclock, htotal, hbend, hbstart, vtotal, vbend, vbstart) VALUES
    (:machine_id, :tag, :type, :rotate, :flipx, :width, :height, :refresh,
     :pixclock, :htotal, :hbend, :hbstart, :vtotal, :vbend, :vbstart)";

const ADD_SOUND: &str = "UPDATE Machine SET channels=:channels
    WHERE (machine_id = :machine_id)";

const ADD_INPUT: &str = "UPDATE Machine SET
    service=:service, tilt=:tilt, players=:players, coins=:coins
    WHERE (machine_id = :machine_id)";

const CREATE_DIP_SWITCH: &str = "CREATE TABLE IF NOT EXISTS DipSwitch (
    dip_switch_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    machine_id INTEGER NOT NULL,
    name VARCHAR(30) NOT NULL,
    tag VARCHAR(30) NOT NULL,
    mask INTEGER NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_DIP_SWITCH: &str = "INSERT INTO DipSwitch
    (machine_id, name, tag, mask) VALUES
    (:machine_id, :name, :tag, :mask)";

const CREATE_DIP_LOCATION: &str = "CREATE TABLE IF NOT EXISTS DipLocation (
    dip_switch_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    number INTEGER NOT NULL,
    inverted CHAR(3) NOT NULL,
    FOREIGN KEY (dip_switch_id) REFERENCES DipSwitch (dip_switch_id) ON DELETE CASCADE
)";

const ADD_DIP_LOCATION: &str = "INSERT INTO DipLocation
    (dip_switch_id, name, number, inverted) VALUES
    (:dip_switch_id, :name, :number, :inverted)";

const CREATE_DIP_VALUE: &str = "CREATE TABLE IF NOT EXISTS DipValue (
    dip_switch_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value INTEGER NOT NULL,
    value_default CHAR(3) NOT NULL,
    FOREIGN KEY (dip_switch_id) REFERENCES DipSwitch (dip_switch_id) ON DELETE CASCADE
)";

const ADD_DIP_VALUE: &str = "INSERT INTO DipValue
    (dip_switch_id, name, value, value_default) VALUES
    (:dip_switch_id, :name, :value, :default)";

const CREATE_CONFIGURATION: &str = "CREATE TABLE IF NOT EXISTS Configuration (
    configuration_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    machine_id INTEGER NOT NULL,
    name VARCHAR(30) NOT NULL,
    tag VARCHAR(30) NOT NULL,
    mask INTEGER NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_CONFIGURATION: &str = "INSERT INTO Configuration
    (machine_id, name, tag, mask) VALUES
    (:machine_id, :name, :tag, :mask)";

const CREATE_CONF_LOCATION: &str = "CREATE TABLE IF NOT EXISTS ConfLocation (
    configuration_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    number INTEGER NOT NULL,
    inverted CHAR(3) NOT NULL,
    FOREIGN KEY (configuration_id) REFERENCES Configuration (configuration_id) ON DELETE CASCADE
)";

const ADD_CONF_LOCATION: &str = "INSERT INTO ConfLocation
    (configuration_id, name, number, inverted) VALUES
    (:configuration_id, :name, :number, :inverted)";

const CREATE_CONF_SETTING: &str = "CREATE TABLE IF NOT EXISTS ConfSetting (
    configuration_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    value INTEGER NOT NULL,
    value_default CHAR(3) NOT NULL,
    FOREIGN KEY (configuration_id) REFERENCES Configuration (configuration_id) ON DELETE CASCADE
)";

const ADD_CONF_SETTING: &str = "INSERT INTO ConfSetting
    (configuration_id, name, value, value_default) VALUES
    (:configuration_id, :name, :value, :default)";

const CREATE_CONTROL: &str = "CREATE TABLE IF NOT EXISTS Control (
    machine_id INTEGER NOT NULL,
    type VARCHAR(8) NOT NULL,
    player INTEGER,
    buttons INTEGER,
    reqbuttons INTEGER,
    minimum INTEGER,
    maximum INTEGER,
    sensitivity INTEGER,
    keydelta INTEGER,
    reverse CHAR(3) NOT NULL,
    ways VARCHAR(20),
    ways2 VARCHAR(20),
    ways3 VARCHAR(20),
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_CONTROL: &str = "INSERT INTO Control
    (machine_id, type, player, buttons, reqbuttons, minimum, maximum,
     sensitivity, keydelta, reverse, ways, ways2, ways3) VALUES
    (:machine_id, :type, :player, :buttons, :reqbuttons, :minimum, :maximum,
     :sensitivity, :keydelta, :reverse, :ways, :ways2, :ways3)";

const CREATE_PORT: &str = "CREATE TABLE IF NOT EXISTS Port (
    port_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    machine_id INTEGER NOT NULL,
    tag VARCHAR(80) NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_PORT: &str = "INSERT INTO Port
    (machine_id, tag) VALUES (:machine_id, :tag)";

const CREATE_ANALOG: &str = "CREATE TABLE IF NOT EXISTS Analog (
    port_id INTEGER NOT NULL,
    mask INTEGER NOT NULL,
    FOREIGN KEY (port_id) REFERENCES Port (port_id) ON DELETE CASCADE
)";

const ADD_ANALOG: &str = "INSERT INTO Analog
    (port_id, mask) VALUES (:port_id, :mask)";

const CREATE_ADJUSTER: &str = "CREATE TABLE IF NOT EXISTS Adjuster (
    machine_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    adjuster_default INTEGER NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_ADJUSTER: &str = "INSERT INTO Adjuster
    (machine_id, name, adjuster_default) VALUES (:machine_id, :name, :default)";

const ADD_DRIVER: &str = "UPDATE Machine SET
    status=:status, emulation=:emulation, savestate=:savestate
    WHERE (machine_id = :machine_id)";

const CREATE_FEATURE: &str = "CREATE TABLE IF NOT EXISTS Feature (
    machine_id INTEGER NOT NULL,
    type VARCHAR(20) NOT NULL,
    status VARCHAR(20),
    overall VARCHAR(20),
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_FEATURE: &str = "INSERT INTO Feature
    (machine_id, type, status, overall) VALUES
    (:machine_id, :type, :status, :overall)";

const CREATE_DEVICE: &str = "CREATE TABLE IF NOT EXISTS Device (
    device_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    machine_id INTEGER,
    type VARCHAR(80) NOT NULL,
    tag VARCHAR(80),
    fixed_image VARCHAR(80),
    mandatory VARCHAR(10),
    interface VARCHAR(80),
    name VARCHAR(80),
    briefname VARCHAR(80),
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_DEVICE: &str = "INSERT INTO Device
    (machine_id, type, tag, fixed_image, mandatory, interface) VALUES
    (:machine_id, :type, :tag, :fixed_image, :mandatory, :interface)";

const ADD_INSTANCE: &str = "UPDATE Device SET
    name=:name,briefname=:briefname WHERE (device_id=:device_id)";

const CREATE_EXTENSION: &str = "CREATE TABLE IF NOT EXISTS Extension (
    device_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    FOREIGN KEY (device_id) REFERENCES Device (device_id) ON DELETE CASCADE
)";

const ADD_EXTENSION: &str = "INSERT INTO Extension
    (device_id, name) VALUES (:device_id, :name)";

const CREATE_SLOT: &str = "CREATE TABLE IF NOT EXISTS Slot (
    slot_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    machine_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_SLOT: &str = "INSERT INTO Slot
    (machine_id, name) VALUES (:machine_id, :name)";

const CREATE_SLOT_OPTION: &str = "CREATE TABLE IF NOT EXISTS SlotOption (
    slot_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    devname VARCHAR(80) NOT NULL,
    option_default CHAR(3) NOT NULL,
    FOREIGN KEY (slot_id) REFERENCES Slot (slot_id) ON DELETE CASCADE
)";

const ADD_SLOT_OPTION: &str = "INSERT INTO SlotOption
    (slot_id, name, devname, option_default) VALUES
    (:slot_id, :name, :devname, :default)";

const CREATE_SOFTWARE_LIST: &str = "CREATE TABLE IF NOT EXISTS SoftwareList (
    machine_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    tag VARCHAR(80) NOT NULL,
    status VARCHAR(80) NOT NULL,
    filter VARCHAR(80),
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_SOFTWARE_LIST: &str = "INSERT INTO SoftwareList
    (machine_id, name, tag, status, filter) VALUES
    (:machine_id, :name, :tag, :status, :filter)";

const CREATE_RAM_OPTION: &str = "CREATE TABLE IF NOT EXISTS RAMOption (
    machine_id INTEGER NOT NULL,
    name VARCHAR(80) NOT NULL,
    option_default CHAR(3),
    value INTEGER NOT NULL,
    FOREIGN KEY (machine_id) REFERENCES Machine (machine_id) ON DELETE CASCADE
)";

const ADD_RAM_OPTION: &str = "INSERT INTO RAMOption
    (machine_id, name, option_default, value) VALUES
    (:machine_id, :name, :default, :value)";

pub fn create_tables(db: &Transaction) -> Result<(), Error> {
    db.execute(CREATE_MACHINE, params![])?;
    db.execute(CREATE_MACHINE_INDEX, params![])?;
    db.execute(CREATE_BIOS_SET, params![])?;
    db.execute(CREATE_ROM, params![])?;
    db.execute(CREATE_ROM_INDEX, params![])?;
    db.execute(CREATE_DISK, params![])?;
    db.execute(CREATE_DISK_INDEX, params![])?;
    db.execute(CREATE_DEVICE_REF, params![])?;
    db.execute(CREATE_SAMPLE, params![])?;
    db.execute(CREATE_CHIP, params![])?;
    db.execute(CREATE_DISPLAY, params![])?;
    db.execute(CREATE_DIP_SWITCH, params![])?;
    db.execute(CREATE_DIP_LOCATION, params![])?;
    db.execute(CREATE_DIP_VALUE, params![])?;
    db.execute(CREATE_CONFIGURATION, params![])?;
    db.execute(CREATE_CONF_LOCATION, params![])?;
    db.execute(CREATE_CONF_SETTING, params![])?;
    db.execute(CREATE_CONTROL, params![])?;
    db.execute(CREATE_PORT, params![])?;
    db.execute(CREATE_ADJUSTER, params![])?;
    db.execute(CREATE_ANALOG, params![])?;
    db.execute(CREATE_FEATURE, params![])?;
    db.execute(CREATE_DEVICE, params![])?;
    db.execute(CREATE_EXTENSION, params![])?;
    db.execute(CREATE_SLOT, params![])?;
    db.execute(CREATE_SLOT_OPTION, params![])?;
    db.execute(CREATE_SOFTWARE_LIST, params![])?;
    db.execute(CREATE_RAM_OPTION, params![])?;
    Ok(())
}

pub fn clear_tables(db: &Transaction) -> Result<(), Error> {
    db.execute("DELETE FROM Machine", params![])
        .map(|_| ())
        .map_err(Error::Sql)
}

pub fn xml_to_db(tree: &Document, db: &Transaction) -> Result<(), Error> {
    let root = tree.root_element();

    for machine in root.children().filter(|c| c.tag_name().name() == "machine") {
        add_machine(db, &machine)?;

        let machine_id = db.last_insert_rowid();

        for child in machine.children() {
            match child.tag_name().name() {
                "description" => add_description(db, machine_id, &child)?,
                "year" => add_year(db, machine_id, &child)?,
                "manufacturer" => add_manufacturer(db, machine_id, &child)?,
                "biosset" => add_bios_set(db, machine_id, &child)?,
                "rom" => add_rom(db, machine_id, &child)?,
                "disk" => add_disk(db, machine_id, &child)?,
                "device_ref" => add_device_ref(db, machine_id, &child)?,
                "sample" => add_sample(db, machine_id, &child)?,
                "chip" => add_chip(db, machine_id, &child)?,
                "display" => add_display(db, machine_id, &child)?,
                "sound" => add_sound(db, machine_id, &child)?,
                "input" => add_input(db, machine_id, &child)?,
                "dipswitch" => add_dip_switch(db, machine_id, &child)?,
                "configuration" => add_configuration(db, machine_id, &child)?,
                "port" => add_port(db, machine_id, &child)?,
                "adjuster" => add_adjuster(db, machine_id, &child)?,
                "driver" => add_driver(db, machine_id, &child)?,
                "feature" => add_feature(db, machine_id, &child)?,
                "device" => add_device(db, machine_id, &child)?,
                "slot" => add_slot(db, machine_id, &child)?,
                "softwarelist" => add_software_list(db, machine_id, &child)?,
                "ramoption" => add_ram_option(db, machine_id, &child)?,
                _ => { /*ignore other child types*/ }
            }
        }
    }

    Ok(())
}

fn add_machine(db: &Transaction, machine: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_MACHINE)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
            ":name": machine.attribute("name"),
            ":source_file": machine.attribute("sourcefile"),
            ":is_bios": machine.attribute("isbios").unwrap_or("no"),
            ":is_device": machine.attribute("isdevice").unwrap_or("no"),
            ":is_mechanical": machine.attribute("ismechanical").unwrap_or("no"),
            ":runnable": machine.attribute("runnable").unwrap_or("yes"),
            ":clone_of": machine.attribute("cloneof"),
            ":rom_of": machine.attribute("romof"),
            ":sample_of": machine.attribute("sampleof")
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_description(db: &Transaction, machine_id: i64, description: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DESCRIPTION)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":description": description.text(),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_year(db: &Transaction, machine_id: i64, year: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_YEAR)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":year": year.text(),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_manufacturer(db: &Transaction, machine_id: i64, manufacturer: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_MANUFACTURER)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":manufacturer": manufacturer.text(),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_bios_set(db: &Transaction, machine_id: i64, bios_set: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_BIOS_SET)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":name": bios_set.attribute("name"),
                ":description": bios_set.attribute("description"),
                ":default": bios_set.attribute("default").unwrap_or("no"),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_rom(db: &Transaction, machine_id: i64, rom: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_ROM)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
            ":machine_id": machine_id,
            ":name": rom.attribute("name"),
            ":bios": rom.attribute("bios"),
            ":size": rom.attribute("size").map(|s| i64::from_str(s).expect("invalid size integer")),
            ":crc": rom.attribute("crc"),
            ":sha1": rom.attribute("sha1"),
            ":merge": rom.attribute("merge"),
            ":region": rom.attribute("region"),
            ":offset": rom.attribute("offset"),
            ":status": rom.attribute("status").unwrap_or("good"),
            ":optional": rom.attribute("optional").unwrap_or("no")
        })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_disk(db: &Transaction, machine_id: i64, disk: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DISK)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":machine_id": machine_id,
            ":name": disk.attribute("name"),
            ":sha1": disk.attribute("sha1"),
            ":merge": disk.attribute("merge"),
            ":region": disk.attribute("region"),
            ":index": disk.attribute("index").map(|s| i64::from_str(s).expect("invalid index integer")),
            ":writable": disk.attribute("writable").unwrap_or("no"),
            ":status": disk.attribute("status").unwrap_or("good"),
            ":optional": disk.attribute("optional").unwrap_or("no")
        }
    ))
    .map_err(Error::Sql)
    .map(|_| ())
}

fn add_device_ref(db: &Transaction, machine_id: i64, device_ref: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DEVICE_REF)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":name": device_ref.attribute("name")
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_sample(db: &Transaction, machine_id: i64, sample: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_SAMPLE)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":name": sample.attribute("name")
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_chip(db: &Transaction, machine_id: i64, chip: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_CHIP)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
            ":machine_id": machine_id,
            ":name": chip.attribute("name"),
            ":tag": chip.attribute("tag"),
            ":clock": chip.attribute("clock").map(|s| i64::from_str(s).expect("invalid chip clock"))
        })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_display(db: &Transaction, machine_id: i64, display: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DISPLAY)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":machine_id": machine_id,
            ":tag": display.attribute("tag"),
            ":type": display.attribute("type"),
            ":rotate": display.attribute("rotate").map(|s| i64::from_str(s).expect("invalid rotate integer")).unwrap_or(0),
            ":flipx": display.attribute("flipx").unwrap_or("no"),
            ":width": display.attribute("width").map(|s| i64::from_str(s).expect("invalid width integer")),
            ":height": display.attribute("height").map(|s| i64::from_str(s).expect("invalid height integer")),
            ":refresh": display.attribute("refresh").map(|s| f64::from_str(s).expect("invalid refresh float")),
            ":pixclock": display.attribute("pixclock").map(|s| i64::from_str(s).expect("invalid pixclock integer")),
            ":htotal": display.attribute("htotal").map(|s| i64::from_str(s).expect("invalid htotal integer")),
            ":hbend": display.attribute("hbend").map(|s| i64::from_str(s).expect("invalid hbend integer")),
            ":hbstart": display.attribute("hbstart").map(|s| i64::from_str(s).expect("invalid hbstart integer")),
            ":vtotal": display.attribute("vtotal").map(|s| i64::from_str(s).expect("invalid vtotal integer")),
            ":vbend": display.attribute("vbend").map(|s| i64::from_str(s).expect("invalid vbend integer")),
            ":vbstart": display.attribute("vbstart").map(|s| i64::from_str(s).expect("invalid vbstart integer"))
        }
    ))
    .map_err(Error::Sql)
    .map(|_| ())
}

fn add_sound(db: &Transaction, machine_id: i64, sound: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_SOUND)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":machine_id": machine_id,
            ":channels": sound.attribute("channels").map(|s| i64::from_str(s).expect("invalid channels integer"))
        }
    ))
    .map_err(Error::Sql)
    .map(|_| ())
}

fn add_input(db: &Transaction, machine_id: i64, input: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_INPUT)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":machine_id": machine_id,
            ":service": input.attribute("service").unwrap_or("no"),
            ":tilt": input.attribute("tilt").unwrap_or("no"),
            ":players": input.attribute("players").map(|s| i64::from_str(s).expect("invalid players integer")),
            ":coins": input.attribute("coins").map(|s| i64::from_str(s).expect("invalid coins integer"))
                                                                            }
    ))?;

    input
        .children()
        .filter(|c| c.tag_name().name() == "control")
        .try_for_each(|control| add_control(db, machine_id, &control))
}

fn add_dip_switch(db: &Transaction, machine_id: i64, switch: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DIP_SWITCH)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":machine_id": machine_id,
            ":name": switch.attribute("name"),
            ":tag": switch.attribute("tag"),
            ":mask": switch.attribute("mask").map(|s| i64::from_str(s).expect("invalid switch mask")),
        }))?;

    let switch_id = db.last_insert_rowid();

    for child in switch.children() {
        match child.tag_name().name() {
            "diplocation" => add_dip_location(db, switch_id, &child)?,
            "dipvalue" => add_dip_value(db, switch_id, &child)?,
            _ => { /*ignore other child types*/ }
        }
    }

    Ok(())
}

fn add_dip_location(db: &Transaction, switch_id: i64, location: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DIP_LOCATION)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":dip_switch_id": switch_id,
            ":name": location.attribute("name"),
            ":number": location.attribute("number").map(|s| i64::from_str(s).expect("invalid switch mask")),
            ":inverted": location.attribute("inverted").unwrap_or("no"),
        }))
    .map(|_| ())
    .map_err(Error::Sql)
}

fn add_dip_value(db: &Transaction, switch_id: i64, value: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DIP_VALUE)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":dip_switch_id": switch_id,
            ":name": value.attribute("name"),
            ":value": value.attribute("value").map(|s| i64::from_str(s).expect("invalid switch mask")),
            ":default": value.attribute("default").unwrap_or("no"),
        }))
    .map(|_| ())
    .map_err(Error::Sql)
}

fn add_configuration(db: &Transaction, machine_id: i64, conf: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_CONFIGURATION).and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":machine_id": machine_id,
            ":name": conf.attribute("name"),
            ":tag": conf.attribute("tag"),
            ":mask": conf.attribute("mask").map(|s| i64::from_str(s).expect("invalid switch mask")),
        })
    })?;

    let conf_id = db.last_insert_rowid();

    for child in conf.children() {
        match child.tag_name().name() {
            "conflocation" => add_conf_location(db, conf_id, &child)?,
            "confsetting" => add_conf_setting(db, conf_id, &child)?,
            _ => { /*ignore other child types*/ }
        }
    }

    Ok(())
}

fn add_conf_location(db: &Transaction, conf_id: i64, location: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_CONF_LOCATION)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":configuration_id": conf_id,
            ":name": location.attribute("name"),
            ":number": location.attribute("number").map(|s| i64::from_str(s).expect("invalid switch mask")),
            ":inverted": location.attribute("inverted").unwrap_or("no"),
        }))
    .map(|_| ())
    .map_err(Error::Sql)
}

fn add_conf_setting(db: &Transaction, conf_id: i64, setting: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_CONF_SETTING)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":configuration_id": conf_id,
            ":name": setting.attribute("name"),
            ":value": setting.attribute("value").map(|s| i64::from_str(s).expect("invalid configuration value")),
            ":default": setting.attribute("default").unwrap_or("no"),
        }))
    .map(|_| ())
    .map_err(Error::Sql)
}

fn add_control(db: &Transaction, machine_id: i64, control: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_CONTROL)
    .and_then(|mut stmt| stmt.execute(
        named_params! {
            ":machine_id": machine_id,
            ":type": control.attribute("type"),
            ":player": control.attribute("player").map(|s| i64::from_str(s).expect("invalid player integer")),
            ":buttons": control.attribute("buttons").map(|s| i64::from_str(s).expect("invalid buttons integer")),
            ":reqbuttons": control.attribute("reqbuttons").map(|s| i64::from_str(s).expect("invalid reqbuttons integer")),
            ":minimum": control.attribute("minimum").map(|s| i64::from_str(s).expect("invalid minimum integer")),
            ":maximum": control.attribute("maximum").map(|s| i64::from_str(s).expect("invalid maximum integer")),
            ":sensitivity": control.attribute("sensitivity").map(|s| i64::from_str(s).expect("invalid sensitivity integer")),
            ":keydelta": control.attribute("keydelta").map(|s| i64::from_str(s).expect("invalid keydelta integer")),
            ":reverse": control.attribute("reverse").unwrap_or("no"),
            ":ways": control.attribute("ways"),
            ":ways2": control.attribute("ways2"),
            ":ways3": control.attribute("ways3")
        }
    ))
    .map_err(Error::Sql)
    .map(|_| ())
}

fn add_port(db: &Transaction, machine_id: i64, port: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_PORT).and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":machine_id": machine_id,
            ":tag": port.attribute("tag"),
        })
    })?;

    let port_id = db.last_insert_rowid();

    port.children()
        .filter(|c| c.tag_name().name() == "analog")
        .try_for_each(|analog| add_analog(db, port_id, &analog))
}

fn add_adjuster(db: &Transaction, machine_id: i64, adjuster: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_ADJUSTER)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":name": adjuster.attribute("name"),
                ":default": adjuster.attribute("default").map(|s| i64::from_str(s).expect("invalid analog mask")),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_analog(db: &Transaction, port_id: i64, analog: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_ANALOG)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":port_id": port_id,
                ":mask": analog.attribute("mask").map(|s| i64::from_str(s).expect("invalid analog mask"))
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_driver(db: &Transaction, machine_id: i64, driver: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DRIVER)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":status": driver.attribute("status"),
                ":emulation": driver.attribute("emulation"),
                ":savestate": driver.attribute("savestate")
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_feature(db: &Transaction, machine_id: i64, feature: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_FEATURE)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":type": feature.attribute("type"),
                ":status": feature.attribute("status"),
                ":overall": feature.attribute("overall"),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_device(db: &Transaction, machine_id: i64, device: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_DEVICE).and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":machine_id": machine_id,
            ":type": device.attribute("type"),
            ":tag": device.attribute("tag"),
            ":fixed_image": device.attribute("fixed_image"),
            ":mandatory": device.attribute("mandatory"),
            ":interface": device.attribute("mandatory"),
        })
    })?;

    let device_id = db.last_insert_rowid();

    for child in device.children() {
        match child.tag_name().name() {
            "instance" => add_instance(db, device_id, &child)?,
            "extension" => add_extension(db, device_id, &child)?,
            _ => { /*ignore other child types*/ }
        }
    }

    Ok(())
}

fn add_instance(db: &Transaction, device_id: i64, instance: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_INSTANCE)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":device_id": device_id,
                ":name": instance.attribute("name"),
                ":briefname": instance.attribute("briefname"),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_extension(db: &Transaction, device_id: i64, extension: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_EXTENSION)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":device_id": device_id,
                ":name": extension.attribute("name"),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_slot(db: &Transaction, machine_id: i64, slot: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_SLOT).and_then(|mut stmt| {
        stmt.execute(named_params! {
            ":machine_id": machine_id,
            ":name": slot.attribute("name"),
        })
    })?;

    let slot_id = db.last_insert_rowid();

    slot.children()
        .filter(|c| c.tag_name().name() == "slotoption")
        .try_for_each(|option| add_slot_option(db, slot_id, &option))
}

fn add_slot_option(db: &Transaction, slot_id: i64, option: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_SLOT_OPTION)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":slot_id": slot_id,
                ":name": option.attribute("name"),
                ":devname": option.attribute("devname"),
                ":default": option.attribute("default").unwrap_or("no")
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_software_list(db: &Transaction, machine_id: i64, list: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_SOFTWARE_LIST)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                    ":machine_id": machine_id,
                    ":name": list.attribute("name"),
                    ":tag": list.attribute("tag"),
                    ":status": list.attribute("status"),
                    ":filter": list.attribute("filter"),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

fn add_ram_option(db: &Transaction, machine_id: i64, ram: &Node) -> Result<(), Error> {
    db.prepare_cached(ADD_RAM_OPTION)
        .and_then(|mut stmt| {
            stmt.execute(named_params! {
                ":machine_id": machine_id,
                ":name": ram.attribute("name"),
                ":default": ram.attribute("default"),
                ":value": ram.text().map(|s| i64::from_str(s).expect("invalid ramoption value")),
            })
        })
        .map_err(Error::Sql)
        .map(|_| ())
}

pub fn xml_to_game_db(tree: &Document) -> GameDb {
    let root = tree.root_element();
    let mut db = GameDb::default();

    for machine in root.children().filter(|c| c.tag_name().name() == "machine") {
        db.games.insert(
            machine.attribute("name").unwrap().to_string(),
            xml_to_game(&machine),
        );
    }

    db
}

fn xml_to_game(node: &Node) -> Game {
    let mut game = Game {
        name: node.attribute("name").unwrap().to_string(),
        is_device: node.attribute("isbios") == Some("yes")
            || node.attribute("isdevice") == Some("yes"),
        ..Game::default()
    };

    for child in node.children() {
        match child.tag_name().name() {
            "description" => {
                game.description = child
                    .text()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::default)
            }
            "manufacturer" => {
                game.creator = child
                    .text()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::default)
            }
            "year" => {
                game.year = child
                    .text()
                    .map(|s| s.to_string())
                    .unwrap_or_else(String::default)
            }
            "rom" => {
                if let Some(sha1) = child.attribute("sha1") {
                    game.parts.insert(
                        child.attribute("name").unwrap().to_string(),
                        Part::new_rom(sha1).unwrap(),
                    );
                }
            }
            "disk" => {
                if let Some(sha1) = child.attribute("sha1") {
                    game.parts.insert(
                        Part::name_to_chd(child.attribute("name").unwrap()),
                        Part::new_disk(sha1).unwrap(),
                    );
                }
            }
            "driver" => {
                match child.attribute("status").unwrap() {
                    "good" => game.status = Status::Working,
                    "imperfect" => game.status = Status::Partial,
                    "preliminary" => game.status = Status::NotWorking,
                    _ => { /* do nothing*/ }
                }
            }
            "device_ref" => game
                .devices
                .push(child.attribute("name").unwrap().to_string()),
            _ => { /* ignore other elements*/ }
        }
    }

    game
}
