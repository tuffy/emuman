use crate::dat::DatFile;
use std::collections::BTreeMap;

pub type NointroDb = BTreeMap<String, DatFile>;

pub fn list_all(db: &NointroDb) {
    use prettytable::{cell, format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for (_, datfile) in db.iter() {
        table.add_row(row![datfile.name(), datfile.version()]);
    }

    table.printstd();
}

pub fn list(datfile: &DatFile) {
    let mut games = datfile.games().collect::<Vec<_>>();
    games.sort_unstable();

    use prettytable::{cell, format, row, Table};
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    for game in games {
        table.add_row(row![game]);
    }

    table.printstd();
}
