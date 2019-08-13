use serde_derive::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Serialize, Deserialize)]
pub struct SummaryReportRow {
    pub name: String,
    pub description: String,
}

#[derive(Copy, Clone)]
pub enum SortBy {
    Description,
    Manufacturer,
    Year,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Status {
    Working,
    Partial,
    NotWorking,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReportRow {
    pub name: String,
    pub description: String,
    pub manufacturer: String,
    pub year: String,
    pub status: Status,
}

impl ReportRow {
    pub fn sort_by(&self, other: &ReportRow, sort: SortBy) -> Ordering {
        match sort {
            SortBy::Description => match self.sort_by_description(other) {
                Ordering::Equal => match self.sort_by_manufacturer(other) {
                    Ordering::Equal => self.sort_by_year(other),
                    o => o,
                },
                o => o,
            },
            SortBy::Manufacturer => match self.sort_by_manufacturer(other) {
                Ordering::Equal => match self.sort_by_description(other) {
                    Ordering::Equal => self.sort_by_year(other),
                    o => o,
                },
                o => o,
            },
            SortBy::Year => match self.sort_by_year(other) {
                Ordering::Equal => match self.sort_by_description(other) {
                    Ordering::Equal => self.sort_by_manufacturer(other),
                    o => o,
                },
                o => o,
            },
        }
    }

    #[inline]
    fn sort_by_description(&self, other: &ReportRow) -> Ordering {
        self.description.cmp(&other.description)
    }

    #[inline]
    fn sort_by_manufacturer(&self, other: &ReportRow) -> Ordering {
        self.manufacturer.cmp(&other.manufacturer)
    }

    #[inline]
    fn sort_by_year(&self, other: &ReportRow) -> Ordering {
        self.year.cmp(&other.year)
    }
}
