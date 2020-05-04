use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::fs::File;
use std::io::BufReader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Durability {
    Low,
    Medium,
    High
}

#[derive(Debug, Clone)]
pub enum LogFormat {
    Binary,
    JSON
}

#[derive(Debug, Clone)]
pub struct Options {
    pub durability: Durability,
    pub segment_size: usize,
    pub log_format: LogFormat
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    pub index: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct Segment {
    pub path: PathBuf,
    pub index: u64
}

#[derive(Debug)]
pub struct Reader {
    pub sindex:  u64,
    pub nindex: u64,
    pub rd: BufReader<File>,
}
