pub mod error;

use lazy_static::lazy_static;
use std::fs::{self, File, create_dir_all};
use std::io::BufReader;
use crate::error::Error;
use std::path::Path;

#[derive(Debug)]
pub enum Durability {
    Low,
    Medium,
    High
}

#[derive(Debug)]
pub enum LogFormat {
    Binary,
    JSON
}

#[derive(Debug)]
pub struct Options {
    pub durability: Durability,
    pub segment_size: i64,
    pub log_format: LogFormat
}

lazy_static! {
    pub static ref DefaultOptions: Options = Options {
        durability: Durability::High,
        segment_size: 52428800, // 50 Mb log segment files
        log_format: LogFormat::Binary
    };

    pub static ref MAX_READERS: i8 = 8; 
}

#[derive(Debug, Default)]
pub struct Segment {
    pub path: String,
    pub index: u64
}

#[derive(Debug)]
pub struct Reader {
    pub sindex:  i64,
    pub nindex: u64,
    pub file: File,
    pub rd: BufReader<File>,
}

#[derive(Debug)]
pub struct Log {
    pub path: String,
    pub opts: Options,
    pub closed: bool,
    pub segments: Vec<Segment>,
    pub first_index: u64,
    pub last_index: u64,
    pub file: File,
    pub buffer: Vec<u8>,
    pub file_size: i64,
    pub readers: Vec<Reader>
}

fn abs(path: &str) -> Result<String, Error> {
    if path == ":memory" {
        Err(Error::InMemoryLog)
    } else {
        Ok(String::new())
    }
}

fn load_segments(dir: &str) -> Result<i64, Error>{
    let path = Path::new(dir);
    if !path.is_dir() {
        // return error should be a directory
    }
    let mut start_index = -1;
    let mut end_index = -1;

    let files = fs::read_dir(dir)?;
    for file in files {
        let file = file?;
        let name = file.file_name();
        let file_type = file.file_type()?;
        if file_type.is_dir() || name.len() < 20 {

        }
    }

    Ok(1)
}

impl Log {
    pub fn open(path: &str, opts: Option<&Options>) -> Result<Log, Error>{
        if path == ":memory" {
            return Err(Error::InMemoryLog);
        }

        let options = opts.unwrap_or_else(|| &DefaultOptions);
        let dir = create_dir_all(path)?;


    }
}