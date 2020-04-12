pub mod error;
pub mod batch;

use lazy_static::lazy_static;
use std::fs::{self, File, create_dir_all};
use std::io::BufReader;
use std::io::prelude::*;
use crate::error::Error;
use std::path::{Path, PathBuf};
use std::fmt;
use std::fs::OpenOptions;
use serde::{Deserialize, Serialize};
use std::io::SeekFrom;
use crate::batch::Batch;

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
    index: u64,
    data: String,
}

lazy_static! {
    pub static ref DefaultOptions: Options = Options {
        durability: Durability::High,
        segment_size: 52428800, // 50 Mb log segment files
        log_format: LogFormat::Binary
    };

    pub static ref MAX_READERS: i8 = 8; 
}

#[derive(Debug, Default, Clone)]
pub struct Segment {
    pub path: PathBuf,
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
    pub path: PathBuf,
    pub opts: Options,
    pub closed: bool,
    pub segments: Vec<Segment>,
    pub first_index: u64,
    pub last_index: u64,
    pub file: File,
    pub buffer: Vec<u8>,
    pub file_size: usize,
    pub readers: Vec<Reader>
}

fn abs(path: &str) -> Result<String, Error> {
    if path == ":memory" {
        Err(Error::InMemoryLog)
    } else {
        Ok(String::new())
    }
}

fn load_segments(dir: &str) -> Result<(usize, usize, Vec<Segment>), Error>{
    let path = Path::new(dir);
    if !path.is_dir() {
        // return error should be a directory
    }
    let mut start_index = 0;
    let mut end_index = 0;

    let files = fs::read_dir(dir)?;
    let mut segments: Vec<Segment> = Vec::new();

    for file in files {
        let file = file?;
        let name = file.file_name().into_string().expect("should have valid string name");
        let file_type = file.file_type()?;
        if file_type.is_dir() || name.len() < 20 {
            continue;
        }

        let index = u64::from_str_radix(&name[..20], 10);
        if index.is_err() {
            continue;
        }
        let index = index.expect("should have value");
        if index == 0 {
            continue;
        }

        let start = name.len() == 26 && name.ends_with(".START");
        let end = name.len() == 24 && name.ends_with(".END");

        if name.len() == 20 || start || end {
            if start {
                start_index = segments.len();
            } else if end && end_index == 0 {
                end_index = segments.len();
            }
            segments.push( Segment {
                index,
                path: file.path(),
            })
        }
    }

    Ok((start_index, end_index, segments))
}

fn segment_name(index: u64) -> String {
    format!("{:0>20}", index)
}

fn readEntryJSON(reader: &BufReader<File>) {
    // let mut line = Vec::new();
    // let len = reader.read_line(buf: &mut String);

}

fn readEntryBinary(reader: &mut BufReader<&File>, discard_data: bool) -> Result<(u64, Vec<u8>), std::io::Error>{
    // encoded as index data_size data
    let mut index_buf = [0; 8];
    reader.read_exact(&mut index_buf)?; // error

    let mut data_size_buf = [0; 8];
    reader.read_exact(&mut data_size_buf)?;

    let data_size = usize::from_be_bytes(data_size_buf);

    let mut data: Vec<u8> = vec![Default::default(); data_size];
    reader.read_exact(&mut data)?;

    Ok((u64::from_be_bytes(index_buf), data))
}

fn readEntry(reader: &mut BufReader<File>, log_format: &LogFormat) {
    // match log_format {
    //     LogFormat::JSON  => {

    //     }
    // }
}

fn appendJSONEntry(entry: &Entry) {

}

fn appendBinaryEntry(buffer: &[u8], entry: &Entry) {
    let index_bytes = entry.index.to_be();
    let data_bytees = entry.data.as_bytes();
}

impl Log {
    pub fn open(dir: &str, opts: Option<&Options>) -> Result<Log, Error>{
        if dir == ":memory:" {
            return Err(Error::InMemoryLog);
        }

        let path_dir = Path::new(&dir);
        let options = opts.unwrap_or_else(|| &DefaultOptions);
        // create all directory
        create_dir_all(dir)?;

        let (start_index, end_index, mut segments) = load_segments(&dir)?;

        let mut first_index = 1;
        let mut last_index = 0;

        let mut file: File;

        if segments.len() == 0 {
            let file_path = path_dir.join(&segment_name(1));
            file = File::create(&file_path)?;

            segments.push(
                Segment {
                    index: 1,
                    path: file_path
                }
            );
        };

        if start_index != 0 {
            if end_index != 0 {
                return Err(Error::Corrupt);
            }

            for index in 0..start_index {
                let file_path = segments[index].path.clone();
                fs::remove_file(file_path)?;
            }

            segments =  segments[start_index..].to_vec();
            // rename START segment
            let org_path = segments[0].path.to_str().expect("should have a valid path");
            let file_name_index = org_path.len() - ".start".len();
            // rename 
            fs::rename(org_path, &org_path[..file_name_index])?;
            segments[0].path = Path::new(&org_path[..file_name_index]).to_path_buf();
        };

        if end_index != 0 {
            for index in ((end_index + 1)..segments.len() - 1).rev() {
                fs::remove_file(segments[index].path.clone())?;
            }

            segments =  segments[..end_index+1].to_vec();

            if segments.len() > 1 && segments[segments.len() - 2 ].index == segments[segments.len() - 1].index {
                let len = segments.len();
                segments[len - 2] = segments[len - 1].clone();
                segments = segments[..segments.len() - 1].to_vec();
            }

            // rename END segment
            let org_path = segments[segments.len() - 1].path.to_str().expect("should have a valid path");
            let file_name_index = org_path.len() - ".end".len();
            // rename 
            fs::rename(org_path, &org_path[..file_name_index])?;
            segments[0].path = Path::new(&org_path[..file_name_index]).to_path_buf();
        };

        first_index = segments[0].index;
        let last_path = segments[segments.len() - 1].path.clone();
        file = OpenOptions::new().read(true).write(true).open(last_path.clone())?;
        let file_size = file.metadata()?.len();

        // read the last segment to the end of log
        match options.log_format {
            LogFormat::JSON => {
                let data = fs::read_to_string(last_path)?;
                let parsed_data: Vec<Entry> = match serde_json::from_str(&data){
                    Ok(data) => data,
                    Err(_) => return Err(Error::Corrupt) // verify this behaviour
                };
                last_index = parsed_data[parsed_data.len() - 1].index;
                // convert it from json
                // 1
            },
            LogFormat::Binary => {
                let mut reader = BufReader::new(&file);
                loop {
                    match readEntryBinary(&mut reader, true) {
                        Ok((index,_)) => {
                            last_index = index;
                            continue;
                        },
                        Err(e) => {
                            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                                break;
                            }
                            return Err(Error::File(e));
                        }
                    }
                }
            }
        };
        
        // move the write cursor to the end of 
        // the file
        file.seek(SeekFrom::Start(file_size))?;

        Ok(Log {
            path: Path::new(&dir).to_path_buf(),
            opts: options.to_owned(),
            closed: false,
            segments: segments,
            first_index,
            last_index,
            file,
            buffer: Vec::new(),
            file_size: file_size as usize,
            readers: Vec::new(),
        })
    }

    pub fn close(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        };
        self.flush();
        Ok(())
    }

    pub fn flush(&mut self) {
        if self.buffer.len() > 0 {
            // must write buffer or crash
            self.file.write_all(&self.buffer).expect("Flush: Failed to write to file");
            self.buffer = Vec::new();
            if self.opts.durability == Durability::High {
                self.file.sync_all().expect("Flush: Failed to sync data;");
            }
        };
    }

    pub fn write(&mut self, entry: &Entry) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }

        if entry.index != self.last_index + 1 {
            return Err(Error::OutofOrder);
        }

        if self.file_size >= self.opts.segment_size {
            // cycle
            self.cycle();
        }

        // appendEntry
        self.append_entry(&entry);

        Ok(())
    }

    pub fn append_entry(&mut self, entry: &Entry) {
        let mark = self.buffer.len();
        match self.opts.log_format {
            LogFormat::Binary => {

                let mut index_buf = [0; 8];
                let index_bytes = entry.index.to_be_bytes();
                index_buf.clone_from_slice(&index_bytes);

                let data_bytes = entry.data.as_bytes();

                let data_size = data_bytes.len();
                let mut data_size_buf = [0; 8];
                data_size_buf.clone_from_slice(&data_size.to_be_bytes());

                self.buffer.extend(&index_buf);
                self.buffer.extend(&data_size_buf);
                self.buffer.extend(data_bytes);
            },
            LogFormat::JSON => {
                let json_entry = format!("{},", serde_json::to_string(&entry).expect("serialise json"));
                let entry_bytes = json_entry.as_bytes();
                self.buffer.extend(entry_bytes);
            }
        };

        self.file_size += self.buffer.len() - mark;
    }
    pub fn cycle(&mut self) {
        self.flush();
        // std::mem::drop(self.file);

        let segment = Segment {
            index: self.last_index + 1,
            path: self.path.join(&segment_name(self.last_index + 1))
        };

        self.file = File::create(segment.path.clone()).expect("Cycle: Failed to create file");
        self.file_size = 0;
        self.segments.push(segment);
        self.buffer = Vec::new();
    }

    pub fn write_batch(&mut self, batch: &Batch) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        if batch.indexes.len() == 0 {
            return Ok(());
        }
        // check indexes
        for i in 0..batch.indexes.len() {
            if batch.indexes[i] != self.last_index + i as u64 + 1 {
                return Err(Error::OutofOrder);
            }
        }

        if self.file_size >= self.opts.segment_size {
            self.cycle();
        }

        for i in 0..batch.indexes.len() {
            // let data = 
        }


        Ok(())
    }

    pub fn close_reader(&self) {

    }
}