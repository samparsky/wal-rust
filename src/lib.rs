pub mod error;
pub mod batch;
pub mod primitives;

use lazy_static::lazy_static;
use std::fs::{self, File, create_dir_all};
use std::io::BufReader;
use std::io::prelude::*;
use crate::error::Error;
use std::path::{Path, PathBuf};
use std::fmt;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use crate::batch::Batch;
use crate::primitives::*;

lazy_static! {
    pub static ref DefaultOptions: Options = Options {
        durability: Durability::High,
        segment_size: 52428800, // 50 Mb log segment files
        log_format: LogFormat::Binary
    };

    pub static ref MAX_READERS: usize = 8; 
}

/**
 * improvements allow fixed size data / data of arbitrary length
 */

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

fn read_entry_binary(reader: &mut BufReader<&File>, discard_data: bool) -> Result<Entry, std::io::Error>{
    // encoded as index data_size data
    let mut index_buf = [0; 8];
    reader.read_exact(&mut index_buf)?; // error

    let mut data_size_buf = [0; 8];
    reader.read_exact(&mut data_size_buf)?;

    let data_size = usize::from_be_bytes(data_size_buf);

    let mut data: Vec<u8> = vec![Default::default(); data_size];
    reader.read_exact(&mut data)?;

    Ok(Entry { index: u64::from_be_bytes(index_buf), data })
}

fn read_entry_json(reader: &mut BufReader<&File>, log_format: &LogFormat) -> Result<Entry, Error> {
    let mut buf = String::new();
    match reader.read_line(&mut buf) {
        Err(e) => if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Err(Error::Corrupt)
        } else {
            return Err(Error::File(e))
        },
        _ => {},
    };

    let entry: Entry = match serde_json::from_str(&buf) {
        Ok(entry) => entry,
        Err(e) =>  return Err(Error::Corrupt),
    };

    return Ok(entry)
}

impl<'a> Log<'a> {
    pub fn open(dir: &str, opts: Option<&Options>) -> Result<Log<'a>, Error>{
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
                    match read_entry_binary(&mut reader, true) {
                        Ok(entry) => {
                            last_index = entry.index;
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

                let data_size = entry.data.len();
                let mut data_size_buf = [0; 8];
                data_size_buf.clone_from_slice(&data_size.to_be_bytes());

                self.buffer.extend(&index_buf);
                self.buffer.extend(&data_size_buf);
                self.buffer.extend(&entry.data);
            },
            LogFormat::JSON => {
                let json_entry = format!("{}\n", serde_json::to_string(&entry).expect("serialise json"));
                let entry_bytes = json_entry.as_bytes();
                self.buffer.extend(entry_bytes);
            }
        };

        self.file_size += self.buffer.len() - mark;
    }

    pub fn read_entry(&self, reader: &mut BufReader<&File>) -> Result<Entry, Error> {
        match self.opts.log_format {
            LogFormat::Binary => read_entry_binary(reader, false).map_err(Error::File),
            LogFormat::JSON => read_entry_json(reader, &self.opts.log_format)
        }
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

    pub fn write_batch(&mut self, batch: &mut Batch) -> Result<(), Error> {
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

        let mut skip = 0;
        for i in 0..batch.indexes.len() {
            let index = batch.indexes[i];
            let data = batch.datas[skip..batch.data_sizes[i]].to_vec();
            self.append_entry(&Entry { index, data });
            skip += batch.data_sizes[i];
        }

        if self.opts.durability == Durability::Medium || self.opts.durability == Durability::High || self.buffer.len() >= 4096 {
            self.flush();
        }

        self.last_index = batch.indexes[batch.indexes.len() - 1];
        // reset the batch for reuse
        batch.clear();

        Ok(())
    }

    pub fn firstindex(&self) -> Result<u64, Error>{
        if self.closed {
            return Err(Error::Closed);
        }

        if self.last_index == 0 {
            return Ok(0);
        }

        Ok(self.first_index)
    }

    pub fn lastindex(&self) -> Result<u64, Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        Ok(self.last_index)
    }

    pub fn read(&mut self, index: u64) -> Result<Vec<u8>, Error> {
        if self.closed {
            return Err(Error::Closed);
        }

        if index == 0 || index < self.first_index || index > self.last_index {
            return Err(Error::NotFound);
        }

        // find an opened reader
        let mut r = None;
        for i in 0..self.readers.len() {
            if self.readers[i].nindex == index {
                // r = Some(&mut self.readers[i]);
                r = Some(index);
                break;
            }
        }

        let mut reader_index = match r {
            Some(r) => r,
            // Reader not found, open a new reader and return the entry at index
            None => return self.open_reader(index),
        };

        // Read next entry from reader
        let sindex = self.readers[reader_index as usize].sindex;
        let nindex = self.readers[reader_index as usize].nindex;
        loop {
            let entry = match self.read_entry(&mut self.readers[reader_index as usize].rd) {
                Err(e) => {
                    if let Error::File(e) = e {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            if sindex as usize == self.segments.len() - 1 {
                                // At the ned of the last segment file
                                if self.buffer.len() > 0 {
                                    self.file.write_all(&self.buffer);
                                    self.buffer = Vec::new();
                                    continue;
                                }

                                self.close_reader(&self.readers[reader_index as usize]);
                                return Err(Error::Corrupt);
                            }
                            // close the old reader, open new one
                            self.close_reader(&self.readers[reader_index as usize]);
                            return self.open_reader(index);
                        }
                    }
                    Entry { index: 0, data: Vec::new() }
                }  
                Ok(e) => e.to_owned(),
            };

            if entry.index != index {
                self.close_reader(&self.readers[reader_index as usize]);
                return Err(Error::Corrupt);
            }

            self.readers[reader_index as usize].nindex += 1;
            
            if nindex == self.last_index + 1 {
                // read the last entry, close the reader
                self.close_reader(&self.readers[reader_index as usize])
            }

            return Ok(entry.data)
        }
    }

    fn find_segment(&self, index: u64) -> u64 {
        let mut i: u64 = 0;
        let mut j = self.segments.len() as u64;

        while i < j {
            let h = i + (j - i) / 2;
            if index >= self.segments[h as usize].index {
                i = h + 1;
            } else {
                j = h;
            }
        };

        i - 1 as u64
    }

    fn open_reader(&mut self, index: u64) -> Result<Vec<u8>, Error> {
        let sindex = self.find_segment(index);
        let mut nindex = self.segments[sindex as usize].index;
        let file = File::open(self.segments[sindex as usize].path.clone())?;
        let mut buf_reader = BufReader::new(&file);

        if sindex as usize == self.segments.len() - 1 {
            if self.buffer.len() > 0 {
                // this means we are reading from the the last segment which
                // has an in memory buffer, therefore flush the buffer
                // before reading from file
                self.file.write_all(&self.buffer).expect("OpenReader: failed to write");
                self.buffer = Vec::new(); 
            }
        }

        // scan the file for entry at index
        loop {
            let data = self.read_entry(&mut buf_reader)?;
            if data.index != nindex {
                return Err(Error::Corrupt);
            }

            nindex = data.index + 1;

            if data.index == index {
                // create new reader push it to the front
                let reader = Reader {
                    sindex,
                    nindex,
                    // file,
                    rd: buf_reader
                };

                self.readers.insert(0, reader);
                // loop and close readers
                while self.readers.len() > *MAX_READERS {
                    self.close_reader(&mut self.readers[self.readers.len() - 1]);
                }
            }

            return Ok(data.data);
        }

        // Ok(Vec::new())
    }

    pub fn close_reader(&mut self, reader: &Reader) {
        for i in 0..self.readers.len() {
            if self.readers[i].sindex == reader.sindex && self.readers[i].nindex == reader.nindex {
                self.readers[i] = self.readers[self.readers.len() - 1];
                self.readers.remove(self.readers.len() - 1);
                break;
            }
        }
    }

    pub fn truncate_back(&mut self, last_index: u64) -> Result<(), Error>{
        if self.closed {
            return Err(Error::Closed);
        }
        let index = last_index;

        if index == 0 || self.last_index == 0 || index > self.last_index || index < self.first_index {
            return Err(Error::OutOfRange);
        }

        if self.buffer.len() > 0 {
            self.file.write_all(&self.buffer).expect("should write to file");
            self.buffer = Vec::new();
        }

        // close all readers
        // TODO check do I need readers array???

        if index == self.last_index {
            return Ok(())
        }

        let sindex = self.find_segment(index);

        // Open file
        let file = File::open(&self.segments[sindex as usize].path)?;

        // Read all entries prior to entry at index
        let mut reader = BufReader::new(&file);
        let mut found = false;
        // let mut offset = 0;
        loop {
            let ridx = self.read_entry(&mut reader)?;
            if ridx.index == index {
                // offset = reader.buffer().len();
                // offset = file.seek(SeekFrom::Start(file_size))?;
                found = true;
                break;

            }

        }

        if !found {
            return Err(Error::Corrupt);
        }

        // create a temp file in the log dir & copy all of data
        // up to offset

        let temp_filepath = self.path.join("TEMP");
        let mut temp_file = File::create(&temp_filepath)?;

        // copy read data into temp file
        std::io::copy(&mut reader, &mut temp_file)?;

        drop(temp_file); // close temp_file
        drop(file); // close file

        // rename the temp file to end file
        let end_filename = self.segments[sindex as usize].path.clone();

        for i in (0..self.segments.len()).rev() {
            if i < sindex as usize { break; }
            fs::remove_file(&self.segments[i].path).expect("should remove file");
        }

        self.segments.truncate((sindex + 1) as usize);
        fs::rename(&temp_filepath, &end_filename)?;

        self.file = OpenOptions::new().read(true).write(true).open(end_filename.clone())?;
        let file_size = self.file.metadata()?.len();
        self.file_size = file_size as usize;
        self.buffer = Vec::new();
        self.last_index = index;

        // move the write cursor to the end of 
        // the file
        self.file.seek(SeekFrom::Start(file_size))?;
        
        Ok(())
    }

    pub fn truncate_front(&mut self, index: u64) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }

        if index == 0 || self.last_index == 0 || index > self.last_index || index < self.first_index {
            return Err(Error::OutOfRange);
        }

        if self.buffer.len() > 0 {
            self.file.write_all(&self.buffer).expect("should write to file");
            self.buffer = Vec::new();
        }

        // close all readers
        // TODO check do I need readers array???

        if index == self.last_index {
            return Ok(())
        }

        let sindex = self.find_segment(index);
        // Open file
        let mut file = File::open(&self.segments[sindex as usize].path)?;
        let mut reader = BufReader::new(&file);

        if index > self.segments[sindex as usize].index {
            
            let found: bool;
            loop {
                let ridx = self.read_entry(&mut reader)?;
                if ridx.index == index + 1 {
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(Error::Corrupt);
            }
        }

        // Read all entries prior to entry at index
        let temp_filepath = self.path.join("TEMP");
        let mut temp_file = File::create(&temp_filepath)?;
        reader.seek(SeekFrom::Start(index + 1));

        std::io::copy(&mut reader, &mut temp_file)?;

        drop(file);
        drop(temp_file);

        let end_filename = self.segments[sindex as usize].path.clone();

        for i in 0..sindex+1 {
            fs::remove_file(&self.segments[i as usize].path).expect("should remove file");
        }

        fs::rename(&temp_filepath, &end_filename)?;

        self.segments.insert((sindex+1) as usize,  Segment {
            index,
            path: end_filename.clone()
        });

        if self.segments.len() == 1 {
            self.file = OpenOptions::new().read(true).write(true).open(end_filename.clone())?;
            let file_size = self.file.metadata()?.len();
            self.file_size = file_size as usize;
            self.buffer = Vec::new();
        }

        self.first_index = index;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn open_log() {
        unimplemented!();
    }
}