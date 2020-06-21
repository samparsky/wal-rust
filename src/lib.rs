pub mod error;
pub mod batch;
pub mod primitives;

use lazy_static::lazy_static;
use std::fs::{self, File, create_dir_all};
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use crate::error::Error;
use std::path::{Path, PathBuf};
use std::fmt;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use crate::batch::Batch;
use crate::primitives::*;

lazy_static! {
    pub static ref DEFAULT_OPTIONS: Options = Options {
        durability: Durability::High,
        segment_size: 52428800, // 50 Mb log segment files
        log_format: LogFormat::JSON
    };

    pub static ref MAX_READERS: usize = 8; 
    pub static ref MAX_BUFFER_SIZE: usize = 8096;
}

/**
 * improvements allow fixed size data / data of arbitrary length
 */

trait WAL {
    // fn open(dir: &str, opts: Option<&Options>) -> Result<Log, Error>;
    fn close(&mut self) -> Result<(), Error>;
    fn flush(&mut self);
    fn write(&mut self, entry: &Entry) -> Result<(), Error>;
    fn append_entry(&mut self, entry: &Entry);
    fn read_entry(&self, reader: &mut BufReader<&File>) -> Result<Entry, Error>;
    fn cycle(&mut self);
    fn firstindex(&self) -> Result<u64, Error>;
    fn lastindex(&self) -> Result<u64, Error>;
    fn read(&mut self, index: u64) -> Result<Vec<u8>, Error>;
    fn close_reader(&mut self, reader: &Reader);
    fn truncate_back(&mut self, last_index: u64) -> Result<(), Error>;
    fn truncate_front(&mut self, index: u64) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct Log {
    pub path: PathBuf,
    pub opts: Options,
    pub closed: bool,
    segments: Vec<Segment>,
    first_index: u64,
    last_index: u64,
    file: BufWriter<File>,
    file_size: usize,
    readers: Vec<Reader>
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

        let index = match u64::from_str_radix(&name[..20], 10) {
            Err(_) => continue,
            Ok(i) if i == 0 => continue,
            Ok(i) => i,
        };

        let is_start = name.len() == 26 && name.ends_with(".START");
        let is_end = name.len() == 24 && name.ends_with(".END");

        if name.len() == 20 || is_start || is_end {
            if is_start {
                start_index = segments.len();
            } else if is_end && end_index == 0 {
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

fn read_entry_binary(reader: &mut BufReader<File>, discard_data: bool) -> Result<Entry, std::io::Error>{
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

fn read_entry_binary_ref(reader: &mut BufReader<&File>, discard_data: bool) -> Result<Entry, std::io::Error>{
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

fn read_entry_json_ref(reader: &mut BufReader<&File>) -> Result<Entry, Error> {
    let mut buf = String::new();
    // println!("buf {}", buf.len());
    let read_result = reader.read_line(&mut buf);
    // println!("{:?}", read_result);
    match read_result {
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof && buf.len() > 0  => return Err(Error::Corrupt),
        Err(e) => return Err(Error::File(e)),
        Ok(0) => return Err(Error::File(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "empty file"))),
        _ => {},
    };
    // println!("buf2 {}", buf);
    let entry: Entry = match serde_json::from_str(&buf) {
        Ok(entry) => entry,
        Err(_e) =>  return Err(Error::Corrupt),
    };

    return Ok(entry)
}

fn read_entry_json(reader: &mut BufReader<File>, _log_format: &LogFormat) -> Result<Entry, Error> {
    let mut buf = String::new();
    match reader.read_line(&mut buf) {
        Err(e) => if e.kind() == std::io::ErrorKind::UnexpectedEof && buf.len() > 0 {
            println!("buf {}",  buf);
            return Err(Error::Corrupt)
        } else {
            return Err(Error::File(e))
        },
        _ => {},
    };
    println!("buffer contents {}", buf);
    println!("buffer contents len {}", buf.len());
    
    if buf.len() == 0 || buf.len() == 1 {
        return Err(Error::File(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "empty file")));
    }

    let entry: Entry = match serde_json::from_str(&buf) {
        Ok(entry) => entry,
        Err(_e) =>  return Err(Error::Corrupt),
    };
    println!("entry 2 {:?}", entry);
    return Ok(entry)
}

impl Log {
    pub fn open(dir: &str, opts: Option<&Options>) -> Result<Log, Error>{
        println!("in testing ");
        if dir == ":memory:" {
            return Err(Error::InMemoryLog);
        }

        let path_dir = Path::new(&dir);
        let options = opts.unwrap_or_else(|| &DEFAULT_OPTIONS);
        // create all directory
        create_dir_all(dir)?;

        let (start_index, end_index, mut segments) = load_segments(&dir)?;

        // let mut first_index = 1;
        // let mut last_index = 0;

        // let mut file: File;
        println!("start index {}", start_index);
        println!("end index {}", end_index);
        println!("segments {:?}", segments);

        if segments.len() == 0 {
            let file_path = path_dir.join(&segment_name(1));
            // create file
            File::create(&file_path)?;

            segments.push(
                Segment {
                    index: 1,
                    path: file_path
                }
            );
        };

        if start_index != 0 {
            if end_index != 0 {
                // There should not be a START and END at the same time
                return Err(Error::Corrupt);
            }
            // Delete all files leading up to START
            for index in 0..start_index {
                let file_path = segments[index].path.clone();
                fs::remove_file(file_path)?;
            }

            segments =  segments[start_index..].to_vec();
            // rename START segment
            let org_path = segments[0].path.to_str().expect("should have a valid path");
            let file_name_index = org_path.len() - ".START".len();
            // rename 
            fs::rename(org_path, &org_path[..file_name_index])?;
            segments[0].path = Path::new(&org_path[..file_name_index]).to_path_buf();
        };

        if end_index != 0 {
            // Delete all files following END
            for index in ((end_index + 1)..segments.len() - 1).rev() {
                fs::remove_file(segments[index].path.clone())?;
            }

            segments =  segments[..end_index+1].to_vec();

            if segments.len() > 1 && segments[segments.len() - 2 ].index == segments[segments.len() - 1].index {
                let len = segments.len();
                // remove the segment prior to the END segment because it shares
			    // the same starting index.
                segments[len - 2] = segments[len - 1].clone();
                segments.pop(); // remove last item
                // segments = segments[..segments.len() - 1].to_vec();
            }

            // rename END segment
            let org_path = segments[segments.len() - 1].path.to_str().expect("should have a valid path");
            let file_name_index = org_path.len() - ".END".len();
            // rename 
            fs::rename(org_path, &org_path[..file_name_index])?;
            let len = segments.len();
            segments[len - 1].path = Path::new(&org_path[..file_name_index]).to_path_buf();
        };

        let first_index = segments[0].index;
        let mut last_index = 0;
        let last_path = segments[segments.len() - 1].path.clone();
        let file = OpenOptions::new().read(true).write(true).open(last_path.clone())?;
        let file_size = file.metadata()?.len();

        // read the last segment to the end of log
        let mut reader = BufReader::new(&file);

        match options.log_format {
            LogFormat::JSON => {
                loop {
                    match read_entry_json_ref(&mut reader) {
                        Ok(entry) => {
                            last_index = entry.index;
                            continue;
                        },
                        Err(Error::File(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e),
                    }
                }
            },
            LogFormat::Binary => {
                loop {
                    match read_entry_binary_ref(&mut reader, true) {
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

        println!("after testing",);

        let mut writer = BufWriter::new(file);
        
        // move the write cursor to the end of 
        // the file

        // @TODO this should be BufWriter seek
        // and not file seek
        // file.seek(SeekFrom::Start(file_size))?;
        writer.seek(SeekFrom::Start(file_size))?;

        Ok(Log {
            path: Path::new(&dir).to_path_buf(),
            opts: options.to_owned(),
            closed: false,
            segments: segments,
            first_index,
            last_index,
            file: writer,
            // buffer: Vec::new(),
            file_size: file_size as usize,
            readers: Vec::new(),
        })
    }

    pub fn close(&mut self) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        };
        self.flush();
        self.closed = true;
        self.segments.clear();
        self.readers.clear();
        Ok(())
    }

    fn flush(&mut self) {
        if self.file.buffer().len() > 0 {
            // must write buffer or crash
            // self.file.write_all(&self.buffer).expect("Flush: Failed to write to file");
            // self.buffer = Vec::new();
            self.file.flush().expect("Flush: Failed to write to file");
            if self.opts.durability == Durability::High {
                self.file.get_ref().sync_all().expect("Flush: Failed to sync data;");
            }
        };
    }

    pub fn sync(&mut self) {
        if self.file.buffer().len() > 0 {
            // must write buffer or crash
            // self.file.write_all(&self.buffer).expect("Flush: Failed to write to file");
            // self.buffer = Vec::new();
            self.file.flush().expect("Flush: Failed to write to file");
            if self.opts.durability < Durability::High {
                self.file.get_ref().sync_all().expect("Flush: Failed to sync data;");
            }
        };
    }

    pub fn write<D: AsRef<[u8]>>(&mut self, index: u64, data: D) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }

        if index != self.last_index + 1 {
            return Err(Error::OutofOrder);
        }

        if self.file_size >= self.opts.segment_size {
            // cycle
            self.cycle();
        }

        // appendEntry
        self.append_entry(index, data);

        // check durability
        if self.opts.durability >= Durability::Medium || self.file.buffer().len() > *MAX_BUFFER_SIZE {
            self.flush();
        }
        println!("index {}", index);
        self.last_index = index;

        Ok(())
    }

    fn append_entry<D: AsRef<[u8]>>(&mut self, index: u64, data: D) {
        let mark = self.file.buffer().len();
        match self.opts.log_format {
            LogFormat::Binary => {

                let mut index_buf = [0; 8];
                let index_bytes = index.to_be_bytes();
                index_buf.clone_from_slice(&index_bytes);

                let data_size = data.as_ref().len();
                let mut data_size_buf = [0; 8];
                data_size_buf.clone_from_slice(&data_size.to_be_bytes());

                self.file.write(&index_buf).expect("Failed to append entry index buf");
                self.file.write(&data_size_buf).expect("Failed to append entry index buf");
                self.file.write(&data.as_ref()).expect("Failed to append entry index buf");
            },
            LogFormat::JSON => {
                let entry = Entry {
                    index,
                    data: data.as_ref().to_vec()
                };

                let json_entry = if self.file_size > 0 {
                    // data exists
                    format!("\n{}", serde_json::to_string(&entry).expect("serialise json"))
                } else {
                    format!("{}", serde_json::to_string(&entry).expect("serialise json"))
                };

                let entry_bytes = json_entry.as_bytes();
                self.file.write(&entry_bytes).expect("Failed to append entry json");
            }
        };

        self.file_size += self.file.buffer().len() - mark;
    }

    fn read_entry(&self, reader: &mut BufReader<File>) -> Result<Entry, Error> {
        match self.opts.log_format {
            LogFormat::Binary => read_entry_binary(reader, false).map_err(Error::File),
            LogFormat::JSON => read_entry_json(reader, &self.opts.log_format)
        }
    }

    fn read_entry_with_index(&mut self, reader_index: usize) -> Result<Entry, Error> {
        let reader = &mut self.readers[reader_index].rd;
        match self.opts.log_format {
            LogFormat::Binary => read_entry_binary(reader, false).map_err(Error::File),
            LogFormat::JSON => read_entry_json(reader, &self.opts.log_format)
        }
    }

    fn cycle(&mut self) {
        self.flush();
        // std::mem::drop(self.file);
        // fn cycle(&mut self)
        let segment = Segment {
            index: self.last_index + 1,
            path: self.path.join(&segment_name(self.last_index + 1))
        };

        let file = File::create(segment.path.clone()).expect("Cycle: Failed to create file");
        self.file = BufWriter::new(file);
        self.file_size = 0;
        self.segments.push(segment);
        // self.buffer = Vec::new();
    }

    pub fn write_batch(&mut self, batch: &mut Batch) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        // println!("detecting");
        // check indexes
        if batch.data_sizes.iter().sum::<usize>() != batch.datas.len() {
            return Err(Error::OutofOrder);
        }

        if self.file_size >= self.opts.segment_size {
            self.cycle();
        }

        // println!("{:?}", batch);

        let mut skip = 0;
        for i in 0..batch.data_sizes.len() {
            let index = self.last_index + i as u64 + 1;
            let data = &batch.datas[skip..batch.data_sizes[i] + skip];
            self.append_entry(index, &data);
            skip += batch.data_sizes[i];
        }
        // 8096 -> 8KB
        // @TODO revise this implementation
        if self.opts.durability == Durability::Medium || self.opts.durability == Durability::High || self.file.buffer().len() >= 8096 {
            self.flush();
        }

        self.last_index += batch.data_sizes.len() as u64;
        // reset the batch for reuse
        batch.clear();

        Ok(())
    }
    
    // FirstIndex returns the index of the first entry in the log. Returns zero
    // when log has no entries.
    pub fn firstindex(&self) -> Result<u64, Error> {
        if self.closed {
            return Err(Error::Closed);
        }

        if self.last_index == 0 {
            return Ok(0);
        }

        Ok(self.first_index)
    }
    
    // LastIndex returns the index of the last entry in the log. Returns zero when
    // log has no entries.
    pub fn lastindex(&self) -> Result<u64, Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        Ok(self.last_index)
    }
    
    // Read an entry from the log. This function reads an entry from disk and is
    // optimized for sequential reads. Randomly accessing entries is slow.
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
                r = Some(i);
                break;
            }
        }

        let reader_index = match r {
            Some(r) => r,
            // Reader not found, open a new reader and return the entry at index
            None => return self.open_reader(index),
        };
        println!("checking found a reader");
        // Read next entry from reader
        let sindex = self.readers[reader_index as usize].sindex;
        let nindex = self.readers[reader_index as usize].nindex;

        loop {
            let entry = match self.read_entry_with_index(reader_index) {
                Err(e) => {
                    println!("{:?}", e);
                    if let Error::File(e) = e {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            if sindex as usize == self.segments.len() - 1 {
                                // At the ned of the last segment file
                                if self.file.buffer().len() > 0 {
                                    self.file.flush().expect("Failed to write to file");
                                    // self.buffer = Vec::new();
                                    continue;
                                }
                                println!("in testing found");
                                self.readers.remove(reader_index as usize);
                                return Err(Error::Corrupt);
                            }
                            // close the old reader, open new one
                            self.readers.remove(reader_index as usize);
                            return self.open_reader(index);
                        }
                    }
                    Entry { index: 0, data: Vec::new() }
                }  
                Ok(e) => e.to_owned(),
            };

            println!("{:?}", entry);
            println!("index {}", index);

            if entry.index != index {
                self.readers.remove(reader_index as usize);
                return Err(Error::Corrupt);
            }

            self.readers[reader_index as usize].nindex += 1;
            
            if nindex == self.last_index + 1 {
                // read the last entry, close the reader
                self.readers.remove(reader_index as usize);
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
        let file = File::open(&self.segments[sindex as usize].path)?;
        let mut buf_reader = BufReader::new(file);

        if sindex as usize == self.segments.len() - 1 {
            if self.file.buffer().len() > 0 {
                // this means we are reading from the the last segment which
                // has an in memory buffer, therefore flush the buffer
                // before reading from file
                // self.file.write_all(&self.buffer).expect("OpenReader: failed to write");
                self.file.flush().expect("OpenReader: failed to write");
                // self.buffer = Vec::new(); 
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
                if self.readers.len() > *MAX_READERS {
                    // close the readers that are opened
                    self.readers.drain(self.readers.len() - 1..);
                    // self.close_reader(&mut self.readers[self.readers.len() - 1]);
                }
            }

            return Ok(data.data);
        }

        // Ok(Vec::new())
    }

    // pub fn close_reader(&mut self, reader: &Reader) {
        // for i in 0..self.readers.len() {
        //     if self.readers[i].sindex == reader.sindex && self.readers[i].nindex == reader.nindex {
        //         self.readers[i] = self.readers[self.readers.len() - 1];
        //         self.readers.remove(self.readers.len() - 1);
        //         break;
        //     }
        // }
    // }

    pub fn truncate_back(&mut self, last_index: u64) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        let index = last_index;

        if index == 0 || self.last_index == 0 || index > self.last_index || index < self.first_index {
            return Err(Error::OutOfRange);
        }

        if self.file.buffer().len() > 0 {
            // self.file.write_all(&self.buffer).expect("should write to file");
            self.file.flush().expect("should write to file");
            // self.buffer = Vec::new();
        }

        // close all readers
        // TODO check do I need readers array???
        self.readers.drain(0..);

        if index == self.last_index {
            return Ok(())
        }

        let sindex = self.find_segment(index);

        // Open file
        let file = File::open(&self.segments[sindex as usize].path)?;

        // Read all entries prior to entry at index
        let mut reader = BufReader::new(file);
        let found;
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
        // get file current position
        let current_pos = reader.seek(SeekFrom::Current(0))?;
        // move the reader to the start
        reader.seek(SeekFrom::Start(0))?;
        let mut handle = reader.take(current_pos);
        std::io::copy(&mut handle, &mut temp_file)?;

        drop(temp_file); // close temp_file
        // drop(file); // close file

        // rename the temp file to end file
        let end_filename = self.segments[sindex as usize].path.clone();

        for i in (0..self.segments.len()).rev() {
            if i < sindex as usize { break; }
            fs::remove_file(&self.segments[i].path).expect("should remove file");
        }

        self.segments.truncate((sindex + 1) as usize);

        fs::rename(&temp_filepath, &end_filename)?;

        let file = OpenOptions::new().read(true).write(true).open(end_filename.clone())?;
        let file_size = file.metadata()?.len();
        self.file = BufWriter::new(file);
        self.file_size = file_size as usize;
        // self.buffer = Vec::new();
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

        if self.file.buffer().len() > 0 {
            // self.file.write_all(&self.buffer).expect("should write to file");
            self.file.flush().expect("should write to file");
            // self.buffer = Vec::new();
        }

        // close all readers
        // TODO check do I need readers array???
        self.readers.drain(0..);

        if index == self.first_index {
            return Ok(())
        }

        let sindex = self.find_segment(index);
        // Open file
        let file = File::open(&self.segments[sindex as usize].path)?;
        // @TODO close file
        let mut reader = BufReader::new(file);

        if index > self.segments[sindex as usize].index {
            
            let found: bool;
            loop {
                let ridx = self.read_entry(&mut reader)?;
                if ridx.index == index - 1 {
                    // @TODO Seeker check
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
        // @TODO please confirm behavior
        // reader.seek(SeekFrom::Start(index + 1)).expect("failed to seek");

        std::io::copy(&mut reader, &mut temp_file)?;

        // drop(file);
        drop(temp_file);

        let end_filename = self.path.join(segment_name(index));
        println!("{:?}", end_filename);

        for i in 0..sindex+1 {
            fs::remove_file(&self.segments[i as usize].path).expect("should remove file");
        }

        fs::rename(&temp_filepath, &end_filename)?;

        self.segments.insert((sindex+1) as usize,  Segment {
            index,
            path: end_filename.clone()
        });

        if self.segments.len() == 1 {
            let file = OpenOptions::new().read(true).write(true).open(end_filename.clone())?;
            let file_size = file.metadata()?.len();

            self.file = BufWriter::new(file);
            self.file_size = file_size as usize;
            // self.buffer = Vec::new();
        }

        self.first_index = index;
        Ok(())
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use super::primitives::Options;
    use crate::error::*;
    use crate::Batch;
    use std::fs;
    use std::str;
    use std::path::Path;


    #[test]
    fn durability_low() {
        let base_path = "testlog/low";
        // if Path::new(&base_path).exists() {
        //     fs::remove_dir_all(&base_path).expect("should remove dir");
        // }

        println!("after path");

        // Durability::Low
        let path = format!("{}{}", base_path, "/json");
        test_log(&path, 100, Some(&make_options(512, Durability::Low, LogFormat::JSON)));

        fs::remove_dir_all(&base_path).expect("should remove dir");
    }

    fn data_str(i: u64) -> String {
        format!("data-{}", i)
    }
    
    fn test_first_last(log: &Log, expect_first: u64, expect_last: u64) {
        let first_index = log.firstindex().expect("should return first index");
        assert_eq!(first_index, expect_first, "TestFirstLast; FirstIndex: expected {}, got {}", expect_first, first_index);

        let last_index = log.lastindex().expect("Should return last index");
        assert_eq!(last_index, expect_last, "TestFirstLast; LastIndex: expected {}, got {}", expect_last, last_index);
    }

    
    fn test_log(path: &str, mut N: u64, opts: Option<&Options>) {
        println!("in it");
        // let mut N: u64 = 100;
        // unimplemented!();
        // let path = "testlog/log";
        let mut log = Log::open(path, opts).expect("should open log");

        // FirstIndex - should be zero
        let first_index = log.firstindex().expect("should return first index");
        assert_eq!(first_index, 0, "FirstIndex: expected {}, got {}", 0, first_index);

        // LastIndex
        let last_index = log.lastindex().expect("Should return last index");
        assert_eq!(last_index, 0, "LastIndex: expected {}, got {}", 0, last_index);


        for i in 1..N+1 {
            // write - try to apprend previous index should fial
            match log.write(i-1, data_str(i)) {
                Err(Error::OutofOrder) => {},
                _ => panic!("Write: should throw error")
            };
            
            // Write - append next item
            log.write(i, data_str(i)).expect("Write: should append item successfully");
            println!("i {}", i);
            // Write - get next item
            let data = log.read(i).expect("WriteN: should read item");

            assert_eq!(
                str::from_utf8(&data).expect("should be valid"),
                data_str(i),
                "Write: expected {}, got {}",
                data_str(i),
                str::from_utf8(&data).expect("should be valid"),
            );
        }

        // Read - should fail, not found
        match log.read(0) {
            Err(Error::NotFound) => {},
            _ => panic!("Read: should throw not found error")
        };

        // Read - read back all entries
        for i in 1..N {
            let data = log.read(i).expect("Read: should read entry");
            assert_eq!(
                str::from_utf8(&data).expect("should be valid"),
                data_str(i),
                "Write: expected {}, got {}",
                data_str(i),
                str::from_utf8(&data).expect("should be valid"),
            );
        }

        // Read - read back first half entries
        for i in 1..N/2 {
            let data = log.read(i).expect("Read: should read entry");
            assert_eq!(
                str::from_utf8(&data).expect("should be valid"),
                data_str(i),
                "Write: expected {}, got {}",
                data_str(i),
                str::from_utf8(&data).expect("should be valid"),
            );
        }

        //@TODO Read - random access

        // FirstIndex/LastIndex -- check valid first and last indexes
        
        let first_index = log.firstindex().expect("should get firstindex");
        assert_eq!(first_index, 1, "FirstIndex1: expected {}, got {}", 1, first_index);

        let last_index = log.lastindex().expect("Should return last index");
        assert_eq!(last_index, N, "LastIndex: expected {}, got {}", N, last_index);

        // Close - close the log
        log.close().expect("Close: should close log");

        // Write - try while closed
        match log.write(1, "test") {
            Err(Error::Closed) => {},
            _ => panic!("Close: should fail to write when closed")
        };
        // @TODO WriteBatch - try while closed
        
        // FirstIndex - try while closed
        match log.firstindex() {
            Err(Error::Closed) => {},
            _ => panic!("Close: should fail to write when closed")
        };

        // LastIndex - try while closed
        match log.lastindex() {
            Err(Error::Closed) => {},
            _ => panic!("Close: should fail to write when closed")
        };

        // Get - try while closed
        match log.read(1) {
            Err(Error::Closed) => {},
            _ => panic!("Close: should fail to write when closed")
        };

        // TruncateFront - try while closed
        match log.truncate_front(1) {
            Err(Error::Closed) => {},
            _ => panic!("Close: should fail to write when closed")
        };

        // TruncateBack - try while closed
        match log.truncate_back(1) {
            Err(Error::Closed) => {},
            _ => panic!("Close: should fail to write when closed")
        };

        // Open -- reopen log
        let mut log = Log::open(path, None).expect("should re-open log");
        
        // Read - read back all entries
        for i in 1..N+1 {
            let data = log.read(i).expect("Read: should read entry");
            assert_eq!(
                str::from_utf8(&data).expect("should be valid"),
                data_str(i),
                "Write: expected {}, got {}",
                data_str(i),
                str::from_utf8(&data).expect("should be valid"),
            );
        }

        // FirstIndex - should be 1
        let first_index = log.firstindex().expect("should return first index");
        assert_eq!(first_index, 1, "FirstIndex: expected {}, got {}", 1, first_index);

        // LastIndex
        let last_index = log.lastindex().expect("Should return last index");
        assert_eq!(last_index, N, "LastIndex: expected {}, got {}", N, last_index);

        // Write -- add 50 more items
        for i in N+1..N+51 {
            // Write - append next item
            log.write(i, data_str(i)).expect("Write: should append item successfully");

            // Write - get next item
            let data = log.read(i).expect("Write50: should read item");

            assert_eq!(
                str::from_utf8(&data).expect("should be valid"),
                data_str(i),
                "Write: expected {}, got {}",
                data_str(i),
                str::from_utf8(&data).expect("should be valid"),
            );
        }
        N += 50;
        // FirstIndex/LastIndex -- check valid first and last indexes

        // FirstIndex - should be 1
        let first_index = log.firstindex().expect("should return first index");
        assert_eq!(first_index, 1, "FirstIndex: expected {}, got {}", 1, first_index);

        let last_index = log.lastindex().expect("Should return last index");
        assert_eq!(last_index, N, "LastIndex: expected {}, got {}", N, last_index);


        // Batch -- test batch writes
        let mut batch = Batch::new();
        // WriteBatch -- should succeed
        log.write_batch(&mut batch).expect("Failed to write batch");

        // Write 100 entries in batches of 10
        for _i in 1..11 {
            for j in 1..11 {
                N += 1;
                batch.write(&data_str(N));
            }
            log.write_batch(&mut batch).expect("Failed to write batch");
        }
        
        // Read -- read back all entries
        for i in 1..N+1 {
            let data = log.read(i).expect("Read: should read entry");
            assert_eq!(
                str::from_utf8(&data).expect("should be valid"),
                data_str(i),
                "Write: expected {}, got {}",
                data_str(i),
                str::from_utf8(&data).expect("should be valid"),
            );
        }

        //@TODO Read -- one random read, so there is an opened reader


        // @TODO TruncateFront -- should fail, out of range

        
    	// TruncateFront -- should fail, out of range
        match log.truncate_front(0) {
            Err(Error::OutOfRange) => {},
            _ => panic!("TruncateFront: Expected OutOfRange error")
        };
        test_first_last(&log,1, N);

        // TruncateFront -- Remove no entries
        match log.truncate_front(1) {
            // Err(Error::OutOfRange) => {},
            Ok(()) => {}
            Err(e) => panic!(format!("TruncateFront Error: {}", e))
        };
        test_first_last(&log, 1, N);
        // TruncateFront -- Remove first 80 entries
        match log.truncate_front(81) {
            // Err(Error::OutOfRange) => {},
            Ok(()) => {},
            _ => panic!("TruncateFront: Expected OutOfRange error")
        };
        test_first_last(&log, 81, N);

        //@TODO Write -- one entry, so the buffer might be activated
        //@TODO Read -- one random read, so there is an opened reader

        // TruncateBack -- should fail, out of range
        //@TODO TruncateBack -- should fail, out of range

        // TruncateBack -- Remove no entries
        println!("N = {}", N);
        match log.truncate_back(N) {
            Ok(()) => {},
            Err(e) => panic!(format!("TruncateBackN: {}", e))
        };
        test_first_last(&log, 81, N);

        // TruncateBack -- Remove last 80 entries
        match log.truncate_back(N - 80) {
            Ok(()) => {},
            Err(e) => panic!(format!("TruncateBack80: {}", e))
        };
        N -= 80;
        test_first_last(&log, 81, N);

        // Close -- close log after truncating
        log.close().expect("Should close file");

        // Open -- open log after truncating
        let mut log = Log::open(path, None).expect("should re-open log after truncating");
        test_first_last(&log, 81, N);

        // Read -- read back all entries
        for i in 81..N+1 {
            let data = log.read(i).expect("Read: should read entry");
            assert_eq!(
                str::from_utf8(&data).expect("should be valid"),
                data_str(i),
                "Write: expected {}, got {}",
                data_str(i),
                str::from_utf8(&data).expect("should be valid"),
            );
        }

        // TruncateFront -- truncate all entries but one
        log.truncate_front(N).expect("TruncateFront: all entries but one");
        test_first_last(&log, N, N);

        // Write --- write on entry
        // println!("ppppppp");
        log.write(N+1, data_str(N+1)).expect("Write: should write on entry");
        N += 1;
        test_first_last(&log, N-1, N);

       // TruncateBack -- truncate all entries but one
        match log.truncate_back(N - 1) {
            Ok(()) => {},
            Err(e) => panic!(format!("TruncateBack1: {}", e))
        };
        N -= 1;
        test_first_last(&log, N, N);

        // log.sync();
        // Write again
        log.write(N+1, data_str(N+1)).expect("Write: should write on entry");
        N += 1;
        // sync
        log.sync();

        test_first_last(&log, N - 1 , N);

    }

    fn make_options(segment_size: u64, durability: Durability, log_format: LogFormat) -> Options {
        Options {
            segment_size: segment_size as usize,
            durability,
            log_format
        }
    }
}