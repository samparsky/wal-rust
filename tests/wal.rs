use wal::primitives::*;
use wal::Log;
use std::fs;

#[test]
fn test_log() {
    // unimplemented!();
    let path = "testlog/log";
    let log = Log::open(path, None).expect("should open log");
    
    fs::remove_dir(path).expect("should remove dir");
}