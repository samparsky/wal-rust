// use wal::primitives::*;
// use wal::Log;
// use std::fs;

// #[test]
// fn test_log() {
//     // unimplemented!();
//     let path = "testlog/log";
//     let log = Log::open(path, None).expect("should open log");

//     // FirstIndex - should be zero
//     let first_index = log.firstindex().expect("should return first index");
//     assert_eq!(first_index, 0, "FirstIndex: expected {}, got {}", 0, first_index);

//     // LastIndex
//     let last_index = log.lastindex().expect("Should return last index");
//     assert_eq!(last_index, 0, "LastIndex: expected {}, got {}", 0, first_index);


//     fs::remove_dir_all(path).expect("should remove dir");
// }