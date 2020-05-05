use wal::Log;
use wal::error::*;
use wal::batch::*;
use std::fs;
use std::str;

fn data_str(i: u64) -> String {
    format!("data-{}", i)
}


fn test_log() {
    let mut N: u64 = 10;
    // unimplemented!();
    let path = "testlog/log";
    let mut log = Log::open(path, None).expect("should open log");

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

        // Write - get next item
        let data = log.read(i).expect("Write: should read item");

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
        let data = log.read(i).expect("Write: should read item");

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


    // TruncateFront -- should fail, out of range
    
    // TruncateBack -- should fail, out of range

    fs::remove_dir_all("testlog").expect("should remove dir");
}

fn main() {
    test_log();
}
