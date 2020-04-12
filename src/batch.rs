#[derive(Debug, Clone)]
pub struct Batch {
    pub indexes: Vec<u64>,
    pub data_sizes: Vec<usize>,
    pub datas: Vec<u8>
}

impl Batch {
    pub fn new() -> Batch {
        Batch {
            indexes: Vec::new(),
            data_sizes: Vec::new(),
            datas: Vec::new(),
        }
    }
    
    pub fn write(&mut self, index: u64, data: Vec<u8>) {
        self.indexes.push(index);
        self.data_sizes.push(data.len());
        self.datas.extend(&data)
    }

    pub fn clear(&mut self) {
        self.indexes = Vec::new();
        self.datas = Vec::new();
        self.data_sizes = Vec::new();
    }
}
