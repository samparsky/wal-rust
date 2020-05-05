#[derive(Debug, Clone)]
pub struct Batch {
    pub data_sizes: Vec<usize>,
    pub datas: Vec<u8>
}

impl Batch {
    pub fn new() -> Batch {
        Batch {
            data_sizes: Vec::new(),
            datas: Vec::new(),
        }
    }
    
    pub fn write(&mut self, data: Vec<u8>) {
        self.data_sizes.push(data.len());
        self.datas.extend(&data)
    }

    pub fn clear(&mut self) {
        self.datas.clear();
        self.data_sizes.clear();
    }
}
