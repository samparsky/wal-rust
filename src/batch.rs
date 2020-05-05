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
    
    pub fn write<D: AsRef<[u8]>>(&mut self, data: D) {
        self.data_sizes.push(data.as_ref().len());
        self.datas.extend(data.as_ref())
    }

    pub fn clear(&mut self) {
        self.datas.clear();
        self.data_sizes.clear();
    }
}
