pub struct ParamsAllocator {
    params: Vec<String>,
}

impl ParamsAllocator {
    pub fn new() -> ParamsAllocator {
        ParamsAllocator { params: Vec::new() }
    }

    pub fn allocate_param(&mut self, name: &str) -> usize {
        self.params.push(name.to_string());
        self.params.len()
    }

    pub fn get_params(&self) -> &Vec<String> {
        &self.params
    }
}
