use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::{Captures, Regex};
use std::collections::HashMap;

//const PARAMS_MATCH_REGEXP = /\$(\d+)\$/g;
lazy_static! {
    static ref PARAMS_MATCH_RE: Regex = Regex::new(r"\$(\d+)\$").unwrap();
}
pub struct ParamsAllocator {
    params: Vec<String>,
}

impl ParamsAllocator {
    pub fn new() -> ParamsAllocator {
        ParamsAllocator { params: Vec::new() }
    }

    pub fn allocate_param(&mut self, name: &str) -> usize {
        self.params.push(name.to_string());
        self.params.len() - 1
    }

    pub fn get_params(&self) -> &Vec<String> {
        &self.params
    }

    pub fn build_sql_and_params(
        &self,
        sql: &str,
        should_reuse_params: bool,
    ) -> Result<(String, Vec<String>), CubeError> {
        let mut params_in_sql_order = Vec::new();
        let mut param_index_map: HashMap<usize, usize> = HashMap::new();
        let result_sql = if should_reuse_params {
            PARAMS_MATCH_RE
                .replace_all(sql, |caps: &Captures| {
                    let ind: usize = caps[1].to_string().parse().unwrap();
                    let new_index = if let Some(index) = param_index_map.get(&ind) {
                        index.clone()
                    } else {
                        params_in_sql_order.push(self.params[ind].clone());
                        let index = params_in_sql_order.len();
                        param_index_map.insert(ind, index);
                        index
                    };
                    format!("${}", new_index) //TODO get placeholder from js part
                })
                .to_string()
        } else {
            PARAMS_MATCH_RE
                .replace_all(sql, |caps: &Captures| {
                    let ind: usize = caps[1].to_string().parse().unwrap();
                    params_in_sql_order.push(self.params[ind].clone());
                    let index = params_in_sql_order.len();
                    format!("${}", index) //TODO get placeholder from js part
                })
                .to_string()
        };
        Ok((result_sql, params_in_sql_order))
    }
}
