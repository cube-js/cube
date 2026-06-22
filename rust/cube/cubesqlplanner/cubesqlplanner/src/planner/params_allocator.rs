use crate::cube_bridge::base_query_options::FilterValue;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::{Captures, Regex};
use std::collections::HashMap;

//const PARAMS_MATCH_REGEXP = /\$(\d+)\$/g;
lazy_static! {
    static ref PARAMS_MATCH_RE: Regex = Regex::new(r"\$_(\d+)_\$").unwrap();
}
pub struct ParamsAllocator {
    params: Vec<FilterValue>,
    export_annotated_sql: bool,
}

impl ParamsAllocator {
    pub fn new(export_annotated_sql: bool) -> ParamsAllocator {
        ParamsAllocator {
            params: Vec::new(),
            export_annotated_sql,
        }
    }

    pub fn make_placeholder(&self, index: usize) -> String {
        format!("$_{}_$", index)
    }

    pub fn allocate_param(&mut self, name: &str) -> String {
        // Params allocated during Rust-side planning are already normalized to a
        // string at the call site, so they enter the channel as `FilterValue::Str`.
        // Native params (from the JS side) join later in `build_sql_and_params`
        // and keep their natural type, including `Null`.
        self.params.push(FilterValue::Str(name.to_string()));
        self.make_placeholder(self.params.len() - 1)
    }

    pub fn get_params(&self) -> &Vec<FilterValue> {
        &self.params
    }

    pub fn build_sql_and_params(
        &self,
        sql: &str,
        native_allocated_params: Vec<FilterValue>,
        should_reuse_params: bool,
        templates: &PlanSqlTemplates,
    ) -> Result<(String, Vec<FilterValue>), CubeError> {
        let (sql, params) = self.add_native_allocated_params(sql, &native_allocated_params)?;

        let mut params_in_sql_order = Vec::new();
        let mut param_index_map: HashMap<usize, usize> = HashMap::new();
        let mut error = None;

        let result_sql = if should_reuse_params {
            PARAMS_MATCH_RE
                .replace_all(&sql, |caps: &Captures| {
                    let ind: usize = caps[1].to_string().parse().unwrap();
                    let new_index = if let Some(index) = param_index_map.get(&ind) {
                        index.clone()
                    } else {
                        let index = params_in_sql_order.len();
                        params_in_sql_order.push(params[ind].clone());
                        param_index_map.insert(ind, index);
                        index
                    };
                    if self.export_annotated_sql {
                        format!("${}$", new_index)
                    } else {
                        match templates.param(new_index) {
                            Ok(res) => res,
                            Err(e) => {
                                if error.is_none() {
                                    error = Some(e);
                                }
                                "$error$".to_string()
                            }
                        }
                    }
                })
                .to_string()
        } else {
            PARAMS_MATCH_RE
                .replace_all(&sql, |caps: &Captures| {
                    let ind: usize = caps[1].to_string().parse().unwrap();
                    let index = params_in_sql_order.len();
                    params_in_sql_order.push(params[ind].clone());
                    match templates.param(index) {
                        Ok(res) => res,
                        Err(e) => {
                            if error.is_none() {
                                error = Some(e);
                            }
                            "$error$".to_string()
                        }
                    }
                })
                .to_string()
        };
        if let Some(error) = error {
            return Err(error);
        }
        Ok((result_sql, params_in_sql_order))
    }

    fn add_native_allocated_params(
        &self,
        sql: &str,
        native_allocated_params: &[FilterValue],
    ) -> Result<(String, Vec<FilterValue>), CubeError> {
        lazy_static! {
            static ref NATIVE_PARAMS_MATCH_RE: Regex = Regex::new(r"\$(\d+)\$").unwrap();
        }

        if native_allocated_params.is_empty() {
            Ok((sql.to_string(), self.params.clone()))
        } else {
            let mut result_params = self.params.clone();
            let sql = NATIVE_PARAMS_MATCH_RE
                .replace_all(sql, |caps: &Captures| {
                    let ind: usize = caps[1].to_string().parse().unwrap();
                    let param = native_allocated_params[ind].clone();
                    result_params.push(param);
                    self.make_placeholder(result_params.len() - 1)
                })
                .to_string();
            Ok((sql, result_params))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocated_params_enter_channel_as_str() {
        let mut allocator = ParamsAllocator::new(false);
        let p0 = allocator.allocate_param("alpha");
        let p1 = allocator.allocate_param("beta");

        assert_eq!(p0, "$_0_$");
        assert_eq!(p1, "$_1_$");
        assert_eq!(
            allocator.get_params(),
            &vec![
                FilterValue::Str("alpha".to_string()),
                FilterValue::Str("beta".to_string()),
            ]
        );
    }

    #[test]
    fn native_params_keep_natural_type_including_null() {
        let mut allocator = ParamsAllocator::new(false);
        allocator.allocate_param("internal_a"); // $_0_$
        allocator.allocate_param("internal_b"); // $_1_$

        // Native placeholders use the single-`$` form; a `null` securityContext
        // field, a number, and a boolean must survive the merge with their type
        // intact — `Null` in particular must NOT collapse to an empty string.
        let sql = "SELECT $_0_$, $_1_$, $0$, $1$, $2$";
        let native = vec![
            FilterValue::Null,
            FilterValue::Num(42.0),
            FilterValue::Bool(true),
        ];

        let (rewritten_sql, params) = allocator
            .add_native_allocated_params(sql, &native)
            .expect("merge should succeed");

        // Native placeholders are rewritten into the internal `$_N_$` space,
        // appended after the two internal params.
        assert_eq!(rewritten_sql, "SELECT $_0_$, $_1_$, $_2_$, $_3_$, $_4_$");
        assert_eq!(
            params,
            vec![
                FilterValue::Str("internal_a".to_string()),
                FilterValue::Str("internal_b".to_string()),
                FilterValue::Null,
                FilterValue::Num(42.0),
                FilterValue::Bool(true),
            ]
        );
    }
}
