use cubenativeutils::CubeError;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::collections::HashMap;

pub struct GranularityHelper {}

impl GranularityHelper {
    pub fn min_granularity(
        granularity_a: &Option<String>,
        granularity_b: &Option<String>,
    ) -> Result<Option<String>, CubeError> {
        if let Some((granularity_a, granularity_b)) =
            granularity_a.as_ref().zip(granularity_b.as_ref())
        {
            let a_parents = Self::granularity_parents(granularity_a)?;
            let b_parents = Self::granularity_parents(granularity_b)?;
            let diff_position = a_parents
                .iter()
                .zip(b_parents.iter())
                .find_position(|(a, b)| a != b);
            if let Some((diff_position, _)) = diff_position {
                if diff_position == 0 {
                    Err(CubeError::user(format!(
                        "Can't find common parent for '{granularity_a}' and '{granularity_b}'"
                    )))
                } else {
                    Ok(Some(a_parents[diff_position - 1].clone()))
                }
            } else {
                if a_parents.len() >= b_parents.len() {
                    Ok(Some(b_parents.last().unwrap().clone()))
                } else {
                    Ok(Some(a_parents.last().unwrap().clone()))
                }
            }
        } else if granularity_a.is_some() {
            Ok(granularity_a.clone())
        } else {
            Ok(granularity_b.clone())
        }
    }

    pub fn granularity_from_interval(interval: &Option<String>) -> Option<String> {
        if let Some(interval) = interval {
            if interval.find("day").is_some() {
                Some("day".to_string())
            } else if interval.find("month").is_some() {
                Some("month".to_string())
            } else if interval.find("year").is_some() {
                Some("year".to_string())
            } else if interval.find("week").is_some() {
                Some("week".to_string())
            } else if interval.find("hour").is_some() {
                Some("hour".to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn granularity_parents(granularity: &str) -> Result<&Vec<String>, CubeError> {
        if let Some(parents) = Self::standard_granularity_parents().get(granularity) {
            Ok(parents)
        } else {
            Err(CubeError::user(format!(
                "Granularity {} not found",
                granularity
            )))
        }
    }

    pub fn standard_granularity_parents() -> &'static HashMap<String, Vec<String>> {
        lazy_static! {
            static ref STANDARD_GRANULARITIES_PARENTS: HashMap<String, Vec<String>> = {
                let mut map = HashMap::new();
                map.insert(
                    "year".to_string(),
                    vec![
                        "second".to_string(),
                        "minute".to_string(),
                        "hour".to_string(),
                        "day".to_string(),
                        "month".to_string(),
                        "quarter".to_string(),
                        "year".to_string(),
                    ],
                );
                map.insert(
                    "quarter".to_string(),
                    vec![
                        "second".to_string(),
                        "minute".to_string(),
                        "hour".to_string(),
                        "day".to_string(),
                        "month".to_string(),
                        "quarter".to_string(),
                    ],
                );
                map.insert(
                    "month".to_string(),
                    vec![
                        "second".to_string(),
                        "minute".to_string(),
                        "hour".to_string(),
                        "day".to_string(),
                        "month".to_string(),
                    ],
                );
                map.insert(
                    "week".to_string(),
                    vec![
                        "second".to_string(),
                        "minute".to_string(),
                        "hour".to_string(),
                        "day".to_string(),
                        "week".to_string(),
                    ],
                );
                map.insert(
                    "day".to_string(),
                    vec![
                        "second".to_string(),
                        "minute".to_string(),
                        "hour".to_string(),
                        "day".to_string(),
                    ],
                );
                map.insert(
                    "hour".to_string(),
                    vec![
                        "second".to_string(),
                        "minute".to_string(),
                        "hour".to_string(),
                    ],
                );
                map.insert(
                    "minute".to_string(),
                    vec!["second".to_string(), "minute".to_string()],
                );
                map.insert("second".to_string(), vec!["second".to_string()]);
                map
            };
        }
        &STANDARD_GRANULARITIES_PARENTS
    }
}
