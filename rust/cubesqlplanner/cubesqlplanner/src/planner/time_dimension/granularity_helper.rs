use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::planner::BaseTimeDimension;
use crate::planner::Granularity;
use chrono::prelude::*;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use itertools::Itertools;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::rc::Rc;

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

    pub fn find_dimension_with_min_granularity(
        dimensions: &Vec<Rc<BaseTimeDimension>>,
    ) -> Result<Rc<BaseTimeDimension>, CubeError> {
        if dimensions.is_empty() {
            return Err(CubeError::internal(
                "No dimensions provided for find_dimension_with_min_granularity".to_string(),
            ));
        }
        let first = Ok(dimensions[0].clone());
        dimensions.iter().skip(1).fold(first, |acc, d| match acc {
            Ok(min_dim) => {
                let min_granularity =
                    Self::min_granularity(&min_dim.get_granularity(), &d.get_granularity())?;
                if min_granularity == min_dim.get_granularity() {
                    Ok(min_dim)
                } else {
                    Ok(d.clone())
                }
            }
            Err(e) => Err(e),
        })
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

    pub fn is_predefined_granularity(granularity: &str) -> bool {
        Self::standard_granularity_parents().contains_key(granularity)
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

    pub fn parse_date_time_in_tz(date: &str, timezone: &Tz) -> Result<DateTime<Tz>, CubeError> {
        let local_dt = Self::parse_date_time(date)?;
        if let Some(result) = timezone.from_local_datetime(&local_dt).single() {
            Ok(result)
        } else {
            Err(CubeError::user(format!(
                "Error while parsing date `{date}` in timezone `{timezone}`"
            )))
        }
    }

    pub fn parse_date_time(date: &str) -> Result<NaiveDateTime, CubeError> {
        let formats = &[
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
        ];

        for format in formats {
            if let Ok(dt) = NaiveDateTime::parse_from_str(date, format) {
                return Ok(dt);
            }
        }

        if let Ok(d) = NaiveDate::parse_from_str(date, "%Y-%m-%d") {
            return Ok(d.and_hms_opt(0, 0, 0).unwrap());
        }

        Err(CubeError::user(format!("Can't parse date: '{}'", date)))
    }

    pub fn make_granularity_obj(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        timezone: Tz,
        cube_name: &String,
        name: &String,
        granularity: Option<String>,
    ) -> Result<Option<Granularity>, CubeError> {
        let granularity_obj = if let Some(granularity) = &granularity {
            if !Self::is_predefined_granularity(&granularity) {
                let path = vec![
                    cube_name.clone(),
                    name.clone(),
                    "granularities".to_string(),
                    granularity.clone(),
                ];
                let granularity_definition = cube_evaluator.resolve_granularity(path)?;
                Some(Granularity::try_new_custom(
                    timezone.clone(),
                    granularity.clone(),
                    granularity_definition.origin,
                    granularity_definition.interval,
                    granularity_definition.offset,
                )?)
            } else {
                Some(Granularity::try_new_predefined(
                    timezone.clone(),
                    granularity.clone(),
                )?)
            }
        } else {
            None
        };
        Ok(granularity_obj)
    }
}
