use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::planner::sql_evaluator::Compiler;
use crate::planner::sql_evaluator::TimeDimensionSymbol;
use crate::planner::{Granularity, QueryDateTimeHelper};
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
        dimensions: &Vec<Rc<TimeDimensionSymbol>>,
    ) -> Result<Rc<TimeDimensionSymbol>, CubeError> {
        if dimensions.is_empty() {
            return Err(CubeError::internal(
                "No dimensions provided for find_dimension_with_min_granularity".to_string(),
            ));
        }
        let first = Ok(dimensions[0].clone());
        dimensions
            .iter()
            .skip(1)
            .fold(first, |acc, d| -> Result<_, CubeError> {
                match acc {
                    Ok(min_dim) => {
                        // TODO: Add support for custom granularities comparison
                        let min_granularity = Self::min_granularity(
                            &min_dim.resolved_granularity()?,
                            &d.resolved_granularity()?,
                        )?;
                        if &min_granularity == min_dim.granularity() {
                            Ok(min_dim)
                        } else {
                            Ok(d.clone())
                        }
                    }
                    Err(e) => Err(e),
                }
            })
    }

    pub fn granularity_from_interval(interval: &Option<String>) -> Option<String> {
        if let Some(interval) = interval {
            if interval.contains("second") {
                Some("second".to_string())
            } else if interval.contains("minute") {
                Some("minute".to_string())
            } else if interval.contains("hour") {
                Some("hour".to_string())
            } else if interval.contains("day") {
                Some("day".to_string())
            } else if interval.contains("week") {
                Some("day".to_string())
            } else if interval.contains("month") {
                Some("month".to_string())
            } else if interval.contains("quarter") {
                Some("month".to_string())
            } else if interval.contains("year") {
                Some("year".to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn granularity_parents(granularity: &str) -> Result<&Vec<String>, CubeError> {
        if let Some(parents) = Self::standard_granularity_hierarchy().get(granularity) {
            Ok(parents)
        } else {
            Err(CubeError::user(format!(
                "Granularity {} not found",
                granularity
            )))
        }
    }

    pub fn is_predefined_granularity(granularity: &str) -> bool {
        Self::standard_granularity_hierarchy().contains_key(granularity)
    }

    pub fn standard_granularity_hierarchy() -> &'static HashMap<String, Vec<String>> {
        lazy_static! {
            static ref STANDARD_GRANULARITY_HIERARCHIES: HashMap<String, Vec<String>> = {
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
        &STANDARD_GRANULARITY_HIERARCHIES
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
        QueryDateTimeHelper::parse_native_date_time(date)
    }

    pub fn make_granularity_obj(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        compiler: &mut Compiler,
        cube_name: &String,
        name: &String,
        granularity: Option<String>,
    ) -> Result<Option<Granularity>, CubeError> {
        let timezone = compiler.timezone();
        let granularity_obj = if let Some(granularity) = &granularity {
            let path = vec![
                cube_name.clone(),
                name.clone(),
                "granularities".to_string(),
                granularity.clone(),
            ];
            let granularity_definition = cube_evaluator.resolve_granularity(path)?;
            let gran_eval_sql = if let Some(gran_sql) = granularity_definition.sql()? {
                Some(compiler.compile_sql_call(&cube_name, gran_sql)?)
            } else {
                None
            };

            if gran_eval_sql.is_some() || !Self::is_predefined_granularity(&granularity) {
                Some(Granularity::try_new_custom(
                    timezone.clone(),
                    granularity.clone(),
                    granularity_definition.static_data().origin.clone(),
                    granularity_definition.static_data().interval.clone(),
                    granularity_definition.static_data().offset.clone(),
                    gran_eval_sql,
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

    // Returns the granularity hierarchy for a td granularity.
    // Note: for custom granularities, returns [...standard_hierarchy_for_min_granularity, granularity_name].
    // custom granularity is at the end of the array, in BaseQuery.js it's first.
    pub fn time_dimension_granularity_hierarchy(
        time_dimension: (&Option<String>, &TimeDimensionSymbol),
    ) -> Result<Vec<String>, CubeError> {
        let granularity = time_dimension.0.clone();

        if let Some(granularity_name) = granularity {
            if Self::is_predefined_granularity(&granularity_name) {
                Ok(Self::granularity_parents(&granularity_name)?.clone())
            } else {
                if let Some(granularity_obj) = time_dimension.1.granularity_obj() {
                    let min_granularity = granularity_obj.min_granularity()?;

                    if let Some(min_gran) = min_granularity {
                        let mut standard_hierarchy = Self::granularity_parents(&min_gran)?.clone();
                        let custom = vec![granularity_name.clone()];
                        standard_hierarchy.extend(custom.clone());
                        Ok(standard_hierarchy)
                    } else {
                        // Safeguard: if no min_granularity, just return the custom granularity name
                        Ok(vec![granularity_name.clone()])
                    }
                } else {
                    // No granularity object but has a name - shouldn't happen, but handle gracefully
                    Err(CubeError::internal(format!(
                        "Time dimension has granularity '{}' but no granularity object",
                        granularity_name
                    )))
                }
            }
        } else {
            Err(CubeError::internal(format!(
                "Time dimension \"{}\" has no granularity specified",
                time_dimension.1.full_name()
            )))
        }
    }

    pub fn min_granularity_for_time_dimensions(
        time_dimension_a: (&Option<String>, &TimeDimensionSymbol),
        time_dimension_b: (&Option<String>, &TimeDimensionSymbol),
    ) -> Result<Option<String>, CubeError> {
        let granularity_a = time_dimension_a.0;
        let granularity_b = time_dimension_b.0;

        if let (Some(gran_a), Some(gran_b)) = (granularity_a.clone(), granularity_b.clone()) {
            let a_hierarchy = Self::time_dimension_granularity_hierarchy(time_dimension_a)?;
            let b_hierarchy = Self::time_dimension_granularity_hierarchy(time_dimension_b)?;

            let diff_position = a_hierarchy
                .iter()
                .zip(b_hierarchy.iter())
                .find_position(|(a, b)| a != b);

            if let Some((diff_position, _)) = diff_position {
                if diff_position == 0 {
                    Err(CubeError::user(format!(
                        "Can't find common parent for '{}' and '{}'",
                        gran_a, gran_b
                    )))
                } else {
                    // Return the granularity before the first difference
                    Ok(Some(a_hierarchy[diff_position - 1].clone()))
                }
            } else {
                // One hierarchy is a prefix of the other or they are identical
                // Return the last element of the shorter hierarchy
                if a_hierarchy.len() >= b_hierarchy.len() {
                    Ok(Some(b_hierarchy.last().unwrap().clone()))
                } else {
                    Ok(Some(a_hierarchy.last().unwrap().clone()))
                }
            }
        } else if granularity_a.is_some() {
            Ok(granularity_a.clone())
        } else if granularity_b.is_some() {
            Ok(granularity_b.clone())
        } else {
            Ok(None)
        }
    }
}
