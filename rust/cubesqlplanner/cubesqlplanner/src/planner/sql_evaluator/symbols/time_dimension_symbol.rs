use super::MemberSymbol;
use crate::planner::query_tools::QueryTools;
use crate::planner::time_dimension::Granularity;
use crate::planner::{GranularityHelper, QueryDateTime, QueryDateTimeHelper};
use chrono::Duration;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub struct TimeDimensionSymbol {
    base_symbol: Rc<MemberSymbol>,
    full_name: String,
    granularity: Option<String>,
    granularity_obj: Option<Granularity>,
    date_range: Option<(String, String)>,
    alias_suffix: String,
}

impl TimeDimensionSymbol {
    pub fn new(
        base_symbol: Rc<MemberSymbol>,
        granularity: Option<String>,
        granularity_obj: Option<Granularity>,
        date_range: Option<(String, String)>,
    ) -> Self {
        let name_suffix = if let Some(granularity) = &granularity {
            granularity.clone()
        } else {
            "day".to_string()
        };
        let full_name = format!("{}_{}", base_symbol.full_name(), name_suffix);
        Self {
            base_symbol,
            granularity,
            granularity_obj,
            full_name,
            date_range,
            alias_suffix: name_suffix,
        }
    }

    pub fn base_symbol(&self) -> &Rc<MemberSymbol> {
        &self.base_symbol
    }

    pub fn granularity(&self) -> &Option<String> {
        &self.granularity
    }

    pub fn granularity_obj(&self) -> &Option<Granularity> {
        &self.granularity_obj
    }

    pub fn full_name(&self) -> String {
        self.full_name.clone()
    }

    pub fn alias_suffix(&self) -> String {
        self.alias_suffix.clone()
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        self.base_symbol.get_dependencies()
    }

    pub fn owned_by_cube(&self) -> bool {
        self.base_symbol.owned_by_cube()
    }

    pub fn get_dependencies_as_time_dimensions(&self) -> Vec<Rc<MemberSymbol>> {
        self.get_dependencies()
            .into_iter()
            .map(|s| match s.as_ref() {
                MemberSymbol::Dimension(dimension_symbol) => {
                    if dimension_symbol.dimension_type() == "time" {
                        let result = Self::new(
                            s.clone(),
                            self.granularity.clone(),
                            self.granularity_obj.clone(),
                            self.date_range.clone(),
                        );
                        Rc::new(MemberSymbol::TimeDimension(result))
                    } else {
                        s.clone()
                    }
                }
                _ => s.clone(),
            })
            .collect()
    }

    pub fn get_dependencies_with_path(&self) -> Vec<(Rc<MemberSymbol>, Vec<String>)> {
        self.base_symbol.get_dependencies_with_path()
    }

    pub fn cube_name(&self) -> String {
        self.base_symbol.cube_name()
    }

    pub fn is_multi_stage(&self) -> bool {
        self.base_symbol.is_multi_stage()
    }

    pub fn is_reference(&self) -> bool {
        self.base_symbol.is_reference()
    }

    pub fn name(&self) -> String {
        self.base_symbol.name()
    }

    pub fn date_range_granularity(
        &self,
        query_tools: Rc<QueryTools>,
    ) -> Result<Option<String>, CubeError> {
        if let Some(date_range) = &self.date_range {
            let tz = query_tools.timezone();
            let from_date_str =
                QueryDateTimeHelper::format_from_date(&date_range.0, query_tools.clone())?;
            let to_date_str =
                QueryDateTimeHelper::format_to_date(&date_range.1, query_tools.clone())?;
            let start = QueryDateTime::from_date_str(tz, &from_date_str)?;
            let end = QueryDateTime::from_date_str(tz, &to_date_str)?;
            let end = end.add_duration(Duration::milliseconds(1))?;
            let start_granularity = start.granularity();
            let end_granularity = end.granularity();
            GranularityHelper::min_granularity(&Some(start_granularity), &Some(end_granularity))
        } else {
            Ok(None)
        }
    }

    pub fn rollup_granularity(
        &self,
        query_tools: Rc<QueryTools>,
    ) -> Result<Option<String>, CubeError> {
        if let Some(granularity_obj) = &self.granularity_obj {
            let date_range_granularity = self.date_range_granularity(query_tools.clone())?;
            let self_granularity = granularity_obj.min_granularity()?;

            GranularityHelper::min_granularity(&date_range_granularity, &self_granularity)
        } else {
            let date_range_granularity = self.date_range_granularity(query_tools.clone())?;

            Ok(date_range_granularity)
        }
    }

    pub fn get_range_for_time_series(
        &self,
        date_range: Option<Vec<String>>,
        tz: Tz,
    ) -> Result<Option<(String, String)>, CubeError> {
        let res = if let Some(date_range) = &date_range {
            if date_range.len() != 2 {
                return Err(CubeError::user(format!(
                    "Invalid date range: {:?}",
                    date_range
                )));
            } else {
                if let Some(granularity_obj) = &self.granularity_obj {
                    if !granularity_obj.is_predefined_granularity() {
                        let start = QueryDateTime::from_date_str(tz, &date_range[0])?;
                        let start = granularity_obj.align_date_to_origin(start)?;
                        let end = QueryDateTime::from_date_str(tz, &date_range[1])?;

                        Some((start.to_string(), end.to_string()))
                    } else {
                        Some((date_range[0].clone(), date_range[1].clone()))
                    }
                } else {
                    Some((date_range[0].clone(), date_range[1].clone()))
                }
            }
        } else {
            None
        };
        Ok(res)
    }
}
