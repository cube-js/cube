use crate::planner::sql_evaluator::symbols::CalendarDimensionTimeShift;
use crate::planner::sql_evaluator::DimensionTimeShift;
use cubenativeutils::CubeError;
use std::collections::HashMap;

#[derive(Clone, Default, Debug)]
pub struct TimeShiftState {
    pub dimensions_shifts: HashMap<String, DimensionTimeShift>,
}

impl TimeShiftState {
    pub fn is_empty(&self) -> bool {
        self.dimensions_shifts.is_empty()
    }

    pub fn extract_time_shifts(
        &self,
    ) -> Result<
        (
            HashMap<String, DimensionTimeShift>,
            HashMap<String, CalendarDimensionTimeShift>,
        ),
        CubeError,
    > {
        let mut time_shifts = HashMap::new();
        let mut calendar_time_shifts = HashMap::new();

        for (key, shift) in self.dimensions_shifts.iter() {
            if let Ok(dimension) = shift.dimension.as_dimension() {
                // 1. Shift might be referenced by name or by interval
                // 2. Shift body might be defined in calendar dimension as:
                //      * sql reference
                //      * interval + type

                if let Some(dim_shift_name) = &shift.name {
                    if let Some((dim_key, cts)) =
                        dimension.calendar_time_shift_for_named_interval(dim_shift_name)
                    {
                        calendar_time_shifts.insert(dim_key.clone(), cts.clone());
                    } else if let Some(_calendar_pk) = dimension.time_shift_pk_full_name() {
                        return Err(CubeError::internal(format!(
                            "Time shift with name {} not found for dimension {}",
                            dim_shift_name,
                            dimension.full_name()
                        )));
                    }
                } else if let Some(dim_shift_interval) = &shift.interval {
                    if let Some((dim_key, cts)) =
                        dimension.calendar_time_shift_for_interval(dim_shift_interval)
                    {
                        calendar_time_shifts.insert(dim_key.clone(), cts.clone());
                    } else if let Some(calendar_pk) = dimension.time_shift_pk_full_name() {
                        let mut shift = shift.clone();
                        shift.interval = Some(dim_shift_interval.inverse());
                        time_shifts.insert(calendar_pk, shift.clone());
                    } else {
                        time_shifts.insert(key.clone(), shift.clone());
                    }
                }
            } else {
                time_shifts.insert(key.clone(), shift.clone());
            }
        }

        Ok((time_shifts, calendar_time_shifts))
    }
}
