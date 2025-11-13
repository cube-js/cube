use super::CalendarDimensionTimeShift;

/// Represents a calendar dimension with time shift capabilities
#[derive(Clone)]
pub struct CalendarDimension {
    time_shift: Vec<CalendarDimensionTimeShift>,
    time_shift_pk_full_name: Option<String>,
    is_self_time_shift_pk: bool,
}

impl CalendarDimension {
    pub fn new(
        time_shift: Vec<CalendarDimensionTimeShift>,
        time_shift_pk_full_name: Option<String>,
        is_self_time_shift_pk: bool,
    ) -> Self {
        Self {
            time_shift,
            time_shift_pk_full_name,
            is_self_time_shift_pk,
        }
    }

    pub fn time_shift(&self) -> &Vec<CalendarDimensionTimeShift> {
        &self.time_shift
    }

    pub fn time_shift_pk_full_name(&self) -> Option<String> {
        self.time_shift_pk_full_name.clone()
    }

    pub fn is_self_time_shift_pk(&self) -> bool {
        self.is_self_time_shift_pk
    }
}