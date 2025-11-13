use super::{CalendarDimensionTimeShift, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

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

    pub fn get_dependencies(&self, deps: &mut Vec<Rc<MemberSymbol>>) {
        for shift in &self.time_shift {
            if let Some(sql) = &shift.sql {
                sql.extract_symbol_deps(deps);
            }
        }
    }

    pub fn get_dependencies_with_path(&self, deps: &mut Vec<(Rc<MemberSymbol>, Vec<String>)>) {
        for shift in &self.time_shift {
            if let Some(sql) = &shift.sql {
                sql.extract_symbol_deps_with_path(deps);
            }
        }
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let time_shift = self
            .time_shift
            .iter()
            .map(|shift| -> Result<_, CubeError> {
                Ok(CalendarDimensionTimeShift {
                    interval: shift.interval.clone(),
                    name: shift.name.clone(),
                    sql: if let Some(sql) = &shift.sql {
                        Some(sql.apply_recursive(f)?)
                    } else {
                        None
                    },
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            time_shift,
            time_shift_pk_full_name: self.time_shift_pk_full_name.clone(),
            is_self_time_shift_pk: self.is_self_time_shift_pk,
        })
    }
}