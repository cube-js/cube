use crate::planner::collectors::find_owned_by_cube_child;
use crate::planner::filter::typed_filter::resolve_base_symbol;
use crate::planner::symbols::CalendarDimensionTimeShift;
use crate::planner::symbols::MemberSymbol;
use crate::planner::DimensionTimeShift;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Per-dimension time-shift accumulator used during multi-stage
/// planning. Keyed by dimension full name; aggregates the shifts
/// applied to each dimension across nested multi-stage scopes.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TimeShiftState {
    pub dimensions_shifts: HashMap<String, DimensionTimeShift>,
}

impl TimeShiftState {
    pub fn is_empty(&self) -> bool {
        self.dimensions_shifts.is_empty()
    }

    /// Looks up the shift for a symbol that may still be wrapped in a
    /// `TimeDimension`, be a reference to the shifted member, or wrap it in
    /// its own SQL. Keys are built either from the chain-resolved dimension
    /// or, for dimension-specific shifts, from the owned member the declared
    /// dimension wraps, so both forms are probed.
    pub fn get_for_symbol(&self, symbol: &Rc<MemberSymbol>) -> Option<&DimensionTimeShift> {
        let resolved = resolve_base_symbol(symbol).resolve_reference_chain();
        if let Some(shift) = self.dimensions_shifts.get(&resolved.full_name()) {
            return Some(shift);
        }
        let owned = find_owned_by_cube_child(&resolved).ok()?;
        self.dimensions_shifts.get(&owned.full_name())
    }

    /// The shift a stored column standing for this member can carry.
    ///
    /// A column is shifted by offsetting it, which only stands in for the
    /// shifted member when the member is a time dimension evaluated in place:
    /// a reference is rendered through to what it points at, and a non-time
    /// member has no meaning under an interval. Both the gate that admits a
    /// pre-aggregation and the node that renders from one ask this, so the two
    /// cannot come to different conclusions.
    pub fn shift_for_substituted_column(
        &self,
        symbol: &Rc<MemberSymbol>,
    ) -> Option<&DimensionTimeShift> {
        let dimension = resolve_base_symbol(symbol).as_dimension().ok()?;
        if dimension.is_reference() || !dimension.is_time() {
            return None;
        }
        self.get_for_symbol(symbol)
    }

    /// True when the symbol itself, or any member it is built from, is
    /// shifted. Unlike `get_for_symbol` this answers whether a shift is
    /// involved at all, not whether one can be attributed to the symbol.
    pub fn has_shift_under(&self, symbol: &Rc<MemberSymbol>) -> bool {
        let symbol = resolve_base_symbol(symbol);
        if self.dimensions_shifts.contains_key(&symbol.full_name()) {
            return true;
        }
        symbol
            .get_dependencies()
            .iter()
            .any(|dep| self.has_shift_under(dep))
    }

    /// Splits the accumulated shifts into two maps: regular
    /// `DimensionTimeShift`s applied at render time, and
    /// `CalendarDimensionTimeShift`s that come from a calendar
    /// cube's own time-shift declarations. A named shift that does
    /// not match a calendar declaration is an error.
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
