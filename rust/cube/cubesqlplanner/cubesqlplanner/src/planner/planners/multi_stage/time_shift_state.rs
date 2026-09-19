use crate::planner::collectors::find_owned_by_cube_child;
use crate::planner::filter::typed_filter::resolve_base_symbol;
use crate::planner::symbols::CalendarDimensionTimeShift;
use crate::planner::symbols::DimensionSymbol;
use crate::planner::symbols::MemberSymbol;
use crate::planner::DimensionTimeShift;
use crate::planner::SqlInterval;
use cubenativeutils::CubeError;
use itertools::Itertools;
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

    /// Resolves the accumulated shifts into the three forms the render
    /// pipeline needs: interval shifts applied by offsetting the member's
    /// expression, calendar shifts that render the calendar cube's own
    /// declaration, and what a `FILTER_PARAMS` binding may restate under
    /// either. A named shift that does not match a calendar declaration is an
    /// error.
    pub fn extract_time_shifts(&self) -> Result<ExtractedTimeShifts, CubeError> {
        let mut extracted = ExtractedTimeShifts::default();

        // Sorted, so that two dimensions of one calendar cube resolving to the
        // same key always leave the same one standing: a `HashMap`'s order
        // would otherwise make the SQL differ between runs of one query.
        for (key, shift) in self.dimensions_shifts.iter().sorted_by_key(|(key, _)| *key) {
            if let Ok(dimension) = shift.dimension.as_dimension() {
                // 1. Shift might be referenced by name or by interval
                // 2. Shift body might be defined in calendar dimension as:
                //      * sql reference
                //      * interval + type

                if let Some(dim_shift_name) = &shift.name {
                    if let Some((dim_key, cts)) =
                        dimension.calendar_time_shift_for_named_interval(dim_shift_name)
                    {
                        extracted.add_calendar_shift(&dimension, &dim_key, Some(dim_shift_name));
                        extracted.calendar_shifts.insert(dim_key, cts);
                    } else if let Some(_calendar_pk) = dimension.time_shift_pk_full_name() {
                        return Err(CubeError::user(format!(
                            "Time shift with name {} not found for dimension {}",
                            dim_shift_name,
                            dimension.full_name()
                        )));
                    }
                } else if let Some(dim_shift_interval) = &shift.interval {
                    if let Some((dim_key, cts)) =
                        dimension.calendar_time_shift_for_interval(dim_shift_interval)
                    {
                        extracted.add_calendar_shift(&dimension, &dim_key, cts.name.as_ref());
                        extracted.calendar_shifts.insert(dim_key, cts);
                    } else if let Some(calendar_pk) = dimension.time_shift_pk_full_name() {
                        // Interval arithmetic straight on the calendar's primary
                        // key, bypassing its mapping. The rows still arrive
                        // through the shifted join, so a binding restating the
                        // reporting bounds would cut them off.
                        extracted.add_calendar_shift(&dimension, &calendar_pk, None);
                        let mut shift = shift.clone();
                        shift.interval = Some(dim_shift_interval.inverse());
                        extracted.interval_shifts.insert(calendar_pk, shift);
                    } else {
                        extracted
                            .filter_params_shifts
                            .add_interval(key.clone(), dim_shift_interval.clone());
                        extracted.interval_shifts.insert(key.clone(), shift.clone());
                    }
                }
            } else {
                if let Some(interval) = &shift.interval {
                    extracted
                        .filter_params_shifts
                        .add_interval(key.clone(), interval.clone());
                }
                extracted.interval_shifts.insert(key.clone(), shift.clone());
            }
        }

        Ok(extracted)
    }
}

/// The shifts of one stage, resolved into the forms the render pipeline
/// consumes.
#[derive(Default)]
pub struct ExtractedTimeShifts {
    /// Shifts applied by offsetting the member's expression by an interval.
    pub interval_shifts: HashMap<String, DimensionTimeShift>,
    /// Shifts that render a calendar cube's own time-shift declaration,
    /// keyed by the calendar's primary-key full name.
    pub calendar_shifts: HashMap<String, CalendarDimensionTimeShift>,
    /// What a `FILTER_PARAMS` binding may restate under the stage's shifts.
    pub filter_params_shifts: FilterParamsTimeShifts,
}

impl ExtractedTimeShifts {
    // Recorded per cube, not per member: the shift moves the primary key the
    // fact table joins to, so it moves every row the stage reads. Both the
    // asked-for dimension's cube and the primary key's are kept, since a view
    // can re-export the one without the other.
    fn add_calendar_shift(
        &mut self,
        dimension: &Rc<DimensionSymbol>,
        pk_full_name: &str,
        name: Option<&String>,
    ) {
        let shift = CalendarShift {
            name: name.cloned(),
            declared_names: dimension
                .time_shift()
                .iter()
                .filter_map(|declared| declared.name.clone())
                .collect(),
        };
        self.filter_params_shifts
            .add_calendar_cube(dimension.cube_name().clone(), shift.clone());
        if let Some((pk_cube, _)) = pk_full_name.split_once('.') {
            self.filter_params_shifts
                .add_calendar_cube(pk_cube.to_string(), shift);
        }
    }
}

/// What a `FILTER_PARAMS` binding can state about the time shift active on the
/// member it names. An interval shift is carried by offsetting the column, so
/// the plain binding renders shifted; a calendar shift is a mapping in the
/// calendar's own table that no expression over the column reproduces, so only
/// a binding naming it renders.
#[derive(Clone, Debug, PartialEq)]
pub enum FilterParamsTimeShift {
    Interval(SqlInterval),
    Calendar(CalendarShift),
}

/// The calendar shift a stage applies, as a `FILTER_PARAMS` binding sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct CalendarShift {
    /// The name a binding addresses this shift by; `None` when the calendar
    /// declaration has none, leaving the shift unaddressable.
    pub name: Option<String>,
    /// Every shift name the calendar declares, so a binding on one of the
    /// cube's other members is checked against the calendar rather than against
    /// the member it binds.
    pub declared_names: Vec<String>,
}

/// The stage's time shifts as `FILTER_PARAMS` rendering sees them.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct FilterParamsTimeShifts {
    interval_shifts: HashMap<String, SqlInterval>,
    calendar_cubes: HashMap<String, CalendarShift>,
}

impl FilterParamsTimeShifts {
    fn add_interval(&mut self, key: String, interval: SqlInterval) {
        self.interval_shifts.insert(key, interval);
    }

    fn add_calendar_cube(&mut self, cube_name: String, shift: CalendarShift) {
        self.calendar_cubes.insert(cube_name, shift);
    }

    /// The shift a `FILTER_PARAMS` binding on this member has to account for.
    pub fn get_for_symbol(&self, symbol: &Rc<MemberSymbol>) -> Option<FilterParamsTimeShift> {
        let resolved = resolve_base_symbol(symbol).resolve_reference_chain();
        if let Some(shift) = self.calendar_cubes.get(&resolved.cube_name()) {
            return Some(FilterParamsTimeShift::Calendar(shift.clone()));
        }
        if let Some(interval) = self.interval_shifts.get(&resolved.full_name()) {
            return Some(FilterParamsTimeShift::Interval(interval.clone()));
        }
        let owned = find_owned_by_cube_child(&resolved).ok()?;
        self.interval_shifts
            .get(&owned.full_name())
            .map(|interval| FilterParamsTimeShift::Interval(interval.clone()))
    }
}
