use super::super::LogicalSchema;
use crate::planner::symbols::transforms;
use crate::planner::{MeasureRenderModifier, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Copy of the schema with `modifier` set on every measure that has
/// no render modifier yet. Every schema member is rewritten, so
/// measures embedded in a dimension's expression tree get the form
/// too — the form belongs to the measure as rendered in this select,
/// not to its position in the schema.
pub fn measures_render_modifier_in_schema(
    schema: &LogicalSchema,
    modifier: &MeasureRenderModifier,
) -> Result<Rc<LogicalSchema>, CubeError> {
    let stamp = |members: &Vec<Rc<MemberSymbol>>| {
        members
            .iter()
            .map(|m| transforms::measures_render_modifier(m, modifier))
            .collect::<Result<Vec<_>, _>>()
    };
    let mut new = schema.clone();
    new.time_dimensions = stamp(&new.time_dimensions)?;
    new.dimensions = stamp(&new.dimensions)?;
    new.measures = stamp(&new.measures)?;
    Ok(Rc::new(new))
}
