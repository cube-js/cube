use super::super::LogicalSchema;
use crate::planner::symbols::transforms;
use crate::planner::MeasureRenderModifier;
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Copy of the schema with `modifier` set on every measure that has
/// no render modifier yet.
pub fn measures_render_modifier_in_schema(
    schema: &LogicalSchema,
    modifier: MeasureRenderModifier,
) -> Result<Rc<LogicalSchema>, CubeError> {
    let mut new = schema.clone();
    new.measures = new
        .measures
        .iter()
        .map(|m| transforms::measures_render_modifier(m, modifier))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Rc::new(new))
}
