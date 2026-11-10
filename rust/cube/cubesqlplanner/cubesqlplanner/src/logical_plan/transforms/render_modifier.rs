use super::super::{LogicalQueryModifiers, LogicalSchema, Query};
use crate::planner::symbols::transforms;
use crate::planner::{MeasureRenderModifier, MemberSymbol, OrderByItem};
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
    let schema = schema.map_symbols(&|m| transforms::measures_render_modifier(m, modifier))?;
    Ok(Rc::new(schema))
}

/// Copy of the query with `modifier` set on the measures it renders: the
/// schema, the filters, and the ORDER BY measures the schema does not
/// project (the projected ones sort by their schema symbol).
pub fn measures_render_modifier_in_query(
    query: &Rc<Query>,
    modifier: &MeasureRenderModifier,
) -> Result<Rc<Query>, CubeError> {
    let stamp = |symbol: &Rc<MemberSymbol>| transforms::measures_render_modifier(symbol, modifier);
    let schema = measures_render_modifier_in_schema(query.schema(), modifier)?;
    let filter = query.filter().map_symbols(&stamp)?;
    let modifers = query.modifers();
    let order_by = modifers
        .order_by
        .iter()
        .map(|item| -> Result<_, CubeError> {
            if schema
                .find_member_positions(item.member_symbol().id())
                .is_empty()
            {
                Ok(OrderByItem::new(stamp(&item.member_symbol())?, item.desc()))
            } else {
                Ok(item.clone())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    let modifers = LogicalQueryModifiers {
        order_by,
        ..modifers.as_ref().clone()
    };
    Ok(query.with_schema_filter_and_modifers(schema, Rc::new(filter), Rc::new(modifers)))
}
