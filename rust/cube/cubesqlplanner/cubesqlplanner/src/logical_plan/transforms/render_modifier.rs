use super::super::{LogicalFilter, LogicalQueryModifiers, LogicalSchema, Query};
use crate::planner::filter::FilterItem;
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

/// Copy of the query with `modifier` set on the measures it renders: the
/// schema, the filters, and the ORDER BY measures the schema does not
/// project (the projected ones sort by their schema symbol).
pub fn measures_render_modifier_in_query(
    query: &Rc<Query>,
    modifier: &MeasureRenderModifier,
) -> Result<Rc<Query>, CubeError> {
    let stamp = |symbol: &Rc<MemberSymbol>| transforms::measures_render_modifier(symbol, modifier);
    let stamp_items = |items: &Vec<FilterItem>| {
        items
            .iter()
            .map(|item| transforms::map_filter_item_symbols(item, &stamp))
            .collect::<Result<Vec<_>, _>>()
    };
    let schema = measures_render_modifier_in_schema(query.schema(), modifier)?;
    let filter = query.filter();
    let filter = LogicalFilter {
        dimensions_filters: stamp_items(&filter.dimensions_filters)?,
        time_dimensions_filters: stamp_items(&filter.time_dimensions_filters)?,
        measures_filter: stamp_items(&filter.measures_filter)?,
        segments: stamp_items(&filter.segments)?,
    };
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
        offset: modifers.offset,
        limit: modifers.limit.clone(),
        ungrouped: modifers.ungrouped,
        distinct: modifers.distinct,
        order_by,
    };
    Ok(query.with_schema_filter_and_modifers(schema, Rc::new(filter), Rc::new(modifers)))
}
