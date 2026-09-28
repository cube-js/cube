use super::super::MemberSymbol;
use crate::planner::filter::{Filter, FilterItem};
use cubenativeutils::CubeError;
use std::rc::Rc;

/// Rebuilds a whole filter with every member evaluator replaced by
/// `f` of itself.
pub fn map_filter_symbols<F>(filter: Option<Filter>, f: &F) -> Result<Option<Filter>, CubeError>
where
    F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
{
    filter
        .map(|filter| -> Result<Filter, CubeError> {
            Ok(Filter {
                items: filter
                    .items
                    .iter()
                    .map(|item| map_filter_item_symbols(item, f))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        })
        .transpose()
}

/// Rebuilds a filter tree with every member evaluator replaced by
/// `f` of itself.
pub fn map_filter_item_symbols<F>(filter_item: &FilterItem, f: &F) -> Result<FilterItem, CubeError>
where
    F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
{
    let mut result = filter_item.clone();
    match &mut result {
        FilterItem::Group(group) => {
            let mut new_group = group.as_ref().clone();
            for item in new_group.items.iter_mut() {
                *item = map_filter_item_symbols(item, f)?;
            }
            *group = Rc::new(new_group);
        }
        FilterItem::Item(item) => {
            *item = item.with_member_evaluator(f(&item.raw_member_evaluator())?)?;
        }
        FilterItem::Segment(item) => {
            *item = item.with_member_evaluator(f(&item.member_evaluator())?);
        }
    }
    Ok(result)
}
