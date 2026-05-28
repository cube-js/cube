use crate::planner::filter::{FilterGroup, FilterItem};
use std::rc::Rc;

pub fn exclude_members(member_names: &[String], filters: &[FilterItem]) -> Vec<FilterItem> {
    let mut result = Vec::new();
    for item in filters.iter() {
        match item {
            FilterItem::Group(group) => {
                let new_items = exclude_members(member_names, &group.items);
                if !new_items.is_empty() {
                    result.push(FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        new_items,
                    ))));
                }
            }
            FilterItem::Item(itm) => {
                if !member_names.contains(&itm.member_name()) {
                    result.push(FilterItem::Item(itm.clone()));
                }
            }
            FilterItem::Segment(seg) => {
                if !member_names.contains(&seg.full_name()) {
                    result.push(FilterItem::Segment(seg.clone()));
                }
            }
        }
    }
    result
}

pub fn keep_only_members(member_names: &[String], filters: &[FilterItem]) -> Vec<FilterItem> {
    let mut result = Vec::new();
    for item in filters.iter() {
        match item {
            FilterItem::Group(group) => {
                let new_items = keep_only_members(member_names, &group.items);
                if !new_items.is_empty() {
                    result.push(FilterItem::Group(Rc::new(FilterGroup::new(
                        group.operator.clone(),
                        new_items,
                    ))));
                }
            }
            FilterItem::Item(itm) => {
                if member_names.contains(&itm.member_name()) {
                    result.push(FilterItem::Item(itm.clone()));
                }
            }
            FilterItem::Segment(seg) => {
                if member_names.contains(&seg.full_name()) {
                    result.push(FilterItem::Segment(seg.clone()));
                }
            }
        }
    }
    result
}

pub fn has_filter_for_member(member_name: &String, filters: &[FilterItem]) -> bool {
    for item in filters.iter() {
        match item {
            FilterItem::Group(group) => {
                if has_filter_for_member(member_name, &group.items) {
                    return true;
                }
            }
            FilterItem::Item(itm) => {
                if &itm.member_name() == member_name {
                    return true;
                }
            }
            FilterItem::Segment(_) => {}
        }
    }
    false
}
