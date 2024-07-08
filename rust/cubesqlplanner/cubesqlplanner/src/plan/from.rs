use super::aggregation::Aggregation;
use super::select::Select;
use std::sync::Arc;

pub enum From {
    Empty,
    Select(Arc<Select>),
    Aggregation(Arc<Aggregation>),
}
