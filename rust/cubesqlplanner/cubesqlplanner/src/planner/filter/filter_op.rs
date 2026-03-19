use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

use super::base_filter::FilterType;
use super::FilterOperator;

#[derive(Clone, Debug)]
pub enum FilterOp {
    Legacy {
        operator: FilterOperator,
        values: Vec<Option<String>>,
    },
}

#[derive(Clone)]
pub struct TypedFilter {
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<MemberSymbol>,
    filter_type: FilterType,
    op: FilterOp,
}

impl TypedFilter {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<MemberSymbol>,
        filter_type: FilterType,
        operator: FilterOperator,
        values: Option<Vec<Option<String>>>,
    ) -> Result<Rc<Self>, cubenativeutils::CubeError> {
        let values = values.unwrap_or_default();
        let op = FilterOp::Legacy { operator, values };

        Ok(Rc::new(Self {
            query_tools,
            member_evaluator,
            filter_type,
            op,
        }))
    }

    pub fn member_evaluator(&self) -> &Rc<MemberSymbol> {
        &self.member_evaluator
    }

    pub fn filter_type(&self) -> &FilterType {
        &self.filter_type
    }
}
