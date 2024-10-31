use itertools::Itertools;

use super::{Expr, Filter, From, FromSource, OrderBy, Select, Subquery};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct SelectBuilder {
    pub projection: Vec<Expr>,
    pub from: From,
    pub filter: Option<Filter>,
    pub group_by: Vec<Expr>,
    pub having: Option<Filter>,
    pub order_by: Vec<OrderBy>,
    pub context: Rc<VisitorContext>,
    pub ctes: Vec<Rc<Subquery>>,
    pub is_distinct: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl SelectBuilder {
    pub fn new(from: From, context: Rc<VisitorContext>) -> Self {
        Self {
            projection: vec![],
            from,
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            context,
            ctes: vec![],
            is_distinct: false,
            limit: None,
            offset: None,
        }
    }

    pub fn set_projection(&mut self, projection: Vec<Expr>) {
        self.projection = projection;
    }

    pub fn set_filter(&mut self, filter: Option<Filter>) {
        self.filter = filter;
    }

    pub fn set_group_by(&mut self, group_by: Vec<Expr>) {
        self.group_by = group_by;
    }

    pub fn set_having(&mut self, having: Option<Filter>) {
        self.having = having;
    }

    pub fn set_order_by(&mut self, order_by: Vec<OrderBy>) {
        self.order_by = order_by;
    }

    pub fn set_distinct(&mut self) {
        self.is_distinct = true;
    }

    pub fn set_limit(&mut self, limit: Option<usize>) {
        self.limit = limit;
    }

    pub fn set_offset(&mut self, offset: Option<usize>) {
        self.offset = offset;
    }
    pub fn set_ctes(&mut self, ctes: Vec<Rc<Subquery>>) {
        self.ctes = ctes;
    }

    pub fn build(self) -> Select {
        Select {
            projection: self.projection,
            from: self.from,
            filter: self.filter,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            context: self.context,
            ctes: self.ctes,
            is_distinct: self.is_distinct,
            limit: self.limit,
            offset: self.offset,
        }
    }
}
