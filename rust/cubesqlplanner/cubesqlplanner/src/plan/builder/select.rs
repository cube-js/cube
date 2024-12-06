use crate::plan::{
    AliasedExpr, Cte, Expr, Filter, From, MemberExpression, OrderBy, Schema, Select,
};
use crate::planner::{BaseMember, VisitorContext};
use std::rc::Rc;

pub struct SelectBuilder {
    projection_columns: Vec<AliasedExpr>,
    from: From,
    filter: Option<Filter>,
    group_by: Vec<Expr>,
    having: Option<Filter>,
    order_by: Vec<OrderBy>,
    context: Rc<VisitorContext>,
    ctes: Vec<Rc<Cte>>,
    is_distinct: bool,
    limit: Option<usize>,
    offset: Option<usize>,
    input_schema: Rc<Schema>,
}

impl SelectBuilder {
    pub fn new(from: From, context: VisitorContext) -> Self {
        let input_schema = from.schema.clone();
        Self {
            projection_columns: vec![],
            from,
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            context: Rc::new(context),
            ctes: vec![],
            is_distinct: false,
            limit: None,
            offset: None,
            input_schema,
        }
    }

    pub fn add_projection_member(
        &mut self,
        member: &Rc<dyn BaseMember>,
        source: Option<String>,
        alias: Option<String>,
    ) {
        let alias = if let Some(alias) = alias {
            alias
        } else {
            self.input_schema.resolve_member_alias(&member, &source)
        };
        let expr = Expr::Member(MemberExpression::new(member.clone(), source));
        let aliased_expr = AliasedExpr {
            expr,
            alias: alias.clone(),
        };

        self.projection_columns.push(aliased_expr);
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
    pub fn set_ctes(&mut self, ctes: Vec<Rc<Cte>>) {
        self.ctes = ctes;
    }

    pub fn build(self) -> Select {
        Select {
            projection_columns: self.projection_columns,
            from: self.from,
            filter: self.filter,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            context: self.context.clone(),
            ctes: self.ctes,
            is_distinct: self.is_distinct,
            limit: self.limit,
            offset: self.offset,
        }
    }
}
