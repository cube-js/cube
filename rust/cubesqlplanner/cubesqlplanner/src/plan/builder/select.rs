use crate::plan::{
    AliasedExpr, Cte, Expr, Filter, From, MemberExpression, OrderBy, Schema, Select,
    SingleAliasedSource, SingleSource,
};

use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::sql_evaluator::symbols::MemberSymbol;
use crate::planner::{BaseMember, VisitorContext};
use std::collections::HashMap;
use std::rc::Rc;

pub struct SelectBuilder {
    projection_columns: Vec<AliasedExpr>,
    from: From,
    filter: Option<Filter>,
    group_by: Vec<Expr>,
    having: Option<Filter>,
    order_by: Vec<OrderBy>,
    nodes_factory: SqlNodesFactory,
    ctes: Vec<Rc<Cte>>,
    is_distinct: bool,
    limit: Option<usize>,
    offset: Option<usize>,
    input_schema: Rc<Schema>,
    result_schema: Schema,
}

impl SelectBuilder {
    pub fn new(from: From, nodes_factory: SqlNodesFactory) -> Self {
        let input_schema = from.schema.clone();
        Self {
            projection_columns: vec![],
            from,
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            nodes_factory,
            ctes: vec![],
            is_distinct: false,
            limit: None,
            offset: None,
            input_schema,
            result_schema: Schema::empty(),
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
        self.resolve_render_reference_for_member(&member.member_evaluator());

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

    fn resolve_render_reference_for_member(&mut self, member: &Rc<MemberSymbol>) {
        let member_name = member.full_name();
        if !self
            .nodes_factory
            .render_references()
            .contains_key(&member_name)
        {
            if let Some(reference) = self
                .input_schema
                .resolve_member_reference(&member_name, &None)
            {
                self.nodes_factory
                    .add_render_reference(member_name, reference);
            } else {
                for dep in member.get_dependencies() {
                    self.resolve_render_reference_for_member(&dep);
                }
            }
        }
    }

    fn make_cube_references(&self) -> HashMap<String, String> {
        let mut refs = HashMap::new();
        match &self.from.source {
            crate::plan::FromSource::Single(source) => {
                self.add_cube_reference_if_needed(source, &mut refs)
            }
            crate::plan::FromSource::Join(join) => {
                self.add_cube_reference_if_needed(&join.root, &mut refs);
                for join_item in join.joins.iter() {
                    self.add_cube_reference_if_needed(&join_item.from, &mut refs);
                }
            }
            crate::plan::FromSource::Empty => {}
        }
        refs
    }

    fn add_cube_reference_if_needed(
        &self,
        source: &SingleAliasedSource,
        refs: &mut HashMap<String, String>,
    ) {
        match &source.source {
            SingleSource::Cube(cube) => {
                refs.insert(cube.name().clone(), source.alias.clone());
            }
            _ => {}
        }
    }

    pub fn build(mut self) -> Select {
        let cube_references = self.make_cube_references();
        self.nodes_factory.set_cube_name_references(cube_references);
        Select {
            projection_columns: self.projection_columns,
            from: self.from,
            filter: self.filter,
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            context: Rc::new(VisitorContext::new(&self.nodes_factory)),
            ctes: self.ctes,
            is_distinct: self.is_distinct,
            limit: self.limit,
            offset: self.offset,
        }
    }
}
