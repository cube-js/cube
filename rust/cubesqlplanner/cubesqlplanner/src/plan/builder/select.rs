use crate::plan::{
    AliasedExpr, Cte, Expr, Filter, From, MemberExpression, OrderBy, QualifiedColumnName, Schema,
    SchemaColumn, Select, SingleAliasedSource, SingleSource,
};

use crate::plan::expression::FunctionExpression;
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::{BaseMember, VisitorContext};
use std::collections::HashMap;
use std::rc::Rc;

pub struct SelectBuilder {
    projection_columns: Vec<AliasedExpr>,
    from: Rc<From>,
    filter: Option<Filter>,
    group_by: Vec<Expr>,
    having: Option<Filter>,
    order_by: Vec<OrderBy>,
    ctes: Vec<Rc<Cte>>,
    is_distinct: bool,
    limit: Option<usize>,
    offset: Option<usize>,
    result_schema: Schema,
}

impl SelectBuilder {
    pub fn new(from: Rc<From>) -> Self {
        Self {
            projection_columns: vec![],
            from,
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            ctes: vec![],
            is_distinct: false,
            limit: None,
            offset: None,
            result_schema: Schema::empty(),
        }
    }

    pub fn new_from_select(select: Rc<Select>) -> Self {
        Self {
            projection_columns: select.projection_columns.clone(),
            from: select.from.clone(),
            filter: select.filter.clone(),
            group_by: select.group_by.clone(),
            having: select.having.clone(),
            order_by: select.order_by.clone(),
            ctes: select.ctes.clone(),
            is_distinct: select.is_distinct,
            limit: select.limit,
            offset: select.offset,
            result_schema: Schema::clone(&select.schema),
        }
    }

    pub fn add_projection_member(&mut self, member: &Rc<dyn BaseMember>, alias: Option<String>) {
        let alias = if let Some(alias) = alias {
            alias
        } else {
            member.alias_name()
        };

        let expr = Expr::Member(MemberExpression::new(member.clone()));
        let aliased_expr = AliasedExpr {
            expr,
            alias: alias.clone(),
        };

        self.projection_columns.push(aliased_expr);
        self.result_schema
            .add_column(SchemaColumn::new(alias.clone(), Some(member.full_name())));
    }

    pub fn add_projection_coalesce_member(
        &mut self,
        member: &Rc<dyn BaseMember>,
        references: Vec<QualifiedColumnName>,
        alias: Option<String>,
    ) {
        let alias = if let Some(alias) = alias {
            alias
        } else {
            member.alias_name()
        };

        let expr = Expr::Function(FunctionExpression {
            function: "COALESCE".to_string(),
            arguments: references
                .into_iter()
                // TODO unwrap
                .map(|r| Expr::Reference(r))
                .collect(),
        });
        let aliased_expr = AliasedExpr {
            expr,
            alias: alias.clone(),
        };

        self.projection_columns.push(aliased_expr);
        self.result_schema
            .add_column(SchemaColumn::new(alias.clone(), Some(member.full_name())));
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

    fn make_asteriks_schema(&self) -> Rc<Schema> {
        let schema = match &self.from.source {
            crate::plan::FromSource::Empty => Rc::new(Schema::empty()),
            crate::plan::FromSource::Single(source) => source.source.schema(),
            crate::plan::FromSource::Join(join) => {
                let mut schema = Schema::empty();
                schema.merge(join.root.source.schema().as_ref());
                for itm in join.joins.iter() {
                    schema.merge(itm.from.source.schema().as_ref())
                }
                Rc::new(schema)
            }
        };
        schema
    }

    pub fn build(self, mut nodes_factory: SqlNodesFactory) -> Select {
        let cube_references = self.make_cube_references();
        nodes_factory.set_cube_name_references(cube_references);
        let schema = if self.projection_columns.is_empty() {
            self.make_asteriks_schema()
        } else {
            Rc::new(self.result_schema)
        };
        Select {
            projection_columns: self.projection_columns,
            from: self.from,
            filter: self.filter.clone(),
            group_by: self.group_by,
            having: self.having,
            order_by: self.order_by,
            context: Rc::new(VisitorContext::new(&nodes_factory, self.filter)),
            ctes: self.ctes,
            is_distinct: self.is_distinct,
            limit: self.limit,
            offset: self.offset,
            schema,
        }
    }
}
