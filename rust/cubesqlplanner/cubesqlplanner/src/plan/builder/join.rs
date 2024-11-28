use crate::plan::{Join, JoinCondition, JoinItem, QueryPlan, Schema, Select, SingleAliasedSource};
use crate::planner::BaseCube;
use std::rc::Rc;

pub struct JoinBuilder {
    root: SingleAliasedSource,
    joins: Vec<JoinItem>,
}

impl JoinBuilder {
    pub fn new(root: SingleAliasedSource) -> Self {
        Self {
            root,
            joins: vec![],
        }
    }

    pub fn new_from_cube(cube: Rc<BaseCube>, alias: Option<String>) -> Self {
        Self::new(SingleAliasedSource::new_from_cube(cube, alias))
    }

    pub fn new_from_table_reference(
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
    ) -> Self {
        Self::new(SingleAliasedSource::new_from_table_reference(
            reference, schema, alias,
        ))
    }

    pub fn new_from_subquery(plan: Rc<QueryPlan>, alias: String) -> Self {
        Self::new(SingleAliasedSource::new_from_subquery(plan, alias))
    }

    pub fn new_from_subselect(plan: Rc<Select>, alias: String) -> Self {
        Self::new(SingleAliasedSource::new_from_subquery(
            Rc::new(QueryPlan::Select(plan)),
            alias,
        ))
    }

    pub fn left_join_subselect(&mut self, subquery: Rc<Select>, alias: String, on: JoinCondition) {
        self.join_subselect(subquery, alias, on, false)
    }

    pub fn inner_join_subselect(&mut self, subquery: Rc<Select>, alias: String, on: JoinCondition) {
        self.join_subselect(subquery, alias, on, true)
    }

    pub fn left_join_cube(&mut self, cube: Rc<BaseCube>, alias: Option<String>, on: JoinCondition) {
        self.join_cube(cube, alias, on, false)
    }

    pub fn inner_join_cube(
        &mut self,
        cube: Rc<BaseCube>,
        alias: Option<String>,
        on: JoinCondition,
    ) {
        self.join_cube(cube, alias, on, true)
    }

    pub fn left_join_table_reference(
        &mut self,
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
        on: JoinCondition,
    ) {
        self.join_table_reference(reference, schema, alias, on, false)
    }

    pub fn inner_join_table_reference(
        &mut self,
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
        on: JoinCondition,
    ) {
        self.join_table_reference(reference, schema, alias, on, true)
    }

    pub fn build(self) -> Rc<Join> {
        Rc::new(Join {
            root: self.root,
            joins: self.joins,
        })
    }

    fn join_subselect(
        &mut self,
        subquery: Rc<Select>,
        alias: String,
        on: JoinCondition,
        is_inner: bool,
    ) {
        let subquery = Rc::new(QueryPlan::Select(subquery));
        let from = SingleAliasedSource::new_from_subquery(subquery, alias);
        self.joins.push(JoinItem { from, on, is_inner })
    }

    fn join_cube(
        &mut self,
        cube: Rc<BaseCube>,
        alias: Option<String>,
        on: JoinCondition,
        is_inner: bool,
    ) {
        let from = SingleAliasedSource::new_from_cube(cube, alias);
        self.joins.push(JoinItem { from, on, is_inner })
    }

    fn join_table_reference(
        &mut self,
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
        on: JoinCondition,
        is_inner: bool,
    ) {
        let from = SingleAliasedSource::new_from_table_reference(reference, schema, alias);
        self.joins.push(JoinItem { from, on, is_inner })
    }
}
