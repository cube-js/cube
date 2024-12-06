use super::sql_evaluator::EvaluationNode;
use super::VisitorContext;
use crate::plan::Schema;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub trait BaseMember {
    fn to_sql(&self, context: Rc<VisitorContext>, schema: Rc<Schema>) -> Result<String, CubeError>;
    fn alias_name(&self) -> String;
    fn member_evaluator(&self) -> Rc<EvaluationNode>;
    fn full_name(&self) -> String {
        self.member_evaluator().full_name()
    }
    fn as_base_member(self: Rc<Self>) -> Rc<dyn BaseMember>;
    fn cube_name(&self) -> &String;
    fn name(&self) -> &String;
    fn alias_suffix(&self) -> Option<String> {
        None
    }
}

pub struct BaseMemberHelper {}

impl BaseMemberHelper {
    pub fn upcast_vec_to_base_member<T: BaseMember>(vec: &Vec<Rc<T>>) -> Vec<Rc<dyn BaseMember>> {
        vec.iter()
            .map(|itm| itm.clone().as_base_member())
            .collect_vec()
    }

    pub fn iter_as_base_member<'a, T: BaseMember>(
        vec: &'a Vec<Rc<T>>,
    ) -> impl Iterator<Item = Rc<dyn BaseMember + 'static>> + 'a {
        vec.iter().map(|itm| itm.clone().as_base_member())
    }

    pub fn to_alias_vec(members: &Vec<Rc<dyn BaseMember>>) -> Vec<String> {
        members.iter().map(|m| m.alias_name()).collect_vec()
    }

    pub fn to_reference_map(members: &Vec<Rc<dyn BaseMember>>) -> HashMap<String, String> {
        members
            .iter()
            .map(|m| (m.full_name(), m.alias_name()))
            .collect::<HashMap<String, String>>()
    }
}
