use super::query_tools::QueryTools;
use super::sql_evaluator::MemberSymbol;
use super::sql_templates::PlanSqlTemplates;
use super::{evaluate_with_context, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub trait BaseMember {
    fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError>;
    fn alias_name(&self) -> String;
    fn member_evaluator(&self) -> Rc<MemberSymbol>;
    fn full_name(&self) -> String;
    fn as_base_member(self: Rc<Self>) -> Rc<dyn BaseMember>;
    fn cube_name(&self) -> &String;
    fn name(&self) -> &String;
    fn alias_suffix(&self) -> Option<String> {
        None
    }
}

pub struct MemberSymbolRef {
    member_evaluator: Rc<MemberSymbol>,
    query_tools: Rc<QueryTools>,
    default_alias: String,
    cube_name: String,
    name: String,
}

impl MemberSymbolRef {
    pub fn try_new(
        member_evaluator: Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
    ) -> Result<Rc<Self>, CubeError> {
        let default_alias = match member_evaluator.as_ref() {
            &MemberSymbol::TimeDimension(_)
            | &MemberSymbol::Dimension(_)
            | &MemberSymbol::Measure(_) => BaseMemberHelper::default_alias(
                &member_evaluator.cube_name(),
                &member_evaluator.name(),
                &member_evaluator.alias_suffix(),
                query_tools.clone(),
            )?,
            MemberSymbol::MemberExpression(_)
            | MemberSymbol::CubeName(_)
            | MemberSymbol::CubeTable(_) => query_tools.alias_name(&member_evaluator.name()),
        };
        let cube_name = member_evaluator.cube_name();
        let name = member_evaluator.name();
        Ok(Rc::new(Self {
            member_evaluator,
            default_alias,
            query_tools,
            cube_name,
            name,
        }))
    }
}

impl BaseMember for MemberSymbolRef {
    fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        evaluate_with_context(
            &self.member_evaluator,
            self.query_tools.clone(),
            context,
            templates,
        )
    }

    fn alias_name(&self) -> String {
        self.default_alias.clone()
    }

    fn member_evaluator(&self) -> Rc<MemberSymbol> {
        self.member_evaluator.clone()
    }

    fn full_name(&self) -> String {
        self.member_evaluator.full_name()
    }

    fn as_base_member(self: Rc<Self>) -> Rc<dyn BaseMember> {
        self.clone()
    }

    fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn name(&self) -> &String {
        &self.name
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

    pub fn extract_symbols_from_members(
        members: &Vec<Rc<dyn BaseMember>>,
    ) -> Vec<Rc<MemberSymbol>> {
        members.iter().map(|m| m.member_evaluator()).collect_vec()
    }

    pub fn default_alias(
        cube_name: &String,
        member_name: &String,
        member_suffix: &Option<String>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let cube_alias = query_tools.alias_for_cube(cube_name)?;
        Ok(PlanSqlTemplates::memeber_alias_name(
            &cube_alias,
            &member_name,
            member_suffix,
        ))
    }
}
