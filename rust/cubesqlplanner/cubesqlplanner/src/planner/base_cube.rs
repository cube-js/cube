use super::query_tools::QueryTools;
use super::sql_evaluator::MemberSymbol;
use super::{evaluate_with_context, VisitorContext};
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct BaseCube {
    cube_name: String,
    members: HashSet<String>,
    member_evaluator: Rc<MemberSymbol>,
    query_tools: Rc<QueryTools>,
}
impl BaseCube {
    pub fn try_new(
        cube_name: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<MemberSymbol>,
    ) -> Result<Rc<Self>, CubeError> {
        let members = query_tools
            .base_tools()
            .all_cube_members(cube_name.clone())?
            .into_iter()
            .collect::<HashSet<_>>();

        Ok(Rc::new(Self {
            cube_name,
            members,
            member_evaluator,
            query_tools,
        }))
    }

    pub fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let cube_sql =
            evaluate_with_context(&self.member_evaluator, self.query_tools.clone(), context)?;
        Ok(cube_sql)
    }

    pub fn name(&self) -> &String {
        &self.cube_name
    }

    pub fn members(&self) -> &HashSet<String> {
        &self.members
    }

    pub fn has_member(&self, name: &str) -> bool {
        self.members.contains(name)
    }

    pub fn default_alias(&self) -> String {
        self.query_tools.alias_name(&self.cube_name)
    }

    pub fn default_alias_with_prefix(&self, prefix: &Option<String>) -> String {
        let alias = self.default_alias();
        if let Some(prefix) = prefix {
            format!("{prefix}_{alias}")
        } else {
            alias
        }
    }
}
