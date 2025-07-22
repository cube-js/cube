use super::query_tools::QueryTools;
use super::sql_evaluator::MemberSymbol;
use super::{evaluate_with_context, VisitorContext};
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct BaseCube {
    cube_name: String,
    members: HashSet<String>,
    member_evaluator: Rc<MemberSymbol>,
    definition: Rc<dyn CubeDefinition>,
    query_tools: Rc<QueryTools>,
}
impl BaseCube {
    pub fn try_new(
        cube_name: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<MemberSymbol>,
    ) -> Result<Rc<Self>, CubeError> {
        let definition = query_tools
            .cube_evaluator()
            .cube_from_path(cube_name.clone())?;
        let members = query_tools
            .base_tools()
            .all_cube_members(cube_name.clone())?
            .into_iter()
            .collect::<HashSet<_>>();

        Ok(Rc::new(Self {
            cube_name,
            members,
            member_evaluator,
            definition,
            query_tools,
        }))
    }

    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let cube_sql = evaluate_with_context(&self.member_evaluator, context, templates)?;
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
        if let Some(alias) = self.sql_alias() {
            alias.clone()
        } else {
            self.query_tools.alias_name(&self.cube_name)
        }
    }

    pub fn sql_alias(&self) -> &Option<String> {
        &self.definition.static_data().sql_alias
    }

    pub fn default_alias_with_prefix(&self, prefix: &Option<String>) -> String {
        let alias = self.default_alias();
        let res = if let Some(prefix) = prefix {
            format!("{prefix}_{alias}")
        } else {
            alias
        };
        self.query_tools.alias_name(&res)
    }
}
