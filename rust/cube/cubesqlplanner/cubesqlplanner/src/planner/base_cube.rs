use super::query_tools::QueryTools;
use super::{CubeId, CubeRef, CubeTableSymbol};
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::physical_plan::VisitorContext;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::collections::HashSet;
use std::rc::Rc;

pub struct BaseCube {
    cube_id: CubeId,
    members: HashSet<String>,
    cube_table_symbol: Rc<CubeTableSymbol>,
    definition: Rc<dyn CubeDefinition>,
    joined_alias: Option<String>,
    query_tools: Rc<QueryTools>,
}
impl BaseCube {
    pub fn try_new(
        cube_id: CubeId,
        query_tools: Rc<QueryTools>,
        cube_table_symbol: Rc<CubeTableSymbol>,
    ) -> Result<Rc<Self>, CubeError> {
        let definition = query_tools
            .cube_evaluator()
            .cube_from_path(cube_id.target().to_string())?;
        let members = query_tools
            .base_tools()
            .all_cube_members(cube_id.target().to_string())?
            .into_iter()
            .collect::<HashSet<_>>();
        let joined_alias = if cube_id.is_joined() {
            Some(query_tools.model_cubes().alias_base(&cube_id)?)
        } else {
            None
        };

        Ok(Rc::new(Self {
            cube_id,
            members,
            cube_table_symbol,
            definition,
            joined_alias,
            query_tools,
        }))
    }

    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let cube_ref = CubeRef::Table(self.cube_table_symbol.clone());
        let visitor = context.make_visitor(context.query_tools());
        let node_processor = context.node_processor();
        visitor.evaluate_cube_ref(&cube_ref, node_processor, templates)
    }

    pub fn cube_id(&self) -> &CubeId {
        &self.cube_id
    }

    pub fn members(&self) -> &HashSet<String> {
        &self.members
    }

    pub fn has_member(&self, name: &str) -> bool {
        self.members.contains(name)
    }

    pub fn default_alias(&self) -> String {
        if let Some(alias) = &self.joined_alias {
            return alias.clone();
        }
        if let Some(alias) = self.sql_alias() {
            alias.clone()
        } else {
            self.query_tools.alias_name(&self.cube_id.to_string())
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
