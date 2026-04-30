use super::*;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::structs::TemplateCalcGroup;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct CalcGroupItem {
    pub symbol: Rc<MemberSymbol>,
    pub values: Vec<String>,
}

impl CalcGroupItem {
    pub fn group_alias(&self) -> String {
        format!("{}_values", self.symbol.alias())
    }
}

#[derive(Clone)]
pub struct CalcGroupsJoin {
    from: Rc<From>,
    calc_groups: Vec<CalcGroupItem>,
}

impl CalcGroupsJoin {
    pub fn try_new(from: Rc<From>, calc_groups: Vec<CalcGroupItem>) -> Result<Rc<Self>, CubeError> {
        if let FromSource::CalcGroupsJoin(_) = from.source {
            return Err(CubeError::internal(format!(
                "Nested CalcGroupsJoin not supported"
            )));
        }
        Ok(Rc::new(Self { from, calc_groups }))
    }

    pub fn from(&self) -> &Rc<From> {
        &self.from
    }

    pub fn calc_groups(&self) -> &Vec<CalcGroupItem> {
        &self.calc_groups
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let base_sql = self.from.to_sql(templates, context)?;
        let original_sql = match &self.from.source {
            FromSource::Empty => None,
            _ => Some(base_sql),
        };

        let template_groups = self
            .calc_groups
            .iter()
            .map(|calc_group| TemplateCalcGroup {
                name: calc_group.symbol.name(),
                alias: calc_group.group_alias(),
                values: calc_group.values.clone(),
            })
            .collect_vec();
        let res = templates.calc_groups_join(original_sql, template_groups)?;

        Ok(res)
    }
}
