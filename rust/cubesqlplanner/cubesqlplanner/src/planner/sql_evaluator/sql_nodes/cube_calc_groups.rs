use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::DimensionSymbol;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::structs::TemplateCalcGroup;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone)]
pub struct CalcGroupItem {
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Default, Clone)]
pub struct CalcGroupsItems {
    items: HashMap<String, Vec<CalcGroupItem>>,
}

impl CalcGroupsItems {
    pub fn add(&mut self, cube_name: String, dimension_name: String, values: Vec<String>) {
        let items = self.items.entry(cube_name).or_default();
        if !items.iter().any(|itm| itm.name == dimension_name) {
            items.push(CalcGroupItem {
                name: dimension_name,
                values,
            })
        }
    }

    pub fn get(&self, cube_name: &str) -> Option<&Vec<CalcGroupItem>> {
        self.items.get(cube_name)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

pub struct CubeCalcGroupsSqlNode {
    input: Rc<dyn SqlNode>,
    items: CalcGroupsItems,
}

impl CubeCalcGroupsSqlNode {
    pub fn new(input: Rc<dyn SqlNode>, items: CalcGroupsItems) -> Rc<Self> {
        Rc::new(Self { input, items })
    }
}

impl SqlNode for CubeCalcGroupsSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let input = self.input.to_sql(
            visitor,
            node,
            query_tools.clone(),
            node_processor.clone(),
            templates,
        )?;
        let res = match node.as_ref() {
            MemberSymbol::CubeTable(ev) => {
                let res = if let Some(groups) = self.items.get(ev.cube_name()) {
                    let template_groups = groups
                        .iter()
                        .map(|group| TemplateCalcGroup {
                            name: group.name.clone(),
                            values: group.values.clone(),
                        })
                        .collect_vec();
                    let res = templates.calc_groups_join(&ev.cube_name(), &input, template_groups)?;
                    format!("({})", res)
                } else {
                    input
                };

                res
            }
            _ => input,
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
