use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::structs::{TemplateCalcGroup, TemplateCalcSingleValue};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct CalcGroupItem {
    pub name: String,
    pub values: Vec<String>,
}

#[derive(Default, Clone, Debug)]
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
                let res = if let Some(calc_groups) = self.items.get(ev.cube_name()) {
                    let mut single_values = vec![];
                    let mut template_groups = vec![];
                    for calc_group in calc_groups {
                        if calc_group.values.len() == 1 {
                            single_values.push(TemplateCalcSingleValue {
                                name: calc_group.name.clone(),
                                value: calc_group.values[0].clone(),
                            })
                        } else {
                            template_groups.push(TemplateCalcGroup {
                                name: calc_group.name.clone(),
                                alias: format!("{}_values", calc_group.name),
                                values: calc_group.values.clone(),
                            })
                        }
                    }
                    let res = templates.calc_groups_join(
                        &ev.cube_name(),
                        &input,
                        single_values,
                        template_groups,
                    )?;
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
