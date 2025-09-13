use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::symbols::{
    Case, CaseDefinition, CaseLabel, CaseSwitchDefinition,
};
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{CaseSwitchItem, MemberSymbol};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct CaseSqlNode {
    input: Rc<dyn SqlNode>,
}

impl CaseSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }

    pub fn case_to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        case: &CaseDefinition,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let mut when_then = Vec::new();
        for itm in case.items.iter() {
            let when = itm.sql.eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?;
            let then = match &itm.label {
                CaseLabel::String(s) => templates.quote_string(&s)?,
                CaseLabel::Sql(sql) => sql.eval(
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                    templates,
                )?,
            };
            when_then.push((when, then));
        }
        let else_label = match &case.else_label {
            CaseLabel::String(s) => templates.quote_string(&s)?,
            CaseLabel::Sql(sql) => sql.eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?,
        };
        templates.case(None, when_then, Some(else_label))
    }
    pub fn case_switch_to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        case: &CaseSwitchDefinition,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if case.items.len() == 1 && case.else_sql.is_none() {
            return case.items[0].sql.eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            );
        }
        if case.items.is_empty() && case.else_sql.is_some() {
            return case.else_sql.as_ref().unwrap().eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            );
        }
        let expr = match &case.switch {
            CaseSwitchItem::Sql(sql_call) => sql_call.eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?,
            CaseSwitchItem::Member(member_symbol) => {
                visitor.apply(&member_symbol, node_processor.clone(), templates)?
            }
        };
        let mut when_then = Vec::new();
        for itm in case.items.iter() {
            let when = templates.quote_string(&itm.value)?;
            let then = itm.sql.eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?;
            when_then.push((when, then));
        }
        let else_label = if let Some(else_sql) = &case.else_sql {
            Some(else_sql.eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?)
        } else {
            None
        };
        templates.case(Some(expr), when_then, else_label)
    }
}

impl SqlNode for CaseSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                if let Some(case) = ev.case() {
                    match case {
                        Case::Case(case) => {
                            self.case_to_sql(visitor, case, query_tools, node_processor, templates)?
                        }
                        Case::CaseSwitch(case) => self.case_switch_to_sql(
                            visitor,
                            case,
                            query_tools,
                            node_processor,
                            templates,
                        )?,
                    }
                } else {
                    self.input.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?
                }
            }
            MemberSymbol::Measure(ev) => {
                if let Some(case) = ev.case() {
                    match case {
                        Case::Case(case) => {
                            self.case_to_sql(visitor, case, query_tools, node_processor, templates)?
                        }
                        Case::CaseSwitch(case) => self.case_switch_to_sql(
                            visitor,
                            case,
                            query_tools,
                            node_processor,
                            templates,
                        )?,
                    }
                } else {
                    self.input.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Case node processor called for wrong node",
                )));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
