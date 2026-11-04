use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::{Case, CaseDefinition, CaseLabel, CaseSwitchDefinition};
use crate::planner::{CaseSwitchItem, MemberSymbol};
use crate::utils::sql_expression_scanner::is_top_level_compound;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Renders members defined via a `case:` body — both the classic
/// `CASE WHEN cond THEN label …` and the switch-style
/// `CASE switch WHEN value THEN sql …`. Members without a case body
/// pass through to `input`.
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
        // `CASE ELSE x END` is invalid SQL, so a branchless case is just its else value.
        if case.items.is_empty() {
            let sql = match &case.else_label {
                CaseLabel::String(s) => templates.quote_string(&s)?,
                CaseLabel::Sql(sql) => sql.eval(visitor, node_processor, query_tools, templates)?,
            };
            return Ok(unwrapped_case_value(visitor, sql));
        }
        // All sub-SQLs end up inside `CASE … END` — a safe wrap.
        let inner_visitor = visitor.with_arg_needs_paren_safe(false);
        let mut when_then = Vec::new();
        for itm in case.items.iter() {
            let when = itm.sql.eval(
                &inner_visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?;
            let then = match &itm.label {
                CaseLabel::String(s) => templates.quote_string(&s)?,
                CaseLabel::Sql(sql) => sql.eval(
                    &inner_visitor,
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
                &inner_visitor,
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
        // Degenerate shortcuts return the inner SQL without a `CASE` wrap.
        if case.items.len() == 1 && case.else_sql.is_none() {
            let sql = case.items[0].sql.eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?;
            return Ok(unwrapped_case_value(visitor, sql));
        }
        if case.items.is_empty() {
            if let Some(else_sql) = &case.else_sql {
                let sql = else_sql.eval(
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                    templates,
                )?;
                return Ok(unwrapped_case_value(visitor, sql));
            }
        }
        let inner_visitor = visitor.with_arg_needs_paren_safe(false);
        let expr = match &case.switch {
            CaseSwitchItem::Sql(sql_call) => sql_call.eval(
                &inner_visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?,
            CaseSwitchItem::Member(member_symbol) => {
                inner_visitor.apply(&member_symbol, node_processor.clone(), templates)?
            }
        };
        let mut when_then = Vec::new();
        for itm in case.items.iter() {
            let when = templates.quote_string(&itm.value)?;
            let then = itm.sql.eval(
                &inner_visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?;
            when_then.push((when, then));
        }
        let else_label = if let Some(else_sql) = &case.else_sql {
            Some(else_sql.eval(
                &inner_visitor,
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

// A case value rendered without its `CASE … END` wrap loses that wrap's
// isolation; measures have no parenthesizing node above this one to restore it.
fn unwrapped_case_value(visitor: &SqlEvaluatorVisitor, sql: String) -> String {
    if visitor.arg_needs_paren_safe() && is_top_level_compound(&sql) {
        format!("({})", sql)
    } else {
        sql
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
            MemberSymbol::TimeDimension(_)
            | MemberSymbol::MemberExpression(_)
            | MemberSymbol::Ref(_) => {
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
