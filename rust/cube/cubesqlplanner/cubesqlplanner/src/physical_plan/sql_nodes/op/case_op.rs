use crate::physical_plan::sql_nodes::NodeProcessor;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::{Case, CaseDefinition, CaseLabel, CaseSwitchDefinition};
use crate::planner::{CaseSwitchItem, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;

use super::{OpCtx, OpExec};

/// Renders a member that is defined via `case:` / `case_switch:` rules as a
/// SQL `CASE … END`. Members without a case definition fall through to the
/// rest of the pipeline so plain dimensions/measures keep their normal path.
#[derive(Clone)]
pub struct CaseOp;

impl CaseOp {
    fn case_to_sql(
        visitor: &SqlEvaluatorVisitor,
        case: &CaseDefinition,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<NodeProcessor>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
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
                CaseLabel::String(s) => templates.quote_string(s)?,
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
            CaseLabel::String(s) => templates.quote_string(s)?,
            CaseLabel::Sql(sql) => sql.eval(
                &inner_visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?,
        };
        templates.case(None, when_then, Some(else_label))
    }

    fn case_switch_to_sql(
        visitor: &SqlEvaluatorVisitor,
        case: &CaseSwitchDefinition,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<NodeProcessor>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        // Degenerate shortcuts return the inner SQL as-is — propagate the outer
        // visitor so an enclosing ParenthesizeOp still sees the compound flag.
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
        let inner_visitor = visitor.with_arg_needs_paren_safe(false);
        let expr = match &case.switch {
            CaseSwitchItem::Sql(sql_call) => sql_call.eval(
                &inner_visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?,
            CaseSwitchItem::Member(member_symbol) => {
                inner_visitor.apply(member_symbol, node_processor.clone(), templates)?
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

impl OpExec for CaseOp {
    fn exec(&self, ctx: &mut OpCtx<'_>) -> Result<String, CubeError> {
        let case_opt: Option<&Case> = match ctx.sym.as_ref() {
            MemberSymbol::Dimension(ev) => ev.case(),
            MemberSymbol::Measure(ev) => ev.case(),
            _ => {
                return Err(CubeError::internal(
                    "Case op called for non-dimension/measure symbol".to_string(),
                ));
            }
        };
        let Some(case) = case_opt else {
            return ctx.render_tail();
        };
        match case {
            Case::Case(c) => Self::case_to_sql(
                &ctx.visitor,
                c,
                ctx.query_tools.clone(),
                ctx.node_processor.clone(),
                ctx.templates,
            ),
            Case::CaseSwitch(c) => Self::case_switch_to_sql(
                &ctx.visitor,
                c,
                ctx.query_tools.clone(),
                ctx.node_processor.clone(),
                ctx.templates,
            ),
        }
    }
}
