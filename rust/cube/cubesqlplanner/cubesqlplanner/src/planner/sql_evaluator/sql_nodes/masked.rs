use super::SqlNode;
use crate::cube_bridge::base_query_options::FilterItem as NativeFilterItem;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct MaskedSqlNode {
    input: Rc<dyn SqlNode>,
    ungrouped: bool,
}

impl MaskedSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self {
            input,
            ungrouped: false,
        })
    }

    pub fn new_ungrouped(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self {
            input,
            ungrouped: true,
        })
    }

    fn resolve_mask(
        &self,
        node: &Rc<MemberSymbol>,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<Option<String>, CubeError> {
        let full_name = node.full_name();
        if !query_tools.is_member_masked(&full_name) {
            return Ok(None);
        }

        let mask_filter = query_tools.member_mask_filter(&full_name).cloned();

        let masked_sql = if let Some(mask_call) = node.mask_sql() {
            if self.ungrouped {
                if let MemberSymbol::Measure(_) = node.as_ref() {
                    if mask_call.dependencies_count() > 0 {
                        return Ok(None);
                    }
                }
            }
            mask_call.eval(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )?
        } else {
            "(NULL)".to_string()
        };

        if let Some(filter_item) = mask_filter {
            let original_sql = self.input.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            )?;
            let filter_sql = self.compile_filter_to_sql(&filter_item, visitor, node_processor, query_tools.clone(), templates)?;
            if let Some(filter_sql) = filter_sql {
                Ok(Some(format!("CASE WHEN {} THEN {} ELSE {} END", filter_sql, original_sql, masked_sql)))
            } else {
                Ok(Some(masked_sql))
            }
        } else {
            Ok(Some(masked_sql))
        }
    }

    fn compile_filter_to_sql(
        &self,
        filter_item: &NativeFilterItem,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<Option<String>, CubeError> {
        self.render_native_filter(filter_item, visitor, node_processor, query_tools, templates)
    }

    fn render_native_filter(
        &self,
        item: &NativeFilterItem,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<Option<String>, CubeError> {
        if let Some(items) = &item.or {
            let parts: Vec<String> = items
                .iter()
                .filter_map(|i| self.render_native_filter(i, visitor, node_processor.clone(), query_tools.clone(), templates).transpose())
                .collect::<Result<Vec<_>, _>>()?;
            if parts.is_empty() {
                return Ok(None);
            }
            Ok(Some(parts.iter().map(|p| format!("({})", p)).collect::<Vec<_>>().join(" OR ")))
        } else if let Some(items) = &item.and {
            let parts: Vec<String> = items
                .iter()
                .filter_map(|i| self.render_native_filter(i, visitor, node_processor.clone(), query_tools.clone(), templates).transpose())
                .collect::<Result<Vec<_>, _>>()?;
            if parts.is_empty() {
                return Ok(None);
            }
            Ok(Some(parts.iter().map(|p| format!("({})", p)).collect::<Vec<_>>().join(" AND ")))
        } else if let (Some(member), Some(operator)) = (item.member(), &item.operator) {
            let member_symbol = query_tools.evaluator_compiler()
                .borrow_mut()
                .add_dimension_evaluator(member.clone())?;
            let column_sql = self.input.to_sql(visitor, &member_symbol, query_tools.clone(), node_processor, templates)?;
            let filter_sql = self.render_filter_condition(&column_sql, operator, &item.values, &query_tools)?;
            Ok(Some(filter_sql))
        } else {
            Ok(None)
        }
    }

    fn render_filter_condition(
        &self,
        column_sql: &str,
        operator: &str,
        values: &Option<Vec<Option<String>>>,
        query_tools: &Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let vals: Vec<String> = values
            .as_ref()
            .map(|v| v.iter().filter_map(|x| x.clone()).collect())
            .unwrap_or_default();

        match operator {
            "equals" => {
                if vals.len() == 1 {
                    Ok(format!("{} = {}", column_sql, query_tools.allocate_param(&vals[0])))
                } else if vals.len() > 1 {
                    let params: Vec<String> = vals.iter().map(|v| query_tools.allocate_param(v)).collect();
                    Ok(format!("{} IN ({})", column_sql, params.join(", ")))
                } else {
                    Ok(format!("{} IS NULL", column_sql))
                }
            }
            "notEquals" => {
                if vals.len() == 1 {
                    Ok(format!("{} <> {}", column_sql, query_tools.allocate_param(&vals[0])))
                } else if vals.len() > 1 {
                    let params: Vec<String> = vals.iter().map(|v| query_tools.allocate_param(v)).collect();
                    Ok(format!("{} NOT IN ({})", column_sql, params.join(", ")))
                } else {
                    Ok(format!("{} IS NOT NULL", column_sql))
                }
            }
            "set" => Ok(format!("{} IS NOT NULL", column_sql)),
            "notSet" => Ok(format!("{} IS NULL", column_sql)),
            "gt" => Ok(format!("{} > {}", column_sql, query_tools.allocate_param(&vals[0]))),
            "gte" => Ok(format!("{} >= {}", column_sql, query_tools.allocate_param(&vals[0]))),
            "lt" => Ok(format!("{} < {}", column_sql, query_tools.allocate_param(&vals[0]))),
            "lte" => Ok(format!("{} <= {}", column_sql, query_tools.allocate_param(&vals[0]))),
            _ => Ok("1 = 1".to_string()),
        }
    }
}

impl SqlNode for MaskedSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        if let Some(masked) = self.resolve_mask(
            node,
            visitor,
            node_processor.clone(),
            query_tools.clone(),
            templates,
        )? {
            return Ok(masked);
        }
        self.input
            .to_sql(visitor, node, query_tools, node_processor, templates)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
