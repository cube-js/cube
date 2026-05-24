use super::SqlNode;
use crate::physical_plan::filter::ToSql;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Intercepts rendering for members masked by query-tools and
/// substitutes the configured `mask_sql` expression. When a mask
/// filter is set, wraps the result in a `CASE WHEN filter THEN
/// original ELSE mask END`. Pass-through for non-masked members.
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

        let mask_filter = query_tools.member_mask_filter(&full_name);

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

        let Some(filter_item) = mask_filter else {
            return Ok(Some(masked_sql));
        };

        let original_sql = self.input.to_sql(
            visitor,
            node,
            query_tools.clone(),
            node_processor,
            templates,
        )?;
        // TODO: support FILTER_PARAMS in mask filter SQL by passing
        // proper FiltersContext with filter_params_columns.
        // Use self.input as node_processor so member references inside the filter
        // resolve through the unmasked chain — prevents recursion through MaskedSqlNode
        // when the filter member is itself masked.
        let filter_sql = filter_item.to_sql(
            visitor,
            self.input.clone(),
            query_tools,
            templates,
            &FiltersContext::default(),
        )?;
        if filter_sql.is_empty() {
            Ok(Some(masked_sql))
        } else {
            Ok(Some(templates.case(
                None,
                vec![(filter_sql, original_sql)],
                Some(masked_sql),
            )?))
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
