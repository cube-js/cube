use super::SqlNode;
use crate::physical_plan::filter::ToSql;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashSet;
use std::rc::Rc;

/// Intercepts rendering for members masked by query-tools and
/// substitutes the configured `mask_sql` expression. When a mask
/// filter is set, wraps the result in a `CASE WHEN filter THEN
/// original ELSE mask END`. Pass-through for non-masked members.
pub struct MaskedSqlNode {
    input: Rc<dyn SqlNode>,
    ungrouped: bool,
    // Full names of the members present in the query GROUP BY. Used to decide
    // whether conditional masking can be applied to an aggregate measure.
    group_by_members: HashSet<String>,
    // When true this node never applies masking and just delegates to `input`.
    // Used to build an "unmasked" copy of the whole processor tree that still
    // dispatches by member kind (see `unmasked_root`).
    skip: bool,
    // A kind-dispatching processor (a full `RootSqlNode`-based tree built in
    // `skip` mode) used to render mask-filter member references. It routes a
    // dimension reference through the dimension chain and a measure reference
    // through the measure chain, while skipping masking — so a measure mask
    // filter that references a dimension is rendered correctly and a filter
    // member that is itself masked does not recurse.
    unmasked_root: Option<Rc<dyn SqlNode>>,
}

impl MaskedSqlNode {
    pub fn new(
        input: Rc<dyn SqlNode>,
        group_by_members: HashSet<String>,
        skip: bool,
        unmasked_root: Option<Rc<dyn SqlNode>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            input,
            ungrouped: false,
            group_by_members,
            skip,
            unmasked_root,
        })
    }

    pub fn new_ungrouped(
        input: Rc<dyn SqlNode>,
        group_by_members: HashSet<String>,
        skip: bool,
        unmasked_root: Option<Rc<dyn SqlNode>>,
    ) -> Rc<Self> {
        Rc::new(Self {
            input,
            ungrouped: true,
            group_by_members,
            skip,
            unmasked_root,
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

        // Conditional masking renders `CASE WHEN filter THEN original ELSE mask END`.
        // For an aggregate (grouped) measure this produces invalid SQL on strict
        // GROUP BY engines when the filter references members that are not part of
        // the GROUP BY: the predicate is evaluated at row grain while the measure is
        // aggregated. The same row-level filter is already enforced in the query
        // WHERE clause, so we render the mask value directly for such measures. In
        // ungrouped queries the measure is rendered at row grain, so the CASE WHEN
        // is valid and is kept.
        if !self.ungrouped {
            if let MemberSymbol::Measure(_) = node.as_ref() {
                let filter_members = filter_item.all_member_evaluators();
                let all_in_group_by = !filter_members.is_empty()
                    && filter_members
                        .iter()
                        .all(|m| self.group_by_members.contains(&m.full_name()));
                if !all_in_group_by {
                    return Ok(Some(masked_sql));
                }
            }
        }

        let original_sql = self.input.to_sql(
            visitor,
            node,
            query_tools.clone(),
            node_processor,
            templates,
        )?;
        // TODO: support FILTER_PARAMS in mask filter SQL by passing
        // proper FiltersContext with filter_params_columns.
        // Render the filter through the unmasked, kind-dispatching root so a
        // filter member that is a dimension (e.g. an aggregate measure masked by
        // a row filter on a group-by dimension) is routed through the dimension
        // chain instead of the measure chain (which only accepts measures and
        // would otherwise error). `skip` masking also prevents recursion when the
        // filter member is itself masked. Falls back to `self.input` if no
        // unmasked root was wired in.
        let filter_processor = self
            .unmasked_root
            .clone()
            .unwrap_or_else(|| self.input.clone());
        let filter_sql = filter_item.to_sql(
            visitor,
            filter_processor,
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
        if !self.skip {
            if let Some(masked) = self.resolve_mask(
                node,
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            )? {
                return Ok(masked);
            }
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
