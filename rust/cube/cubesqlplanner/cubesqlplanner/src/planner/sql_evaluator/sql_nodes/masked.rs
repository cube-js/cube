use super::SqlNode;
use crate::cube_bridge::base_query_options::FilterItem as NativeFilterItem;
use crate::plan::filter::FilterItem;
use crate::planner::filter::compiler::FilterCompiler;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
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
                node_processor,
                templates,
            )?;
            let filter_sql =
                self.compile_filter_to_sql(&filter_item, query_tools.clone(), templates)?;
            if let Some(filter_sql) = filter_sql {
                Ok(Some(format!(
                    "CASE WHEN {} THEN {} ELSE {} END",
                    filter_sql, original_sql, masked_sql
                )))
            } else {
                Ok(Some(masked_sql))
            }
        } else {
            Ok(Some(masked_sql))
        }
    }

    fn compile_filter_to_sql(
        &self,
        native_filter: &NativeFilterItem,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
    ) -> Result<Option<String>, CubeError> {
        let filter_item = {
            let mut compiler = query_tools.evaluator_compiler().borrow_mut();
            let mut filter_compiler = FilterCompiler::new(&mut compiler, query_tools.clone());
            filter_compiler.add_item(native_filter)?;
            let (dimension_filters, _, _) = filter_compiler.extract_result();
            if dimension_filters.is_empty() {
                return Ok(None);
            }
            if dimension_filters.len() == 1 {
                dimension_filters.into_iter().next().unwrap()
            } else {
                FilterItem::Group(Rc::new(crate::plan::filter::FilterGroup::new(
                    crate::plan::filter::FilterGroupOperator::And,
                    dimension_filters,
                )))
            }
        };
        let context = Rc::new(VisitorContext::new_with_node_processor(
            query_tools.clone(),
            self.input.clone(),
        ));
        let sql = filter_item.to_sql(templates, context)?;
        if sql.is_empty() {
            Ok(None)
        } else {
            Ok(Some(sql))
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
