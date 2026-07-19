use super::ToSql;
use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::filter::{Filter, FilterItem};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::FiltersContext;
use cubenativeutils::CubeError;
use std::rc::Rc;

impl ToSql for FilterItem {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
        let res = match self {
            FilterItem::Group(group) => {
                let operator = format!(" {} ", group.operator.to_string());
                let items_sql = group
                    .items
                    .iter()
                    .map(|itm| {
                        itm.to_sql(
                            visitor,
                            node_processor.clone(),
                            query_tools.clone(),
                            templates,
                            filters_ctx,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|itm| !itm.is_empty())
                    .collect::<Vec<_>>();
                if items_sql.is_empty() {
                    "".to_string()
                } else {
                    let result = items_sql.join(&operator);
                    format!("({})", result)
                }
            }
            FilterItem::Item(item) => {
                let sql =
                    item.to_sql(visitor, node_processor, query_tools, templates, filters_ctx)?;
                format!("({})", sql)
            }
            FilterItem::Segment(item) => {
                let sql =
                    item.to_sql(visitor, node_processor, query_tools, templates, filters_ctx)?;
                format!("({})", sql)
            }
        };
        Ok(res)
    }
}

impl ToSql for Filter {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node_processor: Rc<dyn SqlNode>,
        query_tools: Rc<QueryTools>,
        templates: &PlanSqlTemplates,
        filters_ctx: &FiltersContext,
    ) -> Result<String, CubeError> {
        let res = self
            .items
            .iter()
            .map(|itm| {
                itm.to_sql(
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                    templates,
                    filters_ctx,
                )
            })
            .collect::<Result<Vec<_>, _>>()?
            .join(" AND ");
        Ok(res)
    }
}
