use crate::compile::engine::provider::CubeContext;
use crate::compile::rewrite::analysis::LogicalPlanAnalysis;
use crate::compile::rewrite::rewriter::RewriteRules;
use crate::compile::rewrite::LogicalPlanLanguage;
use crate::compile::rewrite::OrderAsc;
use crate::compile::rewrite::OrderMember;
use crate::compile::rewrite::OrderReplacerColumnNameToMember;
use crate::compile::rewrite::OrderReplacerCube;
use crate::compile::rewrite::SortExprAsc;
use crate::compile::rewrite::TableScanSourceTableName;
use crate::compile::rewrite::{
    column_name_to_member_name, cube_scan_order, cube_scan_order_empty_tail, expr_column_name,
    order, order_replacer, referenced_columns, sort, sort_exp, sort_exp_empty_tail, sort_expr,
};
use crate::compile::rewrite::{cube_scan, rewrite, transforming_rewrite};
use crate::var;
use crate::var_iter;
use egg::{EGraph, Rewrite, Subst};
use std::ops::Index;
use std::sync::Arc;

pub struct OrderRules {
    _cube_context: Arc<CubeContext>,
}

impl RewriteRules for OrderRules {
    fn rewrite_rules(&self) -> Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>> {
        vec![
            transforming_rewrite(
                "push-down-sort",
                sort(
                    "?expr",
                    cube_scan(
                        "?source_table_name",
                        "?members",
                        "?filters",
                        "CubeScanOrder",
                        "?limit",
                        "?offset",
                        "?cube_aliases",
                        "?table_name",
                    ),
                ),
                cube_scan(
                    "?source_table_name",
                    "?members",
                    "?filters",
                    order_replacer("?expr", "?aliases", "?cube"),
                    "?limit",
                    "?offset",
                    "?cube_aliases",
                    "?table_name",
                ),
                self.push_down_sort(
                    "?source_table_name",
                    "?expr",
                    "?members",
                    "?aliases",
                    "?cube",
                ),
            ),
            transforming_rewrite(
                "order-replacer",
                order_replacer(
                    sort_exp(
                        sort_expr("?expr", "?asc", "?nulls_first"),
                        "?tail_group_expr",
                    ),
                    "?aliases",
                    "?cube",
                ),
                cube_scan_order(
                    order("?order_member", "?order_asc"),
                    order_replacer("?tail_group_expr", "?aliases", "?cube"),
                ),
                self.transform_order(
                    "?expr",
                    "?asc",
                    "?aliases",
                    "?cube",
                    "?order_member",
                    "?order_asc",
                ),
            ),
            rewrite(
                "order-replacer-tail-proj",
                order_replacer(sort_exp_empty_tail(), "?aliases", "?cube"),
                cube_scan_order_empty_tail(),
            ),
        ]
    }
}

impl OrderRules {
    pub fn new(cube_context: Arc<CubeContext>) -> Self {
        Self {
            _cube_context: cube_context,
        }
    }

    fn push_down_sort(
        &self,
        table_name_var: &'static str,
        sort_exp_var: &'static str,
        members_var: &'static str,
        aliases_var: &'static str,
        cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let table_name_var = var!(table_name_var);
        let sort_exp_var = var!(sort_exp_var);
        let members_var = var!(members_var);
        let aliases_var = var!(aliases_var);
        let cube_var = var!(cube_var);
        move |egraph, subst| {
            for table_name in var_iter!(egraph[subst[table_name_var]], TableScanSourceTableName) {
                if let Some(referenced_expr) =
                    &egraph.index(subst[sort_exp_var]).data.referenced_expr
                {
                    if let Some(member_name_to_expr) = egraph
                        .index(subst[members_var])
                        .data
                        .member_name_to_expr
                        .clone()
                    {
                        let column_name_to_member_name =
                            column_name_to_member_name(member_name_to_expr, table_name.to_string());
                        let referenced_columns =
                            referenced_columns(referenced_expr.clone(), table_name.to_string());
                        let table_name = table_name.to_string();
                        if referenced_columns
                            .iter()
                            .all(|c| column_name_to_member_name.contains_key(c))
                        {
                            subst.insert(
                                aliases_var,
                                egraph.add(LogicalPlanLanguage::OrderReplacerColumnNameToMember(
                                    OrderReplacerColumnNameToMember(
                                        column_name_to_member_name.into_iter().collect(),
                                    ),
                                )),
                            );

                            subst.insert(
                                cube_var,
                                egraph.add(LogicalPlanLanguage::OrderReplacerCube(
                                    OrderReplacerCube(Some(table_name)),
                                )),
                            );
                            return true;
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_order(
        &self,
        expr_var: &'static str,
        asc_var: &'static str,
        column_name_to_member_var: &'static str,
        cube_var: &'static str,
        order_member_var: &'static str,
        order_asc_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = expr_var.parse().unwrap();
        let asc_var = asc_var.parse().unwrap();
        let column_name_to_member_var = column_name_to_member_var.parse().unwrap();
        let order_member_var = order_member_var.parse().unwrap();
        let order_asc_var = order_asc_var.parse().unwrap();
        let cube_var = cube_var.parse().unwrap();
        move |egraph, subst| {
            let expr = egraph[subst[expr_var]]
                .data
                .original_expr
                .as_ref()
                .expect(&format!(
                    "Original expr wasn't prepared for {:?}",
                    egraph[subst[expr_var]]
                ));
            for cube in var_iter!(egraph[subst[cube_var]], OrderReplacerCube) {
                let column_name = expr_column_name(expr.clone(), &cube);
                for asc in var_iter!(egraph[subst[asc_var]], SortExprAsc) {
                    let asc = *asc;
                    for column_name_to_member in var_iter!(
                        egraph[subst[column_name_to_member_var]],
                        OrderReplacerColumnNameToMember
                    ) {
                        if let Some((_, member_name)) = column_name_to_member
                            .iter()
                            .find(|(c, _)| c == &column_name)
                        {
                            let member_name = member_name.to_string();
                            subst.insert(
                                order_member_var,
                                egraph.add(LogicalPlanLanguage::OrderMember(OrderMember(
                                    member_name.to_string(),
                                ))),
                            );

                            subst.insert(
                                order_asc_var,
                                egraph.add(LogicalPlanLanguage::OrderAsc(OrderAsc(asc))),
                            );
                            return true;
                        }
                    }
                }
            }

            false
        }
    }
}
