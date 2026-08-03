use crate::{
    compile::rewrite::{
        analysis::Member,
        column_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, ColumnExprColumn, LogicalPlanLanguage,
        WrapperReplacerContextAliasToCube, WrapperReplacerContextGroupedSubqueries,
    },
    var, var_iter,
};
use egg::Subst;

impl WrapperRules {
    pub fn column_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-column",
                wrapper_pushdown_replacer(
                    column_expr("?name"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "WrapperReplacerContextPushToCube:false",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                wrapper_pullup_replacer(
                    column_expr("?name"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "WrapperReplacerContextPushToCube:false",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
            ),
            // TODO This is half measure implementation to propagate ungrouped simple measure towards aggregate node that easily allow replacement of aggregation functions
            // We need to support it for complex aka `number` measures
            transforming_rewrite(
                "wrapper-push-down-column-simple-measure-in-projection",
                wrapper_pushdown_replacer(
                    column_expr("?name"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "WrapperReplacerContextPushToCube:true",
                        "WrapperReplacerContextInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                wrapper_pullup_replacer(
                    column_expr("?name"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "WrapperReplacerContextPushToCube:true",
                        "WrapperReplacerContextInProjection:true",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                self.pushdown_simple_measure("?name", "?cube_members"),
            ),
            // TODO time dimension support
            transforming_rewrite(
                "wrapper-push-down-dimension",
                wrapper_pushdown_replacer(
                    column_expr("?name"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "WrapperReplacerContextPushToCube:true",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                wrapper_pullup_replacer(
                    "?dimension",
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "WrapperReplacerContextPushToCube:true",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                self.pushdown_dimension(
                    "?alias_to_cube",
                    "?name",
                    "?cube_members",
                    "?dimension",
                    "?grouped_subqueries",
                ),
            ),
        ]);
    }

    fn pushdown_dimension(
        &self,
        alias_to_cube_var: &'static str,
        column_name_var: &'static str,
        members_var: &'static str,
        dimension_var: &'static str,
        grouped_subqueries_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let alias_to_cube_var = var!(alias_to_cube_var);
        let column_name_var = var!(column_name_var);
        let members_var = var!(members_var);
        let dimension_var = var!(dimension_var);
        let grouped_subqueries_var = var!(grouped_subqueries_var);
        move |egraph, subst| {
            let columns: Vec<_> = var_iter!(egraph[subst[column_name_var]], ColumnExprColumn)
                .cloned()
                .collect();
            for column in columns.iter() {
                for alias_to_cube in var_iter!(
                    egraph[subst[alias_to_cube_var]],
                    WrapperReplacerContextAliasToCube
                )
                .cloned()
                {
                    //FIXME We always add subquery column as dimension. I'm not 100% sure that this is the correct solution
                    if let Some(col_relation) = &column.relation {
                        if &alias_to_cube[0].0 != col_relation
                            && col_relation.starts_with("__subquery")
                        {
                            let column_expr_column =
                                egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                    ColumnExprColumn(column.clone()),
                                ));

                            let column_expr =
                                egraph.add(LogicalPlanLanguage::ColumnExpr([column_expr_column]));
                            subst.insert(dimension_var, column_expr);
                            return true;
                        }
                    }
                }

                // Treat any column from grouped subquery as dimension, and pullup even when push to cube is enabled
                // Column expressions can refer to grouped queries even without explicit relation
                // TODO implement proper name resolution here
                if let Some(col_relation) = &column.relation {
                    for grouped_subqueries in var_iter!(
                        egraph[subst[grouped_subqueries_var]],
                        WrapperReplacerContextGroupedSubqueries
                    ) {
                        if grouped_subqueries.iter().any(|subq| subq == col_relation) {
                            // Found grouped subquery, can "replace" column with itself
                            let column_expr_column =
                                egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                                    ColumnExprColumn(column.clone()),
                                ));

                            let column_expr =
                                egraph.add(LogicalPlanLanguage::ColumnExpr([column_expr_column]));
                            subst.insert(dimension_var, column_expr);
                            return true;
                        }
                    }
                }

                if let Some((member, _)) = &egraph[subst[members_var]]
                    .data
                    .find_member_by_alias(&column.name)
                {
                    if matches!(
                        member.1,
                        Member::Dimension { .. }
                            | Member::TimeDimension { .. }
                            | Member::Segment { .. }
                            | Member::ChangeUser { .. }
                            | Member::VirtualField { .. }
                            | Member::LiteralMember { .. }
                    ) {
                        let column_expr_column = egraph.add(LogicalPlanLanguage::ColumnExprColumn(
                            ColumnExprColumn(column.clone()),
                        ));

                        let column_expr =
                            egraph.add(LogicalPlanLanguage::ColumnExpr([column_expr_column]));
                        subst.insert(dimension_var, column_expr);
                        return true;
                    }
                }
            }
            false
        }
    }

    fn pushdown_simple_measure(
        &self,
        column_name_var: &'static str,
        members_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let column_name_var = var!(column_name_var);
        let members_var = var!(members_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let columns: Vec<_> = var_iter!(egraph[subst[column_name_var]], ColumnExprColumn)
                .cloned()
                .collect();
            for column in columns {
                if let Some(((Some(member), _, _), _)) = egraph[subst[members_var]]
                    .data
                    .find_member_by_alias(&column.name)
                {
                    if let Some(measure) = meta.find_measure_with_name(member) {
                        if measure.agg_type != Some("number".to_string()) {
                            return true;
                        }
                    }
                }
            }
            false
        }
    }
}
