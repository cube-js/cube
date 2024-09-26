use crate::{
    compile::rewrite::{
        agg_fun_expr, alias_expr, analysis::LogicalPlanAnalysis, column_expr, original_expr_name,
        rewrite, rules::wrapper::WrapperRules, transforming_chain_rewrite, transforming_rewrite,
        udaf_expr, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        AggregateFunctionExprDistinct, AggregateFunctionExprFun, AggregateUDFExprFun,
        AliasExprAlias, ColumnExprColumn, LogicalPlanLanguage, WrapperPullupReplacerAliasToCube,
    },
    transport::V1CubeMetaExt,
    var, var_iter,
};
use datafusion::physical_plan::aggregates::AggregateFunction;
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn window_function_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-aggregate-function",
                wrapper_pushdown_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pushdown_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    )],
                    "?distinct",
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-aggregate-function",
                agg_fun_expr(
                    "?fun",
                    vec![wrapper_pullup_replacer(
                        "?expr",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    )],
                    "?distinct",
                ),
                wrapper_pullup_replacer(
                    agg_fun_expr("?fun", vec!["?expr"], "?distinct"),
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                self.transform_agg_fun_expr("?fun", "?distinct", "?alias_to_cube"),
            ),
            transforming_chain_rewrite(
                "wrapper-push-down-measure-aggregate-function",
                wrapper_pushdown_replacer(
                    "?udaf",
                    "?alias_to_cube",
                    "?ungrouped",
                    "?in_projection",
                    "?cube_members",
                ),
                vec![("?udaf", udaf_expr("?fun", vec![column_expr("?column")]))],
                alias_expr(
                    wrapper_pushdown_replacer(
                        "?output",
                        "?alias_to_cube",
                        "?ungrouped",
                        "?in_projection",
                        "?cube_members",
                    ),
                    "?alias",
                ),
                self.transform_measure_udaf_expr(
                    "?udaf",
                    "?fun",
                    "?column",
                    "?alias_to_cube",
                    "?output",
                    "?alias",
                ),
            ),
        ]);
    }

    fn transform_agg_fun_expr(
        &self,
        fun_var: &'static str,
        distinct_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let fun_var = var!(fun_var);
        let distinct_var = var!(distinct_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                WrapperPullupReplacerAliasToCube
            )
            .cloned()
            {
                if let Some(sql_generator) = meta.sql_generator_by_alias_to_cube(&alias_to_cube) {
                    for fun in var_iter!(egraph[subst[fun_var]], AggregateFunctionExprFun).cloned()
                    {
                        for distinct in
                            var_iter!(egraph[subst[distinct_var]], AggregateFunctionExprDistinct)
                        {
                            let fun = if *distinct && fun == AggregateFunction::Count {
                                "COUNT_DISTINCT".to_string()
                            } else {
                                fun.to_string()
                            };

                            if sql_generator
                                .get_sql_templates()
                                .templates
                                .contains_key(&format!("functions/{}", fun.as_str()))
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            false
        }
    }

    fn transform_measure_udaf_expr(
        &self,
        udaf_var: &'static str,
        fun_var: &'static str,
        column_var: &'static str,
        alias_to_cube_var: &'static str,
        output_var: &'static str,
        alias_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let udaf_var = var!(udaf_var);
        let fun_var = var!(fun_var);
        let column_var = var!(column_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let output_var = var!(output_var);
        let alias_var = var!(alias_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Some(original_alias) = original_expr_name(egraph, subst[udaf_var]) else {
                return false;
            };

            for fun in var_iter!(egraph[subst[fun_var]], AggregateUDFExprFun) {
                if fun.to_lowercase() != "measure" {
                    continue;
                }

                for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn) {
                    for alias_to_cube in var_iter!(
                        egraph[subst[alias_to_cube_var]],
                        WrapperPullupReplacerAliasToCube
                    ) {
                        let Some((_, cube)) = meta.find_cube_by_column(alias_to_cube, column)
                        else {
                            continue;
                        };

                        let Some(measure) = cube.lookup_measure(&column.name) else {
                            continue;
                        };

                        let Some(agg_type) = &measure.agg_type else {
                            continue;
                        };

                        let out_fun_distinct = match agg_type.as_str() {
                            "string" | "time" | "boolean" | "number" => None,
                            "count" => Some((AggregateFunction::Count, false)),
                            "countDistinct" => Some((AggregateFunction::Count, true)),
                            "countDistinctApprox" => {
                                Some((AggregateFunction::ApproxDistinct, false))
                            }
                            "sum" => Some((AggregateFunction::Sum, false)),
                            "avg" => Some((AggregateFunction::Avg, false)),
                            "min" => Some((AggregateFunction::Min, false)),
                            "max" => Some((AggregateFunction::Max, false)),
                            _ => continue,
                        };

                        let column_expr_id =
                            egraph.add(LogicalPlanLanguage::ColumnExpr([subst[column_var]]));

                        let output_id = out_fun_distinct
                            .map(|(out_fun, distinct)| {
                                let fun_id =
                                    egraph.add(LogicalPlanLanguage::AggregateFunctionExprFun(
                                        AggregateFunctionExprFun(out_fun),
                                    ));
                                let args_tail_id = egraph
                                    .add(LogicalPlanLanguage::AggregateFunctionExprArgs(vec![]));
                                let args_id =
                                    egraph.add(LogicalPlanLanguage::AggregateFunctionExprArgs(
                                        vec![column_expr_id, args_tail_id],
                                    ));
                                let distinct_id =
                                    egraph.add(LogicalPlanLanguage::AggregateFunctionExprDistinct(
                                        AggregateFunctionExprDistinct(distinct),
                                    ));

                                egraph.add(LogicalPlanLanguage::AggregateFunctionExpr([
                                    fun_id,
                                    args_id,
                                    distinct_id,
                                ]))
                            })
                            .unwrap_or(column_expr_id);

                        subst.insert(output_var, output_id);

                        subst.insert(
                            alias_var,
                            egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                                original_alias,
                            ))),
                        );
                        return true;
                    }
                }
            }
            false
        }
    }
}
