use crate::{
    compile::rewrite::{
        aggregate_split_pullup_replacer, aggregate_split_pushdown_replacer,
        analysis::{LogicalPlanAnalysis, OriginalExpr},
        cast_expr, projection_split_pullup_replacer, projection_split_pushdown_replacer, rewrite,
        rules::split::SplitRules,
        transforming_rewrite, AliasExprAlias, CastExprDataType, LiteralExprValue,
        LogicalPlanLanguage, ScalarFunctionExprFun,
    },
    var, var_iter, CubeError,
};
use datafusion::{
    arrow::datatypes::DataType as ArrowDataType,
    logical_plan::{DFSchema, Expr},
    physical_plan::functions::BuiltinScalarFunction,
    scalar::ScalarValue,
};
use egg::{EGraph, Rewrite, Subst};

impl SplitRules {
    pub fn cast_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend([
            transforming_rewrite(
                "split-cast-push-down-aggregate",
                aggregate_split_pushdown_replacer(
                    cast_expr("?expr", "?data_type"),
                    "?list_node",
                    "?alias_to_cube",
                ),
                cast_expr(
                    aggregate_split_pushdown_replacer("?new_expr", "?list_node", "?alias_to_cube"),
                    "?data_type",
                ),
                self.transform_cast("?expr", "?data_type", "?new_expr"),
            ),
            rewrite(
                "split-cast-pull-up-aggregate",
                cast_expr(
                    aggregate_split_pullup_replacer(
                        "?inner_expr",
                        "?outer_expr",
                        "?list_node",
                        "?alias_to_cube",
                    ),
                    "?data_type",
                ),
                aggregate_split_pullup_replacer(
                    "?inner_expr",
                    cast_expr("?outer_expr", "?data_type"),
                    "?list_node",
                    "?alias_to_cube",
                ),
            ),
            transforming_rewrite(
                "split-cast-push-down-projection",
                projection_split_pushdown_replacer(
                    cast_expr("?expr", "?data_type"),
                    "?list_node",
                    "?alias_to_cube",
                ),
                cast_expr(
                    projection_split_pushdown_replacer("?new_expr", "?list_node", "?alias_to_cube"),
                    "?data_type",
                ),
                self.transform_cast("?expr", "?data_type", "?new_expr"),
            ),
            rewrite(
                "split-cast-pull-up-projection",
                cast_expr(
                    projection_split_pullup_replacer(
                        "?inner_expr",
                        "?outer_expr",
                        "?list_node",
                        "?alias_to_cube",
                    ),
                    "?data_type",
                ),
                projection_split_pullup_replacer(
                    "?inner_expr",
                    cast_expr("?outer_expr", "?data_type"),
                    "?list_node",
                    "?alias_to_cube",
                ),
            ),
        ]);
    }

    fn transform_cast(
        &self,
        expr_var: &'static str,
        data_type_var: &'static str,
        new_expr_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let expr_var = var!(expr_var);
        let data_type_var = var!(data_type_var);
        let new_expr_var = var!(new_expr_var);
        move |egraph, subst| {
            let expr_id = subst[expr_var];
            let res = egraph[expr_id]
                .data
                .original_expr
                .as_ref()
                .ok_or(CubeError::internal(format!(
                    "Original expr wasn't prepared for {:?}",
                    expr_id
                )));

            if let Ok(OriginalExpr::Expr(expr)) = res {
                match expr {
                    Expr::Column(_) => {
                        for data_type in var_iter!(egraph[subst[data_type_var]], CastExprDataType) {
                            if data_type == &ArrowDataType::Date32 {
                                let name = expr.name(&DFSchema::empty()).unwrap();
                                let granularity_value_id = egraph.add(
                                    LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(
                                        ScalarValue::Utf8(Some("day".to_string())),
                                    )),
                                );
                                let granularity_id = egraph
                                    .add(LogicalPlanLanguage::LiteralExpr([granularity_value_id]));
                                let date_trunc_args_empty_tail_id =
                                    egraph.add(LogicalPlanLanguage::ScalarFunctionExprArgs(vec![]));
                                let date_trunc_args_column_id =
                                    egraph.add(LogicalPlanLanguage::ScalarFunctionExprArgs(vec![
                                        subst[expr_var],
                                        date_trunc_args_empty_tail_id,
                                    ]));
                                let date_trunc_args_id =
                                    egraph.add(LogicalPlanLanguage::ScalarFunctionExprArgs(vec![
                                        granularity_id,
                                        date_trunc_args_column_id,
                                    ]));
                                let date_trunc_name_id =
                                    egraph.add(LogicalPlanLanguage::ScalarFunctionExprFun(
                                        ScalarFunctionExprFun(BuiltinScalarFunction::DateTrunc),
                                    ));
                                let date_trunc_id =
                                    egraph.add(LogicalPlanLanguage::ScalarFunctionExpr([
                                        date_trunc_name_id,
                                        date_trunc_args_id,
                                    ]));
                                let alias_id = egraph
                                    .add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(name)));
                                let alias_expr_id = egraph
                                    .add(LogicalPlanLanguage::AliasExpr([date_trunc_id, alias_id]));

                                subst.insert(new_expr_var, alias_expr_id);
                                return true;
                            }
                        }
                    }
                    _ => (),
                }
            }

            subst.insert(new_expr_var, subst[expr_var]);
            true
        }
    }
}
