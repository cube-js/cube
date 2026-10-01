use crate::{
    compile::rewrite::{
        analysis::{Member, OriginalExpr},
        binary_expr, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, wrapper_pullup_replacer, wrapper_pushdown_replacer,
        wrapper_replacer_context, BinaryExprOp, WrapperReplacerContextAliasToCube,
    },
    transport::SqlTemplates,
    var, var_iter,
};
use datafusion::{
    arrow::datatypes::DataType,
    logical_plan::{DFField, DFSchema, ExprSchemable, Operator},
    optimizer::utils::expr_to_columns,
};
use egg::Subst;
use std::ops::ControlFlow;

impl WrapperRules {
    pub fn binary_expr_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-binary-expr",
                wrapper_pushdown_replacer(binary_expr("?left", "?op", "?right"), "?context"),
                binary_expr(
                    wrapper_pushdown_replacer("?left", "?context"),
                    "?op",
                    wrapper_pushdown_replacer("?right", "?context"),
                ),
            ),
            transforming_rewrite(
                "wrapper-pull-up-binary-expr",
                binary_expr(
                    wrapper_pullup_replacer(
                        "?left",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                    "?op",
                    wrapper_pullup_replacer(
                        "?right",
                        wrapper_replacer_context(
                            "?alias_to_cube",
                            "?push_to_cube",
                            "?in_projection",
                            "?cube_members",
                            "?grouped_subqueries",
                            "?ungrouped_scan",
                            "?input_data_source",
                        ),
                    ),
                ),
                wrapper_pullup_replacer(
                    binary_expr("?left", "?op", "?right"),
                    wrapper_replacer_context(
                        "?alias_to_cube",
                        "?push_to_cube",
                        "?in_projection",
                        "?cube_members",
                        "?grouped_subqueries",
                        "?ungrouped_scan",
                        "?input_data_source",
                    ),
                ),
                self.transform_binary_expr(
                    "?op",
                    "?input_data_source",
                    "?left",
                    "?right",
                    "?cube_members",
                    "?alias_to_cube",
                ),
            ),
        ]);
    }

    fn transform_binary_expr(
        &self,
        operator_var: &'static str,
        input_data_source_var: &'static str,
        left_var: &'static str,
        right_var: &'static str,
        members_var: &'static str,
        alias_to_cube_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let operator_var = var!(operator_var);
        let input_data_source_var = var!(input_data_source_var);
        let left_var = var!(left_var);
        let right_var = var!(right_var);
        let members_var = var!(members_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        move |egraph, subst| {
            let Ok(data_source) = Self::get_data_source(egraph, subst, input_data_source_var)
            else {
                return false;
            };

            if !Self::can_rewrite_template(&data_source, &meta, "expressions/binary") {
                return false;
            }

            for op in var_iter!(egraph[subst[operator_var]], BinaryExprOp)
                .cloned()
                .collect::<Vec<_>>()
            {
                match op {
                    Operator::Modulo => {
                        let supports_float = |templates: &SqlTemplates| {
                            templates.contains_template("operators/float_modulo")
                        };
                        let supported = match Self::template_sql_generator(&data_source, &meta) {
                            ControlFlow::Continue(generator) => {
                                supports_float(&generator.get_sql_templates())
                            }
                            ControlFlow::Break(true) => {
                                !meta.data_source_to_sql_generator.is_empty()
                                    && meta.data_source_to_sql_generator.values().all(|generator| {
                                        supports_float(&generator.get_sql_templates())
                                    })
                            }
                            ControlFlow::Break(false) => false,
                        };
                        if supported {
                            return true;
                        }
                        let Some(aliases) = var_iter!(
                            egraph[subst[alias_to_cube_var]],
                            WrapperReplacerContextAliasToCube
                        )
                        .next()
                        .cloned() else {
                            return false;
                        };
                        // Numeric-only dialects require floating remainder to stay local.
                        let supported_type = |data_type| {
                            matches!(
                                data_type,
                                DataType::Int8
                                    | DataType::Int16
                                    | DataType::Int32
                                    | DataType::Int64
                                    | DataType::UInt8
                                    | DataType::UInt16
                                    | DataType::UInt32
                                    | DataType::UInt64
                                    | DataType::Decimal(_, _)
                            )
                        };
                        return [left_var, right_var].iter().all(|operand| {
                            let Some(OriginalExpr::Expr(expr)) =
                                egraph[subst[*operand]].data.original_expr.clone()
                            else {
                                return false;
                            };
                            if let Ok(data_type) = expr.get_type(&DFSchema::empty()) {
                                return supported_type(data_type);
                            }
                            let mut columns = std::collections::HashSet::new();
                            if expr_to_columns(&expr, &mut columns).is_err() {
                                return false;
                            }
                            let fields = columns
                                .iter()
                                .map(|column| {
                                    let data_type = egraph[subst[members_var]]
                                        .data
                                        .find_member_by_column(column)
                                        .and_then(|((_, member, _), _)| match member {
                                            Member::LiteralMember { value, .. } => {
                                                Some(value.get_datatype())
                                            }
                                            Member::Measure { name, .. }
                                                if meta
                                                    .find_measure_with_name(name)
                                                    .is_some_and(|measure| {
                                                        matches!(
                                                            measure.agg_type.as_deref(),
                                                            Some(
                                                                "count"
                                                                    | "countDistinct"
                                                                    | "countDistinctApprox"
                                                            )
                                                        )
                                                    }) =>
                                            {
                                                Some(DataType::Int64)
                                            }
                                            _ => member
                                                .name()
                                                .and_then(|name| meta.find_df_data_type(name)),
                                        })
                                        .or_else(|| {
                                            meta.find_cube_by_column(&aliases, column).and_then(
                                                |(_, cube)| {
                                                    meta.find_df_data_type(&format!(
                                                        "{}.{}",
                                                        cube.name, column.name
                                                    ))
                                                },
                                            )
                                        })?;
                                    Some(DFField::new(
                                        column.relation.as_deref(),
                                        &column.name,
                                        data_type,
                                        true,
                                    ))
                                })
                                .collect::<Option<Vec<_>>>();
                            let Some(fields) = fields else {
                                return false;
                            };
                            let Ok(schema) =
                                DFSchema::new_with_metadata(fields, Default::default())
                            else {
                                return false;
                            };
                            expr.get_type(&schema).map(supported_type).unwrap_or(false)
                        });
                    }
                    Operator::Like | Operator::NotLike => {
                        if Self::can_rewrite_template(&data_source, &meta, "expressions/like") {
                            return true;
                        }
                    }
                    Operator::ILike | Operator::NotILike => {
                        if Self::can_rewrite_template(&data_source, &meta, "expressions/ilike") {
                            return true;
                        }
                    }
                    _ => return true,
                }
            }

            false
        }
    }
}
