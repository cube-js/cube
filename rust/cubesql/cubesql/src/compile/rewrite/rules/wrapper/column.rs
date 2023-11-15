use crate::{
    compile::rewrite::{
        analysis::LogicalPlanAnalysis, column_expr, column_name_to_member_vec, rewrite,
        rules::wrapper::WrapperRules, transforming_rewrite, wrapper_pullup_replacer,
        wrapper_pushdown_replacer, ColumnExprColumn, LogicalPlanLanguage,
    },
    var, var_iter,
};
use egg::{EGraph, Rewrite, Subst};

impl WrapperRules {
    pub fn column_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![
            rewrite(
                "wrapper-push-down-column",
                wrapper_pushdown_replacer(
                    column_expr("?name"),
                    "?alias_to_cube",
                    "WrapperPullupReplacerUngrouped:false",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    column_expr("?name"),
                    "?alias_to_cube",
                    "WrapperPullupReplacerUngrouped:false",
                    "?cube_members",
                ),
            ),
            // TODO time dimension support
            transforming_rewrite(
                "wrapper-push-down-dimension",
                wrapper_pushdown_replacer(
                    column_expr("?name"),
                    "?alias_to_cube",
                    "WrapperPullupReplacerUngrouped:true",
                    "?cube_members",
                ),
                wrapper_pullup_replacer(
                    "?dimension",
                    "?alias_to_cube",
                    "WrapperPullupReplacerUngrouped:true",
                    "?cube_members",
                ),
                self.pushdown_dimension("?name", "?cube_members", "?dimension"),
            ),
        ]);
    }

    fn pushdown_dimension(
        &self,
        column_name_var: &'static str,
        members_var: &'static str,
        dimension_var: &'static str,
    ) -> impl Fn(&mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut Subst) -> bool {
        let column_name_var = var!(column_name_var);
        let members_var = var!(members_var);
        let dimension_var = var!(dimension_var);
        let cube_context = self.cube_context.clone();
        move |egraph, subst| {
            for column in var_iter!(egraph[subst[column_name_var]], ColumnExprColumn).cloned() {
                if let Some(member_name_to_expr) =
                    egraph[subst[members_var]].data.member_name_to_expr.clone()
                {
                    let column_name_to_member_name = column_name_to_member_vec(member_name_to_expr);
                    if let Some((_, Some(member))) = column_name_to_member_name
                        .iter()
                        .find(|(cn, _)| cn == &column.name)
                    {
                        if cube_context
                            .meta
                            .find_dimension_with_name(member.to_string())
                            .is_some()
                            || cube_context.meta.is_synthetic_field(member.to_string())
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
            }
            false
        }
    }
}
