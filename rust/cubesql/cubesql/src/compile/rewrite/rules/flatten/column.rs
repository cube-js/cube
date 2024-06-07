use crate::{
    compile::rewrite::{
        alias_expr, analysis::LogicalPlanAnalysis, column_expr,
        converter::LogicalPlanToLanguageConverter, flatten_pushdown_replacer, literal_expr,
        rewrite, rules::flatten::FlattenRules, transforming_rewrite, AliasExprAlias,
        ColumnExprColumn, FlattenPushdownReplacerTopLevel, LogicalPlanLanguage,
    },
    var, var_iter, CubeError,
};
use egg::Rewrite;

impl FlattenRules {
    pub fn column_rules(&self, rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>) {
        rules.extend(vec![
            transforming_rewrite(
                "flatten-column-pushdown",
                flatten_pushdown_replacer(
                    column_expr("?column"),
                    "?inner_expr",
                    "?inner_alias",
                    "?top_level",
                ),
                alias_expr("?out_expr", "?column_alias"),
                self.flatten_column(
                    "?column",
                    "?top_level",
                    "?inner_expr",
                    "?inner_alias",
                    "?column_alias",
                    "?out_expr",
                ),
            ),
            rewrite(
                "flatten-literal",
                flatten_pushdown_replacer(
                    literal_expr("?literal"),
                    "?inner_expr",
                    "?inner_alias",
                    "?top_level",
                ),
                literal_expr("?literal"),
            ),
        ])
    }

    fn flatten_column(
        &self,
        column_var: &str,
        top_level_var: &str,
        inner_expr_var: &str,
        _inner_alias_var: &str,
        column_alias_var: &str,
        out_expr_var: &str,
    ) -> impl Fn(&mut egg::EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>, &mut egg::Subst) -> bool
    {
        let column_var = var!(column_var);
        let top_level_var = var!(top_level_var);
        let inner_expr_var = var!(inner_expr_var);
        // let inner_alias_var = var!(inner_alias_var);
        let column_alias_var = var!(column_alias_var);
        let out_expr_var = var!(out_expr_var);
        let flat_list = self.config_obj.push_down_pull_up_split();
        move |egraph, subst| {
            for column in var_iter!(egraph[subst[column_var]], ColumnExprColumn).cloned() {
                for top_level in var_iter!(
                    egraph[subst[top_level_var]],
                    FlattenPushdownReplacerTopLevel
                )
                .cloned()
                {
                    if let Some(expr_to_alias) =
                        egraph[subst[inner_expr_var]].data.expr_to_alias.clone()
                    {
                        // TODO support full qualified column
                        if let Some((expr, _, _)) = expr_to_alias
                            .iter()
                            .find(|(_, alias, _)| alias == &column.name)
                        {
                            // Currently there are no cases where it can fail when adding expression
                            let output_expr = LogicalPlanToLanguageConverter::add_expr(
                                egraph,
                                &expr,
                                flat_list,
                            )
                            .map_err(|e| {
                                CubeError::internal(format!(
                                    "FlattenColumnPushdown: Can't add expression to egraph: {:?}: {}",
                                    expr, e
                                ))
                            })
                            .unwrap();
                            let alias = egraph.add(LogicalPlanLanguage::AliasExprAlias(
                                AliasExprAlias(if top_level {
                                    column.name.to_string()
                                } else {
                                    column.flat_name()
                                }),
                            ));
                            subst.insert(column_alias_var, alias);
                            subst.insert(out_expr_var, output_expr);
                            return true;
                        }
                    }
                }
            }
            false
        }
    }
}
