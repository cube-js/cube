use crate::{
    compile::rewrite::{
        agg_fun_expr,
        analysis::LogicalPlanAnalysis,
        column_expr, literal_expr,
        rules::{members::MemberRules, split::SplitRules},
        AggregateFunctionExprDistinct, AggregateFunctionExprFun,
        AggregateSplitPushDownReplacerAliasToCube, ColumnExprColumn, LogicalPlanLanguage,
        ProjectionSplitPushDownReplacerAliasToCube,
    },
    transport::V1CubeMetaExt,
    var, var_iter,
};
use datafusion::{logical_plan::Column, physical_plan::aggregates::AggregateFunction};
use egg::Rewrite;

impl SplitRules {
    pub fn aggregate_function_rules(
        &self,
        rules: &mut Vec<Rewrite<LogicalPlanLanguage, LogicalPlanAnalysis>>,
    ) {
        self.single_arg_split_point_rules(
            "aggregate-function",
            || agg_fun_expr("?fun_name", vec![column_expr("?column")], "?distinct"),
            || agg_fun_expr("?fun_name", vec![column_expr("?column")], "?distinct"),
            // ?distinct would always match
            |alias_column| agg_fun_expr("?output_fun_name", vec![alias_column], "?distinct"),
            self.transform_aggregate_function(
                Some("?fun_name"),
                Some("?column"),
                Some("?distinct"),
                "?output_fun_name",
                "?alias_to_cube",
            ),
            true,
            rules,
        );
        self.single_arg_split_point_rules(
            "aggregate-function-simple-count",
            || agg_fun_expr("?fun_name", vec![literal_expr("?literal")], "?distinct"),
            || agg_fun_expr("?fun_name", vec![literal_expr("?literal")], "?distinct"),
            // ?distinct would always match
            |alias_column| agg_fun_expr("?output_fun_name", vec![alias_column], "?distinct"),
            self.transform_aggregate_function(
                Some("?fun_name"),
                None,
                Some("?distinct"),
                "?output_fun_name",
                "?alias_to_cube",
            ),
            true,
            rules,
        );
    }

    pub fn transform_aggregate_function(
        &self,
        fun_name_var: Option<&str>,
        column_var: Option<&str>,
        distinct_var: Option<&str>,
        output_fun_var: &str,
        alias_to_cube_var: &str,
    ) -> impl Fn(
        bool,
        &mut egg::EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        &mut egg::Subst,
    ) -> bool
           + Sync
           + Send
           + Clone
           + 'static {
        let fun_name_var = fun_name_var.map(|fun_name_var| var!(fun_name_var));
        let column_var = column_var.map(|column_var| var!(column_var));
        let distinct_var = distinct_var.map(|distinct_var| var!(distinct_var));
        let output_fun_var = var!(output_fun_var);
        let alias_to_cube_var = var!(alias_to_cube_var);
        let meta = self.meta_context.clone();
        move |is_projection, egraph, subst| {
            for alias_to_cube in var_iter!(
                egraph[subst[alias_to_cube_var]],
                AggregateSplitPushDownReplacerAliasToCube
            )
            .chain(var_iter!(
                egraph[subst[alias_to_cube_var]],
                ProjectionSplitPushDownReplacerAliasToCube
            ))
            .cloned()
            {
                for column in column_var
                    .map(|column_var| {
                        var_iter!(egraph[subst[column_var]], ColumnExprColumn)
                            .map(|c| c.clone())
                            .collect()
                    })
                    .unwrap_or(vec![Column::from_name(
                        MemberRules::default_count_measure_name(),
                    )])
                {
                    if let Some((_, cube)) = meta.find_cube_by_column(&alias_to_cube, &column) {
                        // TODO Use aliases to find the cube and measure
                        // TODO Support dimension split?
                        if let Some(measure) = cube.lookup_measure(&column.name) {
                            if let Some((fun_name_var, distinct_var)) =
                                fun_name_var.zip(distinct_var)
                            {
                                for fun in
                                    var_iter!(egraph[subst[fun_name_var]], AggregateFunctionExprFun)
                                {
                                    for distinct in var_iter!(
                                        egraph[subst[distinct_var]],
                                        AggregateFunctionExprDistinct
                                    ) {
                                        let output_fun = if is_projection {
                                            fun.clone()
                                        } else {
                                            // TODO this is not quite correct and output aggregation should be derived from measure definition
                                            match fun {
                                                AggregateFunction::Count if *distinct => continue,
                                                AggregateFunction::Count => AggregateFunction::Sum,
                                                AggregateFunction::Sum => AggregateFunction::Sum,
                                                AggregateFunction::Min => AggregateFunction::Min,
                                                AggregateFunction::Max => AggregateFunction::Max,
                                                _ => continue,
                                            }
                                        };

                                        let output_fun = egraph.add(
                                            LogicalPlanLanguage::AggregateFunctionExprFun(
                                                AggregateFunctionExprFun(output_fun),
                                            ),
                                        );

                                        subst.insert(output_fun_var, output_fun);

                                        return true;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            false
        }
    }
}
