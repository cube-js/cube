use crate::{
    compile::rewrite::{
        cube_scan_wrapper, distinct, rewrite,
        rewriter::{CubeEGraph, CubeRewrite},
        rules::wrapper::WrapperRules,
        transforming_rewrite, union, wrapped_union, wrapper_pullup_replacer,
        wrapper_replacer_context, CubeScanWrapperFinalized, LogicalPlanLanguage, UnionAlias,
        WrappedUnionAlias, WrapperReplacerContextAliasToCube,
        WrapperReplacerContextGroupedSubqueries, WrapperReplacerContextInputDataSource,
    },
    var, var_iter,
};
use egg::{Id, Subst};
use std::collections::HashSet;

impl WrapperRules {
    pub fn union_rules(&self, rules: &mut Vec<CubeRewrite>) {
        rules.extend(vec![
            transforming_rewrite(
                "wrapper-push-down-union",
                union("?union_inputs", "?union_alias"),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_union(
                            "?wrapped_union_inputs",
                            "WrappedUnionDistinct:false",
                            "?wrapped_union_alias",
                        ),
                        wrapper_replacer_context(
                            "?alias_to_cube_out",
                            // A set operation is evaluated by the data source, so nothing
                            // above it can be pushed into a Cube load query anymore
                            "WrapperReplacerContextPushToCube:false",
                            "WrapperReplacerContextInProjection:false",
                            "?cube_members_out",
                            "?grouped_subqueries_out",
                            "WrapperReplacerContextUngroupedScan:false",
                            "?input_data_source_out",
                        ),
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
                self.transform_union(
                    "?union_inputs",
                    "?union_alias",
                    "?wrapped_union_inputs",
                    "?wrapped_union_alias",
                    "?alias_to_cube_out",
                    "?cube_members_out",
                    "?grouped_subqueries_out",
                    "?input_data_source_out",
                ),
            ),
            // `UNION` is `UNION ALL` with the duplicates dropped, so the data source can do
            // both at once and the deduplication does not have to happen in DataFusion
            rewrite(
                "wrapper-push-down-distinct-to-union",
                distinct(cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_union("?inputs", "WrappedUnionDistinct:false", "?alias"),
                        "?context",
                    ),
                    "CubeScanWrapperFinalized:false",
                )),
                cube_scan_wrapper(
                    wrapper_pullup_replacer(
                        wrapped_union("?inputs", "WrappedUnionDistinct:true", "?alias"),
                        "?context",
                    ),
                    "CubeScanWrapperFinalized:false",
                ),
            ),
        ]);
    }

    fn transform_union(
        &self,
        union_inputs_var: &'static str,
        union_alias_var: &'static str,
        wrapped_union_inputs_var: &'static str,
        wrapped_union_alias_var: &'static str,
        alias_to_cube_out_var: &'static str,
        cube_members_out_var: &'static str,
        grouped_subqueries_out_var: &'static str,
        input_data_source_out_var: &'static str,
    ) -> impl Fn(&mut CubeEGraph, &mut Subst) -> bool {
        let union_inputs_var = var!(union_inputs_var);
        let union_alias_var = var!(union_alias_var);
        let wrapped_union_inputs_var = var!(wrapped_union_inputs_var);
        let wrapped_union_alias_var = var!(wrapped_union_alias_var);
        let alias_to_cube_out_var = var!(alias_to_cube_out_var);
        let cube_members_out_var = var!(cube_members_out_var);
        let grouped_subqueries_out_var = var!(grouped_subqueries_out_var);
        let input_data_source_out_var = var!(input_data_source_out_var);
        move |egraph, subst| {
            let Some(inputs) = Self::list_node_ids(egraph, subst[union_inputs_var]) else {
                return false;
            };
            // A union of one is not a union, and an empty one has no data source to speak of
            if inputs.len() < 2 {
                return false;
            }

            let mut wrapped_inputs = Vec::with_capacity(inputs.len());
            let mut data_source = None;
            for input in inputs {
                let Some((wrapped_input, input_data_source)) = Self::wrapped_input(egraph, input)
                else {
                    return false;
                };
                // Every input has to end up in the same query, so they all have to reach the
                // same data source
                match &data_source {
                    None => data_source = Some(input_data_source),
                    Some(data_source) if data_source == &input_data_source => {}
                    Some(_) => return false,
                }
                wrapped_inputs.push(wrapped_input);
            }

            let Some(data_source) = data_source else {
                return false;
            };

            let mut list = egraph.add(LogicalPlanLanguage::WrappedUnionInputs(vec![]));
            for input in wrapped_inputs.into_iter().rev() {
                list = egraph.add(LogicalPlanLanguage::WrappedUnionInputs(vec![input, list]));
            }
            subst.insert(wrapped_union_inputs_var, list);

            let Some(alias) = var_iter!(egraph[subst[union_alias_var]], UnionAlias)
                .cloned()
                .next()
            else {
                return false;
            };
            subst.insert(
                wrapped_union_alias_var,
                egraph.add(LogicalPlanLanguage::WrappedUnionAlias(WrappedUnionAlias(
                    alias,
                ))),
            );

            subst.insert(
                input_data_source_out_var,
                egraph.add(LogicalPlanLanguage::WrapperReplacerContextInputDataSource(
                    WrapperReplacerContextInputDataSource(Some(data_source)),
                )),
            );
            // The inputs are complete queries by now, so nothing above the union can reach
            // the cubes and members they were built from
            subst.insert(
                alias_to_cube_out_var,
                egraph.add(LogicalPlanLanguage::WrapperReplacerContextAliasToCube(
                    WrapperReplacerContextAliasToCube(vec![]),
                )),
            );
            subst.insert(
                cube_members_out_var,
                egraph.add(LogicalPlanLanguage::CubeScanMembers(vec![])),
            );
            subst.insert(
                grouped_subqueries_out_var,
                egraph.add(
                    LogicalPlanLanguage::WrapperReplacerContextGroupedSubqueries(
                        WrapperReplacerContextGroupedSubqueries(vec![]),
                    ),
                ),
            );

            true
        }
    }

    /// Ids of every element of a `UnionInputs` list, in order.
    fn list_node_ids(egraph: &CubeEGraph, list: Id) -> Option<Vec<Id>> {
        let mut ids = vec![];
        let mut seen = HashSet::new();
        let mut current = list;
        loop {
            // A list that contains itself is not a list this rule can work with, and
            // following it would not terminate
            if !seen.insert(current) {
                return None;
            }
            let mut tail = None;
            for node in egraph[current].nodes.iter() {
                match node {
                    LogicalPlanLanguage::UnionInputs(params) => match params[..] {
                        [] => return Some(ids),
                        [head, rest] => {
                            ids.push(head);
                            tail = Some(rest);
                        }
                        _ => return None,
                    },
                    _ => {}
                }
                if tail.is_some() {
                    break;
                }
            }
            current = tail?;
        }
    }

    /// The plan a union input wraps, together with the data source it reaches, when that
    /// input is a wrapper that is still open for nodes to be pushed into it.
    fn wrapped_input(egraph: &CubeEGraph, input: Id) -> Option<(Id, String)> {
        for node in egraph[input].nodes.iter() {
            let LogicalPlanLanguage::CubeScanWrapper([wrapped, finalized]) = node else {
                continue;
            };
            if !var_iter!(egraph[*finalized], CubeScanWrapperFinalized).any(|finalized| !finalized)
            {
                continue;
            }
            for node in egraph[*wrapped].nodes.iter() {
                let LogicalPlanLanguage::WrapperPullupReplacer([plan, context]) = node else {
                    continue;
                };
                for node in egraph[*context].nodes.iter() {
                    let LogicalPlanLanguage::WrapperReplacerContext(params) = node else {
                        continue;
                    };
                    let Some(data_source) =
                        var_iter!(egraph[params[6]], WrapperReplacerContextInputDataSource)
                            .flat_map(|data_source| data_source.clone())
                            .next()
                    else {
                        continue;
                    };
                    return Some((*plan, data_source));
                }
            }
        }
        None
    }
}
