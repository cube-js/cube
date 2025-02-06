use crate::{
    compile::{
        engine::udf::MEASURE_UDAF_NAME,
        rewrite::{
            converter::{is_expr_node, node_to_expr, LogicalPlanToLanguageConverter},
            expr_column_name,
            rewriter::{CubeEGraph, EGraphDebugState},
            AggregateUDFExprFun, AliasExprAlias, AllMembersAlias, AllMembersCube, ChangeUserCube,
            ColumnExprColumn, DimensionName, FilterMemberMember, FilterMemberOp, LiteralExprValue,
            LiteralMemberRelation, LiteralMemberValue, LogicalPlanLanguage, MeasureName,
            ScalarFunctionExprFun, SegmentMemberMember, SegmentName, TableScanSourceTableName,
            TimeDimensionDateRange, TimeDimensionGranularity, TimeDimensionName, VirtualFieldCube,
            VirtualFieldName,
        },
        CubeContext,
    },
    transport::ext::{V1CubeMetaDimensionExt, V1CubeMetaMeasureExt, V1CubeMetaSegmentExt},
    var_iter, var_list_iter, CubeError,
};
use datafusion::{
    arrow::{
        array::NullArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    logical_plan::{Column, DFSchema, Expr},
    physical_plan::{
        functions::{BuiltinScalarFunction, Volatility},
        planner::DefaultPhysicalPlanner,
        ColumnarValue, PhysicalPlanner,
    },
    scalar::ScalarValue,
};
use egg::{Analysis, DidMerge, EGraph, Id};
use hashbrown;
use std::{cmp::Ordering, fmt::Debug, ops::Index, sync::Arc};

pub type MemberNameToExpr = (Option<String>, Member, Expr);

#[derive(Clone, Debug)]
pub struct LogicalPlanData {
    pub iteration_timestamp: usize,
    pub original_expr: Option<OriginalExpr>,
    pub member_name_to_expr: Option<MemberNamesToExpr>,
    pub trivial_push_down: Option<usize>,
    pub column: Option<Column>,
    pub expr_to_alias: Option<Vec<(Expr, String, Option<bool>)>>,
    pub referenced_expr: Option<Vec<Expr>>,
    pub constant: Option<ConstantFolding>,
    pub constant_in_list: Option<Vec<ScalarValue>>,
    pub cube_reference: Option<String>,
    pub filter_operators: Option<Vec<(String, String)>>,
    pub is_empty_list: Option<bool>,
}

#[derive(Debug, Clone)]
pub enum OriginalExpr {
    Expr(Expr),
    List(Vec<Expr>),
}

#[derive(Clone, Debug)]
pub struct MemberNamesToExpr {
    /// List of MemberNameToExpr.
    pub list: Vec<MemberNameToExpr>,
    /// Results of lookup_member_by_column_name represented as indexes into `list`.
    // Note that using Vec<(String, usize)> had nearly identical performance the last time that was
    // benchmarked.
    pub cached_lookups: hashbrown::HashMap<String, usize>,
    /// The lookups in [uncached_lookups_offset, list.len()) are not completely cached.
    pub uncached_lookups_offset: usize,
}

#[derive(Debug, Clone)]
pub enum ConstantFolding {
    Scalar(ScalarValue),
    List(Vec<ScalarValue>),
}

#[derive(Debug, Clone)]
pub enum Member {
    Dimension {
        name: String,
        expr: Expr,
    },
    Measure {
        name: String,
        expr: Expr,
    },
    Segment {
        name: String,
        expr: Expr,
    },
    TimeDimension {
        name: String,
        expr: Expr,
        granularity: Option<String>,
        date_range: Option<Vec<String>>,
    },
    ChangeUser {
        cube: String,
        expr: Expr,
    },
    VirtualField {
        name: String,
        cube: String,
        expr: Expr,
    },
    LiteralMember {
        value: ScalarValue,
        expr: Expr,
        relation: Option<String>,
    },
}

impl Member {
    pub fn add_to_egraph(
        &self,
        egraph: &mut EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
        flat_list: bool,
    ) -> Result<Id, CubeError> {
        match self {
            Member::Dimension { name, expr } => {
                let dimension_name = egraph.add(LogicalPlanLanguage::DimensionName(DimensionName(
                    name.to_string(),
                )));
                let expr = LogicalPlanToLanguageConverter::add_expr(egraph, &expr, flat_list)?;
                Ok(egraph.add(LogicalPlanLanguage::Dimension([dimension_name, expr])))
            }
            Member::Measure { name, expr } => {
                let measure_name = egraph.add(LogicalPlanLanguage::MeasureName(MeasureName(
                    name.to_string(),
                )));
                let expr = LogicalPlanToLanguageConverter::add_expr(egraph, &expr, flat_list)?;
                Ok(egraph.add(LogicalPlanLanguage::Measure([measure_name, expr])))
            }
            Member::Segment { name, expr } => {
                let segment_name = egraph.add(LogicalPlanLanguage::SegmentName(SegmentName(
                    name.to_string(),
                )));
                let expr = LogicalPlanToLanguageConverter::add_expr(egraph, &expr, flat_list)?;
                Ok(egraph.add(LogicalPlanLanguage::Segment([segment_name, expr])))
            }
            Member::TimeDimension {
                name,
                expr,
                granularity,
                date_range,
            } => {
                let time_dimension_name = egraph.add(LogicalPlanLanguage::TimeDimensionName(
                    TimeDimensionName(name.to_string()),
                ));
                let time_dimension_granularity =
                    egraph.add(LogicalPlanLanguage::TimeDimensionGranularity(
                        TimeDimensionGranularity(granularity.clone()),
                    ));
                let time_dimension_date_range =
                    egraph.add(LogicalPlanLanguage::TimeDimensionDateRange(
                        TimeDimensionDateRange(date_range.clone()),
                    ));
                let expr = LogicalPlanToLanguageConverter::add_expr(egraph, &expr, flat_list)?;
                Ok(egraph.add(LogicalPlanLanguage::TimeDimension([
                    time_dimension_name,
                    time_dimension_granularity,
                    time_dimension_date_range,
                    expr,
                ])))
            }
            Member::ChangeUser { cube, expr } => {
                let change_user_cube = egraph.add(LogicalPlanLanguage::ChangeUserCube(
                    ChangeUserCube(cube.to_string()),
                ));
                let expr = LogicalPlanToLanguageConverter::add_expr(egraph, &expr, flat_list)?;
                Ok(egraph.add(LogicalPlanLanguage::ChangeUser([change_user_cube, expr])))
            }
            Member::LiteralMember {
                value,
                expr,
                relation,
            } => {
                let literal_member_value = egraph.add(LogicalPlanLanguage::LiteralMemberValue(
                    LiteralMemberValue(value.clone()),
                ));
                let literal_member_relation =
                    egraph.add(LogicalPlanLanguage::LiteralMemberRelation(
                        LiteralMemberRelation(relation.clone()),
                    ));
                let expr = LogicalPlanToLanguageConverter::add_expr(egraph, &expr, flat_list)?;
                Ok(egraph.add(LogicalPlanLanguage::LiteralMember([
                    literal_member_value,
                    expr,
                    literal_member_relation,
                ])))
            }
            Member::VirtualField { name, cube, expr } => {
                let virtual_field_name = egraph.add(LogicalPlanLanguage::VirtualFieldName(
                    VirtualFieldName(name.to_string()),
                ));
                let virtual_field_cube = egraph.add(LogicalPlanLanguage::VirtualFieldCube(
                    VirtualFieldCube(cube.to_string()),
                ));
                let expr = LogicalPlanToLanguageConverter::add_expr(egraph, &expr, flat_list)?;
                Ok(egraph.add(LogicalPlanLanguage::VirtualField([
                    virtual_field_name,
                    virtual_field_cube,
                    expr,
                ])))
            }
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Member::Dimension { name, .. } => Some(name),
            Member::Measure { name, .. } => Some(name),
            Member::Segment { name, .. } => Some(name),
            Member::TimeDimension { name, .. } => Some(name),
            Member::ChangeUser { .. } => None,
            Member::VirtualField { name, .. } => Some(name),
            Member::LiteralMember { .. } => None,
        }
    }

    pub fn cube(&self) -> Option<String> {
        match self {
            Member::Dimension { name, .. }
            | Member::Measure { name, .. }
            | Member::Segment { name, .. }
            | Member::TimeDimension { name, .. } => {
                Some(name.split(".").next().unwrap().to_string())
            }
            Member::ChangeUser { cube, .. } => Some(cube.clone()),
            Member::VirtualField { cube, .. } => Some(cube.clone()),
            Member::LiteralMember { .. } => None,
        }
    }
}

#[derive(Clone)]
pub struct LogicalPlanAnalysis {
    /* This is 0, when creating the EGraph.  It's set to 1 before iteration 0,
    2 before the iteration 1, etc. */
    pub iteration_timestamp: usize,
    /// Debug info, used with egraph-debug
    /// Will be filled by special hook in Runner
    pub debug_states: Vec<EGraphDebugState>,
    cube_context: Arc<CubeContext>,
    planner: Arc<DefaultPhysicalPlanner>,
}

pub struct SingleNodeIndex<'a> {
    egraph: &'a EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
}

impl<'a> Index<Id> for SingleNodeIndex<'a> {
    type Output = LogicalPlanLanguage;

    fn index(&self, index: Id) -> &Self::Output {
        // TODO As we replace inside lists for casts here can be multiple terminal nodes
        // assert!(
        //     self.egraph.index(index).nodes.len() == 1,
        //     "Single node expected but {:?} found",
        //     self.egraph.index(index).nodes
        // );
        &self
            .egraph
            .index(index)
            .nodes
            .iter()
            .find(|n| !matches!(n, LogicalPlanLanguage::QueryParam(_)))
            .unwrap_or(&self.egraph.index(index).nodes[0])
    }
}

impl LogicalPlanAnalysis {
    pub fn new(cube_context: Arc<CubeContext>, planner: Arc<DefaultPhysicalPlanner>) -> Self {
        Self {
            iteration_timestamp: 0,
            debug_states: vec![],
            cube_context,
            planner,
        }
    }

    pub fn store_egraph_debug_state(egraph: &mut CubeEGraph) {
        debug_assert_eq!(
            egraph.analysis.iteration_timestamp,
            egraph.analysis.debug_states.len()
        );
        let state = EGraphDebugState::new(egraph);
        egraph.analysis.debug_states.push(state);
    }

    fn make_original_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<OriginalExpr> {
        let id_to_original_expr = |id| {
            egraph[id].data.original_expr.clone().ok_or_else(|| {
                CubeError::internal(format!(
                    "Original expr wasn't prepared for {:?}",
                    egraph[id]
                ))
            })
        };
        let id_to_expr = |id| {
            id_to_original_expr(id).and_then(|e| match e {
                OriginalExpr::Expr(expr) => Ok(expr),
                OriginalExpr::List(_) => Err(CubeError::internal(format!(
                    "Original expr list can't be used in expr eval {:?}",
                    egraph[id]
                ))),
            })
        };
        let original_expr = if is_expr_node(enode) {
            node_to_expr(
                enode,
                &egraph.analysis.cube_context,
                &id_to_expr,
                &SingleNodeIndex { egraph },
            )
            .ok()
            .map(|expr| OriginalExpr::Expr(expr))
        } else {
            // While not used directly in expression evaluation OriginalExpr::List is used to trigger parent data invalidation
            // Going forward should be used for expression evaluation as well
            match enode {
                LogicalPlanLanguage::CaseExprWhenThenExpr(params)
                | LogicalPlanLanguage::CaseExprElseExpr(params)
                | LogicalPlanLanguage::CaseExprExpr(params)
                | LogicalPlanLanguage::AggregateFunctionExprArgs(params)
                | LogicalPlanLanguage::AggregateUDFExprArgs(params)
                | LogicalPlanLanguage::ScalarFunctionExprArgs(params)
                | LogicalPlanLanguage::ScalarUDFExprArgs(params) => {
                    let mut list = Vec::new();
                    for id in params {
                        match id_to_original_expr(*id).ok()? {
                            OriginalExpr::Expr(expr) => list.push(expr),
                            OriginalExpr::List(exprs) => list.extend(exprs),
                        }
                    }
                    Some(OriginalExpr::List(list))
                }
                _ => None,
            }
        };
        original_expr
    }

    fn make_trivial_push_down(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<usize> {
        let trivial_push_down = |id| egraph.index(id).data.trivial_push_down;
        match enode {
            LogicalPlanLanguage::ColumnExpr(_) => Some(0),
            LogicalPlanLanguage::LiteralExpr(_) => Some(0),
            LogicalPlanLanguage::QueryParam(_) => Some(0),
            LogicalPlanLanguage::AliasExpr(params) => trivial_push_down(params[0]),
            LogicalPlanLanguage::ProjectionExpr(params)
            | LogicalPlanLanguage::AggregateAggrExpr(params)
            | LogicalPlanLanguage::AggregateGroupExpr(params)
            | LogicalPlanLanguage::AggregateFunctionExprArgs(params)
            | LogicalPlanLanguage::AggregateUDFExprArgs(params) => {
                let mut trivial = 0;
                for id in params.iter() {
                    trivial = trivial_push_down(*id)?.max(trivial);
                }
                Some(trivial)
            }
            LogicalPlanLanguage::ScalarFunctionExprFun(ScalarFunctionExprFun(fun)) => {
                if fun == &BuiltinScalarFunction::DateTrunc {
                    Some(0)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::ScalarFunctionExpr(params) => {
                let mut trivial = 0;
                for id in params.iter() {
                    trivial = trivial_push_down(*id)?.max(trivial);
                }
                Some(trivial + 1)
            }
            LogicalPlanLanguage::ScalarFunctionExprArgs(params) => {
                let mut trivial = 0;
                for id in params.iter() {
                    trivial = trivial_push_down(*id)?.max(trivial);
                }
                Some(trivial)
            }
            LogicalPlanLanguage::AggregateUDFExprFun(AggregateUDFExprFun(fun)) => {
                if fun.to_lowercase() == MEASURE_UDAF_NAME {
                    Some(0)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::AggregateUDFExpr(params) => {
                let mut trivial = 0;
                for id in params.iter() {
                    trivial = trivial_push_down(*id)?.max(trivial);
                }
                Some(trivial + 1)
            }
            // TODO if there's an aggregate function then we should have more complex logic than that
            // LogicalPlanLanguage::AggregateFunctionExprFun(AggregateFunctionExprFun(fun)) => {
            //     match fun {
            //         AggregateFunction::Count
            //         | AggregateFunction::Sum
            //         | AggregateFunction::Avg
            //         | AggregateFunction::Min
            //         | AggregateFunction::Max => Some(0),
            //         _ => None,
            //     }
            // }
            // LogicalPlanLanguage::AggregateFunctionExpr(params) => {
            //     let mut trivial = 0;
            //     for id in params.iter() {
            //         trivial = trivial_push_down(*id)?.max(trivial);
            //     }
            //     Some(trivial + 1)
            // }
            // LogicalPlanLanguage::AggregateFunctionExprDistinct(_) => Some(0),
            _ => None,
        }
    }

    fn make_member_name_to_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<MemberNamesToExpr> {
        let column_name = |id| egraph.index(id).data.column.clone();
        let id_to_column_name_to_expr = |id| {
            Some(
                egraph
                    .index(id)
                    .data
                    .member_name_to_expr
                    .as_ref()?
                    .list
                    .clone(),
            )
        };
        let original_expr = |id| {
            egraph
                .index(id)
                .data
                .original_expr
                .clone()
                .and_then(|e| match e {
                    OriginalExpr::Expr(expr) => Some(expr),
                    OriginalExpr::List(_) => None,
                })
        };
        let literal_member_relation = |id| {
            egraph
                .index(id)
                .iter()
                .find(|plan| {
                    if let LogicalPlanLanguage::LiteralMemberRelation(_) = plan {
                        true
                    } else {
                        false
                    }
                })
                .map(|plan| match plan {
                    LogicalPlanLanguage::LiteralMemberRelation(LiteralMemberRelation(relation)) => {
                        relation
                    }
                    _ => panic!("Unexpected non-literal member relation"),
                })
        };
        let mut map = Vec::new();
        let list = match enode {
            LogicalPlanLanguage::Measure(params) => {
                if let Some(_) = column_name(params[1]) {
                    let expr = original_expr(params[1])?;
                    let measure_name = var_iter!(egraph[params[0]], MeasureName).next().unwrap();
                    map.push((
                        Some(measure_name.to_string()),
                        Member::Measure {
                            name: measure_name.clone(),
                            expr: expr.clone(),
                        },
                        expr.clone(),
                    ));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::Dimension(params) => {
                if let Some(_) = column_name(params[1]) {
                    let expr = original_expr(params[1])?;
                    let dimension_name =
                        var_iter!(egraph[params[0]], DimensionName).next().unwrap();
                    map.push((
                        Some(dimension_name.to_string()),
                        Member::Dimension {
                            name: dimension_name.clone(),
                            expr: expr.clone(),
                        },
                        expr,
                    ));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::Segment(params) => {
                if let Some(_) = column_name(params[1]) {
                    let expr = original_expr(params[1])?;
                    let segment_name = var_iter!(egraph[params[0]], SegmentName).next().unwrap();
                    map.push((
                        Some(segment_name.to_string()),
                        Member::Segment {
                            name: segment_name.clone(),
                            expr: expr.clone(),
                        },
                        expr,
                    ));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::ChangeUser(params) => {
                if let Some(_) = column_name(params[1]) {
                    let expr = original_expr(params[1])?;
                    let cube = var_iter!(egraph[params[0]], ChangeUserCube).next().unwrap();
                    map.push((
                        Some(format!("{}.__user", cube)),
                        Member::ChangeUser {
                            cube: cube.clone(),
                            expr: expr.clone(),
                        },
                        expr,
                    ));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::VirtualField(params) => {
                if let Some(_) = column_name(params[2]) {
                    let field_name = var_iter!(egraph[params[0]], VirtualFieldName)
                        .next()
                        .unwrap();
                    let cube = var_iter!(egraph[params[1]], VirtualFieldCube)
                        .next()
                        .unwrap();
                    let expr = original_expr(params[2])?;
                    map.push((
                        Some(format!("{cube}.{field_name}")),
                        Member::VirtualField {
                            name: field_name.to_string(),
                            cube: cube.to_string(),
                            expr: expr.clone(),
                        },
                        expr,
                    ));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::AllMembers(params) => {
                if let Some((cube, alias)) = var_iter!(egraph[params[0]], AllMembersCube)
                    .next()
                    .zip(var_iter!(egraph[params[1]], AllMembersAlias).next())
                {
                    let cube = egraph
                        .analysis
                        .cube_context
                        .meta
                        .find_cube_with_name(&cube)?;
                    for measure in cube.measures.iter() {
                        map.push((
                            Some(measure.name.clone()),
                            Member::Measure {
                                name: measure.name.clone(),
                                expr: Expr::Column(Column {
                                    relation: Some(alias.to_string()),
                                    name: measure.get_real_name(),
                                }),
                            },
                            Expr::Column(Column {
                                relation: Some(alias.to_string()),
                                name: measure.get_real_name(),
                            }),
                        ));
                    }
                    for dimension in cube.dimensions.iter() {
                        map.push((
                            Some(dimension.name.clone()),
                            Member::Dimension {
                                name: dimension.name.clone(),
                                expr: Expr::Column(Column {
                                    relation: Some(alias.to_string()),
                                    name: dimension.get_real_name(),
                                }),
                            },
                            Expr::Column(Column {
                                relation: Some(alias.to_string()),
                                name: dimension.get_real_name(),
                            }),
                        ));
                    }
                    for segment in cube.segments.iter() {
                        map.push((
                            Some(segment.name.clone()),
                            Member::Segment {
                                name: segment.name.clone(),
                                expr: Expr::Column(Column {
                                    relation: Some(alias.to_string()),
                                    name: segment.get_real_name(),
                                }),
                            },
                            Expr::Column(Column {
                                relation: Some(alias.to_string()),
                                name: segment.get_real_name(),
                            }),
                        ));
                    }

                    map.push((
                        Some(format!("{}.{}", cube.name, "__user")),
                        Member::ChangeUser {
                            cube: cube.name.clone(),
                            expr: Expr::Column(Column {
                                relation: Some(alias.to_string()),
                                name: "__user".to_string(),
                            }),
                        },
                        Expr::Column(Column {
                            relation: Some(alias.to_string()),
                            name: "__user".to_string(),
                        }),
                    ));

                    map.push((
                        Some(format!("{}.{}", cube.name, "__cubeJoinField")),
                        Member::VirtualField {
                            name: "__cubeJoinField".to_string(),
                            cube: cube.name.clone(),
                            expr: Expr::Column(Column {
                                relation: Some(alias.to_string()),
                                name: "__cubeJoinField".to_string(),
                            }),
                        },
                        Expr::Column(Column {
                            relation: Some(alias.to_string()),
                            name: "__cubeJoinField".to_string(),
                        }),
                    ));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::TimeDimension(params) => {
                if let Some(_) = column_name(params[3]) {
                    let expr = original_expr(params[3])?;
                    let time_dimension_name = var_iter!(egraph[params[0]], TimeDimensionName)
                        .next()
                        .unwrap();
                    let time_dimension_granularity =
                        var_iter!(egraph[params[1]], TimeDimensionGranularity)
                            .next()
                            .unwrap();
                    let time_dimension_date_range =
                        var_iter!(egraph[params[2]], TimeDimensionDateRange)
                            .next()
                            .unwrap();
                    map.push((
                        Some(time_dimension_name.to_string()),
                        Member::TimeDimension {
                            name: time_dimension_name.clone(),
                            expr: expr.clone(),
                            granularity: time_dimension_granularity.clone(),
                            date_range: time_dimension_date_range.clone(),
                        },
                        expr,
                    ));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::LiteralMember(params) => {
                if let Some(relation) = literal_member_relation(params[2]) {
                    let scalar_value = var_iter!(egraph[params[0]], LiteralMemberValue)
                        .next()
                        .unwrap();
                    let expr = original_expr(params[1])?;
                    let name = expr_column_name(&expr, &None);
                    let column_expr = Expr::Column(Column {
                        relation: relation.clone(),
                        name,
                    });
                    map.push((
                        None,
                        Member::LiteralMember {
                            value: scalar_value.clone(),
                            expr: column_expr.clone(),
                            relation: relation.clone(),
                        },
                        column_expr,
                    ));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::CubeScanMembers(params) => {
                for id in params.iter() {
                    map.extend(id_to_column_name_to_expr(*id)?);
                }
                Some(map)
            }
            LogicalPlanLanguage::CubeScan(params) => {
                map.extend(id_to_column_name_to_expr(params[1])?);
                Some(map)
            }
            _ => None,
        };
        list.map(|x| MemberNamesToExpr {
            list: x,
            cached_lookups: hashbrown::HashMap::new(),
            uncached_lookups_offset: 0,
        })
    }

    fn make_filter_operators(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<(String, String)>> {
        let filter_operators = |id| egraph.index(id).data.filter_operators.clone();
        match enode {
            LogicalPlanLanguage::CubeScanFilters(params) => {
                let mut map = Vec::new();
                for id in params.iter() {
                    map.extend(filter_operators(*id)?.into_iter());
                }
                Some(map)
            }
            LogicalPlanLanguage::FilterOp(params) => filter_operators(params[0]),
            LogicalPlanLanguage::FilterOpFilters(params) => {
                let mut map = Vec::new();
                for id in params.iter() {
                    map.extend(filter_operators(*id)?.into_iter());
                }
                Some(map)
            }
            LogicalPlanLanguage::FilterMember(params) => {
                let member = var_iter!(egraph[params[0]], FilterMemberMember)
                    .next()
                    .unwrap()
                    .to_string();
                let op = var_iter!(egraph[params[1]], FilterMemberOp)
                    .next()
                    .unwrap()
                    .to_string();
                Some(vec![(member, op)])
            }
            LogicalPlanLanguage::SegmentMember(params) => {
                let member = var_iter!(egraph[params[0]], SegmentMemberMember)
                    .next()
                    .unwrap()
                    .to_string();
                Some(vec![(member, "equals".to_string())])
            }
            LogicalPlanLanguage::ChangeUserMember(_) => {
                Some(vec![("__user".to_string(), "equals".to_string())])
            }
            _ => None,
        }
    }

    fn make_expr_to_alias(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<(Expr, String, Option<bool>)>> {
        let original_expr = |id| {
            egraph
                .index(id)
                .data
                .original_expr
                .clone()
                .and_then(|e| match e {
                    OriginalExpr::Expr(expr) => Some(expr),
                    OriginalExpr::List(_) => None,
                })
        };
        let id_to_column_name = |id| egraph.index(id).data.column.clone();
        let column_name_to_alias = |id| egraph.index(id).data.expr_to_alias.clone();
        let mut map = Vec::new();
        match enode {
            LogicalPlanLanguage::AliasExpr(params) => {
                map.push((
                    original_expr(params[0])?,
                    var_iter!(egraph[params[1]], AliasExprAlias)
                        .next()
                        .unwrap()
                        .to_string(),
                    Some(true),
                ));
                Some(map)
            }
            LogicalPlanLanguage::ProjectionExpr(params) => {
                for id in params.iter() {
                    if let Some(col_name) = id_to_column_name(*id) {
                        map.push((original_expr(*id)?, col_name.name.to_string(), None));
                        continue;
                    }
                    if let Some(expr) = original_expr(*id) {
                        match expr {
                            Expr::Alias(_, _) => (),
                            expr @ _ => {
                                let expr_name = expr.name(&DFSchema::empty());
                                map.push((expr, expr_name.ok()?, Some(false)));
                                continue;
                            }
                        };
                    }
                    map.extend(column_name_to_alias(*id)?.into_iter());
                }
                Some(map)
            }
            _ => None,
        }
    }

    fn make_referenced_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<Expr>> {
        let referenced_columns = |id| egraph.index(id).data.referenced_expr.clone();
        let original_expr = |id| {
            egraph
                .index(id)
                .data
                .original_expr
                .clone()
                .and_then(|e| match e {
                    OriginalExpr::Expr(expr) => Some(expr),
                    OriginalExpr::List(_) => None,
                })
        };
        let column_name = |id| egraph.index(id).data.column.clone();
        let push_referenced_columns = |id, columns: &mut Vec<Expr>| -> Option<()> {
            if let Some(col) = column_name(id) {
                let expr = Expr::Column(col);
                columns.push(expr);
            } else {
                columns.extend(referenced_columns(id)?);
            }
            Some(())
        };
        let mut vec = Vec::new();
        match enode {
            LogicalPlanLanguage::ColumnExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::AliasExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::AnyExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                push_referenced_columns(params[2], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::BinaryExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                push_referenced_columns(params[2], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::LikeExpr(params) => {
                push_referenced_columns(params[2], &mut vec)?;
                push_referenced_columns(params[3], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::InListExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::IsNullExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::IsNotNullExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::NotExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::BetweenExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::CastExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::NegativeExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::CaseExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                push_referenced_columns(params[1], &mut vec)?;
                push_referenced_columns(params[2], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::ScalarFunctionExpr(params) => {
                push_referenced_columns(params[1], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::ScalarUDFExpr(params) => {
                push_referenced_columns(params[1], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::AggregateUDFExpr(params) => {
                push_referenced_columns(params[1], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::AggregateFunctionExpr(params) => {
                push_referenced_columns(params[1], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::GroupingSetExpr(params) => {
                push_referenced_columns(params[0], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::LiteralExpr(_) => Some(vec),
            LogicalPlanLanguage::QueryParam(_) => Some(vec),
            LogicalPlanLanguage::SortExpr(params) => {
                if column_name(params[0]).is_some() {
                    let expr = original_expr(params[0])?;
                    vec.push(expr);
                    Some(vec)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::SortExp(params)
            | LogicalPlanLanguage::AggregateGroupExpr(params)
            | LogicalPlanLanguage::AggregateAggrExpr(params)
            | LogicalPlanLanguage::ProjectionExpr(params)
            | LogicalPlanLanguage::CaseExprWhenThenExpr(params)
            | LogicalPlanLanguage::CaseExprElseExpr(params)
            | LogicalPlanLanguage::CaseExprExpr(params)
            | LogicalPlanLanguage::AggregateFunctionExprArgs(params)
            | LogicalPlanLanguage::AggregateUDFExprArgs(params)
            | LogicalPlanLanguage::ScalarFunctionExprArgs(params)
            | LogicalPlanLanguage::ScalarUDFExprArgs(params) => {
                for p in params.iter() {
                    vec.extend(referenced_columns(*p)?.into_iter());
                }

                Some(vec)
            }

            LogicalPlanLanguage::GroupingSetExprMembers(params) => {
                for p in params.iter() {
                    vec.extend(referenced_columns(*p)?.into_iter());
                }

                Some(vec)
            }
            _ => None,
        }
    }

    fn make_constant(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<ConstantFolding> {
        let constant_node = |id| egraph.index(id).data.constant.clone();
        let constant_expr = |id| {
            egraph
                .index(id)
                .data
                .constant
                .clone()
                .and_then(|c| {
                    if let ConstantFolding::Scalar(c) = c {
                        Some(Expr::Literal(c))
                    } else {
                        None
                    }
                })
                .ok_or_else(|| CubeError::internal("Not a constant".to_string()))
        };
        match enode {
            LogicalPlanLanguage::LiteralExpr(_) => {
                let result = node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &constant_expr,
                    &SingleNodeIndex { egraph },
                );
                let expr = result.ok()?;
                match expr {
                    Expr::Literal(value) => Some(ConstantFolding::Scalar(value)),
                    _ => panic!("Expected Literal but got: {:?}", expr),
                }
            }
            LogicalPlanLanguage::AliasExpr(params) => constant_node(params[0]),
            LogicalPlanLanguage::ScalarUDFExpr(_) => {
                let expr = node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &constant_expr,
                    &SingleNodeIndex { egraph },
                )
                .ok()?;
                if let Expr::ScalarUDF { fun, .. } = &expr {
                    if &fun.name == "eval_now" {
                        Self::eval_constant_expr(
                            &egraph,
                            &Expr::ScalarFunction {
                                fun: BuiltinScalarFunction::Now,
                                args: vec![],
                            },
                        )
                    } else if &fun.name == "eval_current_date" {
                        Self::eval_constant_expr(
                            &egraph,
                            &Expr::ScalarFunction {
                                fun: BuiltinScalarFunction::CurrentDate,
                                args: vec![],
                            },
                        )
                    } else if &fun.name == "eval_utc_timestamp" {
                        Self::eval_constant_expr(
                            &egraph,
                            &Expr::ScalarFunction {
                                fun: BuiltinScalarFunction::UtcTimestamp,
                                args: vec![],
                            },
                        )
                    } else if &fun.name == "str_to_date"
                        || &fun.name == "date_add"
                        || &fun.name == "date_sub"
                        || &fun.name == "date"
                        || &fun.name == "date_to_timestamp"
                    {
                        Self::eval_constant_expr(&egraph, &expr)
                    } else {
                        None
                    }
                } else {
                    panic!("Expected ScalarUDF but got: {:?}", expr);
                }
            }
            LogicalPlanLanguage::ScalarFunctionExpr(_) => {
                let expr = node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &constant_expr,
                    &SingleNodeIndex { egraph },
                )
                .ok()?;

                if let Expr::ScalarFunction { fun, .. } = &expr {
                    // Removed stable evaluation as it affects caching and SQL push down.
                    // Whatever stable function should be evaluated it should be addressed as a special rewrite rule
                    // as it seems LogicalPlanAnalysis can't change it's state.
                    if fun.volatility() == Volatility::Immutable {
                        Self::eval_constant_expr(&egraph, &expr)
                    } else {
                        None
                    }
                } else {
                    panic!("Expected ScalarFunctionExpr but got: {:?}", expr);
                }
            }
            LogicalPlanLanguage::ScalarFunctionExprArgs(params)
            | LogicalPlanLanguage::ScalarUDFExprArgs(params) => {
                let mut list = Vec::new();
                for id in params.iter() {
                    match constant_node(*id)? {
                        ConstantFolding::Scalar(v) => list.push(v),
                        ConstantFolding::List(v) => list.extend(v),
                    };
                }
                // TODO ConstantFolding::List currently used only to trigger redo analysis for it's parents.
                // TODO It should be used also when actual lists are evaluated as a part of node_to_expr() call.
                // TODO In case multiple node variant exists ConstantFolding::List will choose one which contains actual constants.
                Some(ConstantFolding::List(list))
            }
            LogicalPlanLanguage::AnyExpr(_) | LogicalPlanLanguage::NegativeExpr(_) => {
                let expr = node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &constant_expr,
                    &SingleNodeIndex { egraph },
                )
                .ok()?;

                Self::eval_constant_expr(&egraph, &expr)
            }
            LogicalPlanLanguage::CastExpr(_) => {
                let expr = node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &constant_expr,
                    &SingleNodeIndex { egraph },
                )
                .ok()?;

                // Some casts from string can have unpredictable behavior
                if let Expr::Cast { expr, data_type } = &expr {
                    match expr.as_ref() {
                        Expr::Literal(ScalarValue::Utf8(Some(value))) => match (value, data_type) {
                            // Timezone set in Config
                            (_, DataType::Timestamp(_, _)) => (),
                            (_, DataType::Date32 | DataType::Date64) => (),
                            (_, DataType::Interval(_)) => (),
                            _ => return None,
                        },
                        _ => (),
                    }
                }

                Self::eval_constant_expr(&egraph, &expr)
            }
            LogicalPlanLanguage::BinaryExpr(_) => {
                let expr = node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &constant_expr,
                    &SingleNodeIndex { egraph },
                )
                .ok()?;
                Self::eval_constant_expr(&egraph, &expr)
            }
            _ => None,
        }
    }

    fn make_constant_in_list(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<ScalarValue>> {
        let constant = |id| egraph.index(id).data.constant.clone();
        let constant_in_list = |id| egraph.index(id).data.constant_in_list.clone();
        match enode {
            LogicalPlanLanguage::InListExprList(params) => Some(
                params
                    .iter()
                    .map(|id| {
                        constant(*id)
                            .and_then(|c| {
                                if let ConstantFolding::Scalar(c) = c {
                                    Some(vec![c])
                                } else {
                                    None
                                }
                            })
                            .or_else(|| constant_in_list(*id))
                    })
                    .collect::<Option<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect(),
            ),
            _ => None,
        }
    }

    fn eval_constant_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        expr: &Expr,
    ) -> Option<ConstantFolding> {
        let schema = DFSchema::empty();
        let arrow_schema = Arc::new(schema.to_owned().into());
        let physical_expr = match egraph.analysis.planner.create_physical_expr(
            &expr,
            &schema,
            &arrow_schema,
            &egraph.analysis.cube_context.state,
        ) {
            Ok(res) => res,
            Err(e) => {
                log::trace!("Can't plan expression: {:?}", e);
                return None;
            }
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "placeholder",
                DataType::Null,
                true,
            )])),
            vec![Arc::new(NullArray::new(1))],
        )
        .unwrap();
        let value = match physical_expr.evaluate(&batch) {
            Ok(res) => res,
            Err(e) => {
                log::trace!("Can't evaluate expression: {:?}", e);
                return None;
            }
        };
        Some(match value {
            ColumnarValue::Scalar(value) => ConstantFolding::Scalar(value),
            ColumnarValue::Array(arr) => {
                if arr.len() == 1 {
                    ConstantFolding::Scalar(ScalarValue::try_from_array(&arr, 0).unwrap())
                } else {
                    log::trace!(
                        "Expected one row but got {} during constant eval",
                        arr.len()
                    );
                    return None;
                }
            }
        })
    }

    fn make_column_name(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Column> {
        let id_to_column_name = |id| egraph.index(id).data.column.clone();
        match enode {
            LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(c)) => Some(c.clone()),
            LogicalPlanLanguage::ColumnExpr(c) => id_to_column_name(c[0]),
            _ => None,
        }
    }

    fn make_cube_reference(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<String> {
        let cube_reference = |id| egraph.index(id).data.cube_reference.clone();
        match enode {
            LogicalPlanLanguage::TableScanSourceTableName(TableScanSourceTableName(c)) => {
                Some(c.to_string())
            }
            LogicalPlanLanguage::CubeScan(params) => cube_reference(params[0]),
            _ => None,
        }
    }

    fn make_is_empty_list(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<bool> {
        let is_empty_list = |id| egraph.index(id).data.is_empty_list;
        match enode {
            LogicalPlanLanguage::FilterOpFilters(params)
            | LogicalPlanLanguage::CubeScanFilters(params)
            | LogicalPlanLanguage::CubeScanOrder(params) => {
                if params.is_empty()
                    || params.iter().all(|p| {
                        if let Some(true) = is_empty_list(*p) {
                            true
                        } else {
                            false
                        }
                    })
                {
                    Some(true)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    #[inline]
    fn merge_option_field<T>(&mut self, field_a: &mut Option<T>, field_b: Option<T>) -> DidMerge {
        let res = if field_a.is_none() && field_b.is_some() {
            *field_a = field_b;
            DidMerge(true, false)
        } else if field_a.is_some() {
            DidMerge(false, true)
        } else {
            DidMerge(false, false)
        };

        res
    }

    fn merge_max_field<T: Ord>(&mut self, a: &mut T, mut b: T) -> DidMerge {
        match Ord::cmp(a, &mut b) {
            Ordering::Less => {
                *a = b;
                DidMerge(true, false)
            }
            Ordering::Equal => DidMerge(false, false),
            Ordering::Greater => DidMerge(false, true),
        }
    }
}

impl Analysis<LogicalPlanLanguage> for LogicalPlanAnalysis {
    type Data = LogicalPlanData;

    fn make(
        egraph: &mut EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Self::Data {
        LogicalPlanData {
            iteration_timestamp: egraph.analysis.iteration_timestamp,
            original_expr: Self::make_original_expr(egraph, enode),
            member_name_to_expr: Self::make_member_name_to_expr(egraph, enode),
            trivial_push_down: Self::make_trivial_push_down(egraph, enode),
            column: Self::make_column_name(egraph, enode),
            expr_to_alias: Self::make_expr_to_alias(egraph, enode),
            referenced_expr: Self::make_referenced_expr(egraph, enode),
            constant: Self::make_constant(egraph, enode),
            constant_in_list: Self::make_constant_in_list(egraph, enode),
            cube_reference: Self::make_cube_reference(egraph, enode),
            is_empty_list: Self::make_is_empty_list(egraph, enode),
            filter_operators: Self::make_filter_operators(egraph, enode),
        }
    }

    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> DidMerge {
        let original_expr = self.merge_option_field(&mut a.original_expr, b.original_expr);
        let member_name_to_expr =
            self.merge_option_field(&mut a.member_name_to_expr, b.member_name_to_expr);
        let trivial_push_down =
            self.merge_option_field(&mut a.trivial_push_down, b.trivial_push_down);
        let column_name_to_alias = self.merge_option_field(&mut a.expr_to_alias, b.expr_to_alias);
        let referenced_columns = self.merge_option_field(&mut a.referenced_expr, b.referenced_expr);
        let constant_in_list = self.merge_option_field(&mut a.constant_in_list, b.constant_in_list);
        let constant = self.merge_option_field(&mut a.constant, b.constant);
        let cube_reference = self.merge_option_field(&mut a.cube_reference, b.cube_reference);
        let is_empty_list = self.merge_option_field(&mut a.is_empty_list, b.is_empty_list);
        let filter_operators = self.merge_option_field(&mut a.filter_operators, b.filter_operators);
        let column_name = self.merge_option_field(&mut a.column, b.column);
        original_expr
            | member_name_to_expr
            | trivial_push_down
            | column_name_to_alias
            | referenced_columns
            | constant_in_list
            | constant
            | cube_reference
            | column_name
            | filter_operators
            | is_empty_list
            | self.merge_max_field(&mut a.iteration_timestamp, b.iteration_timestamp)
    }

    fn modify(egraph: &mut EGraph<LogicalPlanLanguage, Self>, id: Id) {
        if let Some(ConstantFolding::Scalar(c)) = &egraph[id].data.constant {
            // As ConstantFolding goes through Alias we can't add LiteralExpr at this level otherwise it gets dropped.
            // In case there's wrapping node on top of Alias that can be evaluated to LiteralExpr further it gets replaced instead.
            if var_list_iter!(egraph[id], AliasExpr).next().is_some()
                || var_list_iter!(egraph[id], LiteralExpr).next().is_some()
            {
                return;
            }
            let alias_name = egraph[id]
                .data
                .original_expr
                .as_ref()
                .and_then(|e| match e {
                    OriginalExpr::Expr(expr) => Some(expr),
                    OriginalExpr::List(_) => None,
                })
                .map(|expr| expr.name(&DFSchema::empty()).unwrap());
            let c = c.clone();
            let value = egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(c)));
            let literal_expr = egraph.add(LogicalPlanLanguage::LiteralExpr([value]));
            if let Some(alias_name) = alias_name {
                let alias = egraph.add(LogicalPlanLanguage::AliasExprAlias(AliasExprAlias(
                    alias_name.clone(),
                )));
                let alias_expr = egraph.add(LogicalPlanLanguage::AliasExpr([literal_expr, alias]));
                egraph.union(id, alias_expr);
                // egraph[id]
                //     .nodes
                //     .retain(|n| matches!(n, LogicalPlanLanguage::AliasExpr(_)));
            }
        }
    }

    fn allow_ematching_cycles(&self) -> bool {
        false
    }
}
