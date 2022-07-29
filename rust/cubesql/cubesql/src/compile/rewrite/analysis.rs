use crate::{
    compile::{
        engine::provider::CubeContext,
        rewrite::{
            converter::{is_expr_node, node_to_expr},
            AliasExprAlias, ColumnExprColumn, DimensionName, LiteralExprValue, LogicalPlanLanguage,
            MeasureName, SegmentName, TableScanSourceTableName, TimeDimensionName,
        },
    },
    var_iter, CubeError,
};
use datafusion::{
    arrow::{
        array::NullArray,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    logical_plan::{Column, DFSchema, Expr},
    physical_plan::{
        functions::Volatility, planner::DefaultPhysicalPlanner, ColumnarValue, PhysicalPlanner,
    },
    scalar::ScalarValue,
};
use egg::{Analysis, DidMerge, EGraph, Id};
use std::{fmt::Debug, ops::Index, sync::Arc};

#[derive(Clone, Debug)]
pub struct LogicalPlanData {
    pub original_expr: Option<Expr>,
    pub member_name_to_expr: Option<Vec<(String, Expr)>>,
    pub column: Option<Column>,
    pub expr_to_alias: Option<Vec<(Expr, String)>>,
    pub referenced_expr: Option<Vec<Expr>>,
    pub constant: Option<ConstantFolding>,
    pub constant_in_list: Option<Vec<ScalarValue>>,
    pub cube_reference: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ConstantFolding {
    Scalar(ScalarValue),
    List(Vec<ScalarValue>),
}

#[derive(Clone)]
pub struct LogicalPlanAnalysis {
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
        &self.egraph.index(index).nodes[0]
    }
}

impl LogicalPlanAnalysis {
    pub fn new(cube_context: Arc<CubeContext>, planner: Arc<DefaultPhysicalPlanner>) -> Self {
        Self {
            cube_context,
            planner,
        }
    }

    fn make_original_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Expr> {
        let id_to_expr = |id| {
            egraph[id].data.original_expr.clone().ok_or_else(|| {
                CubeError::internal(format!(
                    "Original expr wasn't prepared for {:?}",
                    egraph[id]
                ))
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
        } else {
            None
        };
        original_expr
    }

    fn make_member_name_to_expr(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<(String, Expr)>> {
        let column_name = |id| egraph.index(id).data.column.clone();
        let id_to_column_name_to_expr = |id| egraph.index(id).data.member_name_to_expr.clone();
        let original_expr = |id| egraph.index(id).data.original_expr.clone();
        let mut map = Vec::new();
        match enode {
            LogicalPlanLanguage::Measure(params) => {
                if let Some(_) = column_name(params[1]) {
                    let expr = original_expr(params[1])?;
                    let measure_name = var_iter!(egraph[params[0]], MeasureName).next().unwrap();
                    map.push((measure_name.to_string(), expr.clone()));
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
                    map.push((dimension_name.to_string(), expr));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::Segment(params) => {
                if let Some(_) = column_name(params[1]) {
                    let expr = original_expr(params[1])?;
                    let segment_name = var_iter!(egraph[params[0]], SegmentName).next().unwrap();
                    map.push((segment_name.to_string(), expr));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::ChangeUser(params) => {
                if let Some(expr) = original_expr(params[0]) {
                    map.push(("__user".to_string(), expr));
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
                    map.push((time_dimension_name.to_string(), expr));
                    Some(map)
                } else {
                    None
                }
            }
            LogicalPlanLanguage::CubeScanMembers(params) => {
                for id in params.iter() {
                    map.extend(id_to_column_name_to_expr(*id)?.into_iter());
                }
                Some(map)
            }
            LogicalPlanLanguage::CubeScan(params) => {
                map.extend(id_to_column_name_to_expr(params[1])?.into_iter());
                Some(map)
            }
            _ => None,
        }
    }

    fn make_expr_to_alias(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<Vec<(Expr, String)>> {
        let original_expr = |id| egraph.index(id).data.original_expr.clone();
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
                ));
                Some(map)
            }
            LogicalPlanLanguage::ProjectionExpr(params) => {
                for id in params.iter() {
                    if let Some(col_name) = id_to_column_name(*id) {
                        map.push((original_expr(*id)?, col_name.name.to_string()));
                    } else {
                        map.extend(column_name_to_alias(*id)?.into_iter());
                    }
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
        let original_expr = |id| egraph.index(id).data.original_expr.clone();
        let column_name = |id| egraph.index(id).data.column.clone();
        let push_referenced_columns = |id, columns: &mut Vec<Expr>| -> Option<()> {
            if let Some(col) = column_name(id) {
                let expr = Expr::Column(col);
                columns.push(expr);
            } else {
                columns.extend(referenced_columns(id)?.into_iter());
            }
            Some(())
        };
        let mut vec = Vec::new();
        match enode {
            LogicalPlanLanguage::ColumnExpr(params) => {
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
            LogicalPlanLanguage::AggregateFunctionExpr(params) => {
                push_referenced_columns(params[1], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::LiteralExpr(_) => Some(vec),
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
            | LogicalPlanLanguage::CaseExprWhenThenExpr(params)
            | LogicalPlanLanguage::CaseExprElseExpr(params)
            | LogicalPlanLanguage::CaseExprExpr(params)
            | LogicalPlanLanguage::AggregateFunctionExprArgs(params)
            | LogicalPlanLanguage::ScalarFunctionExprArgs(params) => {
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
                let expr = node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &constant_expr,
                    &SingleNodeIndex { egraph },
                )
                .ok()?;
                match expr {
                    Expr::Literal(value) => Some(ConstantFolding::Scalar(value)),
                    _ => panic!("Expected Literal but got: {:?}", expr),
                }
            }
            LogicalPlanLanguage::ScalarUDFExpr(_) => {
                let expr = node_to_expr(
                    enode,
                    &egraph.analysis.cube_context,
                    &constant_expr,
                    &SingleNodeIndex { egraph },
                )
                .ok()?;
                if let Expr::ScalarUDF { fun, .. } = &expr {
                    if &fun.name == "str_to_date"
                        || &fun.name == "date_add"
                        || &fun.name == "date_sub"
                        || &fun.name == "date"
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
                    if fun.volatility() == Volatility::Immutable
                        || fun.volatility() == Volatility::Stable
                    {
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
            LogicalPlanLanguage::AnyExpr(_) => {
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

                // Ignore any string casts as local timestamps casted incorrectly
                if let Expr::Cast { expr, .. } = &expr {
                    if let Expr::Literal(ScalarValue::Utf8(_)) = expr.as_ref() {
                        return None;
                    }
                }

                // TODO: Support decimal type in filters and remove it
                if let Expr::Cast {
                    data_type: DataType::Decimal(_, _),
                    ..
                } = &expr
                {
                    return None;
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

                match &expr {
                    Expr::BinaryExpr { left, right, .. } => match (&**left, &**right) {
                        (Expr::Literal(ScalarValue::IntervalYearMonth(_)), Expr::Literal(_))
                        | (Expr::Literal(ScalarValue::IntervalDayTime(_)), Expr::Literal(_))
                        | (Expr::Literal(ScalarValue::IntervalMonthDayNano(_)), Expr::Literal(_))
                        | (Expr::Literal(_), Expr::Literal(ScalarValue::IntervalYearMonth(_)))
                        | (Expr::Literal(_), Expr::Literal(ScalarValue::IntervalDayTime(_)))
                        | (Expr::Literal(_), Expr::Literal(ScalarValue::IntervalMonthDayNano(_))) => {
                            return None
                        }
                        _ => (),
                    },
                    _ => (),
                }

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
        egraph: &EGraph<LogicalPlanLanguage, LogicalPlanAnalysis>,
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
            LogicalPlanLanguage::Extension(params) => cube_reference(params[0]),
            _ => None,
        }
    }

    fn merge_option_field<T: Clone>(
        &mut self,
        a: &mut LogicalPlanData,
        mut b: LogicalPlanData,
        field: fn(&mut LogicalPlanData) -> &mut Option<T>,
    ) -> (DidMerge, LogicalPlanData) {
        let res = if field(a).is_none() && field(&mut b).is_some() {
            *field(a) = field(&mut b).clone();
            DidMerge(true, false)
        } else if field(a).is_some() {
            DidMerge(false, true)
        } else {
            DidMerge(false, false)
        };
        (res, b)
    }
}

impl Analysis<LogicalPlanLanguage> for LogicalPlanAnalysis {
    type Data = LogicalPlanData;

    fn make(egraph: &EGraph<LogicalPlanLanguage, Self>, enode: &LogicalPlanLanguage) -> Self::Data {
        LogicalPlanData {
            original_expr: Self::make_original_expr(egraph, enode),
            member_name_to_expr: Self::make_member_name_to_expr(egraph, enode),
            column: Self::make_column_name(egraph, enode),
            expr_to_alias: Self::make_expr_to_alias(egraph, enode),
            referenced_expr: Self::make_referenced_expr(egraph, enode),
            constant: Self::make_constant(egraph, enode),
            constant_in_list: Self::make_constant_in_list(egraph, enode),
            cube_reference: Self::make_cube_reference(egraph, enode),
        }
    }

    fn merge(&mut self, a: &mut Self::Data, b: Self::Data) -> DidMerge {
        let (original_expr, b) = self.merge_option_field(a, b, |d| &mut d.original_expr);
        let (member_name_to_expr, b) =
            self.merge_option_field(a, b, |d| &mut d.member_name_to_expr);
        let (column_name_to_alias, b) = self.merge_option_field(a, b, |d| &mut d.expr_to_alias);
        let (referenced_columns, b) = self.merge_option_field(a, b, |d| &mut d.referenced_expr);
        let (constant_in_list, b) = self.merge_option_field(a, b, |d| &mut d.constant_in_list);
        let (constant, b) = self.merge_option_field(a, b, |d| &mut d.constant);
        let (cube_reference, b) = self.merge_option_field(a, b, |d| &mut d.cube_reference);
        let (column_name, _) = self.merge_option_field(a, b, |d| &mut d.column);
        original_expr
            | member_name_to_expr
            | column_name_to_alias
            | referenced_columns
            | constant_in_list
            | constant
            | cube_reference
            | column_name
    }

    fn modify(egraph: &mut EGraph<LogicalPlanLanguage, Self>, id: Id) {
        if let Some(ConstantFolding::Scalar(c)) = &egraph[id].data.constant {
            let c = c.clone();
            let value = egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(c)));
            let literal_expr = egraph.add(LogicalPlanLanguage::LiteralExpr([value]));
            egraph.union(id, literal_expr);
        }
    }
}
