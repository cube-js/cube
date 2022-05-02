use crate::arrow::array::NullArray;
use crate::arrow::datatypes::{DataType, Field, Schema};
use crate::compile::engine::provider::CubeContext;
use crate::compile::rewrite::converter::{is_expr_node, node_to_expr};
use crate::compile::rewrite::AliasExprAlias;
use crate::compile::rewrite::ColumnExprColumn;
use crate::compile::rewrite::DimensionName;
use crate::compile::rewrite::LiteralExprValue;
use crate::compile::rewrite::LogicalPlanLanguage;
use crate::compile::rewrite::MeasureName;
use crate::compile::rewrite::TableScanSourceTableName;
use crate::compile::rewrite::TimeDimensionName;
use crate::var_iter;
use crate::CubeError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_plan::{DFSchema, Expr};
use datafusion::physical_plan::functions::Volatility;
use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
use datafusion::physical_plan::{ColumnarValue, PhysicalPlanner};
use datafusion::scalar::ScalarValue;
use egg::{Analysis, DidMerge};
use egg::{EGraph, Id};
use std::fmt::Debug;
use std::ops::Index;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct LogicalPlanData {
    pub original_expr: Option<Expr>,
    pub member_name_to_expr: Option<Vec<(String, Expr)>>,
    pub column_name: Option<String>,
    pub expr_to_alias: Option<Vec<(Expr, String)>>,
    pub referenced_expr: Option<Vec<Expr>>,
    pub constant: Option<ScalarValue>,
    pub constant_in_list: Option<Vec<ScalarValue>>,
    pub cube_reference: Option<String>,
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
        let column_name = |id| egraph.index(id).data.column_name.clone();
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
        let id_to_column_name = |id| egraph.index(id).data.column_name.clone();
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
                        map.push((original_expr(*id)?, col_name));
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
        let column_name = |id| egraph.index(id).data.column_name.clone();
        let push_referenced_columns = |id, columns: &mut Vec<Expr>| -> Option<()> {
            if column_name(id).is_some() {
                let expr = original_expr(id)?;
                columns.push(expr);
            } else {
                columns.extend(referenced_columns(id)?.into_iter());
            }
            Some(())
        };
        let mut vec = Vec::new();
        match enode {
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
            LogicalPlanLanguage::ScalarFunctionExpr(params) => {
                push_referenced_columns(params[1], &mut vec)?;
                Some(vec)
            }
            LogicalPlanLanguage::ScalarFunctionExprArgs(params) => {
                for p in params.iter() {
                    vec.extend(referenced_columns(*p)?.into_iter());
                }

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
            LogicalPlanLanguage::SortExp(params) => {
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
    ) -> Option<ScalarValue> {
        let constant_expr = |id| {
            egraph
                .index(id)
                .data
                .constant
                .clone()
                .map(|c| Expr::Literal(c))
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
                    Expr::Literal(value) => Some(value),
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
                    if &fun.name == "str_to_date" || &fun.name == "date_add" || &fun.name == "date"
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
                    if fun.volatility() == Volatility::Immutable {
                        Self::eval_constant_expr(&egraph, &expr)
                    } else {
                        None
                    }
                } else {
                    panic!("Expected ScalarFunctionExpr but got: {:?}", expr);
                }
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
                            .map(|c| vec![c])
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
    ) -> Option<ScalarValue> {
        let schema = DFSchema::empty();
        let arrow_schema = Arc::new(schema.to_owned().into());
        let physical_expr = egraph
            .analysis
            .planner
            .create_physical_expr(
                &expr,
                &schema,
                &arrow_schema,
                &egraph.analysis.cube_context.state,
            )
            .expect(&format!("Can't plan expression: {:?}", expr));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "placeholder",
                DataType::Null,
                true,
            )])),
            vec![Arc::new(NullArray::new(1))],
        )
        .unwrap();
        let value = physical_expr
            .evaluate(&batch)
            .expect(&format!("Can't evaluate expression: {:?}", expr));
        Some(match value {
            ColumnarValue::Scalar(value) => value,
            ColumnarValue::Array(arr) => {
                if arr.len() == 1 {
                    ScalarValue::try_from_array(&arr, 0).unwrap()
                } else {
                    panic!(
                        "Expected one row but got {} during constant eval",
                        arr.len()
                    )
                }
            }
        })
    }

    fn make_column_name(
        egraph: &EGraph<LogicalPlanLanguage, Self>,
        enode: &LogicalPlanLanguage,
    ) -> Option<String> {
        let id_to_column_name = |id| egraph.index(id).data.column_name.clone();
        match enode {
            LogicalPlanLanguage::ColumnExprColumn(ColumnExprColumn(c)) => Some(c.name.to_string()),
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
            column_name: Self::make_column_name(egraph, enode),
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
        let (cube_reference, b) = self.merge_option_field(a, b, |d| &mut d.cube_reference);
        let (column_name, _) = self.merge_option_field(a, b, |d| &mut d.column_name);
        original_expr
            | member_name_to_expr
            | column_name_to_alias
            | referenced_columns
            | constant_in_list
            | cube_reference
            | column_name
    }

    fn modify(egraph: &mut EGraph<LogicalPlanLanguage, Self>, id: Id) {
        if let Some(c) = &egraph[id].data.constant {
            let c = c.clone();
            let value = egraph.add(LogicalPlanLanguage::LiteralExprValue(LiteralExprValue(c)));
            let literal_expr = egraph.add(LogicalPlanLanguage::LiteralExpr([value]));
            egraph.union(id, literal_expr);
        }
    }
}
