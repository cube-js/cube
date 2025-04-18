use crate::cube_ext::stream::StreamWithSchema;
use crate::queryplanner::planning::Snapshots;
use crate::CubeError;
use async_trait::async_trait;
use datafusion::arrow::array::{
    make_array, make_builder, Array, ArrayRef, BooleanBuilder, MutableArrayData, UInt64Array,
};
use datafusion::arrow::compute::kernels::numeric::add;
use datafusion::arrow::compute::{concat, concat_batches, filter, SortOptions};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::{Column, DFSchema, DFSchemaRef, DataFusionError, ScalarValue};
use datafusion::execution::{
    FunctionRegistry, SendableRecordBatchStream, SessionState, TaskContext,
};
use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams, Alias};
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{
    EmitTo, Expr, GroupsAccumulator, LogicalPlan, UserDefinedLogicalNode,
};
use datafusion::physical_expr::aggregate::{AggregateExprBuilder, AggregateFunctionExpr};
use datafusion::physical_expr::{
    EquivalenceProperties, GroupsAccumulatorAdapter, LexOrdering, LexRequirement, Partitioning,
    PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement,
};
// TODO upgrade DF
// use datafusion::physical_plan::aggregates::group_values::new_group_values;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    collect, ColumnarValue, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::physical_planner::{
    create_aggregate_expr_and_maybe_filter, ExtensionPlanner, PhysicalPlanner,
};
use datafusion::{arrow, physical_expr, physical_plan};
use datafusion_proto::bytes::Serializeable;
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::LogicalExprNode;
use itertools::Itertools;
use log::debug;
use prost::Message;
use serde_derive::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::{max, Ordering};
use std::collections::HashMap;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct RollingWindowAggregate {
    pub schema: DFSchemaRef,
    pub input: Arc<LogicalPlan>,
    pub dimension: Column,
    pub dimension_alias: String,
    pub from: Expr,
    pub to: Expr,
    pub every: Expr,
    pub partition_by: Vec<Column>,
    pub rolling_aggs: Vec<Expr>,
    pub rolling_aggs_alias: Vec<String>,
    pub group_by_dimension: Option<Expr>,
    pub aggs: Vec<Expr>,
    pub lower_bound: Option<Expr>,
    pub upper_bound: Option<Expr>,
    pub offset_to_end: bool,
}

impl PartialOrd for RollingWindowAggregate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // TODO upgrade DF: Figure out what dyn_ord is used for.

        macro_rules! exit_early {
            ( $x:expr ) => {{
                let res = $x;
                if res != Ordering::Equal {
                    return Some(res);
                }
            }};
        }

        let RollingWindowAggregate {
            schema,
            input,
            dimension,
            dimension_alias,
            from,
            to,
            every,
            partition_by,
            rolling_aggs,
            rolling_aggs_alias,
            group_by_dimension,
            aggs,
            lower_bound,
            upper_bound,
            offset_to_end,
        } = self;

        exit_early!(input.partial_cmp(&other.input)?);
        exit_early!(dimension.cmp(&other.dimension));
        exit_early!(dimension_alias.cmp(&other.dimension_alias));
        exit_early!(from.partial_cmp(&other.from)?);
        exit_early!(from.partial_cmp(&other.from)?);
        exit_early!(to.partial_cmp(&other.to)?);
        exit_early!(every.partial_cmp(&other.every)?);
        exit_early!(partition_by.cmp(&other.partition_by));
        exit_early!(rolling_aggs.partial_cmp(&other.rolling_aggs)?);
        exit_early!(rolling_aggs_alias.cmp(&other.rolling_aggs_alias));
        exit_early!(group_by_dimension.partial_cmp(&other.group_by_dimension)?);
        exit_early!(aggs.partial_cmp(&other.aggs)?);
        exit_early!(lower_bound.partial_cmp(&other.lower_bound)?);
        exit_early!(upper_bound.partial_cmp(&other.upper_bound)?);
        exit_early!(upper_bound.partial_cmp(&other.upper_bound)?);

        if schema.eq(&other.schema) {
            Some(Ordering::Equal)
        } else {
            // Everything but the schema was equal, but schema.eq(&other.schema) returned false.  It must be the schema is
            // different (and incomparable?).  Returning None.
            None
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RollingWindowAggregateSerialized {
    // Column
    pub dimension: Vec<u8>,
    pub dimension_alias: String,
    // Expr
    pub from: Vec<u8>,
    // Expr
    pub to: Vec<u8>,
    // Expr
    pub every: Vec<u8>,
    // Vec<Column>
    pub partition_by: Vec<Vec<u8>>,
    // Vec<Expr>
    pub rolling_aggs: Vec<Vec<u8>>,
    pub rolling_aggs_alias: Vec<String>,
    // Option<Expr>
    pub group_by_dimension: Option<Vec<u8>>,
    // Vec<Expr>
    pub aggs: Vec<Vec<u8>>,
    // Option<Expr>
    pub lower_bound: Option<Vec<u8>>,
    // Option<Expr>
    pub upper_bound: Option<Vec<u8>>,
    pub offset_to_end: bool,
}

impl RollingWindowAggregate {
    pub fn schema_from(
        input: &LogicalPlan,
        dimension: &Column,
        partition_by: &Vec<Column>,
        rolling_aggs: &Vec<Expr>,
        dimension_alias: &String,
        rolling_aggs_alias: &Vec<String>,
        from: &Expr,
    ) -> Result<DFSchemaRef, CubeError> {
        let fields = exprlist_to_fields(
            vec![from.clone()]
                .into_iter()
                .chain(partition_by.iter().map(|c| Expr::Column(c.clone())))
                .chain(rolling_aggs.iter().cloned())
                .zip(
                    vec![dimension_alias.as_str()]
                        .into_iter()
                        .map(|s| (s, None))
                        .chain(partition_by.iter().map(|c| (c.name(), c.relation.as_ref())))
                        .chain(rolling_aggs_alias.iter().map(|a| (a.as_str(), None))),
                )
                .map(|(e, (alias, relation))| {
                    Expr::Alias(Alias {
                        expr: Box::new(e),
                        name: alias.to_string(),
                        relation: relation.cloned(),
                    })
                })
                .collect_vec()
                .as_slice(),
            input,
        )?;

        Ok(Arc::new(DFSchema::new_with_metadata(
            fields,
            input.schema().metadata().clone(),
        )?))
    }

    pub fn from_serialized(
        serialized: RollingWindowAggregateSerialized,
        inputs: &[LogicalPlan],
        registry: &dyn FunctionRegistry,
    ) -> Result<RollingWindowAggregate, CubeError> {
        assert_eq!(inputs.len(), 1);
        let partition_by = serialized
            .partition_by
            .into_iter()
            .map(|c| datafusion_proto_common::Column::decode(c.as_slice()).map(|c| c.into()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| CubeError::from_error(e))?;
        let rolling_aggs = serialized
            .rolling_aggs
            .into_iter()
            .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
            .collect::<Result<Vec<_>, _>>()?;
        let dimension = datafusion_proto_common::Column::decode(serialized.dimension.as_slice())
            .map_err(|e| CubeError::from_error(e))?
            .into();
        let from = Expr::from_bytes_with_registry(serialized.from.as_slice(), registry)?;
        Ok(RollingWindowAggregate {
            schema: RollingWindowAggregate::schema_from(
                &inputs[0],
                &dimension,
                &partition_by,
                &rolling_aggs,
                &serialized.dimension_alias,
                &serialized.rolling_aggs_alias,
                &from,
            )?,
            input: Arc::new(inputs[0].clone()),
            dimension,
            dimension_alias: serialized.dimension_alias,
            from,
            to: Expr::from_bytes_with_registry(serialized.to.as_slice(), registry)?,
            every: Expr::from_bytes_with_registry(serialized.every.as_slice(), registry)?,
            partition_by,
            rolling_aggs,
            rolling_aggs_alias: serialized.rolling_aggs_alias,
            group_by_dimension: serialized
                .group_by_dimension
                .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
                .transpose()?,
            aggs: serialized
                .aggs
                .into_iter()
                .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
                .collect::<Result<Vec<_>, _>>()?,
            lower_bound: serialized
                .lower_bound
                .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
                .transpose()?,
            upper_bound: serialized
                .upper_bound
                .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
                .transpose()?,
            offset_to_end: serialized.offset_to_end,
        })
    }

    pub fn to_serialized(&self) -> Result<RollingWindowAggregateSerialized, CubeError> {
        Ok(RollingWindowAggregateSerialized {
            dimension: datafusion_proto_common::Column::from(&self.dimension).encode_to_vec(),
            dimension_alias: self.dimension_alias.clone(),
            from: self.from.to_bytes()?.to_vec(),
            to: self.to.to_bytes()?.to_vec(),
            every: self.every.to_bytes()?.to_vec(),
            partition_by: self
                .partition_by
                .iter()
                .map(|c| datafusion_proto_common::Column::from(c).encode_to_vec())
                .collect::<Vec<_>>(),
            rolling_aggs: self
                .rolling_aggs
                .iter()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .collect::<Result<Vec<_>, _>>()?,
            rolling_aggs_alias: self.rolling_aggs_alias.clone(),
            group_by_dimension: self
                .group_by_dimension
                .as_ref()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .transpose()?,
            aggs: self
                .aggs
                .iter()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .collect::<Result<Vec<_>, _>>()?,
            lower_bound: self
                .lower_bound
                .as_ref()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .transpose()?,
            upper_bound: self
                .upper_bound
                .as_ref()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .transpose()?,
            offset_to_end: self.offset_to_end,
        })
    }
}

impl UserDefinedLogicalNode for RollingWindowAggregate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "RollingWindowAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn check_invariants(
        &self,
        _check: datafusion::logical_expr::InvariantLevel,
        _plan: &LogicalPlan,
    ) -> datafusion::error::Result<()> {
        // TODO upgrade DF: Might there be something to check?
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut e = vec![
            Expr::Column(self.dimension.clone()),
            self.from.clone(),
            self.to.clone(),
            self.every.clone(),
        ];
        e.extend_from_slice(self.lower_bound.as_slice());
        e.extend_from_slice(self.upper_bound.as_slice());
        e.extend(self.partition_by.iter().map(|c| Expr::Column(c.clone())));
        e.extend_from_slice(self.rolling_aggs.as_slice());
        e.extend_from_slice(self.aggs.as_slice());
        if let Some(d) = &self.group_by_dimension {
            e.push(d.clone());
        }
        e
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ROLLING WINDOW: dimension={}, from={:?}, to={:?}, every={:?}",
            self.dimension, self.from, self.to, self.every
        )
    }

    fn with_exprs_and_inputs(
        &self,
        mut exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Arc<dyn UserDefinedLogicalNode>> {
        assert_eq!(inputs.len(), 1);
        assert_eq!(
            exprs.len(),
            4 + self.partition_by.len()
                + self.rolling_aggs.len()
                + self.aggs.len()
                + self.group_by_dimension.as_ref().map(|_| 1).unwrap_or(0)
                + self.lower_bound.as_ref().map(|_| 1).unwrap_or(0)
                + self.upper_bound.as_ref().map(|_| 1).unwrap_or(0)
        );
        let input = inputs[0].clone();
        let dimension = match &exprs[0] {
            Expr::Column(c) => c.clone(),
            o => panic!("Expected column for dimension, got {:?}", o),
        };
        let from = exprs[1].clone();
        let to = exprs[2].clone();
        let every = exprs[3].clone();

        let lower_bound = if self.lower_bound.is_some() {
            Some(exprs.remove(4))
        } else {
            None
        };

        let upper_bound = if self.upper_bound.is_some() {
            Some(exprs.remove(4))
        } else {
            None
        };

        let exprs = &exprs[4..];

        let partition_by = exprs[..self.partition_by.len()]
            .iter()
            .map(|c| match c {
                Expr::Column(c) => c.clone(),
                o => panic!("Expected column for partition_by, got {:?}", o),
            })
            .collect_vec();
        let exprs = &exprs[self.partition_by.len()..];

        let rolling_aggs = exprs[..self.rolling_aggs.len()].to_vec();
        let exprs = &exprs[self.rolling_aggs.len()..];

        let aggs = exprs[..self.aggs.len()].to_vec();
        let exprs = &exprs[self.aggs.len()..];

        let group_by_dimension = if self.group_by_dimension.is_some() {
            debug_assert_eq!(exprs.len(), 1);
            Some(exprs[0].clone())
        } else {
            debug_assert_eq!(exprs.len(), 0);
            None
        };

        Ok(Arc::new(RollingWindowAggregate {
            schema: self.schema.clone(),
            input: Arc::new(input),
            dimension,
            dimension_alias: self.dimension_alias.clone(),
            from,
            to,
            every,
            partition_by,
            rolling_aggs,
            rolling_aggs_alias: self.rolling_aggs_alias.clone(),
            group_by_dimension,
            aggs,
            lower_bound,
            upper_bound,
            offset_to_end: self.offset_to_end,
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut state = state;
        self.hash(&mut state);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other
            .as_any()
            .downcast_ref::<RollingWindowAggregate>()
            .map(|s| self.eq(s))
            .unwrap_or(false)
    }

    fn dyn_ord(&self, other: &dyn UserDefinedLogicalNode) -> Option<Ordering> {
        other
            .as_any()
            .downcast_ref::<RollingWindowAggregate>()
            .and_then(|s| self.partial_cmp(s))
    }
}

pub struct RollingWindowPlanner {}

#[async_trait]
impl ExtensionPlanner for RollingWindowPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        ctx_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let node = match node.as_any().downcast_ref::<RollingWindowAggregate>() {
            None => return Ok(None),
            Some(n) => n,
        };
        assert_eq!(physical_inputs.len(), 1);
        let input = &physical_inputs[0];
        let input_dfschema = node.input.schema().as_ref();
        let input_schema = input.schema();

        let phys_col = |c: &Column| -> Result<_, DataFusionError> {
            Ok(physical_expr::expressions::Column::new(
                &c.name,
                input_dfschema.index_of_column(c)?,
            ))
        };
        let dimension = phys_col(&node.dimension)?;
        let dimension_type = input_schema.field(dimension.index()).data_type();

        let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let from = planner.create_physical_expr(&node.from, input_dfschema, ctx_state)?;
        let from = expect_non_null_scalar("FROM", from.evaluate(&empty_batch)?, dimension_type)?;

        let to = planner.create_physical_expr(&node.to, input_dfschema, ctx_state)?;
        let to = expect_non_null_scalar("TO", to.evaluate(&empty_batch)?, dimension_type)?;

        let every = planner.create_physical_expr(&node.every, input_dfschema, ctx_state)?;
        let every = expect_non_null_scalar("EVERY", every.evaluate(&empty_batch)?, dimension_type)?;

        let lower_bound = if let Some(lower_bound) = node.lower_bound.as_ref() {
            let lower_bound =
                planner.create_physical_expr(&lower_bound, input_dfschema, ctx_state)?;
            Some(expect_non_null_scalar(
                "Lower bound",
                lower_bound.evaluate(&empty_batch)?,
                dimension_type,
            )?)
        } else {
            None
        };

        let upper_bound = if let Some(upper_bound) = node.upper_bound.as_ref() {
            let upper_bound =
                planner.create_physical_expr(&upper_bound, input_dfschema, ctx_state)?;
            Some(expect_non_null_scalar(
                "Upper bound",
                upper_bound.evaluate(&empty_batch)?,
                dimension_type,
            )?)
        } else {
            None
        };

        if to < from {
            return Err(DataFusionError::Plan("TO is less than FROM".to_string()));
        }
        if add_dim(&from, &every)? <= from {
            return Err(DataFusionError::Plan("EVERY must be positive".to_string()));
        }

        let rolling_aggs = node
            .rolling_aggs
            .iter()
            .map(|e| -> Result<_, DataFusionError> {
                match e {
                    Expr::AggregateFunction(AggregateFunction {
                        func,
                        params: AggregateFunctionParams { args, .. },
                    }) => {
                        let (agg, _, _) = create_aggregate_expr_and_maybe_filter(
                            e,
                            input_dfschema,
                            &input_schema,
                            ctx_state.execution_props(),
                        )?;
                        Ok(RollingAgg {
                            agg: agg.into(),
                            lower_bound: lower_bound.clone(),
                            upper_bound: upper_bound.clone(),
                            offset_to_end: node.offset_to_end,
                        })
                    }
                    _ => panic!("expected ROLLING() aggregate, got {:?}", e),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        let group_by_dimension = node
            .group_by_dimension
            .as_ref()
            .map(|d| planner.create_physical_expr(d, input_dfschema, ctx_state))
            .transpose()?;
        let aggs = node
            .aggs
            .iter()
            .map(|a| {
                create_aggregate_expr_and_maybe_filter(
                    a,
                    input_dfschema,
                    &input_schema,
                    ctx_state.execution_props(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|(a, _, _)| a.into())
            .collect::<Vec<_>>();

        // TODO: filter inputs by date.
        // Do preliminary sorting.
        let mut sort_key = Vec::with_capacity(input_schema.fields().len());
        let mut group_key = Vec::with_capacity(input_schema.fields().len() - 1);
        for c in &node.partition_by {
            let c = phys_col(c)?;
            sort_key.push(PhysicalSortExpr {
                expr: Arc::new(c.clone()),
                options: Default::default(),
            });
            group_key.push(c);
        }
        sort_key.push(PhysicalSortExpr {
            expr: Arc::new(dimension.clone()),
            options: Default::default(),
        });

        let sort = Arc::new(SortExec::new(LexOrdering::new(sort_key), input.clone()));

        let schema = node.schema.as_arrow();

        Ok(Some(Arc::new(RollingWindowAggExec {
            properties: PlanProperties::new(
                // TODO make it maintaining input ordering
                // EquivalenceProperties::new_with_orderings(schema.clone().into(), &[sort_key]),
                EquivalenceProperties::new(schema.clone().into()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Both, // TODO upgrade DF
                Boundedness::Bounded,
            ),
            sorted_input: sort,
            group_key,
            rolling_aggs,
            dimension,
            group_by_dimension,
            aggs,
            from,
            to,
            every,
        })))
    }
}

#[derive(Debug, Clone)]
pub struct RollingAgg {
    /// The bound is inclusive.
    pub lower_bound: Option<ScalarValue>,
    /// The bound is inclusive.
    pub upper_bound: Option<ScalarValue>,
    pub agg: Arc<AggregateFunctionExpr>,
    /// When true, all calculations must be done for the last point in the interval.
    pub offset_to_end: bool,
}

#[derive(Debug, Clone)]
pub struct RollingWindowAggExec {
    pub properties: PlanProperties,
    pub sorted_input: Arc<dyn ExecutionPlan>,
    pub group_key: Vec<physical_plan::expressions::Column>,
    pub rolling_aggs: Vec<RollingAgg>,
    pub dimension: physical_plan::expressions::Column,
    pub group_by_dimension: Option<Arc<dyn PhysicalExpr>>,
    pub aggs: Vec<Arc<AggregateFunctionExpr>>,
    pub from: ScalarValue,
    pub to: ScalarValue,
    pub every: ScalarValue,
}

impl DisplayAs for RollingWindowAggExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RollingWindowAggExec")
    }
}

impl ExecutionPlan for RollingWindowAggExec {
    fn name(&self) -> &str {
        "RollingWindowAggExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.sorted_input]
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        let mut sort_key = Vec::with_capacity(self.schema().fields().len());
        for c in &self.group_key {
            sort_key.push(PhysicalSortRequirement::from(PhysicalSortExpr::new(
                Arc::new(c.clone()),
                SortOptions::default(),
            )));
        }
        sort_key.push(PhysicalSortRequirement::from(PhysicalSortExpr::new(
            Arc::new(self.dimension.clone()),
            SortOptions::default(),
        )));

        vec![Some(LexRequirement::new(sort_key))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // TODO actually it can but right now nulls emitted last
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(RollingWindowAggExec {
            properties: self.properties.clone(),
            sorted_input: children.remove(0),
            group_key: self.group_key.clone(),
            rolling_aggs: self.rolling_aggs.clone(),
            dimension: self.dimension.clone(),
            group_by_dimension: self.group_by_dimension.clone(),
            aggs: self.aggs.clone(),
            from: self.from.clone(),
            to: self.to.clone(),
            every: self.every.clone(),
        }))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        assert_eq!(partition, 0);
        let plan = self.clone();
        let schema = self.schema();

        let fut = async move {
            // Sort keeps everything in-memory anyway. So don't stream and keep implementation simple.
            let batches = collect(plan.sorted_input.clone(), context.clone()).await?;
            let input = concat_batches(&plan.sorted_input.schema(), &batches)?;

            let num_rows = input.num_rows();
            let key_cols = plan
                .group_key
                .iter()
                .map(|c| input.columns()[c.index()].clone())
                .collect_vec();

            // TODO upgrade DF: do we need other_cols?
            // let other_cols = input
            //     .columns()
            //     .iter()
            //     .enumerate()
            //     .filter_map(|(i, c)| {
            //         if plan.dimension.index() == i || plan.group_key.iter().any(|c| c.index() == i)
            //         {
            //             None
            //         } else {
            //             Some(c.clone())
            //         }
            //     })
            //     .collect_vec();
            let agg_inputs = plan
                .rolling_aggs
                .iter()
                .map(|r| compute_agg_inputs(r.agg.as_ref(), &input))
                .collect::<Result<Vec<_>, _>>()?;
            let mut accumulators = plan
                .rolling_aggs
                .iter()
                .map(|r| create_group_accumulator(&r.agg))
                .collect::<Result<Vec<_>, _>>()?;
            let mut dimension = input.column(plan.dimension.index()).clone();
            let dim_iter_type = plan.from.data_type();
            if dimension.data_type() != &dim_iter_type {
                // This is to upcast timestamps to nanosecond precision.
                dimension = arrow::compute::cast(&dimension, &dim_iter_type)?;
            }

            let extra_aggs_dimension = plan
                .group_by_dimension
                .as_ref()
                .map(|d| -> Result<_, DataFusionError> {
                    let mut d = d.evaluate(&input)?.into_array(num_rows)?;
                    if d.data_type() != &dim_iter_type {
                        // This is to upcast timestamps to nanosecond precision.
                        d = arrow::compute::cast(&d, &dim_iter_type)?;
                    }
                    Ok(d)
                })
                .transpose()?;

            // TODO upgrade DF: group_by_dimension_group_values was unused.
            // let mut group_by_dimension_group_values =
            //     new_group_values(Arc::new(Schema::new(vec![input
            //         .schema()
            //         .field(plan.dimension.index())
            //         .clone()])))?;
            let extra_aggs_inputs = plan
                .aggs
                .iter()
                .map(|a| compute_agg_inputs(a.as_ref(), &input))
                .collect::<Result<Vec<_>, _>>()?;

            let mut out_dim = Vec::new(); //make_builder(&plan.from.data_type(), 1);
            let key_cols_data = key_cols.iter().map(|c| c.to_data()).collect::<Vec<_>>();
            let mut out_keys = key_cols_data
                .iter()
                .map(|d| MutableArrayData::new(vec![&d], true, 0))
                .collect_vec();
            // let mut out_aggs = Vec::with_capacity(plan.rolling_aggs.len());
            // This filter must be applied prior to returning the values.
            let mut out_aggs_keep = BooleanBuilder::new();
            let extra_agg_nulls = plan
                .aggs
                .iter()
                .map(|a| ScalarValue::try_from(a.field().data_type()))
                .collect::<Result<Vec<_>, _>>()?;
            let mut out_extra_aggs = plan.aggs.iter().map(|a| Vec::new()).collect::<Vec<_>>();
            // let other_cols_data = other_cols.iter().map(|c| c.to_data()).collect::<Vec<_>>();
            // let mut out_other = other_cols_data
            //     .iter()
            //     .map(|d| MutableArrayData::new(vec![&d], true, 0))
            //     .collect_vec();
            let mut row_i = 0;
            let mut any_group_had_values = vec![];

            let row_converter = RowConverter::new(
                plan.group_key
                    .iter()
                    .map(|c| SortField::new(input.schema().field(c.index()).data_type().clone()))
                    .collect_vec(),
            )?;

            let rows = row_converter.convert_columns(key_cols.as_slice())?;

            let mut group_index = 0;
            while row_i < num_rows {
                let group_start = row_i;
                while row_i + 1 < num_rows
                    && (key_cols.len() == 0 || rows.row(row_i) == rows.row(row_i + 1))
                {
                    row_i += 1;
                }
                let group_end = row_i + 1;
                row_i = group_end;

                // Compute aggregate on each interesting date and add them to the output.
                let mut had_values = Vec::new();
                for (ri, r) in plan.rolling_aggs.iter().enumerate() {
                    // Avoid running indefinitely due to all kinds of errors.
                    let mut window_start = group_start;
                    let mut window_end = group_start;
                    let offset_to_end = if r.offset_to_end {
                        Some(&plan.every)
                    } else {
                        None
                    };

                    let mut d = plan.from.clone();
                    let mut d_iter = 0;
                    while d <= plan.to {
                        while window_start < group_end
                            && !meets_lower_bound(
                                &ScalarValue::try_from_array(&dimension, window_start).unwrap(),
                                &d,
                                r.lower_bound.as_ref(),
                                offset_to_end,
                            )?
                        {
                            window_start += 1;
                        }
                        window_end = max(window_end, window_start);
                        while window_end < group_end
                            && meets_upper_bound(
                                &ScalarValue::try_from_array(&dimension, window_end).unwrap(),
                                &d,
                                r.upper_bound.as_ref(),
                                offset_to_end,
                            )?
                        {
                            window_end += 1;
                        }
                        if had_values.len() == d_iter {
                            had_values.push(window_start != window_end);
                        } else {
                            had_values[d_iter] |= window_start != window_end;
                        }

                        // TODO: pick easy performance wins for SUM() and AVG() with subtraction.
                        //       Also experiment with interval trees for other accumulators.
                        // accumulators[ri].reset();
                        let inputs = agg_inputs[ri]
                            .iter()
                            .map(|a| a.slice(window_start, window_end - window_start))
                            .collect_vec();
                        let for_update = inputs.as_slice();
                        accumulators[ri].update_batch(
                            for_update,
                            (0..(window_end - window_start))
                                .map(|_| group_index)
                                .collect_vec()
                                .as_ref(),
                            None,
                            group_index + 1,
                        )?;
                        group_index += 1;

                        // let v = accumulators[ri].evaluate()?;
                        // if ri == out_aggs.len() {
                        //     out_aggs.push(Vec::new()) //make_builder(v.data_type(), 1));
                        // }
                        // out_aggs[ri].push(v);
                        // append_value(out_aggs[ri].as_mut(), &v)?;

                        const MAX_DIM_ITERATIONS: usize = 10_000_000;
                        d_iter += 1;
                        if d_iter == MAX_DIM_ITERATIONS {
                            return Err(DataFusionError::Execution(
                                "reached the limit of iterations for rolling window dimensions"
                                    .to_string(),
                            ));
                        }
                        d = add_dim(&d, &plan.every)?;
                    }
                }

                if any_group_had_values.is_empty() {
                    any_group_had_values = had_values.clone();
                } else {
                    for i in 0..had_values.len() {
                        any_group_had_values[i] |= had_values[i];
                    }
                }

                // Compute non-rolling aggregates for the group.
                let mut dim_to_extra_aggs = HashMap::new();
                if let Some(key) = &extra_aggs_dimension {
                    let mut key_to_rows = HashMap::new();
                    for i in group_start..group_end {
                        key_to_rows
                            .entry(ScalarValue::try_from_array(key.as_ref(), i)?)
                            .or_insert(Vec::new())
                            .push(i as u64);
                    }

                    for (k, rows) in key_to_rows {
                        let mut accumulators = plan
                            .aggs
                            .iter()
                            .map(|a| a.create_accumulator())
                            .collect::<Result<Vec<_>, _>>()?;
                        let rows = UInt64Array::from(rows);
                        let mut values = Vec::with_capacity(accumulators.len());
                        for i in 0..accumulators.len() {
                            let accum_inputs = extra_aggs_inputs[i]
                                .iter()
                                .map(|a| arrow::compute::take(a.as_ref(), &rows, None))
                                .collect::<Result<Vec<_>, _>>()?;
                            accumulators[i].update_batch(&accum_inputs)?;
                            values.push(accumulators[i].evaluate()?);
                        }

                        dim_to_extra_aggs.insert(k, values);
                    }
                }

                // Add keys, dimension and non-aggregate columns to the output.
                let mut d = plan.from.clone();
                let mut d_iter = 0;
                let mut matching_row_lower_bound = 0;
                while d <= plan.to {
                    if !had_values[d_iter] {
                        out_aggs_keep.append_value(false);

                        d_iter += 1;
                        d = add_dim(&d, &plan.every)?;
                        continue;
                    } else {
                        out_aggs_keep.append_value(true);
                    }
                    // append_value(out_dim.as_mut(), &d)?;
                    out_dim.push(d.clone());
                    for i in 0..key_cols.len() {
                        out_keys[i].extend(0, group_start, group_start + 1)
                    }
                    // Add aggregates.
                    match dim_to_extra_aggs.get(&d) {
                        Some(aggs) => {
                            for i in 0..out_extra_aggs.len() {
                                // append_value(out_extra_aggs[i].as_mut(), &aggs[i])?
                                out_extra_aggs[i].push(aggs[i].clone());
                            }
                        }
                        None => {
                            for i in 0..out_extra_aggs.len() {
                                // append_value(out_extra_aggs[i].as_mut(), &extra_agg_nulls[i])?
                                out_extra_aggs[i].push(extra_agg_nulls[i].clone());
                            }
                        }
                    }
                    // Find the matching row to add other columns.
                    while matching_row_lower_bound < group_end
                        && ScalarValue::try_from_array(&dimension, matching_row_lower_bound)
                            .unwrap()
                            < d
                    {
                        matching_row_lower_bound += 1;
                    }
                    // if matching_row_lower_bound < group_end
                    //     && ScalarValue::try_from_array(&dimension, matching_row_lower_bound)
                    //         .unwrap()
                    //         == d
                    // {
                    //     for i in 0..other_cols.len() {
                    //         out_other[i].extend(
                    //             0,
                    //             matching_row_lower_bound,
                    //             matching_row_lower_bound + 1,
                    //         );
                    //     }
                    // } else {
                    //     for o in &mut out_other {
                    //         o.extend_nulls(1);
                    //     }
                    // }
                    d_iter += 1;
                    d = add_dim(&d, &plan.every)?;
                }
            }

            // We also promise to produce null values for dates missing in the input.
            let mut d = plan.from.clone();
            let mut num_empty_dims = 0;
            for i in 0..any_group_had_values.len() {
                if !any_group_had_values[i] {
                    // append_value(out_dim.as_mut(), &d)?;
                    out_dim.push(d.clone());
                    num_empty_dims += 1;
                }
                d = add_dim(&d, &plan.every)?;
            }
            for c in &mut out_keys {
                c.extend_nulls(num_empty_dims);
            }
            // for c in &mut out_other {
            //     c.extend_nulls(num_empty_dims);
            // }
            for i in 0..accumulators.len() {
                // let null = accumulators[i].evaluate()?;

                for j in 0..num_empty_dims {
                    let inputs = agg_inputs[i].iter().map(|a| a.slice(0, 0)).collect_vec();
                    accumulators[i].update_batch(inputs.as_slice(), &[], None, group_index + 1)?;
                    group_index += 1;
                    // append_value(out_aggs[i].as_mut(), &null)?;
                    // out_aggs[i].push(null.clone());
                }
            }
            for i in 0..out_extra_aggs.len() {
                let null = &extra_agg_nulls[i];
                for _ in 0..num_empty_dims {
                    // append_value(out_extra_aggs[i].as_mut(), &null)?;
                    out_extra_aggs[i].push(null.clone());
                }
            }
            for _ in 0..num_empty_dims {
                out_aggs_keep.append_value(true);
            }

            // Produce final output.
            if out_dim.is_empty() {
                return Ok(RecordBatch::new_empty(plan.schema().clone()));
            };

            let mut r =
                Vec::with_capacity(1 + out_keys.len() /*+ out_other.len()*/ + accumulators.len());
            r.push(ScalarValue::iter_to_array(out_dim)?);
            for k in out_keys {
                r.push(make_array(k.freeze()));
            }
            // for o in out_other {
            //     r.push(make_array(o.freeze()));
            // }

            let out_aggs_keep = out_aggs_keep.finish();
            for mut a in accumulators {
                let eval = a.evaluate(EmitTo::All)?;
                r.push(filter(&eval, &out_aggs_keep)?);
            }

            for a in out_extra_aggs {
                r.push(ScalarValue::iter_to_array(a)?)
            }

            let r = RecordBatch::try_new(plan.schema(), r)?;
            Ok(r)
        };

        let stream = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn add_dim(l: &ScalarValue, r: &ScalarValue) -> Result<ScalarValue, DataFusionError> {
    l.add(r)
}

fn compute_agg_inputs(
    a: &AggregateFunctionExpr,
    input: &RecordBatch,
) -> Result<Vec<ArrayRef>, DataFusionError> {
    a.expressions()
        .iter()
        .map(|e| -> Result<_, DataFusionError> {
            Ok(e.evaluate(input)?.into_array(input.num_rows())?)
        })
        .collect::<Result<Vec<_>, _>>()
}

/// Returns `(value, current+bounds)` pair that can be used for comparison to check window bounds.
fn prepare_bound_compare(
    value: &ScalarValue,
    current: &ScalarValue,
    bound: &ScalarValue,
    offset_to_end: Option<&ScalarValue>,
) -> Result<(i64, i64), DataFusionError> {
    let mut added = add_dim(current, bound)?;
    if let Some(offset) = offset_to_end {
        added = add_dim(&added, offset)?;
    }

    let (mut added, value) = match (added, value) {
        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(v))) => (a, v),
        (
            ScalarValue::TimestampNanosecond(Some(a), None),
            ScalarValue::TimestampNanosecond(Some(v), None),
        ) => (a, v),
        (a, v) => panic!("unsupported values in rolling window: ({:?}, {:?})", a, v),
    };

    if offset_to_end.is_some() {
        added -= 1
    }
    Ok((*value, added))
}

fn meets_lower_bound(
    value: &ScalarValue,
    current: &ScalarValue,
    bound: Option<&ScalarValue>,
    offset_to_end: Option<&ScalarValue>,
) -> Result<bool, DataFusionError> {
    let bound = match bound {
        Some(p) => p,
        None => return Ok(true),
    };
    assert!(!bound.is_null());
    assert!(!current.is_null());
    if value.is_null() {
        return Ok(false);
    }
    let (value, added) = prepare_bound_compare(value, current, bound, offset_to_end)?;
    Ok(added <= value)
}

fn meets_upper_bound(
    value: &ScalarValue,
    current: &ScalarValue,
    bound: Option<&ScalarValue>,
    offset_to_end: Option<&ScalarValue>,
) -> Result<bool, DataFusionError> {
    let bound = match bound {
        Some(p) => p,
        None => return Ok(true),
    };
    assert!(!bound.is_null());
    assert!(!current.is_null());
    if value.is_null() {
        return Ok(false);
    }
    let (value, added) = prepare_bound_compare(value, current, bound, offset_to_end)?;
    Ok(value <= added)
}

fn expect_non_null_scalar(
    var: &str,
    v: ColumnarValue,
    dimension_type: &DataType,
) -> Result<ScalarValue, DataFusionError> {
    match v {
        ColumnarValue::Array(_) => Err(DataFusionError::Plan(format!(
            "expected scalar for {}, got array",
            var
        ))),
        ColumnarValue::Scalar(s) if s.is_null() => match dimension_type {
            DataType::Timestamp(_, None) => Ok(ScalarValue::new_interval_dt(0, 0)),
            _ => Ok(ScalarValue::new_zero(dimension_type)?),
        },
        ColumnarValue::Scalar(s) => Ok(s),
    }
}

pub fn create_group_accumulator(
    agg_expr: &AggregateFunctionExpr,
) -> datafusion::common::Result<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator()
    } else {
        let agg_expr_captured = agg_expr.clone();
        let factory = move || agg_expr_captured.create_accumulator();
        Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
    }
}
