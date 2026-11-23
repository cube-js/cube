use super::SqlNode;
use crate::physical_plan::QualifiedColumnName;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::time_dimension::Granularity;
use crate::planner::MemberSymbol;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

/// The columns of a pre-aggregation table a select reads from, keyed by
/// the `full_name` of the member each column stores.
#[derive(Default, Clone)]
pub struct PreAggregationColumns {
    columns: HashMap<String, QualifiedColumnName>,
    // Grain columns of stored time dimensions, keyed by their base member,
    // which may be stored raw in `columns` as well.
    grain_columns: HashMap<String, QualifiedColumnName>,
    // Every grain a member is stored at, for a filter to pick one its bounds
    // fall on.
    grains: HashMap<String, Vec<(QualifiedColumnName, Granularity)>>,
}

impl PreAggregationColumns {
    pub fn insert(&mut self, name: String, column: QualifiedColumnName) {
        self.columns.insert(name, column);
    }

    pub fn insert_grain(
        &mut self,
        name: String,
        column: QualifiedColumnName,
        granularity: Option<Granularity>,
    ) {
        if let Some(granularity) = granularity {
            self.grains
                .entry(name.clone())
                .or_default()
                .push((column.clone(), granularity));
        }
        self.grain_columns.insert(name, column);
    }

    /// A grain column holding the inclusive range `[from, to]` exactly.
    pub fn grain_column_for_range(
        &self,
        name: &str,
        from: &str,
        to: &str,
        tz: Tz,
    ) -> Result<Option<&QualifiedColumnName>, CubeError> {
        for (column, granularity) in self.grains.get(name).into_iter().flatten() {
            if granularity.bounds_on_grain(from, to, tz)? {
                return Ok(Some(column));
            }
        }
        Ok(None)
    }

    /// The raw column, or the grain column of a member stored only as one.
    pub fn get(&self, name: &str) -> Option<&QualifiedColumnName> {
        self.columns
            .get(name)
            .or_else(|| self.grain_columns.get(name))
    }

    /// The grain column, or the raw column of a member stored only as one.
    pub fn get_for_time_dimension_read(&self, name: &str) -> Option<&QualifiedColumnName> {
        self.grain_columns
            .get(name)
            .or_else(|| self.columns.get(name))
    }

    pub fn has_raw(&self, name: &str) -> bool {
        self.columns.contains_key(name)
    }

    pub fn has_grain(&self, name: &str) -> bool {
        self.grain_columns.contains_key(name)
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && self.grain_columns.is_empty()
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.columns.contains_key(name) || self.grain_columns.contains_key(name)
    }
}

/// Renders a member stored in the pre-aggregation as its column,
/// falling through to `input` for any other member.
pub struct PreAggregationColumnSqlNode {
    input: Rc<dyn SqlNode>,
    columns: Rc<PreAggregationColumns>,
}

impl PreAggregationColumnSqlNode {
    pub fn new(input: Rc<dyn SqlNode>, columns: Rc<PreAggregationColumns>) -> Rc<Self> {
        Rc::new(Self { input, columns })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for PreAggregationColumnSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let name = node.full_name();
        let column = if let Some(column) = visitor.stored_column_for(&name) {
            Some(column)
        } else if visitor.time_dimension_read() {
            self.columns.get_for_time_dimension_read(&name)
        } else {
            self.columns.get(&name)
        };
        if let Some(column) = column {
            templates.column_reference(column.source(), column.name())
        } else {
            self.input.to_sql(
                visitor,
                node,
                query_tools.clone(),
                node_processor.clone(),
                templates,
            )
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
