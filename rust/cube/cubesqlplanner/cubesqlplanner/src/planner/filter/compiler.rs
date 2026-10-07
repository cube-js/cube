use super::base_filter::{BaseFilter, FilterType};
use super::FilterOperator;
use crate::cube_bridge::base_query_options::{FilterItem as NativeFilterItem, FilterValue};
use crate::planner::filter::{FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::query_tools::QueryTools;
use crate::planner::time_dimension::resolve_relative_date_range;
use crate::planner::{Compiler, MemberSymbol, SymbolPath, SymbolPathType, TimeDimensionSymbol};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;
use std::str::FromStr;

/// Compiles the data-model filter tree into three streams of
/// `FilterItem`s — filters on dimensions, on time dimensions, and
/// on measures. Time-dimension `date_range` attributes arrive
/// separately via `add_time_dimension_item` and are normalised into
/// `InDateRange` filters.
pub struct FilterCompiler<'a> {
    evaluator_compiler: &'a mut Compiler,
    query_tools: Rc<QueryTools>,
    dimension_filters: Vec<FilterItem>,
    time_dimension_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    member_paths: HashMap<String, SymbolPath>,
}

impl<'a> FilterCompiler<'a> {
    pub fn new(evaluator_compiler: &'a mut Compiler, query_tools: Rc<QueryTools>) -> Self {
        Self {
            evaluator_compiler,
            query_tools,
            dimension_filters: vec![],
            time_dimension_filters: vec![],
            measures_filters: vec![],
            member_paths: HashMap::new(),
        }
    }

    pub fn add_item(&mut self, item: &NativeFilterItem) -> Result<(), CubeError> {
        if let Some(item_type) = self.get_item_type(item, &None)? {
            let compiled_item = self.compile_item(item, &item_type)?;
            match item_type {
                FilterType::Dimension => self.dimension_filters.push(compiled_item),
                FilterType::Measure => self.measures_filters.push(compiled_item),
            }
        }
        Ok(())
    }

    /// Like `add_item`, but a date range on a time dimension goes to
    /// `time_dimension_filters`, where it bounds rolling windows the way a
    /// query's `dateRange` does. Used for the multi-stage `filter.include`.
    pub fn add_include_item(&mut self, item: &NativeFilterItem) -> Result<(), CubeError> {
        if let Some(FilterType::Dimension) = self.get_item_type(item, &None)? {
            let compiled_item = self.compile_item(item, &FilterType::Dimension)?;
            match self.as_time_dimension_date_range(&compiled_item)? {
                Some(filter) => self.time_dimension_filters.push(filter),
                None => self.dimension_filters.push(compiled_item),
            }
            return Ok(());
        }
        self.add_item(item)
    }

    /// The filter re-targeted at the time dimension without granularity, the
    /// member a query's `dateRange` filters, so rollups match it as well.
    fn as_time_dimension_date_range(
        &self,
        item: &FilterItem,
    ) -> Result<Option<FilterItem>, CubeError> {
        let FilterItem::Item(filter) = item else {
            return Ok(None);
        };
        let member = filter.member_evaluator();
        let is_time = member
            .clone()
            .resolve_reference_chain()
            .as_dimension()
            .is_ok_and(|dimension| dimension.is_time());
        if !is_time || !matches!(filter.filter_operator(), FilterOperator::InDateRange) {
            return Ok(None);
        }
        let time_dimension =
            MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(member, None, None, None));
        Ok(Some(FilterItem::Item(BaseFilter::try_new(
            self.query_tools.clone(),
            time_dimension,
            FilterType::Dimension,
            FilterOperator::InDateRange,
            Some(filter.values().clone()),
            None,
        )?)))
    }

    /// Lifts the optional `date_range` of a time-dimension request
    /// into an explicit `InDateRange` filter on
    /// `time_dimension_filters`.
    pub fn add_time_dimension_item(&mut self, item: &Rc<MemberSymbol>) -> Result<(), CubeError> {
        if let Ok(td_item) = item.as_time_dimension() {
            if let Some(date_range) = td_item.date_range_vec() {
                let filter = BaseFilter::try_new(
                    self.query_tools.clone(),
                    item.clone(),
                    FilterType::Dimension,
                    FilterOperator::InDateRange,
                    Some(date_range.into_iter().map(FilterValue::Str).collect()),
                    None,
                )?;
                self.time_dimension_filters.push(FilterItem::Item(filter));
            }
        }
        Ok(())
    }

    /// Consumes the compiler and returns the three collected
    /// streams: `(dimension_filters, time_dimension_filters,
    /// measure_filters)`.
    pub fn extract_result(self) -> (Vec<FilterItem>, Vec<FilterItem>, Vec<FilterItem>) {
        (
            self.dimension_filters,
            self.time_dimension_filters,
            self.measures_filters,
        )
    }

    /// Iterator over every compiled filter item across the three buckets.
    pub fn iter_all_items(&self) -> impl Iterator<Item = &FilterItem> {
        self.dimension_filters
            .iter()
            .chain(self.time_dimension_filters.iter())
            .chain(self.measures_filters.iter())
    }

    fn compile_item(
        &mut self,
        item: &NativeFilterItem,
        item_type: &FilterType,
    ) -> Result<FilterItem, CubeError> {
        let group_op_and_values = if let Some(items) = &item.or {
            Some((FilterGroupOperator::Or, items))
        } else if let Some(items) = &item.and {
            Some((FilterGroupOperator::And, items))
        } else {
            None
        };

        if let Some((op, values)) = group_op_and_values {
            let items = values
                .iter()
                .map(|itm| self.compile_item(itm, item_type))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(FilterItem::Group(Rc::new(FilterGroup::new(op, items))))
        } else {
            if let (Some(member), Some(operator)) = (item.member(), &item.operator) {
                let path = self.member_path(member)?;
                let evaluator = if path.path_type() == &SymbolPathType::Measure {
                    self.evaluator_compiler
                        .add_measure_evaluator_by_path(path)?
                } else {
                    self.evaluator_compiler
                        .add_dimension_or_segment_by_path(path)?
                };
                let operator = FilterOperator::from_str(&operator)?;
                let values = self.resolve_relative_dates(&operator, &item.values)?;
                Ok(FilterItem::Item(BaseFilter::try_new(
                    self.query_tools.clone(),
                    evaluator,
                    item_type.clone(),
                    operator,
                    values,
                    Some(&mut *self.evaluator_compiler),
                )?))
            } else {
                Err(CubeError::user(format!(
                    "Member and operator attributes is required for filter"
                ))) //TODO pring condition
            }
        }
    }

    /// A single relative date value (`this month`, `last 7 days`, ...) becomes
    /// absolute bounds in the query's time zone, as the REST API resolves it
    /// for query filters: both bounds for a range, the start for `beforeDate`
    /// / `afterOrOnDate`, the end for `beforeOrOnDate` / `afterDate`.
    fn resolve_relative_dates(
        &self,
        operator: &FilterOperator,
        values: &Option<Vec<FilterValue>>,
    ) -> Result<Option<Vec<FilterValue>>, CubeError> {
        let Some([FilterValue::Str(value)]) = values.as_deref() else {
            return Ok(values.clone());
        };
        let bounds = match operator {
            FilterOperator::InDateRange
            | FilterOperator::NotInDateRange
            | FilterOperator::BeforeDate
            | FilterOperator::AfterOrOnDate
            | FilterOperator::BeforeOrOnDate
            | FilterOperator::AfterDate => {
                resolve_relative_date_range(value, self.query_tools.timezone())?
            }
            _ => None,
        };
        let Some((start, end)) = bounds else {
            return Ok(values.clone());
        };
        let resolved = match operator {
            FilterOperator::BeforeDate | FilterOperator::AfterOrOnDate => vec![start],
            FilterOperator::BeforeOrOnDate | FilterOperator::AfterDate => vec![end],
            _ => vec![start, end],
        };
        Ok(Some(resolved.into_iter().map(FilterValue::Str).collect()))
    }

    // Resolved once per member: classifying a filter and compiling it both
    // need the path, and resolving it calls into the data model.
    fn member_path(&mut self, member: &String) -> Result<SymbolPath, CubeError> {
        if let Some(path) = self.member_paths.get(member) {
            return Ok(path.clone());
        }
        let path = SymbolPath::parse(self.query_tools.cube_evaluator().clone(), member)?;
        self.member_paths.insert(member.clone(), path.clone());
        Ok(path)
    }

    fn get_item_type(
        &mut self,
        item: &NativeFilterItem,
        expected_type: &Option<FilterType>,
    ) -> Result<Option<FilterType>, CubeError> {
        if let Some(items) = &item.or {
            self.get_item_type_from_vec(&items, expected_type)
        } else if let Some(items) = &item.and {
            self.get_item_type_from_vec(&items, expected_type)
        } else {
            if let (Some(member), Some(operator)) = (item.member(), &item.operator) {
                let operator = FilterOperator::from_str(&operator)?;
                let is_measure_filter_op = matches!(operator, FilterOperator::MeasureFilter);
                let path = self.member_path(member)?;
                if path.path_type() == &SymbolPathType::Measure && !is_measure_filter_op {
                    Ok(Some(FilterType::Measure))
                } else {
                    Ok(Some(FilterType::Dimension))
                }
            } else {
                Err(CubeError::user(format!(
                    "Member and operator attributes is required for filter"
                ))) //TODO print condition
            }
        }
    }

    fn get_item_type_from_vec(
        &mut self,
        items: &Vec<NativeFilterItem>,
        expected_type: &Option<FilterType>,
    ) -> Result<Option<FilterType>, CubeError> {
        let mut result = expected_type.clone();
        for itm in items {
            let item_type = self.get_item_type(&itm, &result)?;
            if let (Some(expected), Some(item_type)) = (&result, &item_type) {
                if expected != item_type {
                    return Err(CubeError::user(format!(
                        "You cannot use dimension and measure in same condition"
                    ))); //TODO pring condition
                }
            } else if result.is_none() {
                result = item_type;
            }
        }
        Ok(result)
    }
}
