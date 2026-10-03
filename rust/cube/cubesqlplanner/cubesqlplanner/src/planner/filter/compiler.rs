use super::base_filter::{BaseFilter, FilterType};
use super::FilterOperator;
use crate::cube_bridge::base_query_options::{FilterItem as NativeFilterItem, FilterValue};
use crate::planner::filter::{FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::query_tools::QueryTools;
use crate::planner::{Compiler, CubeId, MemberSymbol, SymbolPath, SymbolPathType};
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
    owner: Option<CubeId>,
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
            owner: None,
            dimension_filters: vec![],
            time_dimension_filters: vec![],
            measures_filters: vec![],
            member_paths: HashMap::new(),
        }
    }

    /// Filters a member of `owner` declares. Inside a joined cube instance
    /// their members name the data-model cube, so they resolve relative to
    /// the instance.
    pub fn for_owner(mut self, owner: CubeId) -> Self {
        self.owner = Some(owner);
        self
    }

    fn parse_member(&self, member: &str) -> Result<SymbolPath, CubeError> {
        match self.owner.as_ref().filter(|owner| owner.is_joined()) {
            Some(owner) => {
                let parts = member.split('.').map(|s| s.to_string()).collect::<Vec<_>>();
                SymbolPath::parse_parts(self.query_tools.model_cubes(), Some(owner), &parts)
            }
            None => SymbolPath::parse(self.query_tools.model_cubes(), member),
        }
    }

    // TODO classify time-dimension filters into `time_dimension_filters` so
    // callers like the multi-stage `filter:` directive can route them to
    // `QueryProperties::time_dimensions_filters` instead of treating every
    // include as a plain dimension filter.
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
                Ok(FilterItem::Item(BaseFilter::try_new(
                    self.query_tools.clone(),
                    evaluator,
                    item_type.clone(),
                    FilterOperator::from_str(&operator)?,
                    item.values.clone(),
                    Some(&mut *self.evaluator_compiler),
                )?))
            } else {
                Err(CubeError::user(format!(
                    "Member and operator attributes is required for filter"
                ))) //TODO pring condition
            }
        }
    }

    // Resolved once per member: classifying a filter and compiling it both
    // need the path, and resolving it calls into the data model.
    fn member_path(&mut self, member: &String) -> Result<SymbolPath, CubeError> {
        if let Some(path) = self.member_paths.get(member) {
            return Ok(path.clone());
        }
        let path = self.parse_member(member)?;
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
