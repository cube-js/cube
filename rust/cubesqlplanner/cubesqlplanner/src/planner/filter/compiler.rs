use super::base_filter::{BaseFilter, FilterType};
use super::FilterOperator;
use crate::cube_bridge::base_query_options::FilterItem as NativeFilterItem;
use crate::plan::filter::{FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{Compiler, MemberSymbol};
use cubenativeutils::CubeError;
use std::rc::Rc;
use std::str::FromStr;

pub struct FilterCompiler<'a> {
    evaluator_compiler: &'a mut Compiler,
    query_tools: Rc<QueryTools>,
    dimension_filters: Vec<FilterItem>,
    time_dimension_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
}

impl<'a> FilterCompiler<'a> {
    pub fn new(evaluator_compiler: &'a mut Compiler, query_tools: Rc<QueryTools>) -> Self {
        Self {
            evaluator_compiler,
            query_tools,
            dimension_filters: vec![],
            time_dimension_filters: vec![],
            measures_filters: vec![],
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

    pub fn add_time_dimension_item(&mut self, item: &Rc<MemberSymbol>) -> Result<(), CubeError> {
        if let Ok(td_item) = item.as_time_dimension() {
            if let Some(date_range) = td_item.date_range_vec() {
                let filter = BaseFilter::try_new(
                    self.query_tools.clone(),
                    item.clone(),
                    FilterType::Dimension,
                    FilterOperator::InDateRange,
                    Some(date_range.into_iter().map(|v| Some(v)).collect()),
                )?;
                self.time_dimension_filters.push(FilterItem::Item(filter));
            }
        }
        Ok(())
    }

    pub fn extract_result(self) -> (Vec<FilterItem>, Vec<FilterItem>, Vec<FilterItem>) {
        (
            self.dimension_filters,
            self.time_dimension_filters,
            self.measures_filters,
        )
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
                let member_path = member.split(".").map(|m| m.to_string()).collect::<Vec<_>>();
                let evaluator = if self.query_tools.cube_evaluator().is_measure(member_path)? {
                    self.evaluator_compiler
                        .add_measure_evaluator(member.clone())?
                } else {
                    self.evaluator_compiler
                        .add_dimension_evaluator(member.clone())?
                };
                Ok(FilterItem::Item(BaseFilter::try_new(
                    self.query_tools.clone(),
                    evaluator,
                    item_type.clone(),
                    FilterOperator::from_str(&operator)?,
                    item.values.clone(),
                )?))
            } else {
                Err(CubeError::user(format!(
                    "Member and operator attributes is required for filter"
                ))) //TODO pring condition
            }
        }
    }

    fn get_item_type(
        &self,
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
                let member_path = member.split(".").map(|m| m.to_string()).collect::<Vec<_>>();
                if self.query_tools.cube_evaluator().is_measure(member_path)?
                    && !is_measure_filter_op
                {
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
        &self,
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
