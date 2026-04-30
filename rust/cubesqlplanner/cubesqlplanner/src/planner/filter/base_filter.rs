use super::filter_operator::FilterOperator;
use super::typed_filter::{resolve_base_symbol, TypedFilter};
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterType {
    Dimension,
    Measure,
}

// TODO: temporary compatibility proxy — collapse into TypedFilter and update FilterItem consumers
#[derive(Clone)]
pub struct BaseFilter {
    typed_filter: TypedFilter,
}

impl PartialEq for BaseFilter {
    fn eq(&self, other: &Self) -> bool {
        self.typed_filter.filter_type() == other.typed_filter.filter_type()
            && self.typed_filter.operator() == other.typed_filter.operator()
            && self.typed_filter.values() == other.typed_filter.values()
    }
}

impl BaseFilter {
    pub fn try_new(
        query_tools: Rc<crate::planner::query_tools::QueryTools>,
        member_evaluator: Rc<MemberSymbol>,
        filter_type: FilterType,
        filter_operator: FilterOperator,
        values: Option<Vec<Option<String>>>,
    ) -> Result<Rc<Self>, CubeError> {
        let typed_filter = TypedFilter::builder()
            .query_tools(query_tools)
            .member_evaluator(member_evaluator)
            .filter_type(filter_type)
            .operator(filter_operator)
            .values(values)
            .build()?;

        Ok(Rc::new(Self { typed_filter }))
    }

    pub fn change_operator(
        &self,
        filter_operator: FilterOperator,
        values: Vec<Option<String>>,
        use_raw_values: bool,
    ) -> Result<Rc<Self>, CubeError> {
        let typed_filter = self
            .typed_filter
            .to_builder()
            .operator(filter_operator)
            .values(Some(values))
            .use_raw_values(use_raw_values)
            .build()?;

        Ok(Rc::new(Self { typed_filter }))
    }

    pub fn member_evaluator(&self) -> Rc<MemberSymbol> {
        resolve_base_symbol(self.typed_filter.member_evaluator())
    }

    pub fn raw_member_evaluator(&self) -> Rc<MemberSymbol> {
        self.typed_filter.member_evaluator().clone()
    }

    pub fn with_member_evaluator(
        &self,
        member_evaluator: Rc<MemberSymbol>,
    ) -> Result<Rc<Self>, CubeError> {
        let typed_filter = self
            .typed_filter
            .to_builder()
            .member_evaluator(member_evaluator)
            .build()?;

        Ok(Rc::new(Self { typed_filter }))
    }

    pub fn time_dimension_symbol(&self) -> Option<Rc<MemberSymbol>> {
        if self
            .typed_filter
            .member_evaluator()
            .as_time_dimension()
            .is_ok()
        {
            Some(self.typed_filter.member_evaluator().clone())
        } else {
            None
        }
    }

    pub fn values(&self) -> &Vec<Option<String>> {
        self.typed_filter.values()
    }

    pub fn filter_operator(&self) -> &FilterOperator {
        self.typed_filter.operator()
    }

    pub fn use_raw_values(&self) -> bool {
        self.typed_filter.use_raw_values()
    }

    pub fn member_name(&self) -> String {
        self.member_evaluator().full_name()
    }

    pub fn is_single_value_equal(&self) -> bool {
        self.typed_filter.values().len() == 1
            && *self.typed_filter.operator() == FilterOperator::Equal
    }

    pub fn get_value_restrictions(&self) -> Option<Vec<String>> {
        if *self.typed_filter.operator() == FilterOperator::In
            || *self.typed_filter.operator() == FilterOperator::Equal
        {
            Some(
                self.typed_filter
                    .values()
                    .iter()
                    .cloned()
                    .filter_map(|v| v)
                    .collect_vec(),
            )
        } else {
            None
        }
    }

    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        plan_templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let filters_context = context.filters_context();
        if !filters_context.filter_params_columns.is_empty() {
            let symbol_to_match =
                resolve_base_symbol(self.typed_filter.member_evaluator()).resolve_reference_chain();
            if let Some(filter_params_column) = filters_context
                .filter_params_columns
                .get(&symbol_to_match.full_name())
            {
                return self.typed_filter.to_sql_for_filter_params(
                    filter_params_column,
                    plan_templates,
                    filters_context,
                );
            }
        }
        self.typed_filter.to_sql(context, plan_templates)
    }
}
