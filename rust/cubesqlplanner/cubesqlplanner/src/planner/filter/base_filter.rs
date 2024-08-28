use super::filter_operator::FilterOperator;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberEvaluator;
use crate::planner::sql_templates::filter::FilterTemplates;
use crate::planner::{BaseMember, IndexedMember};
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use std::rc::Rc;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterType {
    Dimension,
    Measure,
}

pub struct BaseFilter {
    query_tools: Rc<QueryTools>,
    member_evaluator: Rc<dyn MemberEvaluator>,
    filter_type: FilterType,
    filter_operator: FilterOperator,
    values: Vec<Option<String>>,
    templates: FilterTemplates,
}

impl BaseFilter {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<dyn MemberEvaluator>,
        filter_type: FilterType,
        filter_operator: String,
        values: Option<Vec<Option<String>>>,
    ) -> Result<Rc<Self>, CubeError> {
        let templates = FilterTemplates::new(query_tools.templates_render());
        let values = if let Some(values) = values {
            values
        } else {
            vec![]
        };
        Ok(Rc::new(Self {
            query_tools,
            member_evaluator,
            filter_type,
            filter_operator: FilterOperator::from_str(&filter_operator)?,
            values,
            templates,
        }))
    }

    pub fn to_sql(&self) -> Result<String, CubeError> {
        let member_sql = self.member_evaluator.evaluate(self.query_tools.clone())?;
        let res = match self.filter_operator {
            FilterOperator::Equal => self.equals_where(&member_sql)?,
            FilterOperator::NotEqual => self.not_equals_where(&member_sql)?,
        };
        Ok(res)
    }

    fn equals_where(&self, member_sql: &str) -> Result<String, CubeError> {
        let need_null_check = self.is_need_null_chek(false);
        if self.is_array_value() {
            self.templates.in_where(
                member_sql.to_string(),
                self.filter_and_allocate_values(),
                need_null_check,
            )
        } else if self.is_values_contains_null() {
            self.templates.not_set_where(member_sql.to_string())
        } else {
            self.templates
                .equals(member_sql.to_string(), self.first_param()?, need_null_check)
        }
    }

    fn not_equals_where(&self, member_sql: &str) -> Result<String, CubeError> {
        let need_null_check = self.is_need_null_chek(true);
        if self.is_array_value() {
            self.templates.not_in_where(
                member_sql.to_string(),
                self.filter_and_allocate_values(),
                need_null_check,
            )
        } else if self.is_values_contains_null() {
            self.templates.set_where(member_sql.to_string())
        } else {
            self.templates
                .not_equals(member_sql.to_string(), self.first_param()?, need_null_check)
        }
    }

    fn allocate_param(&self, param: &str) -> String {
        let index = self.query_tools.allocaate_param(param);
        format!("${}", index)
    }

    fn first_param(&self) -> Result<String, CubeError> {
        if self.values.is_empty() {
            Err(CubeError::user(format!(
                "Expected one parameter but nothing found"
            )))
        } else {
            if let Some(value) = &self.values[0] {
                Ok(self.allocate_param(value))
            } else {
                Ok("NULL".to_string())
            }
        }
    }

    fn is_need_null_chek(&self, is_not: bool) -> bool {
        let contains_null = self.is_values_contains_null();
        if is_not {
            !contains_null
        } else {
            contains_null
        }
    }

    fn is_values_contains_null(&self) -> bool {
        self.values.iter().any(|v| v.is_none())
    }

    fn is_array_value(&self) -> bool {
        self.values.len() > 1
    }

    fn filter_and_allocate_values(&self) -> Vec<String> {
        self.values
            .iter()
            .filter_map(|v| v.as_ref().map(|v| self.allocate_param(&v)))
            .collect::<Vec<_>>()
    }
}
