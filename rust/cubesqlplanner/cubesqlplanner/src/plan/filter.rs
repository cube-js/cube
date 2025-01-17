use crate::planner::filter::BaseFilter;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use std::fmt;
use std::rc::Rc;

#[derive(Clone, PartialEq)]
pub enum FilterGroupOperator {
    Or,
    And,
}

#[derive(Clone)]
pub struct FilterGroup {
    pub operator: FilterGroupOperator,
    pub items: Vec<FilterItem>,
}

impl PartialEq for FilterGroup {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator && self.items == other.items
    }
}

impl FilterGroup {
    pub fn new(operator: FilterGroupOperator, items: Vec<FilterItem>) -> Self {
        Self { operator, items }
    }
}

#[derive(Clone, PartialEq)]
pub enum FilterItem {
    Group(Rc<FilterGroup>),
    Item(Rc<BaseFilter>),
}

#[derive(Clone)]
pub struct Filter {
    pub items: Vec<FilterItem>,
}

impl fmt::Display for FilterGroupOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FilterGroupOperator::Or => write!(f, "OR"),
            FilterGroupOperator::And => write!(f, "AND"),
        }
    }
}

impl FilterItem {
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let res = match self {
            FilterItem::Group(group) => {
                let operator = format!(" {} ", group.operator.to_string());
                let items_sql = group
                    .items
                    .iter()
                    .map(|itm| itm.to_sql(templates, context.clone()))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|itm| !itm.is_empty())
                    .collect::<Vec<_>>();
                if items_sql.is_empty() {
                    "".to_string()
                } else {
                    let result = items_sql.join(&operator);
                    format!("({})", result)
                }
            }
            FilterItem::Item(item) => {
                let sql = item.to_sql(context.clone(), templates)?;
                format!("({})", sql)
            }
        };
        Ok(res)
    }

    pub fn all_member_evaluators(&self) -> Vec<Rc<MemberSymbol>> {
        let mut result = Vec::new();
        self.find_all_member_evaluators(&mut result);
        result
    }

    pub fn find_all_member_evaluators(&self, result: &mut Vec<Rc<MemberSymbol>>) {
        match self {
            FilterItem::Group(group) => {
                for item in group.items.iter() {
                    item.find_all_member_evaluators(result)
                }
            }
            FilterItem::Item(item) => result.push(item.member_evaluator().clone()),
        }
    }
}

impl Filter {
    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let res = self
            .items
            .iter()
            .map(|itm| itm.to_sql(templates, context.clone()))
            .collect::<Result<Vec<_>, _>>()?
            .join(" AND ");
        Ok(res)
    }
}
