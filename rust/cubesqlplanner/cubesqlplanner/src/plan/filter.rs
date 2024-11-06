use super::Schema;
use crate::planner::filter::BaseFilter;
use crate::planner::VisitorContext;
use cubenativeutils::CubeError;
use std::fmt;
use std::rc::Rc;

#[derive(Clone)]
pub enum FilterGroupOperator {
    Or,
    And,
}

#[derive(Clone)]
pub struct FilterGroup {
    pub operator: FilterGroupOperator,
    pub items: Vec<FilterItem>,
}

impl FilterGroup {
    pub fn new(operator: FilterGroupOperator, items: Vec<FilterItem>) -> Self {
        Self { operator, items }
    }
}

#[derive(Clone)]
pub enum FilterItem {
    Group(Rc<FilterGroup>),
    Item(Rc<BaseFilter>),
}

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
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let res = match self {
            FilterItem::Group(group) => {
                let operator = format!(" {} ", group.operator.to_string());
                let items_sql = group
                    .items
                    .iter()
                    .map(|itm| itm.to_sql(context.clone(), schema.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                if items_sql.is_empty() {
                    format!("( 1 = 1 )")
                } else {
                    format!("({})", items_sql.join(&operator))
                }
            }
            FilterItem::Item(item) => {
                let sql = item.to_sql(context.clone(), schema)?;
                format!("({})", sql)
            }
        };
        Ok(res)
    }
}

impl Filter {
    pub fn to_sql(
        &self,
        context: Rc<VisitorContext>,
        schema: Rc<Schema>,
    ) -> Result<String, CubeError> {
        let res = self
            .items
            .iter()
            .map(|itm| itm.to_sql(context.clone(), schema.clone()))
            .collect::<Result<Vec<_>, _>>()?
            .join(" AND ");
        Ok(res)
    }
}
