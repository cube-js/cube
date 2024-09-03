use itertools::Itertools;

use crate::planner::filter::BaseFilter;
use cubenativeutils::CubeError;
use std::boxed::Box;
use std::fmt;
use std::rc::Rc;

pub enum FilterGroupOperator {
    Or,
    And,
}

pub struct FilterGroup {
    operator: FilterGroupOperator,
    items: Vec<FilterItem>,
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
    pub fn to_sql(&self) -> Result<String, CubeError> {
        let res = match self {
            FilterItem::Group(group) => {
                let operator = format!(" {} ", group.operator.to_string());
                let items_sql = group
                    .items
                    .iter()
                    .map(|itm| itm.to_sql())
                    .collect::<Result<Vec<_>, _>>()?;
                format!("({})", items_sql.join(&operator))
            }
            FilterItem::Item(item) => {
                let sql = item.to_sql()?;
                format!("({})", sql)
            }
        };
        Ok(res)
    }
}
impl fmt::Display for FilterItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FilterItem::Group(group) => {
                let operator = group.operator.to_string();
                write!(f, "(");
                for item in group.items.iter().take(1) {
                    write!(f, "{}", item)?;
                }
                for item in group.items.iter().skip(1) {
                    write!(f, " {} {}", operator, item)?;
                }
                write!(f, ")");
                Ok(())
            }
            FilterItem::Item(item) => {
                let sql = item.to_sql().map_err(|_| fmt::Error)?;
                write!(f, "({})", sql)
            }
        }
    }
}

impl Filter {
    pub fn to_sql(&self) -> Result<String, CubeError> {
        let res = self
            .items
            .iter()
            .map(|itm| itm.to_sql())
            .collect::<Result<Vec<_>, _>>()?
            .join(" AND ");
        Ok(res)
    }
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for item in self.items.iter().take(1) {
            write!(f, "{}", item)?;
        }
        for item in self.items.iter().skip(1) {
            write!(f, " AND {}", item)?;
        }
        Ok(())
    }
}
