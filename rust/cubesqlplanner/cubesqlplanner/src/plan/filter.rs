use crate::planner::filter::{BaseFilter, BaseSegment};
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
    Segment(Rc<BaseSegment>),
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

impl Filter {
    pub fn all_member_evaluators(&self) -> Vec<Rc<MemberSymbol>> {
        let mut result = Vec::new();
        for item in self.items.iter() {
            item.find_all_member_evaluators(&mut result);
        }
        result
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
            FilterItem::Segment(item) => {
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
            FilterItem::Segment(item) => result.push(item.member_evaluator().clone()),
        }
    }
    /// Find value restrictions for a given symbol across filter tree
    /// Returns:
    /// - None: no restrictions found for this symbol
    /// - Some(vec![]): restrictions exist but result in empty set (contradiction)
    /// - Some(values): list of allowed values for this symbol
    pub fn find_value_restriction(&self, symbol: &Rc<MemberSymbol>) -> Option<Vec<String>> {
        match self {
            FilterItem::Item(item) => {
                // Check if this filter item applies to the target symbol
                if &item.member_evaluator().resolve_reference_chain() == symbol {
                    item.get_value_restrictions()
                } else {
                    None
                }
            }

            FilterItem::Group(group) => match group.operator {
                FilterGroupOperator::Or => {
                    // OR logic: collect all possible values from all branches
                    let mut all_values = Vec::new();
                    let mut found_any_restriction = false;

                    for child in &group.items {
                        match child.find_value_restriction(symbol) {
                            None => {
                                // This branch has no restrictions - OR makes entire result unrestricted
                                return None;
                            }
                            Some(values) => {
                                found_any_restriction = true;
                                // Add all values from this branch to our collection
                                all_values.extend(values);
                            }
                        }
                    }

                    if found_any_restriction {
                        // Remove duplicates and return combined value set
                        all_values.sort();
                        all_values.dedup();
                        Some(all_values)
                    } else {
                        None
                    }
                }

                FilterGroupOperator::And => {
                    // AND logic: find intersection of all restrictions
                    let mut result_values: Option<Vec<String>> = None;

                    for child in &group.items {
                        if let Some(values) = child.find_value_restriction(symbol) {
                            match &result_values {
                                None => {
                                    // First restriction found - use it as base
                                    result_values = Some(values);
                                }
                                Some(existing) => {
                                    // Find intersection with existing restrictions
                                    let intersection: Vec<String> = existing
                                        .iter()
                                        .filter(|v| values.contains(v))
                                        .cloned()
                                        .collect();

                                    if intersection.is_empty() {
                                        // Contradiction: no values satisfy all conditions
                                        return Some(vec![]);
                                    }

                                    result_values = Some(intersection);
                                }
                            }
                        }
                        // If child has no restrictions, AND logic continues with existing restrictions
                    }

                    result_values
                }
            },
            FilterItem::Segment(_) => {
                // Segments don't provide value restrictions for individual symbols
                None
            }
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
