use super::filter::{Filter, FilterGroup, FilterGroupOperator, FilterItem};
use crate::planner::filter::BaseFilter;
use crate::planner::sql_evaluator::DebugSql;

impl DebugSql for BaseFilter {
    fn debug_sql(&self, expand_deps: bool) -> String {
        let member = if expand_deps {
            self.member_evaluator().debug_sql(true)
        } else {
            format!("{{{}}}", self.member_evaluator().full_name())
        };

        let values_str = self
            .values()
            .iter()
            .map(|v| match v {
                Some(val) => format!("'{}'", val),
                None => "NULL".to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "{} {}: [{}]",
            member,
            self.filter_operator().to_string(),
            values_str
        )
    }
}

impl DebugSql for FilterItem {
    fn debug_sql(&self, expand_deps: bool) -> String {
        match self {
            FilterItem::Group(group) => group.debug_sql(expand_deps),
            FilterItem::Item(filter) => filter.debug_sql(expand_deps),
            FilterItem::Segment(segment) => {
                let member = if expand_deps {
                    segment.member_evaluator().debug_sql(true)
                } else {
                    format!("{{{}}}", segment.member_evaluator().full_name())
                };
                format!("SEGMENT({})", member)
            }
        }
    }
}

impl DebugSql for FilterGroup {
    fn debug_sql(&self, expand_deps: bool) -> String {
        if self.items.is_empty() {
            return format!("{}: []", self.operator);
        }

        let items_str = self
            .items
            .iter()
            .map(|item| {
                let item_str = item.debug_sql(expand_deps);
                // Indent each line of the item
                item_str
                    .lines()
                    .map(|line| format!("  {}", line))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect::<Vec<_>>()
            .join(",\n");

        format!("{}: [\n{}\n]", self.operator, items_str)
    }
}

impl DebugSql for Filter {
    fn debug_sql(&self, expand_deps: bool) -> String {
        if self.items.is_empty() {
            return "Filter: []".to_string();
        }

        if self.items.len() == 1 {
            return self.items[0].debug_sql(expand_deps);
        }

        // Multiple items treated as AND group
        let group = FilterGroup::new(FilterGroupOperator::And, self.items.clone());
        group.debug_sql(expand_deps)
    }
}
