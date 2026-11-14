use crate::cube_bridge::base_query_options::FilterValue;
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::driver_tools::DriverTools;
use crate::cube_bridge::join_definition::JoinDefinition;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::member_sql::{CompiledMemberTemplate, MemberSql, SqlTemplate};
use crate::cube_bridge::pre_aggregation_obj::PreAggregationObj;
use crate::cube_bridge::query_context::{QueryContextData, QueryContextTimeDimension};
use crate::cube_bridge::security_context::SecurityContext;
use crate::cube_bridge::sql_templates_render::SqlTemplatesRender;
use crate::cube_bridge::sql_utils::SqlUtils;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::test_fixtures::cube_bridge::{
    mock_compiled, MockDriverTools, MockJoinGraph, MockMemberSql, MockSqlUtils,
};
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of BaseTools for testing
///
/// This mock provides implementations for driver_tools, sql_templates,
/// security_context_for_rust, and sql_utils_for_rust.
/// Other methods throw todo!() errors.
///
/// ```
#[derive(Clone, TypedBuilder)]
pub struct MockBaseTools {
    #[builder(default = Rc::new(MockDriverTools::new()))]
    driver_tools: Rc<MockDriverTools>,

    /// Driver tools returned for `driver_tools(external: true)` — the
    /// external pre-aggregations dialect (CubeStore in production).
    #[builder(default)]
    external_driver_tools: Option<Rc<MockDriverTools>>,

    #[builder(default = Rc::new(MockSqlUtils))]
    sql_utils: Rc<MockSqlUtils>,

    #[builder(default = Rc::new(MockJoinGraph::new()))]
    join_graph: Rc<MockJoinGraph>,

    /// Map of cube_name -> Vec<member_name> for all_cube_members
    #[builder(default = HashMap::new())]
    cube_members: HashMap<String, Vec<String>>,
}

impl MockBaseTools {
    pub fn set_external_driver_tools(&mut self, tools: Rc<MockDriverTools>) {
        self.external_driver_tools = Some(tools);
    }
}

impl Default for MockBaseTools {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl BaseTools for MockBaseTools {
    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }

    fn driver_tools(&self, external: bool) -> Result<Rc<dyn DriverTools>, CubeError> {
        if external {
            if let Some(tools) = &self.external_driver_tools {
                return Ok(tools.clone());
            }
        }
        Ok(self.driver_tools.clone())
    }

    // Mirrors `BaseQuery.driverTools(false)`, which returns the query itself.
    fn sql_templates(&self) -> Result<Rc<dyn SqlTemplatesRender>, CubeError> {
        self.driver_tools.sql_templates()
    }

    fn sql_utils_for_rust(&self) -> Result<Rc<dyn SqlUtils>, CubeError> {
        Ok(self.sql_utils.clone())
    }

    fn get_allocated_params(&self) -> Result<Vec<FilterValue>, CubeError> {
        Ok(vec![])
    }

    fn all_cube_members(&self, path: String) -> Result<Vec<String>, CubeError> {
        Ok(self
            .cube_members
            .get(&path)
            .cloned()
            .unwrap_or_else(Vec::new))
    }

    fn interval_and_minimal_time_unit(&self, _interval: String) -> Result<Vec<String>, CubeError> {
        todo!("interval_and_minimal_time_unit not implemented in mock")
    }

    fn get_pre_aggregation_by_name(
        &self,
        _cube_name: String,
        _name: String,
    ) -> Result<Rc<dyn PreAggregationObj>, CubeError> {
        todo!("get_pre_aggregation_by_name not implemented in mock")
    }

    fn pre_aggregation_table_name(
        &self,
        cube_name: String,
        name: String,
    ) -> Result<String, CubeError> {
        let key = format!("{}.{}", cube_name, name);
        Ok(PlanSqlTemplates::alias_name(&key))
    }

    fn join_tree_for_hints(
        &self,
        hints: Vec<JoinHintItem>,
    ) -> Result<Rc<dyn JoinDefinition>, CubeError> {
        let result = self.join_graph.build_join(hints)?;
        Ok(result as Rc<dyn JoinDefinition>)
    }

    fn try_join_tree_for_hints(
        &self,
        hints: Vec<JoinHintItem>,
    ) -> Result<Vec<Rc<dyn JoinDefinition>>, CubeError> {
        Ok(self
            .join_graph
            .try_build_join(hints)?
            .map(|result| result as Rc<dyn JoinDefinition>)
            .into_iter()
            .collect())
    }

    fn compile_member_sql(
        &self,
        member_sql: Rc<dyn MemberSql>,
        _security_context: Rc<dyn SecurityContext>,
        _arg_names: Vec<String>,
    ) -> Result<CompiledMemberTemplate, CubeError> {
        let mock = member_sql
            .as_any()
            .downcast::<MockMemberSql>()
            .map_err(|_| CubeError::internal("MockBaseTools expects MockMemberSql".to_string()))?;
        Ok(mock.compiled())
    }

    fn evaluate_query_context_condition(
        &self,
        condition: Rc<dyn MemberSql>,
        cube_name: String,
        query_context: QueryContextData,
    ) -> Result<bool, CubeError> {
        let (template, _) = mock_compiled(condition);
        let SqlTemplate::String(condition) = template else {
            return Err(CubeError::internal(
                "Mock query context condition must be a string".to_string(),
            ));
        };
        evaluate_mock_condition(&condition, &cube_name, &query_context)
    }
}

// Mock conditions use the data model syntax, `{QUERY_CONTEXT.pred(CUBE.member) and ...}`,
// but only flat `and` / `or` / `not` terms (`or` binds weaker): no parentheses.
fn evaluate_mock_condition(
    condition: &str,
    cube_name: &str,
    query_context: &QueryContextData,
) -> Result<bool, CubeError> {
    let malformed = || {
        CubeError::internal(format!(
            "Malformed mock query context condition: {}",
            condition
        ))
    };
    let condition = condition
        .trim()
        .strip_prefix('{')
        .and_then(|c| c.strip_suffix('}'))
        .ok_or_else(malformed)?;
    for alternative in condition.split(" or ") {
        let mut all_terms_hold = true;
        for term in alternative.split(" and ") {
            let term = term.trim();
            let (negated, term) = match term.strip_prefix("not ") {
                Some(rest) => (true, rest.trim()),
                None => (false, term),
            };
            let (predicate, member) = term
                .strip_prefix("QUERY_CONTEXT.")
                .and_then(|term| term.strip_suffix(')'))
                .and_then(|term| term.split_once('('))
                .ok_or_else(malformed)?;
            let member = resolve_mock_member(member.trim(), cube_name);
            let value = evaluate_mock_predicate(predicate, &member, query_context)?;
            all_terms_hold &= value != negated;
        }
        if all_terms_hold {
            return Ok(true);
        }
    }
    Ok(false)
}

// `CUBE.x` and a bare `x` name a member of the owning cube; `other.x` is already a path.
fn resolve_mock_member(reference: &str, cube_name: &str) -> String {
    match reference.split_once('.') {
        Some(("CUBE", member)) => format!("{}.{}", cube_name, member),
        Some(_) => reference.to_string(),
        None => format!("{}.{}", cube_name, reference),
    }
}

fn evaluate_mock_predicate(
    predicate: &str,
    member: &str,
    query_context: &QueryContextData,
) -> Result<bool, CubeError> {
    let time_dimension = |matches: fn(&QueryContextTimeDimension) -> bool| {
        query_context
            .time_dimensions
            .iter()
            .any(|td| td.dimension == member && matches(td))
    };
    let filters = || query_context.filters.iter().filter(|f| f.member == member);
    let value = match predicate {
        "has_dimension" | "hasDimension" => {
            query_context.dimensions.iter().any(|d| d == member)
                || time_dimension(|td| td.granularity.is_some())
        }
        "has_filter" | "hasFilter" => {
            filters().next().is_some() || time_dimension(|td| td.date_range.is_some())
        }
        "single_value" | "singleValue" => {
            let filters = filters().collect::<Vec<_>>();
            filters.len() == 1
                && filters[0].operator == "equals"
                && filters[0].values.as_ref().is_some_and(|v| v.len() == 1)
        }
        "has_measure" | "hasMeasure" => query_context.measures.iter().any(|m| m == member),
        "has_segment" | "hasSegment" => query_context.segments.iter().any(|s| s == member),
        _ => {
            return Err(CubeError::internal(format!(
                "Unknown mock query context predicate: {}",
                predicate
            )))
        }
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_context() -> QueryContextData {
        QueryContextData {
            dimensions: vec!["orders.category".to_string()],
            time_dimensions: vec![],
            measures: vec!["orders.count".to_string()],
            filters: vec![],
            segments: vec!["orders.completed_orders".to_string()],
        }
    }

    fn evaluate(condition: &str) -> bool {
        evaluate_mock_condition(condition, "orders", &query_context()).unwrap()
    }

    #[test]
    fn test_member_references_resolve_against_the_owning_cube() {
        assert!(evaluate("{QUERY_CONTEXT.has_dimension(CUBE.category)}"));
        assert!(evaluate("{QUERY_CONTEXT.has_dimension(category)}"));
        assert!(evaluate("{QUERY_CONTEXT.has_dimension(orders.category)}"));
        assert!(!evaluate("{QUERY_CONTEXT.has_dimension(other.category)}"));
    }

    #[test]
    fn test_or_binds_weaker_than_and() {
        assert!(evaluate(
            "{QUERY_CONTEXT.has_dimension(CUBE.status) and QUERY_CONTEXT.hasMeasure(CUBE.count) or QUERY_CONTEXT.has_segment(CUBE.completed_orders)}"
        ));
        assert!(!evaluate(
            "{QUERY_CONTEXT.has_dimension(CUBE.category) and not QUERY_CONTEXT.has_measure(CUBE.count)}"
        ));
    }

    #[test]
    fn test_malformed_condition_is_an_error() {
        assert!(evaluate_mock_condition(
            "has_dimension(CUBE.category)",
            "orders",
            &query_context()
        )
        .is_err());
        assert!(evaluate_mock_condition(
            "{has_dimension(CUBE.category)}",
            "orders",
            &query_context()
        )
        .is_err());
    }
}
