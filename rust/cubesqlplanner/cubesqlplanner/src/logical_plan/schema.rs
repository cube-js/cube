use cubenativeutils::CubeError;
use itertools::Itertools;

use super::pretty_print::*;
use crate::planner::sql_evaluator::collectors::has_multi_stage_members;
use crate::planner::sql_evaluator::MemberSymbol;
use std::collections::HashSet;
use std::fmt;
use std::rc::Rc;

#[derive(Default, Clone)]
pub struct LogicalSchema {
    pub time_dimensions: Vec<Rc<MemberSymbol>>,
    pub dimensions: Vec<Rc<MemberSymbol>>,
    pub measures: Vec<Rc<MemberSymbol>>,
    pub multiplied_measures: HashSet<String>,
}

impl fmt::Debug for LogicalSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LogicalSchema")
    }
}

impl LogicalSchema {
    pub fn set_time_dimensions(mut self, time_dimensions: Vec<Rc<MemberSymbol>>) -> Self {
        self.time_dimensions = time_dimensions;
        self
    }

    pub fn set_dimensions(mut self, dimensions: Vec<Rc<MemberSymbol>>) -> Self {
        self.dimensions = dimensions;
        self
    }

    pub fn set_measures(mut self, measures: Vec<Rc<MemberSymbol>>) -> Self {
        self.measures = measures;
        self
    }

    pub fn set_multiplied_measures(mut self, multiplied_measures: HashSet<String>) -> Self {
        self.multiplied_measures = multiplied_measures;
        self
    }

    pub fn into_rc(self) -> Rc<Self> {
        Rc::new(self)
    }
}

impl LogicalSchema {
    pub fn find_member_positions(&self, name: &str) -> Vec<usize> {
        let mut result = Vec::new();
        for (i, m) in self.dimensions.iter().enumerate() {
            if m.full_name() == name {
                result.push(i);
            }
        }
        for (i, m) in self.time_dimensions.iter().enumerate() {
            if m.full_name() == name {
                result.push(i + self.dimensions.len());
            } else if let Ok(time_dimension) = m.as_time_dimension() {
                if time_dimension.base_symbol().full_name() == name {
                    result.push(i + self.dimensions.len());
                }
            }
        }
        for (i, m) in self.measures.iter().enumerate() {
            if m.full_name() == name {
                result.push(i + self.time_dimensions.len() + self.dimensions.len());
            }
        }
        result
    }

    pub fn all_dimensions(&self) -> impl Iterator<Item = &Rc<MemberSymbol>> {
        self.dimensions.iter().chain(self.time_dimensions.iter())
    }

    pub fn all_members(&self) -> impl Iterator<Item = &Rc<MemberSymbol>> {
        self.all_dimensions().chain(self.measures.iter())
    }

    pub fn has_dimensions(&self) -> bool {
        !self.time_dimensions.is_empty() || !self.dimensions.is_empty()
    }

    pub fn multi_stage_dimensions(&self) -> Result<Vec<Rc<MemberSymbol>>, CubeError> {
        let mut result = vec![];
        for member in self.all_dimensions() {
            if has_multi_stage_members(member, true)? {
                result.push(member.clone())
            }
        }
        Ok(result)
    }

    /// Get the member symbol at a given position (as returned by find_member_positions).
    /// Position ordering: dimensions, then time_dimensions, then measures.
    pub fn get_member_at_position(&self, position: usize) -> Option<Rc<MemberSymbol>> {
        let dim_len = self.dimensions.len();
        let time_dim_len = self.time_dimensions.len();

        if position < dim_len {
            self.dimensions.get(position).cloned()
        } else if position < dim_len + time_dim_len {
            self.time_dimensions.get(position - dim_len).cloned()
        } else {
            self.measures
                .get(position - dim_len - time_dim_len)
                .cloned()
        }
    }
}

impl PrettyPrint for LogicalSchema {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!("-time_dimensions: {}", print_symbols(&self.time_dimensions)),
            state,
        );
        result.println(
            &format!("-dimensions: {}", print_symbols(&self.dimensions)),
            state,
        );
        result.println(
            &format!("-measures: {}", print_symbols(&self.measures)),
            state,
        );
        if !self.multiplied_measures.is_empty() {
            result.println(
                &format!(
                    "-multiplied_measures: {}",
                    self.multiplied_measures.iter().join(", ")
                ),
                state,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::sql_evaluator::symbols::TimeDimensionSymbol;
    use crate::test_fixtures::cube_bridge::MockSchema;
    use crate::test_fixtures::test_utils::TestContext;

    #[test]
    fn test_get_member_at_position_dimension() -> Result<(), CubeError> {
        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema)?;

        let dim = ctx.create_dimension("visitors.source")?;
        let logical_schema = LogicalSchema::default().set_dimensions(vec![dim.clone()]);

        let result = logical_schema.get_member_at_position(0);
        assert!(result.is_some());
        assert_eq!(
            result.as_ref().map(|r| r.full_name()),
            Some(dim.full_name())
        );

        Ok(())
    }

    #[test]
    fn test_get_member_at_position_time_dimension() -> Result<(), CubeError> {
        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema)?;

        let source_dim = ctx.create_dimension("visitors.source")?;
        let time_dim_base = ctx.create_dimension("visitors.created_at")?;
        let time_dim = MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
            time_dim_base,
            Some("day".to_string()),
            None,
            None,
        ));

        let logical_schema = LogicalSchema::default()
            .set_dimensions(vec![source_dim.clone()])
            .set_time_dimensions(vec![time_dim.clone()]);

        let result0 = logical_schema.get_member_at_position(0);
        assert!(result0.is_some());
        assert_eq!(
            result0.as_ref().map(|r| r.full_name()),
            Some(source_dim.full_name())
        );

        let result1 = logical_schema.get_member_at_position(1);
        assert!(result1.is_some());
        assert_eq!(
            result1.as_ref().map(|r| r.full_name()),
            Some(time_dim.full_name())
        );

        Ok(())
    }

    #[test]
    fn test_get_member_at_position_measure() -> Result<(), CubeError> {
        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema)?;

        let dim = ctx.create_dimension("visitors.source")?;
        let time_dim_base = ctx.create_dimension("visitors.created_at")?;
        let time_dim = MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
            time_dim_base,
            Some("day".to_string()),
            None,
            None,
        ));
        let measure = ctx.create_measure("visitors.count")?;

        let logical_schema = LogicalSchema::default()
            .set_dimensions(vec![dim.clone()])
            .set_time_dimensions(vec![time_dim.clone()])
            .set_measures(vec![measure.clone()]);

        let result0 = logical_schema.get_member_at_position(0);
        assert!(result0.is_some());
        assert_eq!(
            result0.as_ref().map(|r| r.full_name()),
            Some(dim.full_name())
        );

        let result1 = logical_schema.get_member_at_position(1);
        assert!(result1.is_some());
        assert_eq!(
            result1.as_ref().map(|r| r.full_name()),
            Some(time_dim.full_name())
        );

        let result2 = logical_schema.get_member_at_position(2);
        assert!(result2.is_some());
        assert_eq!(
            result2.as_ref().map(|r| r.full_name()),
            Some(measure.full_name())
        );

        Ok(())
    }

    #[test]
    fn test_get_member_at_position_out_of_bounds() -> Result<(), CubeError> {
        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema)?;

        let source_dim = ctx.create_dimension("visitors.source")?;
        let measure = ctx.create_measure("visitors.count")?;

        let logical_schema = LogicalSchema::default()
            .set_dimensions(vec![source_dim])
            .set_measures(vec![measure]);

        let result = logical_schema.get_member_at_position(2);
        assert!(result.is_none());

        let result = logical_schema.get_member_at_position(10);
        assert!(result.is_none());

        Ok(())
    }

    #[test]
    fn test_get_member_at_position_empty_schema() {
        let logical_schema = LogicalSchema::default();

        let result = logical_schema.get_member_at_position(0);
        assert!(result.is_none());

        let result = logical_schema.get_member_at_position(1);
        assert!(result.is_none());
    }

    #[test]
    fn test_get_member_at_position_multiple_of_each() -> Result<(), CubeError> {
        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema)?;

        let dim1 = ctx.create_dimension("visitors.source")?;
        let dim2 = ctx.create_dimension("visitors.visitor_id")?;
        let time_dim_base = ctx.create_dimension("visitors.created_at")?;
        let time_dim = MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
            time_dim_base,
            Some("day".to_string()),
            None,
            None,
        ));
        let measure1 = ctx.create_measure("visitors.count")?;
        let measure2 = ctx.create_measure("visitors.total_revenue")?;

        let logical_schema = LogicalSchema::default()
            .set_dimensions(vec![dim1.clone(), dim2.clone()])
            .set_time_dimensions(vec![time_dim.clone()])
            .set_measures(vec![measure1.clone(), measure2.clone()]);

        assert_eq!(
            logical_schema
                .get_member_at_position(0)
                .as_ref()
                .map(|r| r.full_name()),
            Some(dim1.full_name())
        );

        assert_eq!(
            logical_schema
                .get_member_at_position(1)
                .as_ref()
                .map(|r| r.full_name()),
            Some(dim2.full_name())
        );

        assert_eq!(
            logical_schema
                .get_member_at_position(2)
                .as_ref()
                .map(|r| r.full_name()),
            Some(time_dim.full_name())
        );

        assert_eq!(
            logical_schema
                .get_member_at_position(3)
                .as_ref()
                .map(|r| r.full_name()),
            Some(measure1.full_name())
        );

        assert_eq!(
            logical_schema
                .get_member_at_position(4)
                .as_ref()
                .map(|r| r.full_name()),
            Some(measure2.full_name())
        );

        assert!(logical_schema.get_member_at_position(5).is_none());

        Ok(())
    }

    #[test]
    fn test_find_member_positions_consistency_with_get_member_at_position() -> Result<(), CubeError>
    {
        // This test verifies that find_member_positions and get_member_at_position
        // are consistent with each other - a position returned by find_member_positions
        // should retrieve the correct member via get_member_at_position
        let schema = MockSchema::from_yaml_file("common/visitors.yaml");
        let ctx = TestContext::new(schema)?;

        let dim = ctx.create_dimension("visitors.source")?;
        let time_dim_base = ctx.create_dimension("visitors.created_at")?;
        let time_dim = MemberSymbol::new_time_dimension(TimeDimensionSymbol::new(
            time_dim_base.clone(),
            Some("day".to_string()),
            None,
            None,
        ));
        let measure = ctx.create_measure("visitors.count")?;

        let logical_schema = LogicalSchema::default()
            .set_dimensions(vec![dim.clone()])
            .set_time_dimensions(vec![time_dim.clone()])
            .set_measures(vec![measure.clone()]);

        // Test dimension lookup
        let dim_positions = logical_schema.find_member_positions("visitors.source");
        assert_eq!(dim_positions.len(), 1);
        let retrieved_dim = logical_schema.get_member_at_position(dim_positions[0]);
        assert_eq!(
            retrieved_dim.as_ref().map(|r| r.full_name()),
            Some(dim.full_name())
        );

        // Test time dimension lookup (by base name)
        let time_dim_positions = logical_schema.find_member_positions("visitors.created_at");
        assert_eq!(time_dim_positions.len(), 1);
        let retrieved_time_dim = logical_schema.get_member_at_position(time_dim_positions[0]);
        assert_eq!(
            retrieved_time_dim.as_ref().map(|r| r.full_name()),
            Some(time_dim.full_name())
        );

        // Test measure lookup
        let measure_positions = logical_schema.find_member_positions("visitors.count");
        assert_eq!(measure_positions.len(), 1);
        let retrieved_measure = logical_schema.get_member_at_position(measure_positions[0]);
        assert_eq!(
            retrieved_measure.as_ref().map(|r| r.full_name()),
            Some(measure.full_name())
        );

        Ok(())
    }
}
