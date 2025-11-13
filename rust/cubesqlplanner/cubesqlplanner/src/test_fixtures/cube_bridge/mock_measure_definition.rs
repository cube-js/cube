use crate::cube_bridge::case_variant::CaseVariant;
use crate::cube_bridge::measure_definition::{
    MeasureDefinition, MeasureDefinitionStatic, RollingWindow, TimeShiftReference,
};
use crate::cube_bridge::member_order_by::MemberOrderBy;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::struct_with_sql_member::StructWithSqlMember;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::{
    MockMemberOrderBy, MockMemberSql, MockStructWithSqlMember,
};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of MeasureDefinition for testing
#[derive(TypedBuilder)]
pub struct MockMeasureDefinition {
    // Fields from MeasureDefinitionStatic
    #[builder(default = "number".to_string())]
    measure_type: String,
    #[builder(default)]
    owned_by_cube: Option<bool>,
    #[builder(default)]
    multi_stage: Option<bool>,
    #[builder(default)]
    reduce_by_references: Option<Vec<String>>,
    #[builder(default)]
    add_group_by_references: Option<Vec<String>>,
    #[builder(default)]
    group_by_references: Option<Vec<String>>,
    #[builder(default)]
    time_shift_references: Option<Vec<TimeShiftReference>>,
    #[builder(default)]
    rolling_window: Option<RollingWindow>,

    // Optional trait fields
    #[builder(default, setter(strip_option))]
    sql: Option<String>,
    #[builder(default)]
    case: Option<Rc<CaseVariant>>,
    #[builder(default)]
    filters: Option<Vec<Rc<MockStructWithSqlMember>>>,
    #[builder(default)]
    drill_filters: Option<Vec<Rc<MockStructWithSqlMember>>>,
    #[builder(default)]
    order_by: Option<Vec<Rc<MockMemberOrderBy>>>,
}

impl_static_data!(
    MockMeasureDefinition,
    MeasureDefinitionStatic,
    measure_type,
    owned_by_cube,
    multi_stage,
    reduce_by_references,
    add_group_by_references,
    group_by_references,
    time_shift_references,
    rolling_window
);

impl MeasureDefinition for MockMeasureDefinition {
    fn static_data(&self) -> &MeasureDefinitionStatic {
        Box::leak(Box::new(Self::static_data(self)))
    }

    fn has_sql(&self) -> Result<bool, CubeError> {
        Ok(self.sql.is_some())
    }

    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError> {
        match &self.sql {
            Some(sql_str) => Ok(Some(Rc::new(MockMemberSql::new(sql_str)?))),
            None => Ok(None),
        }
    }

    fn has_case(&self) -> Result<bool, CubeError> {
        Ok(self.case.is_some())
    }

    fn case(&self) -> Result<Option<CaseVariant>, CubeError> {
        Ok(self.case.as_ref().map(|c| match &**c {
            CaseVariant::Case(def) => CaseVariant::Case(def.clone()),
            CaseVariant::CaseSwitch(def) => CaseVariant::CaseSwitch(def.clone()),
        }))
    }

    fn has_filters(&self) -> Result<bool, CubeError> {
        Ok(self.filters.is_some())
    }

    fn filters(&self) -> Result<Option<Vec<Rc<dyn StructWithSqlMember>>>, CubeError> {
        match &self.filters {
            Some(filters) => {
                let result: Vec<Rc<dyn StructWithSqlMember>> = filters
                    .iter()
                    .map(|f| f.clone() as Rc<dyn StructWithSqlMember>)
                    .collect();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn has_drill_filters(&self) -> Result<bool, CubeError> {
        Ok(self.drill_filters.is_some())
    }

    fn drill_filters(&self) -> Result<Option<Vec<Rc<dyn StructWithSqlMember>>>, CubeError> {
        match &self.drill_filters {
            Some(filters) => {
                let result: Vec<Rc<dyn StructWithSqlMember>> = filters
                    .iter()
                    .map(|f| f.clone() as Rc<dyn StructWithSqlMember>)
                    .collect();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn has_order_by(&self) -> Result<bool, CubeError> {
        Ok(self.order_by.is_some())
    }

    fn order_by(&self) -> Result<Option<Vec<Rc<dyn MemberOrderBy>>>, CubeError> {
        match &self.order_by {
            Some(order_by) => {
                let result: Vec<Rc<dyn MemberOrderBy>> = order_by
                    .iter()
                    .map(|o| o.clone() as Rc<dyn MemberOrderBy>)
                    .collect();
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_measure() {
        let measure = MockMeasureDefinition::builder()
            .measure_type("count".to_string())
            .sql("COUNT(*)".to_string())
            .build();

        assert_eq!(measure.static_data().measure_type, "count");
        assert!(measure.sql().unwrap().is_some());
    }

    #[test]
    fn test_sum_measure() {
        let measure = MockMeasureDefinition::builder()
            .measure_type("sum".to_string())
            .sql("{CUBE.amount}".to_string())
            .build();

        assert_eq!(measure.static_data().measure_type, "sum");
        let sql = measure.sql().unwrap().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);
    }

    #[test]
    fn test_measure_with_filters() {
        let filters = vec![
            Rc::new(
                MockStructWithSqlMember::builder()
                    .sql("{CUBE.status} = 'active'".to_string())
                    .build(),
            ),
            Rc::new(
                MockStructWithSqlMember::builder()
                    .sql("{CUBE.amount} > 0".to_string())
                    .build(),
            ),
        ];

        let measure = MockMeasureDefinition::builder()
            .measure_type("sum".to_string())
            .sql("{CUBE.amount}".to_string())
            .filters(Some(filters))
            .build();

        let result_filters = measure.filters().unwrap().unwrap();
        assert_eq!(result_filters.len(), 2);
    }

    #[test]
    fn test_measure_with_order_by() {
        let order_by = vec![
            Rc::new(
                MockMemberOrderBy::builder()
                    .sql("{CUBE.created_at}".to_string())
                    .dir("desc".to_string())
                    .build(),
            ),
            Rc::new(
                MockMemberOrderBy::builder()
                    .sql("{CUBE.name}".to_string())
                    .dir("asc".to_string())
                    .build(),
            ),
        ];

        let measure = MockMeasureDefinition::builder()
            .measure_type("count".to_string())
            .sql("COUNT(*)".to_string())
            .order_by(Some(order_by))
            .build();

        let result_order_by = measure.order_by().unwrap().unwrap();
        assert_eq!(result_order_by.len(), 2);
    }

    #[test]
    fn test_measure_with_time_shift() {
        let time_shift_refs = vec![
            TimeShiftReference {
                interval: Some("1 day".to_string()),
                name: Some("yesterday".to_string()),
                shift_type: Some("prior".to_string()),
                time_dimension: Some("created_at".to_string()),
            },
            TimeShiftReference {
                interval: Some("1 week".to_string()),
                name: Some("last_week".to_string()),
                shift_type: Some("prior".to_string()),
                time_dimension: Some("created_at".to_string()),
            },
        ];

        let measure = MockMeasureDefinition::builder()
            .measure_type("sum".to_string())
            .sql("{CUBE.amount}".to_string())
            .time_shift_references(Some(time_shift_refs))
            .build();

        let static_data = measure.static_data();
        let refs = static_data.time_shift_references.as_ref().unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].name, Some("yesterday".to_string()));
    }

    #[test]
    fn test_measure_with_rolling_window() {
        let rolling_window = RollingWindow {
            trailing: Some("7 day".to_string()),
            leading: Some("0 day".to_string()),
            offset: Some("start".to_string()),
            rolling_type: Some("trailing".to_string()),
            granularity: Some("day".to_string()),
        };

        let measure = MockMeasureDefinition::builder()
            .measure_type("sum".to_string())
            .sql("{CUBE.amount}".to_string())
            .rolling_window(Some(rolling_window))
            .build();

        let static_data = measure.static_data();
        let window = static_data.rolling_window.as_ref().unwrap();
        assert_eq!(window.trailing, Some("7 day".to_string()));
        assert_eq!(window.granularity, Some("day".to_string()));
    }

    #[test]
    fn test_measure_with_case() {
        use crate::cube_bridge::case_variant::CaseVariant;
        use crate::cube_bridge::string_or_sql::StringOrSql;
        use crate::test_fixtures::cube_bridge::{
            MockCaseDefinition, MockCaseElseItem, MockCaseItem,
        };

        let when_items = vec![
            Rc::new(
                MockCaseItem::builder()
                    .sql("{CUBE.status} = 'active'".to_string())
                    .label(StringOrSql::String("1".to_string()))
                    .build(),
            ),
            Rc::new(
                MockCaseItem::builder()
                    .sql("{CUBE.status} = 'inactive'".to_string())
                    .label(StringOrSql::String("0".to_string()))
                    .build(),
            ),
        ];

        let else_item = Rc::new(
            MockCaseElseItem::builder()
                .label(StringOrSql::String("0".to_string()))
                .build(),
        );

        let case_def = Rc::new(
            MockCaseDefinition::builder()
                .when(when_items)
                .else_label(else_item)
                .build(),
        );

        let measure = MockMeasureDefinition::builder()
            .measure_type("number".to_string())
            .case(Some(Rc::new(CaseVariant::Case(case_def))))
            .build();

        let case_result = measure.case().unwrap();
        assert!(case_result.is_some());
    }

    #[test]
    fn test_measure_with_references() {
        let measure = MockMeasureDefinition::builder()
            .measure_type("sum".to_string())
            .sql("{CUBE.amount}".to_string())
            .reduce_by_references(Some(vec!["user_id".to_string(), "order_id".to_string()]))
            .add_group_by_references(Some(vec!["status".to_string()]))
            .group_by_references(Some(vec!["category".to_string()]))
            .build();

        assert_eq!(
            measure.static_data().reduce_by_references,
            Some(vec!["user_id".to_string(), "order_id".to_string()])
        );
        assert_eq!(
            measure.static_data().add_group_by_references,
            Some(vec!["status".to_string()])
        );
        assert_eq!(
            measure.static_data().group_by_references,
            Some(vec!["category".to_string()])
        );
    }

    #[test]
    fn test_measure_with_drill_filters() {
        let drill_filters = vec![Rc::new(
            MockStructWithSqlMember::builder()
                .sql("{CUBE.is_drillable} = true".to_string())
                .build(),
        )];

        let measure = MockMeasureDefinition::builder()
            .measure_type("count".to_string())
            .sql("COUNT(*)".to_string())
            .drill_filters(Some(drill_filters))
            .build();

        let result_filters = measure.drill_filters().unwrap().unwrap();
        assert_eq!(result_filters.len(), 1);
    }

    #[test]
    fn test_measure_with_flags() {
        let measure = MockMeasureDefinition::builder()
            .measure_type("sum".to_string())
            .sql("{CUBE.amount}".to_string())
            .multi_stage(Some(true))
            .owned_by_cube(Some(false))
            .build();

        assert_eq!(measure.static_data().multi_stage, Some(true));
        assert_eq!(measure.static_data().owned_by_cube, Some(false));
    }
}