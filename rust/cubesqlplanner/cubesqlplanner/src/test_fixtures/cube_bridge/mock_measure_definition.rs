use crate::cube_bridge::case_variant::CaseVariant;
use crate::cube_bridge::measure_definition::{
    MeasureDefinition, MeasureDefinitionStatic, RollingWindow, TimeShiftReference,
};
use crate::cube_bridge::member_order_by::MemberOrderBy;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::struct_with_sql_member::StructWithSqlMember;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::yaml::measure::YamlMeasureDefinition;
use crate::test_fixtures::cube_bridge::{
    MockMemberOrderBy, MockMemberSql, MockStructWithSqlMember,
};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockMeasureDefinition {
    measure_type: String,
    #[builder(default = Some(false))]
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

    #[builder(default, setter(strip_option(fallback = sql_opt)))]
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

impl MockMeasureDefinition {
    pub fn from_yaml(yaml: &str) -> Result<Rc<Self>, CubeError> {
        let yaml_def: YamlMeasureDefinition = serde_yaml::from_str(yaml)
            .map_err(|e| CubeError::user(format!("Failed to parse YAML: {}", e)))?;
        Ok(yaml_def.build())
    }
}

impl MeasureDefinition for MockMeasureDefinition {
    crate::impl_static_data_method!(MeasureDefinitionStatic);

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
    use indoc::indoc;

    #[test]
    fn test_from_yaml_minimal() {
        let yaml = indoc! {"
            type: count
        "};

        let measure = MockMeasureDefinition::from_yaml(yaml).unwrap();
        assert_eq!(measure.static_data().measure_type, "count");
        assert!(!measure.has_sql().unwrap());
    }

    #[test]
    fn test_from_yaml_with_sql() {
        let yaml = indoc! {"
            type: sum
            sql: \"{CUBE.amount}\"
        "};

        let measure = MockMeasureDefinition::from_yaml(yaml).unwrap();
        assert_eq!(measure.static_data().measure_type, "sum");
        assert!(measure.has_sql().unwrap());
    }

    #[test]
    fn test_from_yaml_with_filters() {
        let yaml = indoc! {"
            type: count
            sql: COUNT(*)
            filters:
              - sql: \"{CUBE}.status = 'active'\"
              - sql: \"{CUBE}.amount > 0\"
        "};

        let measure = MockMeasureDefinition::from_yaml(yaml).unwrap();
        let filters = measure.filters().unwrap().unwrap();
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_from_yaml_with_order_by() {
        let yaml = indoc! {"
            type: count
            sql: COUNT(*)
            order_by:
              - sql: \"{CUBE.created_at}\"
                dir: desc
              - sql: \"{CUBE.name}\"
                dir: asc
        "};

        let measure = MockMeasureDefinition::from_yaml(yaml).unwrap();
        let order_by = measure.order_by().unwrap().unwrap();
        assert_eq!(order_by.len(), 2);
    }

    #[test]
    fn test_from_yaml_with_references() {
        let yaml = indoc! {"
            type: sum
            sql: \"{CUBE.amount}\"
            reduce_by_references: [user_id, order_id]
            add_group_by_references: [status]
            group_by_references: [category]
        "};

        let measure = MockMeasureDefinition::from_yaml(yaml).unwrap();
        let static_data = measure.static_data();

        assert_eq!(
            static_data.reduce_by_references,
            Some(vec!["user_id".to_string(), "order_id".to_string()])
        );
        assert_eq!(
            static_data.add_group_by_references,
            Some(vec!["status".to_string()])
        );
        assert_eq!(
            static_data.group_by_references,
            Some(vec!["category".to_string()])
        );
    }

    #[test]
    fn test_from_yaml_with_case() {
        let yaml = indoc! {"
            type: number
            case:
              when:
                - sql: \"{CUBE}.status = 'active'\"
                  label: \"1\"
                - sql: \"{CUBE}.status = 'inactive'\"
                  label: \"0\"
              else:
                label: \"0\"
        "};

        let measure = MockMeasureDefinition::from_yaml(yaml).unwrap();
        assert!(measure.has_case().unwrap());

        let case_variant = measure.case().unwrap().unwrap();
        match case_variant {
            CaseVariant::Case(case_def) => {
                let when_items = case_def.when().unwrap();
                assert_eq!(when_items.len(), 2);
            }
            _ => panic!("Expected Case variant"),
        }
    }
}
