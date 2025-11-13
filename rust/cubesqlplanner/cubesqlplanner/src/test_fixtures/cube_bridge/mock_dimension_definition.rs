use crate::cube_bridge::case_variant::CaseVariant;
use crate::cube_bridge::dimension_definition::{DimensionDefinition, DimensionDefinitionStatic};
use crate::cube_bridge::geo_item::GeoItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::timeshift_definition::TimeShiftDefinition;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::{MockGeoItem, MockMemberSql, MockTimeShiftDefinition};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

/// Mock implementation of DimensionDefinition for testing
#[derive(TypedBuilder)]
pub struct MockDimensionDefinition {
    // Fields from DimensionDefinitionStatic
    #[builder(default = "string".to_string())]
    dimension_type: String,
    #[builder(default = Some(false))]
    owned_by_cube: Option<bool>,
    #[builder(default)]
    multi_stage: Option<bool>,
    #[builder(default)]
    add_group_by_references: Option<Vec<String>>,
    #[builder(default)]
    sub_query: Option<bool>,
    #[builder(default)]
    propagate_filters_to_sub_query: Option<bool>,
    #[builder(default)]
    values: Option<Vec<String>>,

    // Optional trait fields
    #[builder(default, setter(strip_option))]
    sql: Option<String>,
    #[builder(default)]
    case: Option<Rc<CaseVariant>>,
    #[builder(default, setter(strip_option))]
    latitude: Option<String>,
    #[builder(default, setter(strip_option))]
    longitude: Option<String>,
    #[builder(default)]
    time_shift: Option<Vec<Rc<MockTimeShiftDefinition>>>,
}

impl_static_data!(
    MockDimensionDefinition,
    DimensionDefinitionStatic,
    dimension_type,
    owned_by_cube,
    multi_stage,
    add_group_by_references,
    sub_query,
    propagate_filters_to_sub_query,
    values
);

impl DimensionDefinition for MockDimensionDefinition {
    crate::impl_static_data_method!(DimensionDefinitionStatic);

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

    fn has_latitude(&self) -> Result<bool, CubeError> {
        Ok(self.latitude.is_some())
    }

    fn latitude(&self) -> Result<Option<Rc<dyn GeoItem>>, CubeError> {
        match &self.latitude {
            Some(lat_str) => Ok(Some(Rc::new(
                MockGeoItem::builder().sql(lat_str.clone()).build(),
            ))),
            None => Ok(None),
        }
    }

    fn has_longitude(&self) -> Result<bool, CubeError> {
        Ok(self.longitude.is_some())
    }

    fn longitude(&self) -> Result<Option<Rc<dyn GeoItem>>, CubeError> {
        match &self.longitude {
            Some(lon_str) => Ok(Some(Rc::new(
                MockGeoItem::builder().sql(lon_str.clone()).build(),
            ))),
            None => Ok(None),
        }
    }

    fn has_time_shift(&self) -> Result<bool, CubeError> {
        Ok(self.time_shift.is_some())
    }

    fn time_shift(&self) -> Result<Option<Vec<Rc<dyn TimeShiftDefinition>>>, CubeError> {
        match &self.time_shift {
            Some(shifts) => {
                let result: Vec<Rc<dyn TimeShiftDefinition>> = shifts
                    .iter()
                    .map(|s| s.clone() as Rc<dyn TimeShiftDefinition>)
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
    fn test_string_dimension() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("string".to_string())
            .sql("{CUBE.name}".to_string())
            .build();

        assert_eq!(dim.static_data().dimension_type, "string");
        assert!(dim.has_sql().unwrap());
        assert!(dim.sql().unwrap().is_some());
    }

    #[test]
    fn test_number_dimension() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("number".to_string())
            .sql("{CUBE.count}".to_string())
            .build();

        assert_eq!(dim.static_data().dimension_type, "number");
        assert!(dim.has_sql().unwrap());
    }

    #[test]
    fn test_time_dimension() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("time".to_string())
            .sql("{CUBE.created_at}".to_string())
            .build();

        assert_eq!(dim.static_data().dimension_type, "time");
        assert!(dim.has_sql().unwrap());
    }

    #[test]
    fn test_geo_dimension() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("geo".to_string())
            .latitude("{CUBE.lat}".to_string())
            .longitude("{CUBE.lon}".to_string())
            .build();

        assert_eq!(dim.static_data().dimension_type, "geo");
        assert!(dim.has_latitude().unwrap());
        assert!(dim.has_longitude().unwrap());
        assert!(!dim.has_sql().unwrap());
    }

    #[test]
    fn test_switch_dimension() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("switch".to_string())
            .values(Some(vec!["active".to_string(), "inactive".to_string()]))
            .build();

        assert_eq!(dim.static_data().dimension_type, "switch");
        assert_eq!(
            dim.static_data().values,
            Some(vec!["active".to_string(), "inactive".to_string()])
        );
        assert!(!dim.has_sql().unwrap());
    }

    #[test]
    fn test_dimension_with_time_shift() {
        let time_shift = Rc::new(
            MockTimeShiftDefinition::builder()
                .interval(Some("1 day".to_string()))
                .name(Some("yesterday".to_string()))
                .build(),
        );

        let dim = MockDimensionDefinition::builder()
            .dimension_type("time".to_string())
            .sql("{CUBE.date}".to_string())
            .time_shift(Some(vec![time_shift]))
            .build();

        assert!(dim.has_time_shift().unwrap());
        let shifts = dim.time_shift().unwrap().unwrap();
        assert_eq!(shifts.len(), 1);
    }

    #[test]
    fn test_dimension_with_flags() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("string".to_string())
            .sql("{CUBE.field}".to_string())
            .multi_stage(Some(true))
            .sub_query(Some(true))
            .owned_by_cube(Some(false))
            .build();

        assert_eq!(dim.static_data().multi_stage, Some(true));
        assert_eq!(dim.static_data().sub_query, Some(true));
        assert_eq!(dim.static_data().owned_by_cube, Some(false));
    }

    #[test]
    fn test_sql_parsing_simple() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("string".to_string())
            .sql("{CUBE.field}".to_string())
            .build();

        let sql = dim.sql().unwrap().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);

        // Check compiled template
        use crate::test_fixtures::cube_bridge::{MockSecurityContext, MockSqlUtils};
        let (template, args) = sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();

        match template {
            crate::cube_bridge::member_sql::SqlTemplate::String(s) => {
                assert_eq!(s, "{arg:0}");
            }
            _ => panic!("Expected String template"),
        }

        assert_eq!(args.symbol_paths.len(), 1);
        assert_eq!(args.symbol_paths[0], vec!["CUBE", "field"]);
    }

    #[test]
    fn test_sql_parsing_multiple_refs() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("string".to_string())
            .sql("{CUBE.first_name} || ' ' || {CUBE.last_name}".to_string())
            .build();

        let sql = dim.sql().unwrap().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE"]);

        // Check compiled template
        use crate::test_fixtures::cube_bridge::{MockSecurityContext, MockSqlUtils};
        let (template, args) = sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();

        match template {
            crate::cube_bridge::member_sql::SqlTemplate::String(s) => {
                assert_eq!(s, "{arg:0} || ' ' || {arg:1}");
            }
            _ => panic!("Expected String template"),
        }

        assert_eq!(args.symbol_paths.len(), 2);
        assert_eq!(args.symbol_paths[0], vec!["CUBE", "first_name"]);
        assert_eq!(args.symbol_paths[1], vec!["CUBE", "last_name"]);
    }

    #[test]
    fn test_sql_parsing_cross_cube_refs() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("number".to_string())
            .sql("{CUBE.amount} / {other_cube.total}".to_string())
            .build();

        let sql = dim.sql().unwrap().unwrap();
        assert_eq!(sql.args_names(), &vec!["CUBE", "other_cube"]);

        // Check compiled template
        use crate::test_fixtures::cube_bridge::{MockSecurityContext, MockSqlUtils};
        let (template, args) = sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();

        match template {
            crate::cube_bridge::member_sql::SqlTemplate::String(s) => {
                assert_eq!(s, "{arg:0} / {arg:1}");
            }
            _ => panic!("Expected String template"),
        }

        assert_eq!(args.symbol_paths.len(), 2);
        assert_eq!(args.symbol_paths[0], vec!["CUBE", "amount"]);
        assert_eq!(args.symbol_paths[1], vec!["other_cube", "total"]);
    }

    #[test]
    fn test_geo_sql_parsing() {
        let dim = MockDimensionDefinition::builder()
            .dimension_type("geo".to_string())
            .latitude("{CUBE.latitude}".to_string())
            .longitude("{CUBE.longitude}".to_string())
            .build();

        assert!(!dim.has_sql().unwrap());

        let lat = dim.latitude().unwrap().unwrap();
        let lat_sql = lat.sql().unwrap();

        use crate::test_fixtures::cube_bridge::{MockSecurityContext, MockSqlUtils};
        let (template, args) = lat_sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();

        match template {
            crate::cube_bridge::member_sql::SqlTemplate::String(s) => {
                assert_eq!(s, "{arg:0}");
            }
            _ => panic!("Expected String template"),
        }

        assert_eq!(args.symbol_paths[0], vec!["CUBE", "latitude"]);
    }

    #[test]
    fn test_case_dimension() {
        use crate::cube_bridge::case_variant::CaseVariant;
        use crate::cube_bridge::string_or_sql::StringOrSql;
        use crate::test_fixtures::cube_bridge::{
            MockCaseDefinition, MockCaseElseItem, MockCaseItem,
        };

        let when_items = vec![
            Rc::new(
                MockCaseItem::builder()
                    .sql("{CUBE.status} = 'active'".to_string())
                    .label(StringOrSql::String("Active".to_string()))
                    .build(),
            ),
            Rc::new(
                MockCaseItem::builder()
                    .sql("{CUBE.status} = 'inactive'".to_string())
                    .label(StringOrSql::String("Inactive".to_string()))
                    .build(),
            ),
        ];

        let else_item = Rc::new(
            MockCaseElseItem::builder()
                .label(StringOrSql::String("Unknown".to_string()))
                .build(),
        );

        let case_def = Rc::new(
            MockCaseDefinition::builder()
                .when(when_items)
                .else_label(else_item)
                .build(),
        );

        let dim = MockDimensionDefinition::builder()
            .dimension_type("string".to_string())
            .case(Some(Rc::new(CaseVariant::Case(case_def))))
            .build();

        assert!(dim.has_case().unwrap());
        let case_result = dim.case().unwrap();
        assert!(case_result.is_some());

        if let Some(CaseVariant::Case(case)) = case_result {
            let when = case.when().unwrap();
            assert_eq!(when.len(), 2);
        } else {
            panic!("Expected Case variant");
        }
    }

    #[test]
    fn test_case_switch_dimension() {
        use crate::cube_bridge::case_variant::CaseVariant;
        use crate::test_fixtures::cube_bridge::{
            MockCaseSwitchDefinition, MockCaseSwitchElseItem, MockCaseSwitchItem,
        };

        let when_items = vec![
            Rc::new(
                MockCaseSwitchItem::builder()
                    .value("1".to_string())
                    .sql("{CUBE.active_value}".to_string())
                    .build(),
            ),
            Rc::new(
                MockCaseSwitchItem::builder()
                    .value("0".to_string())
                    .sql("{CUBE.inactive_value}".to_string())
                    .build(),
            ),
        ];

        let else_item = Rc::new(
            MockCaseSwitchElseItem::builder()
                .sql("{CUBE.default_value}".to_string())
                .build(),
        );

        let case_switch = Rc::new(
            MockCaseSwitchDefinition::builder()
                .switch("{CUBE.status_code}".to_string())
                .when(when_items)
                .else_sql(else_item)
                .build(),
        );

        let dim = MockDimensionDefinition::builder()
            .dimension_type("string".to_string())
            .case(Some(Rc::new(CaseVariant::CaseSwitch(case_switch))))
            .build();

        assert!(dim.has_case().unwrap());
        let case_result = dim.case().unwrap();
        assert!(case_result.is_some());

        if let Some(CaseVariant::CaseSwitch(case_switch)) = case_result {
            assert!(case_switch.switch().is_ok());
            let when = case_switch.when().unwrap();
            assert_eq!(when.len(), 2);
        } else {
            panic!("Expected CaseSwitch variant");
        }
    }
}
