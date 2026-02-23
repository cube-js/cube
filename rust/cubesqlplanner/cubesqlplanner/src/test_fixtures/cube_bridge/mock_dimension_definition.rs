use crate::cube_bridge::case_variant::CaseVariant;
use crate::cube_bridge::dimension_definition::{DimensionDefinition, DimensionDefinitionStatic};
use crate::cube_bridge::geo_item::GeoItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::timeshift_definition::TimeShiftDefinition;
use crate::impl_static_data;
use crate::test_fixtures::cube_bridge::yaml::dimension::YamlDimensionDefinition;
use crate::test_fixtures::cube_bridge::{MockGeoItem, MockMemberSql, MockTimeShiftDefinition};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct MockDimensionDefinition {
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
    #[builder(default)]
    primary_key: Option<bool>,

    #[builder(default, setter(strip_option(fallback = sql_opt)))]
    sql: Option<String>,
    #[builder(default)]
    case: Option<Rc<CaseVariant>>,
    #[builder(default, setter(strip_option(fallback = latitude_opt)))]
    latitude: Option<String>,
    #[builder(default, setter(strip_option(fallback = longitude_opt)))]
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
    values,
    primary_key
);

impl MockDimensionDefinition {
    pub fn from_yaml(yaml: &str) -> Result<Rc<Self>, CubeError> {
        let yaml_def: YamlDimensionDefinition = serde_yaml::from_str(yaml)
            .map_err(|e| CubeError::user(format!("Failed to parse YAML: {}", e)))?;
        Ok(Rc::new(yaml_def.build().definition))
    }
}

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
    use indoc::indoc;

    #[test]
    fn test_from_yaml_all_fields() {
        let yaml = indoc! {"
            type: number
            owned_by_cube: true
            multi_stage: true
            sub_query: true
            propagate_filters_to_sub_query: true
            values: [val1, val2]
            add_group_by: ['dim1', 'dim2']
            primary_key: true
            sql: id
            latitude: lat
            longitude: lon
            time_shift:
              - interval: 1 year
                type: prior
        "};

        let dim = MockDimensionDefinition::from_yaml(yaml).unwrap();
        let static_data = dim.static_data();

        assert_eq!(static_data.dimension_type, "number");
        assert_eq!(static_data.multi_stage, Some(true));
        assert_eq!(static_data.sub_query, Some(true));
        assert_eq!(static_data.propagate_filters_to_sub_query, Some(true));
        assert_eq!(
            static_data.values,
            Some(vec!["val1".to_string(), "val2".to_string()])
        );
        assert_eq!(static_data.primary_key, Some(true));
        assert_eq!(
            static_data.add_group_by_references,
            Some(vec!["dim1".to_string(), "dim2".to_string()])
        );
        assert!(dim.has_sql().unwrap());
        assert!(dim.has_latitude().unwrap());
        assert!(dim.has_longitude().unwrap());
        assert!(dim.has_time_shift().unwrap());
    }

    #[test]
    fn test_from_yaml_minimal() {
        let yaml = indoc! {"
            type: string
            sql: status
        "};

        let dim = MockDimensionDefinition::from_yaml(yaml).unwrap();
        let static_data = dim.static_data();

        assert_eq!(static_data.dimension_type, "string");
        assert_eq!(static_data.multi_stage, None);
        assert_eq!(static_data.sub_query, None);
        assert_eq!(static_data.primary_key, None);
        assert!(dim.has_sql().unwrap());
        assert!(!dim.has_latitude().unwrap());
        assert!(!dim.has_longitude().unwrap());
        assert!(!dim.has_time_shift().unwrap());
    }

    #[test]
    fn test_from_yaml_with_case() {
        let yaml = indoc! {"
            type: string
            sql: size_value
            case:
              when:
                - sql: \"{CUBE}.size_value = 'xl-en'\"
                  label: xl
                - sql: \"{CUBE}.size_value = 'xxl'\"
                  label: xxl
              else:
                label: Unknown
        "};

        let dim = MockDimensionDefinition::from_yaml(yaml).unwrap();
        assert!(dim.has_case().unwrap());

        let case_variant = dim.case().unwrap().unwrap();
        match case_variant {
            CaseVariant::Case(case_def) => {
                let when_items = case_def.when().unwrap();
                assert_eq!(when_items.len(), 2);
            }
            _ => panic!("Expected Case variant"),
        }
    }

    #[test]
    fn test_from_yaml_with_case_switch() {
        let yaml = indoc! {"
            type: string
            sql: currency_name
            case:
              switch: \"{CUBE.currency}\"
              when:
                - value: USD
                  sql: \"'dollars'\"
                - value: EUR
                  sql: \"'euros'\"
              else:
                sql: \"'unknown'\"
        "};

        let dim = MockDimensionDefinition::from_yaml(yaml).unwrap();
        assert!(dim.has_case().unwrap());

        let case_variant = dim.case().unwrap().unwrap();
        match case_variant {
            CaseVariant::CaseSwitch(switch_def) => {
                let when_items = switch_def.when().unwrap();
                assert_eq!(when_items.len(), 2);
            }
            _ => panic!("Expected CaseSwitch variant"),
        }
    }
}
