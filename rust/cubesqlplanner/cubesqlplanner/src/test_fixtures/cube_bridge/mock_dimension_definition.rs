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
    values,
    primary_key
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
