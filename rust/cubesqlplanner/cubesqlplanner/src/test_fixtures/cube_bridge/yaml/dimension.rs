use crate::cube_bridge::case_variant::CaseVariant;
use crate::test_fixtures::cube_bridge::yaml::case::YamlCaseVariant;
use crate::test_fixtures::cube_bridge::yaml::timeshift::YamlTimeShiftDefinition;
use crate::test_fixtures::cube_bridge::{MockDimensionDefinition, MockGranularityDefinition};
use serde::Deserialize;
use std::rc::Rc;

#[derive(Debug, Deserialize)]
pub struct YamlGranularityEntry {
    pub name: String,
    pub interval: String,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub offset: Option<String>,
}

pub struct YamlDimensionBuildResult {
    pub definition: MockDimensionDefinition,
    pub granularities: Vec<(String, MockGranularityDefinition)>,
}

#[derive(Debug, Deserialize)]
pub struct YamlDimensionDefinition {
    #[serde(rename = "type")]
    dimension_type: String,
    #[serde(default)]
    multi_stage: Option<bool>,
    #[serde(default, rename = "add_group_by")]
    add_group_by_references: Option<Vec<String>>,
    #[serde(default)]
    sub_query: Option<bool>,
    #[serde(default)]
    propagate_filters_to_sub_query: Option<bool>,
    #[serde(default)]
    values: Option<Vec<String>>,
    #[serde(default)]
    primary_key: Option<bool>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    case: Option<YamlCaseVariant>,
    #[serde(default)]
    latitude: Option<String>,
    #[serde(default)]
    longitude: Option<String>,
    #[serde(default)]
    time_shift: Vec<YamlTimeShiftDefinition>,
    #[serde(default)]
    granularities: Vec<YamlGranularityEntry>,
}

impl YamlDimensionDefinition {
    pub fn build(self) -> YamlDimensionBuildResult {
        let time_shift = if !self.time_shift.is_empty() {
            Some(self.time_shift.into_iter().map(|ts| ts.build()).collect())
        } else {
            None
        };

        let case = self.case.map(|cv| match cv {
            YamlCaseVariant::Case(case_def) => Rc::new(CaseVariant::Case(case_def.build())),
            YamlCaseVariant::CaseSwitch(switch_def) => {
                Rc::new(CaseVariant::CaseSwitch(switch_def.build()))
            }
        });

        let granularities: Vec<(String, MockGranularityDefinition)> = self
            .granularities
            .into_iter()
            .map(|entry| {
                let def = MockGranularityDefinition::builder()
                    .interval(entry.interval)
                    .origin_opt(entry.origin)
                    .offset_opt(entry.offset)
                    .build();
                (entry.name, def)
            })
            .collect();

        let definition = MockDimensionDefinition::builder()
            .dimension_type(self.dimension_type)
            .multi_stage(self.multi_stage)
            .add_group_by_references(self.add_group_by_references)
            .sub_query(self.sub_query)
            .propagate_filters_to_sub_query(self.propagate_filters_to_sub_query)
            .values(self.values)
            .primary_key(self.primary_key)
            .sql_opt(self.sql)
            .case(case)
            .latitude_opt(self.latitude)
            .longitude_opt(self.longitude)
            .time_shift(time_shift)
            .build();

        YamlDimensionBuildResult {
            definition,
            granularities,
        }
    }
}
