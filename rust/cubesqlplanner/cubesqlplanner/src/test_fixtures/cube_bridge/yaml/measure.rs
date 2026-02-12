use crate::cube_bridge::case_variant::CaseVariant;
use crate::cube_bridge::measure_definition::{RollingWindow, TimeShiftReference};
use crate::test_fixtures::cube_bridge::yaml::case::YamlCaseVariant;
use crate::test_fixtures::cube_bridge::{
    MockMeasureDefinition, MockMemberOrderBy, MockStructWithSqlMember,
};
use serde::Deserialize;
use std::rc::Rc;

#[derive(Debug, Deserialize)]
pub struct YamlMeasureDefinition {
    #[serde(rename = "type")]
    measure_type: String,
    #[serde(default)]
    multi_stage: Option<bool>,
    #[serde(default)]
    reduce_by_references: Option<Vec<String>>,
    #[serde(default)]
    add_group_by_references: Option<Vec<String>>,
    #[serde(default)]
    group_by_references: Option<Vec<String>>,
    #[serde(default)]
    time_shift_references: Option<Vec<TimeShiftReference>>,
    #[serde(default)]
    rolling_window: Option<RollingWindow>,
    #[serde(default)]
    sql: Option<String>,
    #[serde(default)]
    case: Option<YamlCaseVariant>,
    #[serde(default)]
    filters: Vec<YamlFilter>,
    #[serde(default)]
    drill_filters: Vec<YamlFilter>,
    #[serde(default)]
    order_by: Vec<YamlOrderBy>,
}

#[derive(Debug, Deserialize)]
struct YamlFilter {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct YamlOrderBy {
    sql: String,
    dir: String,
}

impl YamlMeasureDefinition {
    pub fn build(self) -> Rc<MockMeasureDefinition> {
        let case = self.case.map(|cv| match cv {
            YamlCaseVariant::Case(case_def) => Rc::new(CaseVariant::Case(case_def.build())),
            YamlCaseVariant::CaseSwitch(switch_def) => {
                Rc::new(CaseVariant::CaseSwitch(switch_def.build()))
            }
        });

        let filters = if !self.filters.is_empty() {
            Some(
                self.filters
                    .into_iter()
                    .map(|f| Rc::new(MockStructWithSqlMember::builder().sql(f.sql).build()))
                    .collect(),
            )
        } else {
            None
        };

        let drill_filters = if !self.drill_filters.is_empty() {
            Some(
                self.drill_filters
                    .into_iter()
                    .map(|f| Rc::new(MockStructWithSqlMember::builder().sql(f.sql).build()))
                    .collect(),
            )
        } else {
            None
        };

        let order_by = if !self.order_by.is_empty() {
            Some(
                self.order_by
                    .into_iter()
                    .map(|o| Rc::new(MockMemberOrderBy::builder().sql(o.sql).dir(o.dir).build()))
                    .collect(),
            )
        } else {
            None
        };

        Rc::new(
            MockMeasureDefinition::builder()
                .measure_type(self.measure_type)
                .multi_stage(self.multi_stage)
                .reduce_by_references(self.reduce_by_references)
                .add_group_by_references(self.add_group_by_references)
                .group_by_references(self.group_by_references)
                .time_shift_references(self.time_shift_references)
                .rolling_window(self.rolling_window)
                .sql_opt(self.sql)
                .case(case)
                .filters(filters)
                .drill_filters(drill_filters)
                .order_by(order_by)
                .build(),
        )
    }
}
