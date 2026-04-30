use crate::cube_bridge::string_or_sql::StringOrSql;
use crate::test_fixtures::cube_bridge::{
    MockCaseDefinition, MockCaseElseItem, MockCaseItem, MockCaseSwitchDefinition,
    MockCaseSwitchElseItem, MockCaseSwitchItem,
};
use serde::Deserialize;
use std::rc::Rc;

#[derive(Debug, Deserialize)]
pub struct YamlCaseDefinition {
    when: Vec<YamlCaseItem>,
    #[serde(rename = "else")]
    else_label: YamlCaseElseItem,
}

#[derive(Debug, Deserialize)]
pub struct YamlCaseItem {
    sql: String,
    label: String,
}

#[derive(Debug, Deserialize)]
pub struct YamlCaseElseItem {
    label: String,
}

#[derive(Debug, Deserialize)]
pub struct YamlCaseSwitchDefinition {
    switch: String,
    when: Vec<YamlCaseSwitchItem>,
    #[serde(rename = "else")]
    else_sql: YamlCaseSwitchElseItem,
}

#[derive(Debug, Deserialize)]
pub struct YamlCaseSwitchItem {
    value: String,
    sql: String,
}

#[derive(Debug, Deserialize)]
pub struct YamlCaseSwitchElseItem {
    sql: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum YamlCaseVariant {
    CaseSwitch(YamlCaseSwitchDefinition),
    Case(YamlCaseDefinition),
}

impl YamlCaseDefinition {
    pub fn build(self) -> Rc<MockCaseDefinition> {
        let when_items: Vec<Rc<MockCaseItem>> = self
            .when
            .into_iter()
            .map(|item| {
                Rc::new(
                    MockCaseItem::builder()
                        .sql(item.sql)
                        .label(StringOrSql::String(item.label))
                        .build(),
                )
            })
            .collect();

        let else_item = Rc::new(
            MockCaseElseItem::builder()
                .label(StringOrSql::String(self.else_label.label))
                .build(),
        );

        Rc::new(
            MockCaseDefinition::builder()
                .when(when_items)
                .else_label(else_item)
                .build(),
        )
    }
}

impl YamlCaseSwitchDefinition {
    pub fn build(self) -> Rc<MockCaseSwitchDefinition> {
        let when_items: Vec<Rc<MockCaseSwitchItem>> = self
            .when
            .into_iter()
            .map(|item| {
                Rc::new(
                    MockCaseSwitchItem::builder()
                        .value(item.value)
                        .sql(item.sql)
                        .build(),
                )
            })
            .collect();

        let else_item = Rc::new(
            MockCaseSwitchElseItem::builder()
                .sql(self.else_sql.sql)
                .build(),
        );

        Rc::new(
            MockCaseSwitchDefinition::builder()
                .switch(self.switch)
                .when(when_items)
                .else_sql(else_item)
                .build(),
        )
    }
}
