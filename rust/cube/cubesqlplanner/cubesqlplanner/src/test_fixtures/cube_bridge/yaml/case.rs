use crate::cube_bridge::string_or_sql::StringOrSql;
use crate::test_fixtures::cube_bridge::{
    MockCaseContextDefinition, MockCaseContextItem, MockCaseDefinition, MockCaseElseItem,
    MockCaseItem, MockCaseSwitchDefinition, MockCaseSwitchElseItem, MockCaseSwitchItem,
    MockStructWithSqlMember,
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
    label: YamlCaseLabel,
}

#[derive(Debug, Deserialize)]
pub struct YamlCaseElseItem {
    label: YamlCaseLabel,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum YamlCaseLabel {
    String(String),
    Sql { sql: String },
}

impl YamlCaseLabel {
    fn build(self) -> StringOrSql {
        match self {
            Self::String(s) => StringOrSql::String(s),
            Self::Sql { sql } => {
                StringOrSql::MemberSql(Rc::new(MockStructWithSqlMember::builder().sql(sql).build()))
            }
        }
    }
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
pub struct YamlCaseContextDefinition {
    when: Vec<YamlCaseContextItem>,
    #[serde(rename = "else")]
    else_sql: YamlCaseSwitchElseItem,
}

#[derive(Debug, Deserialize)]
pub struct YamlCaseContextItem {
    context: String,
    sql: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum YamlCaseVariant {
    CaseSwitch(YamlCaseSwitchDefinition),
    CaseContext(YamlCaseContextDefinition),
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
                        .label(item.label.build())
                        .build(),
                )
            })
            .collect();

        let else_item = Rc::new(
            MockCaseElseItem::builder()
                .label(self.else_label.label.build())
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

impl YamlCaseContextDefinition {
    pub fn build(self) -> Rc<MockCaseContextDefinition> {
        let when_items: Vec<Rc<MockCaseContextItem>> = self
            .when
            .into_iter()
            .map(|item| {
                Rc::new(
                    MockCaseContextItem::builder()
                        .context(item.context)
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
            MockCaseContextDefinition::builder()
                .when(when_items)
                .else_sql(else_item)
                .build(),
        )
    }
}
