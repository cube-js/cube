use crate::cube_bridge::base_query_options::{FilterItem, OrderByItem, TimeDimension};
use serde::de;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize)]
pub struct YamlBaseQueryOptions {
    #[serde(default)]
    pub measures: Option<Vec<String>>,
    #[serde(default)]
    pub dimensions: Option<Vec<String>>,
    #[serde(default)]
    pub segments: Option<Vec<String>>,
    #[serde(default)]
    pub time_dimensions: Option<Vec<YamlTimeDimension>>,
    #[serde(default)]
    pub order: Option<Vec<YamlOrderByItem>>,
    #[serde(default)]
    pub filters: Option<Vec<YamlFilterItem>>,
    #[serde(default)]
    pub limit: Option<String>,
    #[serde(default)]
    pub row_limit: Option<String>,
    #[serde(default)]
    pub offset: Option<String>,
    #[serde(default)]
    pub ungrouped: Option<bool>,
    #[serde(default)]
    pub export_annotated_sql: Option<bool>,
    #[serde(default)]
    pub pre_aggregation_query: Option<bool>,
    #[serde(default)]
    pub total_query: Option<bool>,
    #[serde(default)]
    pub cubestore_support_multistage: Option<bool>,
    #[serde(default)]
    pub disable_external_pre_aggregations: Option<bool>,
    #[serde(default)]
    pub pre_aggregation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct YamlOrderByItem {
    pub id: String,
    #[serde(default)]
    pub desc: Option<bool>,
}

impl YamlOrderByItem {
    pub fn into_order_by_item(self) -> OrderByItem {
        OrderByItem {
            id: self.id,
            desc: self.desc,
        }
    }
}

#[derive(Debug)]
pub enum YamlFilterItem {
    Group(YamlFilterGroup),
    Base(YamlBaseFilter),
}

impl<'de> Deserialize<'de> for YamlFilterItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_yaml::Value::deserialize(deserializer)?;

        // Check if it has 'or' or 'and' keys - then it's a Group
        if let serde_yaml::Value::Mapping(ref map) = value {
            let has_or = map.contains_key(serde_yaml::Value::String("or".to_string()));
            let has_and = map.contains_key(serde_yaml::Value::String("and".to_string()));

            if has_or || has_and {
                return serde_yaml::from_value::<YamlFilterGroup>(value)
                    .map(YamlFilterItem::Group)
                    .map_err(de::Error::custom);
            }
        }

        // Otherwise it's a Base filter
        serde_yaml::from_value::<YamlBaseFilter>(value)
            .map(YamlFilterItem::Base)
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
pub struct YamlFilterGroup {
    #[serde(default)]
    pub or: Option<Vec<YamlFilterItem>>,
    #[serde(default)]
    pub and: Option<Vec<YamlFilterItem>>,
}

#[derive(Debug, Deserialize)]
pub struct YamlBaseFilter {
    #[serde(default)]
    pub member: Option<String>,
    #[serde(default)]
    pub dimension: Option<String>,
    #[serde(default)]
    pub operator: Option<String>,
    #[serde(default)]
    pub values: Option<Vec<Option<String>>>,
}

impl YamlFilterItem {
    pub fn into_filter_item(self) -> FilterItem {
        match self {
            YamlFilterItem::Group(group) => FilterItem {
                or: group.or.map(|items| {
                    items
                        .into_iter()
                        .map(|item| item.into_filter_item())
                        .collect()
                }),
                and: group.and.map(|items| {
                    items
                        .into_iter()
                        .map(|item| item.into_filter_item())
                        .collect()
                }),
                member: None,
                dimension: None,
                operator: None,
                values: None,
            },
            YamlFilterItem::Base(base) => FilterItem {
                or: None,
                and: None,
                member: base.member,
                dimension: base.dimension,
                operator: base.operator,
                values: base.values,
            },
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct YamlTimeDimension {
    pub dimension: String,
    #[serde(default)]
    pub granularity: Option<String>,
    #[serde(default, rename = "dateRange")]
    pub date_range: Option<Vec<String>>,
}

impl YamlTimeDimension {
    pub fn into_time_dimension(self) -> TimeDimension {
        TimeDimension {
            dimension: self.dimension,
            granularity: self.granularity,
            date_range: self.date_range,
        }
    }
}
