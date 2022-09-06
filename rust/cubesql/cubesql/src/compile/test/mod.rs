use cubeclient::models::{V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment};

pub fn get_test_meta() -> Vec<V1CubeMeta> {
    vec![
        V1CubeMeta {
            name: "KibanaSampleDataEcommerce".to_string(),
            title: None,
            dimensions: vec![
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.order_date".to_string(),
                    _type: "time".to_string(),
                },
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.customer_gender".to_string(),
                    _type: "string".to_string(),
                },
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.taxful_total_price".to_string(),
                    _type: "number".to_string(),
                },
                V1CubeMetaDimension {
                    name: "KibanaSampleDataEcommerce.has_subscription".to_string(),
                    _type: "boolean".to_string(),
                },
            ],
            measures: vec![
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.count".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("count".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("max".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.minPrice".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("min".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "KibanaSampleDataEcommerce.avgPrice".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("avg".to_string()),
                },
            ],
            segments: vec![
                V1CubeMetaSegment {
                    name: "KibanaSampleDataEcommerce.is_male".to_string(),
                    title: "Ecommerce Male".to_string(),
                    short_title: "Male".to_string(),
                },
                V1CubeMetaSegment {
                    name: "KibanaSampleDataEcommerce.is_female".to_string(),
                    title: "Ecommerce Female".to_string(),
                    short_title: "Female".to_string(),
                },
            ],
        },
        V1CubeMeta {
            name: "Logs".to_string(),
            title: None,
            dimensions: vec![],
            measures: vec![
                V1CubeMetaMeasure {
                    name: "Logs.agentCount".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("countDistinct".to_string()),
                },
                V1CubeMetaMeasure {
                    name: "Logs.agentCountApprox".to_string(),
                    title: None,
                    _type: "number".to_string(),
                    agg_type: Some("countDistinctApprox".to_string()),
                },
            ],
            segments: vec![],
        },
    ]
}
