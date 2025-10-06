use cubeclient::models::*;

#[test]
fn test_deserialize_meta_response_with_format_fields() {
    let json_data = r#"{
      "cubes": [
        {
          "name": "orders",
          "type": "cube",
          "title": "Orders",
          "isVisible": false,
          "public": false,
          "connectedComponent": 1,
          "measures": [
            {
              "name": "orders.count",
              "title": "Orders Count",
              "shortTitle": "Count",
              "cumulativeTotal": false,
              "cumulative": false,
              "type": "number",
              "aggType": "count",
              "drillMembers": [],
              "drillMembersGrouped": {
                "measures": [],
                "dimensions": []
              },
              "meta": {
                "random_field_name": "My custom metadata for meta count"
              },
              "isVisible": false,
              "public": false
            },
            {
              "name": "orders.min_amount",
              "title": "Orders Min Amount",
              "shortTitle": "Min Amount",
              "format": "currency",
              "cumulativeTotal": false,
              "cumulative": false,
              "type": "number",
              "aggType": "min",
              "drillMembers": [],
              "drillMembersGrouped": {
                "measures": [],
                "dimensions": []
              },
              "isVisible": false,
              "public": false
            },
            {
              "name": "orders.total_amount",
              "title": "Orders Total Amount",
              "shortTitle": "Total Amount",
              "format": "currency",
              "cumulativeTotal": false,
              "cumulative": false,
              "type": "number",
              "aggType": "sum",
              "drillMembers": [],
              "drillMembersGrouped": {
                "measures": [],
                "dimensions": []
              },
              "isVisible": false,
              "public": false
            }
          ],
          "dimensions": [
            {
              "name": "orders.id",
              "title": "Orders Id",
              "type": "number",
              "shortTitle": "Id",
              "suggestFilterValues": true,
              "isVisible": false,
              "public": false,
              "primaryKey": true
            },
            {
              "name": "orders.order_sum",
              "title": "Orders Order Sum",
              "type": "number",
              "shortTitle": "Order Sum",
              "suggestFilterValues": true,
              "format": "currency",
              "isVisible": false,
              "public": false,
              "primaryKey": false
            }
          ],
          "segments": [],
          "hierarchies": [],
          "folders": [],
          "nestedFolders": []
        }
      ]
    }"#;

    // Test basic deserialization
    let meta_response: V1MetaResponse =
        serde_json::from_str(json_data).expect("Should successfully deserialize meta response");

    // Verify the response structure
    assert!(meta_response.cubes.is_some());
    let cubes = meta_response.cubes.unwrap();
    assert_eq!(cubes.len(), 1);
}

#[test]
fn test_deserialize_dimension_link_format() {
    let json_data = r#"{
      "cubes": [
        {
          "name": "test_cube",
          "type": "cube",
          "measures": [],
          "dimensions": [
            {
              "name": "test.link_dimension",
              "title": "Test Link Dimension",
              "type": "string",
              "format": {
                "type": "link",
                "label": "View Details"
              }
            }
          ],
          "segments": []
        }
      ]
    }"#;

    let meta_response: V1MetaResponse =
        serde_json::from_str(json_data).expect("Should successfully deserialize link format");

    let cubes = meta_response.cubes.unwrap();
    let dimension = &cubes[0].dimensions[0];

    assert_eq!(
        dimension.format,
        Some(Box::new(V1CubeMetaFormat::V1CubeMetaLinkFormat(Box::new(
            V1CubeMetaLinkFormat {
                label: "View Details".to_string(),
                r#type: V1CubeMetaLinkFormatType::Link,
            }
        ))))
    );
}
