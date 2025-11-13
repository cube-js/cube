use crate::test_fixtures::cube_bridge::{
    MockDimensionDefinition, MockMeasureDefinition, MockSchema, MockSchemaBuilder,
    MockSegmentDefinition,
};

/// Creates a schema for visitors and visitor_checkins cubes
///
/// This schema demonstrates:
/// - Basic dimensions with different types
/// - Geo dimensions with latitude/longitude
/// - Sub-query dimensions that reference other cubes
/// - Dimensions with complex SQL including special characters (question marks)
/// - Time dimensions
pub fn create_visitors_schema() -> MockSchema {
    MockSchemaBuilder::new()
        // visitor_checkins cube - referenced by visitors cube
        .add_cube("visitor_checkins")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .build(),
        )
        .add_dimension(
            "visitor_id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("visitor_id".to_string())
                .build(),
        )
        .add_dimension(
            "minDate",
            MockDimensionDefinition::builder()
                .dimension_type("time".to_string())
                .sql("MIN(created_at)".to_string())
                .build(),
        )
        .add_dimension(
            "minDate1",
            MockDimensionDefinition::builder()
                .dimension_type("time".to_string())
                .sql("MIN(created_at) + INTERVAL '1 day'".to_string())
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .finish_cube()
        // visitors cube - main cube with various dimension types
        .add_cube("visitors")
        .add_dimension(
            "id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("id".to_string())
                .build(),
        )
        .add_dimension(
            "visitor_id",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("{CUBE}.visitor_id".to_string())
                .build(),
        )
        .add_dimension(
            "visitor_id_proxy",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("{visitors.visitor_id}".to_string())
                .build(),
        )
        .add_dimension(
            "visitor_id_twice",
            MockDimensionDefinition::builder()
                .dimension_type("number".to_string())
                .sql("{visitor_id} * 2".to_string())
                .build(),
        )
        .add_dimension(
            "source",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("source".to_string())
                .build(),
        )
        .add_dimension(
            "source_concat_id",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql("CONCAT({CUBE.source}, ' ', {visitors.visitor_id})".to_string())
                .build(),
        )
        .add_dimension(
            "created_at",
            MockDimensionDefinition::builder()
                .dimension_type("time".to_string())
                .sql("created_at".to_string())
                .build(),
        )
        // Sub-query dimension referencing visitor_checkins.minDate
        .add_dimension(
            "minVisitorCheckinDate",
            MockDimensionDefinition::builder()
                .dimension_type("time".to_string())
                .sql("{visitor_checkins.minDate}".to_string())
                .sub_query(Some(true))
                .build(),
        )
        // Sub-query dimension referencing visitor_checkins.minDate1
        .add_dimension(
            "minVisitorCheckinDate1",
            MockDimensionDefinition::builder()
                .dimension_type("time".to_string())
                .sql("{visitor_checkins.minDate1}".to_string())
                .sub_query(Some(true))
                .build(),
        )
        // Geo dimension with latitude and longitude
        .add_dimension(
            "location",
            MockDimensionDefinition::builder()
                .dimension_type("geo".to_string())
                .latitude("latitude".to_string())
                .longitude("longitude".to_string())
                .build(),
        )
        // Dimension with SQL containing question marks (special characters)
        .add_dimension(
            "questionMark",
            MockDimensionDefinition::builder()
                .dimension_type("string".to_string())
                .sql(
                    "replace('some string question string ? ?? ???', 'string', 'with some ? ?? ???')"
                        .to_string(),
                )
                .build(),
        )
        .add_measure(
            "count",
            MockMeasureDefinition::builder()
                .measure_type("count".to_string())
                .sql("COUNT(*)".to_string())
                .build(),
        )
        .add_measure(
            "total_revenue",
            MockMeasureDefinition::builder()
                .measure_type("sum".to_string())
                .sql("revenue".to_string())
                .build(),
        )
        .add_measure(
            "total_revenue_proxy",
            MockMeasureDefinition::builder()
                .measure_type("number".to_string())
                .sql("{total_revenue}".to_string())
                .build(),
        )
        .add_measure(
            "revenue",
            MockMeasureDefinition::builder()
                .measure_type("sum".to_string())
                .sql("{CUBE}.revenue".to_string())
                .build(),
        )
        .add_measure(
            "total_revenue_per_count",
            MockMeasureDefinition::builder()
                .measure_type("number".to_string())
                .sql("{visitors.count} / {total_revenue}".to_string())
                .build(),
        )
        .add_segment(
            "google",
            MockSegmentDefinition::builder()
                .sql("{CUBE.source} = 'google'".to_string())
                .build(),
        )
        .finish_cube()
        .add_view("visitors_visitors_checkins")
        .include_cube("visitors", vec!["id".to_string(), "source_concat_id".to_string()])
        .include_cube("visitors.visitor_checkins", vec!["visitor_id".to_string(), "count".to_string()])
        .finish_view()
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::dimension_definition::DimensionDefinition;
    use crate::cube_bridge::measure_definition::MeasureDefinition;
    use crate::cube_bridge::segment_definition::SegmentDefinition;

    #[test]
    fn test_schema_has_both_cubes() {
        let schema = create_visitors_schema();

        assert!(schema.get_cube("visitors").is_some());
        assert!(schema.get_cube("visitor_checkins").is_some());
    }

    #[test]
    fn test_visitors_dimensions() {
        let schema = create_visitors_schema();

        // Basic dimensions
        assert!(schema.get_dimension("visitors", "visitor_id").is_some());
        assert!(schema.get_dimension("visitors", "source").is_some());
        assert!(schema.get_dimension("visitors", "created_at").is_some());

        // Sub-query dimensions
        let min_checkin = schema
            .get_dimension("visitors", "minVisitorCheckinDate")
            .unwrap();
        assert_eq!(min_checkin.static_data().dimension_type, "time");
        assert_eq!(min_checkin.static_data().sub_query, Some(true));

        let min_checkin1 = schema
            .get_dimension("visitors", "minVisitorCheckinDate1")
            .unwrap();
        assert_eq!(min_checkin1.static_data().dimension_type, "time");
        assert_eq!(min_checkin1.static_data().sub_query, Some(true));

        // Geo dimension
        let location = schema.get_dimension("visitors", "location").unwrap();
        assert_eq!(location.static_data().dimension_type, "geo");
        assert!(location.has_latitude().unwrap());
        assert!(location.has_longitude().unwrap());

        // Dimension with special characters
        let question_mark = schema.get_dimension("visitors", "questionMark").unwrap();
        assert_eq!(question_mark.static_data().dimension_type, "string");
        let sql = question_mark.sql().unwrap().unwrap();
        // Verify SQL contains question marks
        use crate::cube_bridge::member_sql::MemberSql;
        use crate::test_fixtures::cube_bridge::{MockSecurityContext, MockSqlUtils};
        use std::rc::Rc;
        let (template, _args) = sql
            .compile_template_sql(Rc::new(MockSqlUtils), Rc::new(MockSecurityContext))
            .unwrap();
        match template {
            crate::cube_bridge::member_sql::SqlTemplate::String(s) => {
                assert!(s.contains("?"));
            }
            _ => panic!("Expected String template"),
        }
    }

    #[test]
    fn test_visitor_checkins_dimensions() {
        let schema = create_visitors_schema();

        assert!(schema
            .get_dimension("visitor_checkins", "visitor_id")
            .is_some());

        let min_date = schema.get_dimension("visitor_checkins", "minDate").unwrap();
        assert_eq!(min_date.static_data().dimension_type, "time");

        let min_date1 = schema
            .get_dimension("visitor_checkins", "minDate1")
            .unwrap();
        assert_eq!(min_date1.static_data().dimension_type, "time");
    }

    #[test]
    fn test_visitors_measures() {
        let schema = create_visitors_schema();

        let count = schema.get_measure("visitors", "count").unwrap();
        assert_eq!(count.static_data().measure_type, "count");

        let revenue = schema.get_measure("visitors", "total_revenue").unwrap();
        assert_eq!(revenue.static_data().measure_type, "sum");
    }

    #[test]
    fn test_visitors_segments() {
        let schema = create_visitors_schema();

        let google_segment = schema.get_segment("visitors", "google").unwrap();
        let sql = google_segment.sql().unwrap();

        use crate::cube_bridge::member_sql::MemberSql;
        assert_eq!(sql.args_names(), &vec!["CUBE"]);
    }

    #[test]
    fn test_subquery_dimension_references() {
        let schema = create_visitors_schema();

        let min_checkin = schema
            .get_dimension("visitors", "minVisitorCheckinDate")
            .unwrap();
        let sql = min_checkin.sql().unwrap().unwrap();

        use crate::cube_bridge::member_sql::MemberSql;
        // Should reference visitor_checkins.minDate
        assert_eq!(sql.args_names(), &vec!["visitor_checkins"]);
    }

    #[test]
    fn test_geo_dimension_structure() {
        use crate::cube_bridge::geo_item::GeoItem;
        use crate::cube_bridge::member_sql::MemberSql;

        let schema = create_visitors_schema();

        let location = schema.get_dimension("visitors", "location").unwrap();

        assert_eq!(location.static_data().dimension_type, "geo");

        // Test using trait methods
        let latitude = location.latitude().unwrap().unwrap();
        let lat_sql = latitude.sql().unwrap();
        // Verify the SQL is correct - it should have no template parameters
        assert_eq!(lat_sql.args_names().len(), 0);

        let longitude = location.longitude().unwrap().unwrap();
        let lon_sql = longitude.sql().unwrap();
        assert_eq!(lon_sql.args_names().len(), 0);
    }
}

