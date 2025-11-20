use crate::test_fixtures::cube_bridge::MockSchema;

/// Creates a schema for visitors and visitor_checkins cubes
///
/// This schema demonstrates:
/// - Basic dimensions with different types
/// - Geo dimensions with latitude/longitude
/// - Sub-query dimensions that reference other cubes
/// - Dimensions with complex SQL including special characters (question marks)
/// - Time dimensions
pub fn create_visitors_schema() -> MockSchema {
    MockSchema::from_yaml_file("common/visitors.yaml").expect("Failed to load visitors schema")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cube_bridge::dimension_definition::DimensionDefinition;
    use crate::cube_bridge::segment_definition::SegmentDefinition;
    use crate::test_fixtures::cube_bridge::MockBaseTools;

    #[test]
    fn test_schema_has_both_cubes() {
        let schema = create_visitors_schema();

        assert!(schema.get_cube("visitors").is_some());
        assert!(schema.get_cube("visitor_checkins").is_some());
    }

    #[test]
    fn test_visitors_dimensions() {
        use crate::test_fixtures::cube_bridge::MockSecurityContext;
        use std::rc::Rc;

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
        let (template, _args) = sql
            .compile_template_sql(
                Rc::new(MockBaseTools::default()),
                Rc::new(MockSecurityContext),
            )
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

        assert_eq!(sql.args_names(), &vec!["CUBE"]);
    }

    #[test]
    fn test_subquery_dimension_references() {
        let schema = create_visitors_schema();

        let min_checkin = schema
            .get_dimension("visitors", "minVisitorCheckinDate")
            .unwrap();
        let sql = min_checkin.sql().unwrap().unwrap();

        // Should reference visitor_checkins.minDate
        assert_eq!(sql.args_names(), &vec!["visitor_checkins"]);
    }

    #[test]
    fn test_geo_dimension_structure() {
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
