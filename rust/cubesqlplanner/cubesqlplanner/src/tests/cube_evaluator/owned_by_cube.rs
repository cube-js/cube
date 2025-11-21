use crate::test_fixtures::cube_bridge::MockSchema;
use crate::test_fixtures::test_utils::TestContext;

#[test]
fn dimensions_ownships() {
    let schema = MockSchema::from_yaml_file("compilation_tests/ownership_test.yaml");
    let context = TestContext::new(schema).unwrap();

    let owned_dims = vec![
        "users.id",
        "users.userName",
        "users.ownedCase",
        "users.anotherOwnedCase",
        "users.ownedGeo",
        "users.anotherOwnedGeo",
        "orders.orderType",
    ];

    for dim in owned_dims {
        assert!(
            context.create_dimension(dim).unwrap().owned_by_cube(),
            "Dimension {} should be owned by cube",
            dim
        );
    }

    let not_owned_dims = vec![
        "users.userNameProxy",
        "users.complexId",
        "users.notOwnedCase",
        "users.notOwnedCaseOtherCube",
        "users.notOwnedGeo",
        "users.notOwnedGeoTypeOtherCube",
        "users_to_orders.id",
        "users_to_orders.orderType",
    ];

    for dim in not_owned_dims {
        assert!(
            !context.create_dimension(dim).unwrap().owned_by_cube(),
            "Dimension {} should not be owned by cube",
            dim
        );
    }
}

#[test]
fn measures_ownships() {
    let schema = MockSchema::from_yaml_file("compilation_tests/ownership_test.yaml");
    let context = TestContext::new(schema).unwrap();

    let owned_measures = vec![
        "users.count",
        "users.amount",
        "users.minPayment",
        "users.ownedFilter",
        "users.otherOwnedFilter",
        "users.ownedDrillFilter",
        "users.otherOwnedDrillFilter",
    ];

    for meas in owned_measures {
        assert!(
            context.create_measure(meas).unwrap().owned_by_cube(),
            "Measure {} should be owned by cube",
            meas
        );
    }

    let not_owned_measures = vec![
        "users.proxyAmount",
        "users.complexCalculation",
        "users.notOwnedFilter",
        "users.notOwnedFilterOtherCube",
        "users.notOwnedDrillFilter",
        "users.notOwnedDrillFilterOtherCube",
        "users_to_orders.count",
    ];

    for meas in not_owned_measures {
        assert!(
            !context.create_measure(meas).unwrap().owned_by_cube(),
            "Measure {} should not be owned by cube",
            meas
        );
    }
}
