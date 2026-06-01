//! Domain model for the compiled schema.
//!
//! Rust-native, immutable representation of the schema for the
//! Tesseract planner. The intent is to make the model the single
//! source of truth on the Rust side: all structural lookups go
//! through `Model`; the JS bridge is consulted only for invoking
//! `MemberSql` callbacks per request.
//!
//! Scope (initial cut): we cover what the existing `cube_bridge`
//! traits expose for cubes/measures/dimensions/segments/joins, plus
//! access policies, pre-aggregations, and view resolution.
//! Operational concerns from JS (file tracking,
//! split-view UI, folders, free-form `meta`, dev-server diagnostics)
//! are intentionally out — they are not the planner's domain.
//!
//! The model will grow as planner code starts reading new fields;
//! adding a field is cheap, removing it after callers depend on it
//! is not, hence the conservative starting set.

pub mod access_policy;
pub mod builder;
pub mod case;
pub mod cube;
pub mod dimension;
pub mod expression;
pub mod join;
pub mod measure;
pub mod model;
pub mod path;
pub mod pre_aggregation;
pub mod segment;
pub mod view;

pub use access_policy::{
    AccessCondition, AccessFilter, AccessPolicy, MemberLevelAccess, MemberMasking, RowLevelAccess,
};
pub use builder::SchemaModelBuilder;
pub use case::{Case, CaseSwitch, CaseSwitchWhen, CaseVariant, CaseWhen};
pub use cube::{Cube, SqlSource};
pub use dimension::{Dimension, DimensionType, Granularity};
pub use expression::Expression;
pub use join::{Join, Relationship};
pub use measure::{
    Measure, MeasureOrderBy, MeasureType, MultiStageKind, MultiStageSpec, OrderDirection,
    RollingWindowKind, RollingWindowSpec, TimeShiftDirection, TimeShiftSpec,
};
pub use model::{Model, ModelBuilder};
pub use path::{CubeName, MemberPath};
pub use pre_aggregation::{
    EveryInterval, Index, IndexKind, OriginalSqlSpec, PreAggregation, PreAggregationKind,
    RefreshKey, RollupSpec, RollupTimeDimension,
};
pub use segment::Segment;
pub use view::{IncludedMember, IncludedMemberKind, ViewSpec};
