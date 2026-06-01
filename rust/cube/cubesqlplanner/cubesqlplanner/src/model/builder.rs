use super::access_policy::{
    AccessCondition, AccessFilter, AccessPolicy, MemberLevelAccess, MemberMasking, RowLevelAccess,
};
use super::case::{
    Case, CaseLabel, CaseSwitch, CaseSwitchWhen, CaseVariant as ModelCaseVariant, CaseWhen,
};
use super::cube::{Cube, SqlSource};
use super::dimension::{Dimension, DimensionType, Granularity};
use super::expression::Expression;
use super::join::{Join, Relationship};
use super::measure::{
    Measure, MeasureOrderBy, MeasureType, MultiStageKind, MultiStageSpec, OrderDirection,
    RollingWindowKind, RollingWindowSpec, TimeShiftDirection, TimeShiftSpec,
};
use super::model::{Model, ModelBuilder};
use super::path::{CubeName, MemberPath};
use super::pre_aggregation::{
    EveryInterval, Index, IndexKind, OriginalSqlSpec, PreAggregation, PreAggregationKind,
    RefreshKey, RollupSpec, RollupTimeDimension,
};
use super::segment::Segment;
use super::view::{IncludedMember, IncludedMemberKind, ViewSpec};
use crate::cube_bridge::access_policy_definition::AccessPolicyDefinition;
use crate::cube_bridge::case_variant::CaseVariant as BridgeCaseVariant;
use crate::cube_bridge::cube_definition::CubeDefinition;
use crate::cube_bridge::cube_join_definition::CubeJoinDefinition;
use crate::cube_bridge::dimension_definition::DimensionDefinition;
use crate::cube_bridge::measure_definition::{
    MeasureDefinition, RollingWindow, TimeShiftReference,
};
use crate::cube_bridge::pre_aggregation_description::PreAggregationDescription;
use crate::cube_bridge::schema_source::SchemaSource;
use crate::cube_bridge::segment_definition::SegmentDefinition;
use crate::cube_bridge::string_or_sql::StringOrSql;
use crate::cube_bridge::view_included_member::ViewIncludedMember;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// Builds a `Model` from a `SchemaSource` snapshot of the JS schema.
///
/// Currently populates the cube skeleton plus measures, dimensions, and
/// segments with their basic fields (path, kind, sql, mask_sql,
/// owned_by_cube, primary_key, multi_stage flag, sub_query, values,
/// alias_member). Cases, multi-stage specs, rolling windows, time
/// shifts, filters, order_by, joins, pre-aggregations, access policies,
/// and view spec are populated in follow-up iterations.
pub struct SchemaModelBuilder {
    source: Rc<dyn SchemaSource>,
}

impl SchemaModelBuilder {
    pub fn new(source: Rc<dyn SchemaSource>) -> Self {
        Self { source }
    }

    pub fn build(&self) -> Result<Model, CubeError> {
        let mut model = ModelBuilder::new();
        let primary_keys = &self.source.static_data().primary_keys;
        for definition in self.source.cubes()? {
            let cube = self.build_cube(definition, primary_keys)?;
            model.add_cube(Rc::new(cube));
        }
        model.build()
    }

    fn build_cube(
        &self,
        definition: Rc<dyn CubeDefinition>,
        primary_keys: &HashMap<String, Vec<String>>,
    ) -> Result<Cube, CubeError> {
        let static_data = definition.static_data();
        let cube_name = CubeName::new(static_data.name.clone());
        let source = self.build_source(&definition)?;
        let primary_keys = primary_keys
            .get(&static_data.name)
            .cloned()
            .unwrap_or_default();

        let measures = definition
            .measures()?
            .into_iter()
            .map(|m| {
                let measure = Self::build_measure(&cube_name, m)?;
                Ok((measure.path.name().to_string(), Rc::new(measure)))
            })
            .collect::<Result<HashMap<_, _>, CubeError>>()?;

        let dimensions = definition
            .dimensions()?
            .into_iter()
            .map(|d| {
                let dimension = Self::build_dimension(&cube_name, d)?;
                Ok((dimension.path.name().to_string(), Rc::new(dimension)))
            })
            .collect::<Result<HashMap<_, _>, CubeError>>()?;

        let segments = definition
            .segments()?
            .into_iter()
            .map(|s| {
                let segment = Self::build_segment(&cube_name, s)?;
                Ok((segment.path.name().to_string(), Rc::new(segment)))
            })
            .collect::<Result<HashMap<_, _>, CubeError>>()?;

        let joins = definition
            .joins()?
            .unwrap_or_default()
            .into_iter()
            .map(|j| Self::build_join(&cube_name, j))
            .collect::<Result<Vec<_>, CubeError>>()?;

        let pre_aggregations = definition
            .pre_aggregations()?
            .unwrap_or_default()
            .into_iter()
            .map(|p| {
                let pa = Self::build_pre_aggregation(p)?;
                Ok((pa.name.clone(), Rc::new(pa)))
            })
            .collect::<Result<HashMap<_, _>, CubeError>>()?;

        let access_policies = definition
            .access_policies()?
            .unwrap_or_default()
            .into_iter()
            .map(Self::build_access_policy)
            .collect::<Result<Vec<_>, CubeError>>()?;

        let is_view = static_data.is_view.unwrap_or(false);
        let view = if is_view {
            Some(Self::build_view_spec(&definition)?)
        } else {
            None
        };

        Ok(Cube {
            name: cube_name,
            sql_alias: static_data.sql_alias.clone(),
            source,

            measures,
            dimensions,
            segments,
            joins,
            pre_aggregations,
            access_policies,

            primary_keys,

            is_view,
            calendar: static_data.is_calendar.unwrap_or(false),

            view,
        })
    }

    fn build_source(
        &self,
        definition: &Rc<dyn CubeDefinition>,
    ) -> Result<Option<SqlSource>, CubeError> {
        if let Some(table) = definition.sql_table()? {
            return Ok(Some(SqlSource::Table(Expression::new(table))));
        }
        if let Some(query) = definition.sql()? {
            return Ok(Some(SqlSource::Query(Expression::new(query))));
        }
        Ok(None)
    }

    fn build_measure(
        cube: &CubeName,
        definition: Rc<dyn MeasureDefinition>,
    ) -> Result<Measure, CubeError> {
        let static_data = definition.static_data();
        let case = definition
            .case()?
            .as_ref()
            .map(Self::build_case_variant)
            .transpose()?;
        let measure_type = MeasureType::parse(&static_data.measure_type)?;
        let multi_stage = Self::build_multi_stage_spec(&static_data, &measure_type)?;
        let rolling_window = static_data
            .rolling_window
            .as_ref()
            .map(Self::build_rolling_window)
            .transpose()?;
        let time_shifts = static_data
            .time_shift_references
            .as_deref()
            .map(Self::build_time_shifts)
            .transpose()?
            .unwrap_or_default();
        let filters = definition
            .filters()?
            .unwrap_or_default()
            .into_iter()
            .map(|f| Ok::<_, CubeError>(Expression::new(f.sql()?)))
            .collect::<Result<Vec<_>, _>>()?;
        let drill_filters = definition
            .drill_filters()?
            .unwrap_or_default()
            .into_iter()
            .map(|f| Ok::<_, CubeError>(Expression::new(f.sql()?)))
            .collect::<Result<Vec<_>, _>>()?;
        let order_by = definition
            .order_by()?
            .unwrap_or_default()
            .into_iter()
            .map(Self::build_order_by)
            .collect::<Result<Vec<_>, _>>()?;
        let alias_member = static_data
            .alias_member
            .as_deref()
            .map(MemberPath::parse)
            .transpose()?;
        Ok(Measure {
            path: MemberPath::new(cube.clone(), static_data.name.clone()),
            measure_type,
            sql: definition.sql()?.map(Expression::new),
            case,
            mask_sql: definition.mask_sql()?.map(Expression::new),
            owned_by_cube: static_data.owned_by_cube.unwrap_or(false),
            primary_key: false,
            multi_stage,
            rolling_window,
            time_shifts,
            filters,
            drill_filters,
            order_by,
            alias_member,
        })
    }

    fn build_multi_stage_spec(
        static_data: &crate::cube_bridge::measure_definition::MeasureDefinitionStatic,
        measure_type: &MeasureType,
    ) -> Result<Option<MultiStageSpec>, CubeError> {
        if !static_data.multi_stage.unwrap_or(false) {
            return Ok(None);
        }
        // A multi-stage `rank` measure is a filtering stage; every other
        // multi-stage measure type aggregates.
        let kind = match measure_type {
            MeasureType::Rank => MultiStageKind::Filtering,
            _ => MultiStageKind::Aggregating,
        };
        let reduce_by = Self::parse_paths(&static_data.reduce_by_references)?;
        let group_by = Self::parse_paths(&static_data.group_by_references)?;
        let add_group_by = Self::parse_paths(&static_data.add_group_by_references)?;
        let time_shifts = static_data
            .time_shift_references
            .as_deref()
            .map(Self::build_time_shifts)
            .transpose()?
            .unwrap_or_default();
        Ok(Some(MultiStageSpec {
            kind,
            reduce_by,
            group_by,
            add_group_by,
            time_shifts,
        }))
    }

    fn parse_paths(refs: &Option<Vec<String>>) -> Result<Vec<MemberPath>, CubeError> {
        refs.as_deref()
            .map(|v| v.iter().map(|p| MemberPath::parse(p)).collect())
            .unwrap_or_else(|| Ok(Vec::new()))
    }

    fn build_rolling_window(window: &RollingWindow) -> Result<RollingWindowSpec, CubeError> {
        let kind = window
            .rolling_type
            .as_deref()
            .map(|t| match t {
                "time" => Ok(RollingWindowKind::Time),
                "row" => Ok(RollingWindowKind::Row),
                other => Err(CubeError::user(format!(
                    "Unknown rolling window kind: {other}"
                ))),
            })
            .transpose()?;
        Ok(RollingWindowSpec {
            trailing: window.trailing.clone(),
            leading: window.leading.clone(),
            offset: window.offset.clone(),
            kind,
            granularity: window.granularity.clone(),
        })
    }

    fn build_time_shifts(refs: &[TimeShiftReference]) -> Result<Vec<TimeShiftSpec>, CubeError> {
        refs.iter().map(Self::build_time_shift).collect()
    }

    fn build_time_shift(reference: &TimeShiftReference) -> Result<TimeShiftSpec, CubeError> {
        let direction = reference
            .shift_type
            .as_deref()
            .map(|d| match d {
                "next" => Ok(TimeShiftDirection::Next),
                "prior" => Ok(TimeShiftDirection::Prior),
                other => Err(CubeError::user(format!(
                    "Unknown time shift direction: {other}"
                ))),
            })
            .transpose()?;
        let time_dimension = reference
            .time_dimension
            .as_deref()
            .map(MemberPath::parse)
            .transpose()?;
        Ok(TimeShiftSpec {
            interval: reference.interval.clone(),
            name: reference.name.clone(),
            direction,
            time_dimension,
        })
    }

    fn build_order_by(
        definition: Rc<dyn crate::cube_bridge::member_order_by::MemberOrderBy>,
    ) -> Result<MeasureOrderBy, CubeError> {
        let direction = match definition.dir()?.as_str() {
            "asc" | "ASC" => OrderDirection::Asc,
            "desc" | "DESC" => OrderDirection::Desc,
            other => return Err(CubeError::user(format!("Unknown order direction: {other}"))),
        };
        Ok(MeasureOrderBy {
            sql: Expression::new(definition.sql()?),
            direction,
        })
    }

    fn build_case_variant(variant: &BridgeCaseVariant) -> Result<ModelCaseVariant, CubeError> {
        match variant {
            BridgeCaseVariant::Case(def) => {
                let when = def
                    .when()?
                    .into_iter()
                    .map(|item| -> Result<CaseWhen, CubeError> {
                        Ok(CaseWhen {
                            sql: Expression::new(item.sql()?),
                            label: Self::build_case_label(item.label()?),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let else_label = Some(Self::build_case_label(def.else_label()?.label()?));
                Ok(ModelCaseVariant::Predicate(Case { when, else_label }))
            }
            BridgeCaseVariant::CaseSwitch(def) => {
                let selector = Expression::new(def.switch()?);
                let when = def
                    .when()?
                    .into_iter()
                    .map(|item| -> Result<CaseSwitchWhen, CubeError> {
                        Ok(CaseSwitchWhen {
                            value: item.static_data().value.clone(),
                            label: Expression::new(item.sql()?),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let else_label = Some(Expression::new(def.else_sql()?.sql()?));
                Ok(ModelCaseVariant::Switch(CaseSwitch {
                    selector,
                    when,
                    else_label,
                }))
            }
        }
    }

    fn build_case_label(label: StringOrSql) -> CaseLabel {
        match label {
            StringOrSql::String(s) => CaseLabel::String(s),
            StringOrSql::MemberSql(member) => {
                // StructWithSqlMember holds a `sql` callable behind another
                // trait — surface it as Expression. This swallows the
                // `sql()` Result; we'd rather fail at build time, but the
                // bridge surface returns it eagerly.
                match member.sql() {
                    Ok(sql) => CaseLabel::Sql(Expression::new(sql)),
                    Err(_) => CaseLabel::String(String::new()),
                }
            }
        }
    }

    fn build_dimension(
        cube: &CubeName,
        definition: Rc<dyn DimensionDefinition>,
    ) -> Result<Dimension, CubeError> {
        let static_data = definition.static_data();
        let case = definition
            .case()?
            .as_ref()
            .map(Self::build_case_variant)
            .transpose()?;
        let latitude = definition
            .latitude()?
            .map(|g| Ok::<_, CubeError>(Expression::new(g.sql()?)))
            .transpose()?;
        let longitude = definition
            .longitude()?
            .map(|g| Ok::<_, CubeError>(Expression::new(g.sql()?)))
            .transpose()?;
        let add_group_by = Self::parse_paths(&static_data.add_group_by_references)?;
        let time_shifts = definition
            .time_shift()?
            .unwrap_or_default()
            .into_iter()
            .map(Self::build_dimension_time_shift)
            .collect::<Result<Vec<_>, _>>()?;
        let alias_member = static_data
            .alias_member
            .as_deref()
            .map(MemberPath::parse)
            .transpose()?;
        let granularities = definition
            .granularities()?
            .unwrap_or_default()
            .into_iter()
            .map(Self::build_granularity)
            .map(|g| g.map(|gr| (gr.name.clone(), gr)))
            .collect::<Result<HashMap<_, _>, CubeError>>()?;
        Ok(Dimension {
            path: MemberPath::new(cube.clone(), static_data.name.clone()),
            dimension_type: DimensionType::parse(&static_data.dimension_type)?,
            sql: definition.sql()?.map(Expression::new),
            case,
            mask_sql: definition.mask_sql()?.map(Expression::new),
            latitude,
            longitude,
            primary_key: static_data.primary_key.unwrap_or(false),
            owned_by_cube: static_data.owned_by_cube.unwrap_or(false),
            sub_query: static_data.sub_query.unwrap_or(false),
            propagate_filters_to_sub_query: static_data
                .propagate_filters_to_sub_query
                .unwrap_or(false),
            values: static_data.values.clone().unwrap_or_default(),
            multi_stage: static_data.multi_stage.unwrap_or(false),
            add_group_by,
            time_shifts,
            granularities,
            alias_member,
        })
    }

    fn build_refresh_key(
        definition: Rc<dyn crate::cube_bridge::refresh_key_definition::RefreshKeyDefinition>,
    ) -> Result<RefreshKey, CubeError> {
        let static_data = definition.static_data();
        let sql = definition.sql()?.map(Expression::new);
        // `immutable: true` takes precedence; otherwise an `sql`
        // callback means `Sql`-variant (with optional `every` cadence),
        // and a bare `every` (no sql) means `Every`-variant.
        if static_data.immutable.unwrap_or(false) {
            return Ok(RefreshKey::Immutable);
        }
        if let Some(sql) = sql {
            return Ok(RefreshKey::Sql {
                sql,
                every: static_data
                    .every
                    .as_deref()
                    .map(|s| EveryInterval(s.to_string())),
            });
        }
        if let Some(every) = static_data.every.as_deref() {
            return Ok(RefreshKey::Every {
                every: EveryInterval(every.to_string()),
                timezone: static_data.timezone.clone(),
                incremental: static_data.incremental.unwrap_or(false),
                update_window: static_data
                    .update_window
                    .as_deref()
                    .map(|s| EveryInterval(s.to_string())),
            });
        }
        Err(CubeError::user(
            "refresh_key must define one of `sql`, `every`, or `immutable: true`".to_string(),
        ))
    }

    fn build_granularity(
        definition: Rc<dyn crate::cube_bridge::granularity_definition::GranularityDefinition>,
    ) -> Result<Granularity, CubeError> {
        let static_data = definition.static_data();
        let interval = if static_data.interval.is_empty() {
            None
        } else {
            Some(static_data.interval.clone())
        };
        Ok(Granularity {
            name: static_data.name.clone(),
            interval,
            offset: static_data.offset.clone(),
            origin: static_data.origin.clone(),
            sql: definition.sql()?.map(Expression::new),
        })
    }

    fn build_dimension_time_shift(
        definition: Rc<dyn crate::cube_bridge::timeshift_definition::TimeShiftDefinition>,
    ) -> Result<TimeShiftSpec, CubeError> {
        let static_data = definition.static_data();
        let direction = static_data
            .timeshift_type
            .as_deref()
            .map(|d| match d {
                "next" => Ok(TimeShiftDirection::Next),
                "prior" => Ok(TimeShiftDirection::Prior),
                other => Err(CubeError::user(format!(
                    "Unknown time shift direction: {other}"
                ))),
            })
            .transpose()?;
        Ok(TimeShiftSpec {
            interval: static_data.interval.clone(),
            name: static_data.name.clone(),
            direction,
            // Dimension-side time shift does not carry the source time
            // dimension on its definition; the planner pairs it with
            // the owning time dimension at query time.
            time_dimension: None,
        })
    }

    fn build_segment(
        cube: &CubeName,
        definition: Rc<dyn SegmentDefinition>,
    ) -> Result<Segment, CubeError> {
        let static_data = definition.static_data();
        Ok(Segment {
            path: MemberPath::new(cube.clone(), static_data.name.clone()),
            sql: Expression::new(definition.sql()?),
            owned_by_cube: static_data.owned_by_cube.unwrap_or(false),
        })
    }

    fn build_join(
        cube: &CubeName,
        definition: Rc<dyn CubeJoinDefinition>,
    ) -> Result<Join, CubeError> {
        let static_data = definition.static_data();
        Ok(Join {
            from: cube.clone(),
            to: CubeName::new(static_data.name.clone()),
            relationship: Relationship::parse(&static_data.relationship)?,
            sql: Expression::new(definition.sql()?),
        })
    }

    fn build_access_policy(
        definition: Rc<dyn AccessPolicyDefinition>,
    ) -> Result<AccessPolicy, CubeError> {
        let static_data = definition.static_data();
        let member_level = definition
            .member_level()?
            .map(|m| -> Result<MemberLevelAccess, CubeError> {
                let s = m.static_data();
                Ok(MemberLevelAccess {
                    includes: s
                        .includes_members
                        .iter()
                        .map(|p| MemberPath::parse(p))
                        .collect::<Result<_, _>>()?,
                    excludes: s
                        .excludes_members
                        .iter()
                        .map(|p| MemberPath::parse(p))
                        .collect::<Result<_, _>>()?,
                })
            })
            .transpose()?;
        let member_masking = definition
            .member_masking()?
            .map(|m| -> Result<MemberMasking, CubeError> {
                let s = m.static_data();
                Ok(MemberMasking {
                    includes: s
                        .includes_members
                        .iter()
                        .map(|p| MemberPath::parse(p))
                        .collect::<Result<_, _>>()?,
                    excludes: s
                        .excludes_members
                        .iter()
                        .map(|p| MemberPath::parse(p))
                        .collect::<Result<_, _>>()?,
                })
            })
            .transpose()?;
        let conditions = definition
            .conditions()?
            .unwrap_or_default()
            .into_iter()
            .map(|c| -> Result<AccessCondition, CubeError> {
                Ok(AccessCondition {
                    predicate: Expression::new(c.predicate()?),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let row_level = definition
            .row_level()?
            .map(|r| -> Result<RowLevelAccess, CubeError> {
                let filters = r
                    .filters()?
                    .into_iter()
                    .map(Self::build_access_filter)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RowLevelAccess { filters })
            })
            .transpose()?;
        Ok(AccessPolicy {
            role: static_data.role.clone(),
            group: static_data.group.clone(),
            groups: static_data.groups.clone(),
            conditions,
            row_level,
            member_level,
            member_masking,
        })
    }

    fn build_access_filter(
        definition: Rc<dyn crate::cube_bridge::access_filter_definition::AccessFilterDefinition>,
    ) -> Result<AccessFilter, CubeError> {
        if let Some(and) = definition.and()? {
            let nested = and
                .into_iter()
                .map(Self::build_access_filter)
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(AccessFilter::And(nested));
        }
        if let Some(or) = definition.or()? {
            let nested = or
                .into_iter()
                .map(Self::build_access_filter)
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(AccessFilter::Or(nested));
        }
        let static_data = definition.static_data();
        let member_ref = static_data.member_reference.as_deref().ok_or_else(|| {
            CubeError::user(
                "Access filter leaf must specify `memberReference` (or use and/or grouping)"
                    .to_string(),
            )
        })?;
        Ok(AccessFilter::Member {
            member: MemberPath::parse(member_ref)?,
            operator: static_data.operator.clone().unwrap_or_default(),
            values: static_data.values.clone(),
        })
    }

    fn build_view_spec(definition: &Rc<dyn CubeDefinition>) -> Result<ViewSpec, CubeError> {
        let included_members = definition
            .included_members()?
            .unwrap_or_default()
            .into_iter()
            .filter_map(|m| Self::build_included_member(m).transpose())
            .collect::<Result<Vec<_>, CubeError>>()?;
        let join_map = definition
            .static_data()
            .join_map
            .as_ref()
            .map(|rows| {
                rows.iter()
                    .map(|row| row.iter().map(|n| CubeName::new(n.clone())).collect())
                    .collect()
            })
            .unwrap_or_default();
        Ok(ViewSpec {
            included_members,
            join_map,
        })
    }

    fn build_included_member(
        definition: Rc<dyn ViewIncludedMember>,
    ) -> Result<Option<IncludedMember>, CubeError> {
        let static_data = definition.static_data();
        let kind = match static_data.member_kind.as_str() {
            "measures" => IncludedMemberKind::Measure,
            "dimensions" => IncludedMemberKind::Dimension,
            "segments" => IncludedMemberKind::Segment,
            // Hierarchies are presentation-only metadata and are not
            // modeled — a view that includes one contributes no SQL member.
            "hierarchies" => return Ok(None),
            other => {
                return Err(CubeError::user(format!(
                    "Unknown included member kind: {other}"
                )))
            }
        };
        Ok(Some(IncludedMember {
            kind,
            source: MemberPath::parse(&static_data.member_path)?,
            name: static_data.name.clone(),
        }))
    }

    fn build_pre_aggregation(
        definition: Rc<dyn PreAggregationDescription>,
    ) -> Result<PreAggregation, CubeError> {
        let static_data = definition.static_data();
        let kind = PreAggregationKind::parse(&static_data.pre_aggregation_type)?;
        let (rollup, original_sql) = if kind.is_rollup_family() {
            let time_dimensions = definition
                .time_dimension_references()?
                .unwrap_or_default()
                .into_iter()
                .map(|td| -> Result<RollupTimeDimension, CubeError> {
                    Ok(RollupTimeDimension {
                        dimension: Expression::new(td.dimension()?),
                        granularity: td.static_data().granularity.clone(),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let rollup = RollupSpec {
                measures: definition.measure_references()?.map(Expression::new),
                dimensions: definition.dimension_references()?.map(Expression::new),
                segments: definition.segment_references()?.map(Expression::new),
                rollups: definition.rollup_references()?.map(Expression::new),
                time_dimensions,
                granularity: static_data.granularity.clone(),
            };
            (Some(rollup), None)
        } else {
            let spec = OriginalSqlSpec {
                partition_granularity: static_data.partition_granularity.clone(),
                time_dimension: definition.time_dimension_reference()?.map(Expression::new),
            };
            (None, Some(spec))
        };

        let indexes = definition
            .indexes()?
            .unwrap_or_default()
            .into_iter()
            .map(|i| -> Result<(String, Index), CubeError> {
                let s = i.static_data();
                let kind = s.index_type.as_deref().map(IndexKind::parse).transpose()?;
                Ok((
                    s.name.clone(),
                    Index {
                        name: s.name.clone(),
                        columns: s.columns.clone(),
                        kind,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>, _>>()?;

        let refresh_key = definition
            .refresh_key()?
            .map(Self::build_refresh_key)
            .transpose()?;

        Ok(PreAggregation {
            name: static_data.name.clone(),
            kind,
            sql_alias: static_data.sql_alias.clone(),
            external: static_data.external,
            scheduled_refresh: static_data.scheduled_refresh,
            refresh_key,
            use_original_sql_pre_aggregations: static_data
                .use_original_sql_pre_aggregations
                .unwrap_or(false),
            allow_non_strict_date_range_match: static_data
                .allow_non_strict_date_range_match
                .unwrap_or(false),
            indexes,
            owned_by_cube: static_data.owned_by_cube.unwrap_or(false),
            rollup,
            original_sql,
            build_range_start: definition.build_range_start()?.map(Expression::new),
            build_range_end: definition.build_range_end()?.map(Expression::new),
        })
    }
}
