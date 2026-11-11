use super::pretty_print::*;
use super::*;
use crate::planner::{BaseCube, CubeId, InputId, JoinTree, MemberId, MemberSymbol, SqlCall};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// A join against an opaque sub-query from the SQL API `subqueryJoins`. Its `on_sql`
/// references the owning cube and the sub-query `alias` as a literal.
#[derive(Clone)]
pub struct LogicalSubqueryJoinItem {
    pub sql: String,
    pub alias: String,
    pub join_type: String,
    pub on_sql: Rc<SqlCall>,
}

/// A subquery computing dimensions the reader selects, joined to the reader
/// by `keys`. The ref's schema is what the reader may read from it.
#[derive(Clone)]
pub struct DimensionInput {
    pub subquery: Rc<SubqueryRef>,
    pub keys: Vec<Rc<MemberSymbol>>,
}

impl DimensionInput {
    pub fn with_forwarded_input(&self, forwards: &InputForwards) -> Self {
        Self {
            subquery: self.subquery.forwarded(forwards),
            ..self.clone()
        }
    }
}

impl PrettyPrint for DimensionInput {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        result.println(
            &format!(
                "{} {} by {}",
                self.subquery.name(),
                self.subquery.id(),
                print_symbols(&self.keys)
            ),
            state,
        );
    }
}

pub(super) fn forward_dimension_inputs(
    inputs: &[DimensionInput],
    forwards: &InputForwards,
) -> Vec<DimensionInput> {
    inputs
        .iter()
        .map(|input| input.with_forwarded_input(forwards))
        .collect()
}

pub(super) fn pretty_print_dimension_inputs(
    inputs: &[DimensionInput],
    result: &mut PrettyPrintResult,
    state: &PrettyPrintState,
) {
    if inputs.is_empty() {
        return;
    }
    result.println("dimension_inputs:", state);
    let details_state = state.new_level();
    for input in inputs.iter() {
        input.pretty_print(result, &details_state);
    }
}

/// One aliased item of a FROM.
#[derive(Clone)]
pub enum Relation {
    Cube {
        cube: Rc<BaseCube>,
        alias: String,
    },
    /// A subquery of the plan, rendered under its name.
    Subquery(Rc<SubqueryRef>),
    PreAggregation(Rc<PreAggregation>),
    RawSql {
        sql: String,
        alias: String,
        join_type: String,
    },
}

#[derive(Clone)]
pub enum JoinOn {
    Sql(Rc<SqlCall>),
    /// The same members on both sides; a null-safe join also matches missing keys.
    Keys {
        keys: Vec<Rc<MemberSymbol>>,
        null_safe: bool,
    },
}

/// A relation left-joined to the ones before it; a raw SQL join keeps its own type.
#[derive(Clone)]
pub struct FromJoin {
    relation: Relation,
    on: JoinOn,
}

impl FromJoin {
    pub fn relation(&self) -> &Relation {
        &self.relation
    }

    pub fn on(&self) -> &JoinOn {
        &self.on
    }
}

#[derive(Clone)]
pub enum FromShape {
    Empty,
    Chain {
        root: Relation,
        joins: Vec<FromJoin>,
    },
    /// Subqueries merged by their keys; how is up to the physical plan.
    Merge(Rc<Merge>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Provider {
    /// A cube of the chain, for a member it joins another relation by.
    Cube {
        alias: String,
    },
    Input(InputId),
    MergeKey(Vec<InputId>),
    PreAggregation,
    CalcGroup,
}

#[derive(Clone)]
pub struct FromColumn {
    pub member: Rc<MemberSymbol>,
    pub provider: Provider,
}

/// The members a FROM provides as columns. Members of its cubes are not
/// columns: they are rendered as expressions over the cube tables.
#[derive(Clone, Default)]
pub struct FromSchema {
    columns: Vec<FromColumn>,
    by_member: HashMap<MemberId, usize>,
}

impl FromSchema {
    pub fn columns(&self) -> &[FromColumn] {
        &self.columns
    }

    pub fn column(&self, member: &Rc<MemberSymbol>) -> Option<&FromColumn> {
        self.by_member
            .get(&chain_id(member))
            .map(|position| &self.columns[*position])
    }

    fn add(&mut self, member: &Rc<MemberSymbol>, provider: Provider) -> Result<(), CubeError> {
        let id = chain_id(member);
        if let Some(position) = self.by_member.get(&id) {
            let existing = &self.columns[*position].provider;
            if *existing == provider {
                return Ok(());
            }
            return Err(CubeError::internal(format!(
                "{} is provided both by {:?} and by {:?}",
                member.full_name(),
                existing,
                provider
            )));
        }
        self.by_member.insert(id, self.columns.len());
        self.columns.push(FromColumn {
            member: member.clone(),
            provider,
        });
        Ok(())
    }
}

/// A calc group dimension the FROM enumerates: its rows are repeated for
/// each value, and a single value is pinned.
// FIXME: with no values left by the filters it still claims a column, which
// the select renders nothing for.
#[derive(Clone)]
pub struct CalcGroupValues {
    pub dimension: Rc<MemberSymbol>,
    pub values: Vec<String>,
}

fn chain_id(member: &Rc<MemberSymbol>) -> MemberId {
    member.clone().resolve_reference_chain().id().clone()
}

/// The rows a query reads: relations composed by a chain of joins or by a
/// merge, and the columns they provide.
#[derive(Clone)]
pub struct LogicalFrom {
    shape: FromShape,
    calc_groups: Vec<CalcGroupValues>,
    schema: FromSchema,
}

impl LogicalFrom {
    pub fn empty() -> Rc<Self> {
        Rc::new(Self {
            shape: FromShape::Empty,
            calc_groups: vec![],
            schema: FromSchema::default(),
        })
    }

    /// An empty merge reads nothing.
    pub fn merge(merge: Rc<Merge>) -> Result<Rc<Self>, CubeError> {
        if merge.is_empty() {
            return Ok(Self::empty());
        }
        Self::new(FromShape::Merge(merge), vec![])
    }

    pub fn pre_aggregation(pre_aggregation: Rc<PreAggregation>) -> Result<Rc<Self>, CubeError> {
        Self::new(
            FromShape::Chain {
                root: Relation::PreAggregation(pre_aggregation),
                joins: vec![],
            },
            vec![],
        )
    }

    /// The same relations enumerating `calc_groups`.
    pub(super) fn with_calc_groups(
        self: &Rc<Self>,
        calc_groups: Vec<CalcGroupValues>,
    ) -> Result<Rc<Self>, CubeError> {
        let same = |a: &CalcGroupValues, b: &CalcGroupValues| {
            chain_id(&a.dimension) == chain_id(&b.dimension) && a.values == b.values
        };
        if calc_groups.len() == self.calc_groups.len()
            && calc_groups
                .iter()
                .zip(self.calc_groups.iter())
                .all(|(a, b)| same(a, b))
        {
            return Ok(self.clone());
        }
        Self::new(self.shape.clone(), calc_groups)
    }

    /// The cubes of `join`, with the inputs added to the builder.
    pub fn over_join(join: Option<Rc<JoinTree>>) -> LogicalFromBuilder {
        LogicalFromBuilder {
            join,
            alias_prefix: None,
            dimension_inputs: vec![],
            grid_inputs: vec![],
            subquery_joins: vec![],
        }
    }

    fn new(shape: FromShape, calc_groups: Vec<CalcGroupValues>) -> Result<Rc<Self>, CubeError> {
        if let FromShape::Chain { root, joins } = &shape {
            let inputs = std::iter::once(root)
                .chain(joins.iter().map(|join| &join.relation))
                .filter_map(|relation| match relation {
                    Relation::Subquery(input) => Some(input.id()),
                    _ => None,
                })
                .collect_vec();
            if inputs.iter().unique().count() != inputs.len() {
                return Err(CubeError::internal(
                    "A FROM joins the same subquery twice".to_string(),
                ));
            }
        }
        let schema = Self::build_schema(&shape, &calc_groups)?;
        Ok(Rc::new(Self {
            shape,
            calc_groups,
            schema,
        }))
    }

    fn build_schema(
        shape: &FromShape,
        calc_groups: &[CalcGroupValues],
    ) -> Result<FromSchema, CubeError> {
        let mut schema = FromSchema::default();
        // First, so a join key on a calc group reads its enumerated column.
        for calc_group in calc_groups.iter() {
            schema.add(&calc_group.dimension, Provider::CalcGroup)?;
        }
        // FIXME: a subquery providing a calc group the FROM enumerates again is
        // read through the enumeration, not filtered or grouped by its own column.
        let enumerated = calc_groups
            .iter()
            .map(|calc_group| chain_id(&calc_group.dimension))
            .collect::<HashSet<_>>();
        match shape {
            FromShape::Empty => {}
            FromShape::Chain { root, joins } => {
                let relations = std::iter::once((root, None))
                    .chain(joins.iter().map(|join| (&join.relation, Some(&join.on))));
                let mut cubes: Vec<(CubeId, &String)> = vec![];
                for (relation, on) in relations {
                    // A join key is one column on both sides of the join: the
                    // relation joined first provides it.
                    let keys = match on {
                        Some(JoinOn::Keys { keys, .. }) => keys.as_slice(),
                        _ => &[],
                    };
                    for key in keys.iter() {
                        if schema.column(key).is_some() {
                            continue;
                        }
                        let owner = key.clone().resolve_reference_chain().cube_id();
                        if let Some((_, alias)) = cubes.iter().find(|(cube, _)| *cube == owner) {
                            let alias = (*alias).clone();
                            schema.add(key, Provider::Cube { alias })?;
                        }
                    }
                    let is_key = |member: &Rc<MemberSymbol>| {
                        keys.iter().any(|key| chain_id(key) == chain_id(member))
                    };
                    match relation {
                        Relation::Cube { cube, alias } => {
                            cubes.push((cube.cube_id().clone(), alias));
                        }
                        Relation::Subquery(input) => {
                            for member in input.schema().all_members() {
                                if (is_key(member) && schema.column(member).is_some())
                                    || enumerated.contains(&chain_id(member))
                                {
                                    continue;
                                }
                                schema.add(member, Provider::Input(input.id()))?;
                            }
                        }
                        Relation::PreAggregation(pre_aggregation) => {
                            for member in pre_aggregation.schema().all_members() {
                                schema.add(member, Provider::PreAggregation)?;
                            }
                        }
                        Relation::RawSql { .. } => {}
                    }
                }
            }
            FromShape::Merge(merge) => {
                let legs = merge
                    .subqueries()
                    .iter()
                    .map(|leg| leg.id())
                    .unique()
                    .collect_vec();
                for key in merge.schema().all_dimensions() {
                    schema.add(key, Provider::MergeKey(legs.clone()))?;
                }
                for leg in merge.subqueries().iter() {
                    for member in leg.schema().all_members() {
                        if merge.schema().has_dimension(member) {
                            continue;
                        }
                        schema.add(member, Provider::Input(leg.id()))?;
                    }
                }
            }
        }
        Ok(schema)
    }

    pub fn shape(&self) -> &FromShape {
        &self.shape
    }

    pub fn schema(&self) -> &FromSchema {
        &self.schema
    }

    pub fn calc_groups(&self) -> &[CalcGroupValues] {
        &self.calc_groups
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.shape, FromShape::Empty)
    }

    pub fn as_merge(&self) -> Option<&Rc<Merge>> {
        match &self.shape {
            FromShape::Merge(merge) => Some(merge),
            _ => None,
        }
    }

    pub fn root(&self) -> Option<&Relation> {
        match &self.shape {
            FromShape::Chain { root, .. } => Some(root),
            _ => None,
        }
    }

    pub fn joins(&self) -> &[FromJoin] {
        match &self.shape {
            FromShape::Chain { joins, .. } => joins,
            _ => &[],
        }
    }

    pub fn as_pre_aggregation(&self) -> Option<&Rc<PreAggregation>> {
        match self.root() {
            Some(Relation::PreAggregation(pre_aggregation)) => Some(pre_aggregation),
            _ => None,
        }
    }

    pub fn cube_ids(&self) -> Vec<CubeId> {
        self.relations()
            .filter_map(|relation| match relation {
                Relation::Cube { cube, .. } => Some(cube.cube_id().clone()),
                _ => None,
            })
            .collect()
    }

    /// The subquery `id` among the relations of the chain.
    pub fn input(&self, id: InputId) -> Option<&Rc<SubqueryRef>> {
        self.relations().find_map(|relation| match relation {
            Relation::Subquery(input) if input.id() == id => Some(input),
            _ => None,
        })
    }

    fn relations(&self) -> impl Iterator<Item = &Relation> {
        self.root()
            .into_iter()
            .chain(self.joins().iter().map(|join| &join.relation))
    }
}

impl LogicalNode for LogicalFrom {
    fn as_plan_node(self: &Rc<Self>) -> PlanNode {
        PlanNode::LogicalFrom(self.clone())
    }

    fn inputs(&self) -> Vec<PlanNode> {
        match &self.shape {
            FromShape::Merge(merge) => vec![merge.as_plan_node()],
            FromShape::Chain {
                root: Relation::PreAggregation(pre_aggregation),
                ..
            } => vec![pre_aggregation.as_plan_node()],
            FromShape::Chain { .. } | FromShape::Empty => vec![],
        }
    }

    fn with_inputs(self: Rc<Self>, inputs: Vec<PlanNode>) -> Result<Rc<Self>, CubeError> {
        check_inputs_len(&inputs, self.inputs().len(), self.node_name())?;
        let Some(input) = inputs.into_iter().next() else {
            return Ok(self);
        };
        let shape = match &self.shape {
            FromShape::Merge(_) => {
                return Self::merge(input.into_logical_node()?);
            }
            FromShape::Chain { joins, .. } => FromShape::Chain {
                root: Relation::PreAggregation(input.into_logical_node()?),
                joins: joins.clone(),
            },
            FromShape::Empty => FromShape::Empty,
        };
        // The query reading it resolves the calc groups over the new input.
        Self::new(shape, vec![])
    }

    fn referenced_inputs(&self) -> Vec<InputId> {
        self.relations()
            .filter_map(|relation| match relation {
                Relation::Subquery(input) => Some(input.id()),
                _ => None,
            })
            .collect()
    }

    fn with_forwarded_input(
        self: Rc<Self>,
        forwards: &InputForwards,
    ) -> Result<Rc<Self>, CubeError> {
        let FromShape::Chain { root, joins } = &self.shape else {
            return Ok(self);
        };
        if !forwards.replaces_any(&self.referenced_inputs()) {
            return Ok(self);
        }
        let forward = |relation: &Relation| match relation {
            Relation::Subquery(input) => Relation::Subquery(forwards.collapsed(input)),
            other => other.clone(),
        };
        Self::new(
            FromShape::Chain {
                root: forward(root),
                joins: joins
                    .iter()
                    .map(|join| FromJoin {
                        relation: forward(&join.relation),
                        on: join.on.clone(),
                    })
                    .collect(),
            },
            self.calc_groups.clone(),
        )
    }

    fn node_name(&self) -> &'static str {
        "LogicalFrom"
    }

    fn try_from_plan_node(plan_node: PlanNode) -> Result<Rc<Self>, CubeError> {
        if let PlanNode::LogicalFrom(item) = plan_node {
            Ok(item)
        } else {
            Err(cast_error(&plan_node, "LogicalFrom"))
        }
    }
}

/// Builds the FROM of a query over cubes. The cubes come from a join tree
/// only, so the relations of a FROM always match the edges of its tree.
pub struct LogicalFromBuilder {
    join: Option<Rc<JoinTree>>,
    alias_prefix: Option<String>,
    dimension_inputs: Vec<DimensionInput>,
    grid_inputs: Vec<DimensionInput>,
    subquery_joins: Vec<LogicalSubqueryJoinItem>,
}

impl LogicalFromBuilder {
    /// Prefix for the aliases of the cubes, so the same cubes can be read
    /// again by the select reading this one.
    pub fn alias_prefix(mut self, alias_prefix: String) -> Self {
        self.alias_prefix = Some(alias_prefix);
        self
    }

    /// Subqueries joined by the primary keys of the cube owning their dimensions.
    pub fn dimension_inputs(mut self, inputs: Vec<DimensionInput>) -> Self {
        self.dimension_inputs.extend(inputs);
        self
    }

    /// The multi-stage dimension subquery, joined null-safe after every cube.
    pub fn grid_input(mut self, input: Option<DimensionInput>) -> Self {
        self.grid_inputs.extend(input);
        self
    }

    pub fn subquery_joins(mut self, subquery_joins: Vec<LogicalSubqueryJoinItem>) -> Self {
        self.subquery_joins.extend(subquery_joins);
        self
    }

    /// The FROM and the join tree of its cubes, which the query reading it keeps.
    pub fn build(self) -> Result<(Rc<LogicalFrom>, Option<Rc<JoinTree>>), CubeError> {
        let from = self.build_from()?;
        Ok((from, self.join))
    }

    fn build_from(&self) -> Result<Rc<LogicalFrom>, CubeError> {
        let Some(join) = &self.join else {
            return self.build_without_cubes();
        };
        let cubes = std::iter::once(join.root())
            .chain(join.joins().iter().map(|item| item.cube()))
            .collect_vec();
        // Each input joins right after the first cube owning its dimensions.
        let mut owned: Vec<Vec<&DimensionInput>> = vec![vec![]; cubes.len()];
        for input in self.dimension_inputs.iter() {
            let owner = cubes.iter().position(|cube| {
                input
                    .subquery
                    .schema()
                    .all_dimensions()
                    .any(|dim| &dim.cube_id() == cube.cube_id())
            });
            let Some(owner) = owner else {
                return Err(CubeError::internal(format!(
                    "Dimension input {} is not owned by a cube of the join",
                    input.subquery.name()
                )));
            };
            owned[owner].push(input);
        }
        let primary_key_joins = |inputs: &Vec<&DimensionInput>| {
            inputs
                .iter()
                .map(|input| FromJoin {
                    relation: Relation::Subquery(input.subquery.clone()),
                    on: JoinOn::Keys {
                        keys: input.keys.clone(),
                        null_safe: false,
                    },
                })
                .collect_vec()
        };

        let root = Relation::Cube {
            cube: join.root().clone(),
            alias: join.root().default_alias_with_prefix(&self.alias_prefix),
        };
        let mut joins = primary_key_joins(&owned[0]);
        for (item, inputs) in join.joins().iter().zip(owned.iter().skip(1)) {
            joins.push(FromJoin {
                relation: Relation::Cube {
                    cube: item.cube().clone(),
                    alias: item.cube().default_alias_with_prefix(&self.alias_prefix),
                },
                on: JoinOn::Sql(item.on_sql().clone()),
            });
            joins.extend(primary_key_joins(inputs));
        }
        for input in self.grid_inputs.iter() {
            joins.push(FromJoin {
                relation: Relation::Subquery(input.subquery.clone()),
                on: JoinOn::Keys {
                    keys: input.keys.clone(),
                    null_safe: true,
                },
            });
        }
        for subquery_join in self.subquery_joins.iter() {
            joins.push(FromJoin {
                relation: Relation::RawSql {
                    sql: subquery_join.sql.clone(),
                    alias: subquery_join.alias.clone(),
                    join_type: subquery_join.join_type.clone(),
                },
                on: JoinOn::Sql(subquery_join.on_sql.clone()),
            });
        }
        LogicalFrom::new(FromShape::Chain { root, joins }, vec![])
    }

    fn build_without_cubes(&self) -> Result<Rc<LogicalFrom>, CubeError> {
        if !self.dimension_inputs.is_empty() {
            return Err(CubeError::internal(
                "A primary-key dimension input needs the cube owning its dimensions".to_string(),
            ));
        }
        if !self.subquery_joins.is_empty() {
            return Err(CubeError::internal(
                "A sub-query join needs a cube to join to".to_string(),
            ));
        }
        match self.grid_inputs.as_slice() {
            [] => Ok(LogicalFrom::empty()),
            [input] => LogicalFrom::new(
                FromShape::Chain {
                    root: Relation::Subquery(input.subquery.clone()),
                    joins: vec![],
                },
                vec![],
            ),
            _ => Err(CubeError::internal(
                "A query without cubes reads at most one dimension input".to_string(),
            )),
        }
    }
}

impl PrettyPrint for Relation {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match self {
            Relation::Cube { cube, alias } => {
                result.println(&format!("cube {} AS {}", cube.cube_id(), alias), state)
            }
            Relation::Subquery(input) => {
                result.println(&format!("subquery {} {}", input.name(), input.id()), state)
            }
            Relation::PreAggregation(pre_aggregation) => {
                pre_aggregation.pretty_print(result, state)
            }
            Relation::RawSql {
                alias, join_type, ..
            } => result.println(&format!("raw sql {} AS {}", join_type, alias), state),
        }
    }
}

impl PrettyPrint for LogicalFrom {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
        match &self.shape {
            FromShape::Empty => result.println("Empty", state),
            FromShape::Merge(merge) => merge.pretty_print(result, state),
            FromShape::Chain { root, joins } => {
                root.pretty_print(result, state);
                for join in joins.iter() {
                    let on = match &join.on {
                        JoinOn::Sql(_) => "sql".to_string(),
                        JoinOn::Keys { keys, null_safe } => format!(
                            "{}keys {}",
                            if *null_safe { "null-safe " } else { "" },
                            print_symbols(keys)
                        ),
                    };
                    result.println(&format!("join on {}:", on), state);
                    join.relation.pretty_print(result, &state.new_level());
                }
            }
        }
        for calc_group in self.calc_groups.iter() {
            let name = calc_group.dimension.full_name();
            let values = calc_group
                .values
                .iter()
                .map(|v| format!("'{}'", v))
                .join(", ");
            match calc_group.values.as_slice() {
                [_] => result.println(&format!("pinned calc group {} = {}", name, values), state),
                _ => result.println(
                    &format!("cross join calc group {}: [{}]", name, values),
                    state,
                ),
            }
        }
    }
}
