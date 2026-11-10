use super::{
    CalcGroupsJoin, Join, QueryPlan, ReferenceSubstitutions, Schema, Select, VisitorContext,
};
use crate::physical_plan::sql_nodes::{InputSource, InputSources};
use crate::physical_plan::Union;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{BaseCube, InputId};
use cubenativeutils::CubeError;
use std::rc::Rc;

#[derive(Clone)]
pub enum SingleSource {
    /// A derived table, with the columns its readers resolve members against.
    Subquery(Rc<QueryPlan>, Rc<Schema>),
    Cube(Rc<BaseCube>),
    TableReference(String, Rc<Schema>),
    /// An opaque, pre-rendered SQL sub-query (a complete SELECT) emitted
    /// verbatim wrapped in parentheses. Used for SQL-API `subqueryJoins`,
    /// whose body is built outside the planner.
    RawSubquerySql(String),
}

impl SingleSource {
    pub fn new_subquery(plan: Rc<QueryPlan>) -> Self {
        let schema = plan.schema();
        Self::Subquery(plan, schema)
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let sql = match &self {
            SingleSource::Cube(cube) => {
                let cubesql = cube.to_sql(context.clone(), templates)?;
                format!(" {} ", cubesql)
            }
            SingleSource::Subquery(s, _) => format!("({})", s.to_sql(templates)?),
            SingleSource::TableReference(r, _) => format!(" {} ", r),
            SingleSource::RawSubquerySql(sql) => format!("({})", sql),
        };
        Ok(sql)
    }

    pub fn schema(&self) -> Rc<Schema> {
        match self {
            SingleSource::Subquery(_, schema) => schema.clone(),
            SingleSource::Cube(_) => Rc::new(Schema::empty()),
            SingleSource::TableReference(_, schema) => schema.clone(),
            SingleSource::RawSubquerySql(_) => Rc::new(Schema::empty()),
        }
    }
}

#[derive(Clone)]
pub struct SingleAliasedSource {
    pub source: SingleSource,
    pub alias: String,
    pub inputs: SourceInputs,
}

impl SingleAliasedSource {
    pub fn new_from_cube(cube: Rc<BaseCube>, alias: Option<String>) -> Self {
        let alias = alias.unwrap_or_else(|| cube.default_alias());
        Self {
            source: SingleSource::Cube(cube),
            alias,
            inputs: SourceInputs::default(),
        }
    }

    pub fn new_from_source(source: SingleSource, alias: String) -> Self {
        Self {
            source,
            alias,
            inputs: SourceInputs::default(),
        }
    }

    pub fn new_from_table_reference(
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
    ) -> Self {
        let alias = alias.unwrap_or_else(|| PlanSqlTemplates::alias_name(&reference));
        Self {
            source: SingleSource::TableReference(reference, schema),
            alias,
            inputs: SourceInputs::default(),
        }
    }

    pub fn new_from_subquery(plan: Rc<QueryPlan>, alias: String) -> Self {
        Self {
            source: SingleSource::new_subquery(plan),
            alias,
            inputs: SourceInputs::default(),
        }
    }

    pub fn new_from_subselect(plan: Rc<Select>, alias: String) -> Self {
        Self::new_from_subquery(Rc::new(QueryPlan::Select(plan)), alias)
    }

    pub fn with_inputs(self, inputs: SourceInputs) -> Self {
        Self { inputs, ..self }
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let sql = self.source.to_sql(templates, context)?;
        /* if let SingleSource::Subquery(plan, _) = &self.source {
            if let QueryPlan::Union(_) = plan.as_ref() {
                //FIXME CubeStore (at least old cubestore) don't support alias for union
                if templates.is_external() {
                    return Ok(sql);
                }
            }
        } */

        match &self.source {
            // The alias of a SQL-API sub-query join is already a final, quoted
            // identifier and is referenced verbatim in the join ON condition.
            // Re-quoting it would double the quotes and break the reference.
            SingleSource::RawSubquerySql(_) => templates.query_aliased_prequoted(&sql, &self.alias),
            _ => templates.query_aliased(&sql, &self.alias),
        }
    }
}

/// The plan inputs whose references a source of a select answers: members read
/// from `inputs`, and the keys merged from `keys_of`.
#[derive(Clone, Default)]
pub struct SourceInputs {
    pub inputs: Vec<InputId>,
    pub keys_of: Vec<InputId>,
}

impl SourceInputs {
    /// A source holding the plan input `input`.
    pub fn reading(input: InputId) -> Self {
        Self {
            inputs: vec![input],
            keys_of: vec![],
        }
    }

    /// A source holding every one of `inputs` and the keys merged from them.
    pub fn merging(inputs: Vec<InputId>) -> Self {
        Self {
            inputs: inputs.clone(),
            keys_of: inputs,
        }
    }
}

#[derive(Clone)]
pub enum FromSource {
    Empty,
    Single(SingleAliasedSource),
    Join(Rc<Join>),
    CalcGroupsJoin(Rc<CalcGroupsJoin>),
}

#[derive(Clone)]
pub struct From {
    pub source: FromSource,
}

impl From {
    pub fn new(source: FromSource) -> Rc<Self> {
        Rc::new(Self { source })
    }

    /// The same FROM with its join conditions substituted. Sources are left
    /// alone: a subquery is a select of its own.
    pub fn substitute_join_conditions(
        self: &Rc<Self>,
        substitutions: &ReferenceSubstitutions,
    ) -> Result<Rc<Self>, CubeError> {
        if substitutions.is_empty() {
            return Ok(self.clone());
        }
        Ok(match &self.source {
            FromSource::Empty | FromSource::Single(_) => self.clone(),
            FromSource::Join(join) => {
                Self::new(FromSource::Join(Rc::new(join.substitute(substitutions)?)))
            }
            FromSource::CalcGroupsJoin(calc_groups) => {
                Self::new(FromSource::CalcGroupsJoin(CalcGroupsJoin::try_new(
                    calc_groups
                        .from()
                        .substitute_join_conditions(substitutions)?,
                    calc_groups.calc_groups().clone(),
                )?))
            }
        })
    }

    /// The sources of this FROM answering references to plan inputs.
    pub fn input_sources(&self) -> InputSources {
        let mut sources = InputSources::default();
        let mut add = |source: &SingleAliasedSource| {
            let input_source = InputSource {
                alias: source.alias.clone(),
                schema: source.source.schema(),
            };
            for input in source.inputs.inputs.iter() {
                sources
                    .inputs
                    .entry(*input)
                    .or_default()
                    .push(input_source.clone());
            }
            for input in source.inputs.keys_of.iter() {
                sources.keys.insert(*input, input_source.clone());
            }
        };
        match &self.source {
            FromSource::Single(source) => add(source),
            FromSource::Join(join) => {
                add(&join.root);
                for join_item in join.joins.iter() {
                    add(&join_item.from);
                }
            }
            FromSource::Empty => {}
            FromSource::CalcGroupsJoin(calc_groups) => return calc_groups.from().input_sources(),
        }
        sources
    }

    pub fn new_empty() -> Rc<Self> {
        Self::new(FromSource::Empty)
    }

    pub fn new_from_cube(cube: Rc<BaseCube>, alias: Option<String>) -> Rc<Self> {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_cube(
            cube, alias,
        )))
    }

    pub fn new_from_table_reference(
        reference: String,
        schema: Rc<Schema>,
        alias: Option<String>,
    ) -> Rc<Self> {
        Self::new(FromSource::Single(
            SingleAliasedSource::new_from_table_reference(reference, schema, alias),
        ))
    }

    pub fn new_from_join(join: Rc<Join>) -> Rc<Self> {
        Self::new(FromSource::Join(join))
    }

    pub fn new_from_subquery(plan: Rc<QueryPlan>, alias: String) -> Rc<Self> {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_subquery(
            plan, alias,
        )))
    }

    pub fn new_from_union(union: Rc<Union>, alias: String) -> Rc<Self> {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_subquery(
            Rc::new(QueryPlan::Union(union)),
            alias,
        )))
    }

    pub fn new_from_subselect(plan: Rc<Select>, alias: String) -> Rc<Self> {
        Self::new(FromSource::Single(SingleAliasedSource::new_from_subquery(
            Rc::new(QueryPlan::Select(plan)),
            alias,
        )))
    }

    pub fn new_from_calc_groups_join(join: Rc<CalcGroupsJoin>) -> Rc<Self> {
        Self::new(FromSource::CalcGroupsJoin(join))
    }

    pub fn to_sql(
        &self,
        templates: &PlanSqlTemplates,
        context: Rc<VisitorContext>,
    ) -> Result<String, CubeError> {
        let sql = match &self.source {
            FromSource::Empty => format!(""),
            FromSource::Single(source) => source.to_sql(templates, context.clone())?,
            FromSource::Join(j) => j.to_sql(templates, context.clone())?,
            FromSource::CalcGroupsJoin(j) => j.to_sql(templates, context.clone())?,
        };
        Ok(sql)
    }
}
