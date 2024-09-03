use super::base_join_condition::DimensionJoinCondition;
use super::query_tools::QueryTools;
use super::sql_evaluator::multiplied_measures_collector::collect_multiplied_measures;
use super::sql_evaluator::render_references::RenderReferencesVisitor;
use super::sql_evaluator::{Compiler, EvaluationNode};
use super::IndexedMember;
use super::{
    BaseCube, BaseDimension, BaseJoinCondition, BaseMeasure, BaseTimeDimension, Context,
    PrimaryJoinCondition, SqlJoinCondition,
};
use super::{BaseMember, BaseQuery};
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::plan::{
    Expr, Filter, FilterItem, From, FromSource, GenerationPlan, Join, JoinItem, OrderBy, Select,
};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateQueryBuilder {
    query_tools: Rc<QueryTools>,
    measures: Vec<Rc<BaseMeasure>>,
    dimensions: Vec<Rc<BaseDimension>>,
    time_dimensions: Vec<Rc<BaseTimeDimension>>,
}

impl FullKeyAggregateQueryBuilder {
    pub fn new(
        query_tools: Rc<QueryTools>,
        measures: Vec<Rc<BaseMeasure>>,
        dimensions: Vec<Rc<BaseDimension>>,
        time_dimensions: Vec<Rc<BaseTimeDimension>>,
    ) -> Self {
        Self {
            query_tools,
            measures,
            dimensions,
            time_dimensions,
        }
    }

    pub fn build(self) -> Result<Option<Select>, CubeError> {
        let measures = self.full_key_aggregate_measures()?;
        if measures.multiplied_measures.is_empty() {
            return Ok(None);
        }
        let mut joins = Vec::new();
        if !measures.regular_measures.is_empty() {
            let regular_subquery = self.regular_measures_subquery(&measures.regular_measures)?;
            joins.push(regular_subquery);
        }

        for (cube_name, measures) in measures
            .multiplied_measures
            .clone()
            .into_iter()
            .into_group_map_by(|m| m.cube_name().clone())
        {
            let aggregate_subquery = self.aggregate_subquery(&cube_name, &measures)?;
            joins.push(aggregate_subquery);
        }

        let inner_measures = measures
            .multiplied_measures
            .iter()
            .chain(measures.regular_measures.iter())
            .cloned()
            .collect_vec();
        let aggregate =
            self.outer_measures_join_full_key_aggregate(&inner_measures, &self.measures, joins)?;
        Ok(Some(aggregate))
    }

    fn outer_measures_join_full_key_aggregate(
        &self,
        inner_measures: &Vec<Rc<BaseMeasure>>,
        outer_measures: &Vec<Rc<BaseMeasure>>,
        joins: Vec<Rc<Select>>,
    ) -> Result<Select, CubeError> {
        let root = From::new(FromSource::Subquery(joins[0].clone(), format!("q_0")));
        let mut join_items = vec![];
        let columns_to_select = self.dimensions_for_select();
        for (i, join) in joins.iter().skip(1).enumerate() {
            let left_alias = format!("q_{}", i);
            let right_alias = format!("q_{}", i + 1);
            let from = From::new(FromSource::Subquery(
                join.clone(),
                self.query_tools.escape_column_name(&format!("q_{}", i + 1)),
            ));
            let join_item = JoinItem {
                from,
                on: DimensionJoinCondition::try_new(
                    left_alias,
                    right_alias,
                    columns_to_select.clone(),
                )?,
                is_inner: true,
            };
            join_items.push(join_item);
        }

        let references = inner_measures
            .iter()
            .map(|m| Ok((m.measure().clone(), m.alias_name()?)))
            .collect::<Result<HashMap<_, _>, CubeError>>()?;

        let context = Context::new(None, vec![RenderReferencesVisitor::new(references)], vec![]);

        let select = Select {
            projection: self.dimensions_references_and_measures("q0", outer_measures)?,
            from: From::new(FromSource::Join(Rc::new(Join {
                root,
                joins: join_items,
            }))),
            filter: None,
            group_by: vec![],
            having: None,
            order_by: self.default_order(),
            context,
            is_distinct: false,
        };
        Ok(select)
    }

    fn full_key_aggregate_measures(&self) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        for m in self.measures.iter() {
            if let Some(multiple) =
                collect_multiplied_measures(self.query_tools.clone(), m.member_evaluator())?
            {
                if multiple.multiplied {
                    result.multiplied_measures.push(m.clone());
                } else {
                    result.regular_measures.push(m.clone())
                }
            } else {
                result.regular_measures.push(m.clone())
            }
        }
        Ok(result)
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self.make_join_node()?;
        let select = Select {
            projection: self.select_all_dimensions_and_measures(measures)?,
            from: source,
            filter: None,
            group_by: self.group_by(),
            having: None,
            order_by: vec![],
            context: Context::new_with_cube_alias_prefix("main".to_string()),
            is_distinct: false,
        };
        Ok(Rc::new(select))
    }

    fn aggregate_subquery(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Rc<Select>, CubeError> {
        let primary_keys_dimensions = self.primary_keys_dimensions(key_cube_name)?;
        let keys_query = self.key_query(&primary_keys_dimensions, key_cube_name)?;

        let pk_cube = From::new(FromSource::Cube(
            self.cube_from_path(key_cube_name.clone())?,
        ));
        let mut joins = vec![];
        joins.push(JoinItem {
            from: pk_cube,
            on: PrimaryJoinCondition::try_new(
                key_cube_name.clone(),
                self.query_tools.clone(),
                primary_keys_dimensions,
            )?,
            is_inner: false,
        });
        let join = Rc::new(Join {
            root: From::new(FromSource::Subquery(
                keys_query,
                self.query_tools.escape_column_name("keys"),
            )), //FIXME replace with constant
            joins,
        });
        let select = Select {
            projection: self.dimensions_references_and_measures(
                &self.query_tools.escape_column_name("keys"),
                &measures,
            )?,
            from: From::new(FromSource::Join(join)),
            filter: None,
            group_by: self.group_by(),
            having: None,
            order_by: vec![],
            context: Context::new_with_cube_alias_prefix(format!("{}_key", key_cube_name)),
            is_distinct: false,
        };
        Ok(Rc::new(select))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<BaseDimension>>,
        key_cube_name: &String,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self.make_join_node()?;
        let dimensions = self
            .dimensions
            .iter()
            .chain(dimensions.iter())
            .unique_by(|d| d.dimension())
            .cloned()
            .collect_vec();
        let select = Select {
            projection: dimensions.iter().map(|d| Expr::Field(d.clone())).collect(),
            from: source,
            filter: None,
            group_by: vec![],
            having: None,
            order_by: vec![],
            context: Context::new_with_cube_alias_prefix(format!("{}_key", key_cube_name)),
            is_distinct: true,
        };
        Ok(Rc::new(select))
    }

    fn make_join_node(&self /*TODO dimensions for subqueries*/) -> Result<From, CubeError> {
        let join = self.query_tools.cached_data().join()?.clone();
        let root = From::new(FromSource::Cube(
            self.cube_from_path(join.static_data().root.clone())?,
        ));
        let joins = join.joins()?;
        if joins.items().is_empty() {
            Ok(root)
        } else {
            let join_items = joins
                .items()
                .iter()
                .map(|join| {
                    let definition = join.join()?;
                    let evaluator = self.compile_join_condition(
                        &join.static_data().original_from,
                        definition.sql()?,
                    )?;
                    Ok(JoinItem {
                        from: From::new(FromSource::Cube(
                            self.cube_from_path(join.static_data().original_to.clone())?,
                        )),
                        on: SqlJoinCondition::try_new(
                            join.static_data().original_from.clone(),
                            self.query_tools.clone(),
                            evaluator,
                        )?,
                        is_inner: false,
                    })
                })
                .collect::<Result<Vec<_>, CubeError>>()?;
            let result = From::new(FromSource::Join(Rc::new(Join {
                root,
                joins: join_items,
            })));
            Ok(result)
        }
    }

    fn select_all_dimensions_and_measures(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Vec<Expr>, CubeError> {
        let measures = measures.iter().map(|m| Expr::Field(m.clone()));
        let time_dimensions = self.time_dimensions.iter().map(|d| Expr::Field(d.clone()));
        let dimensions = self.dimensions.iter().map(|d| Expr::Field(d.clone()));
        Ok(dimensions.chain(time_dimensions).chain(measures).collect())
    }

    fn dimensions_references_and_measures(
        &self,
        cube_name: &str,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Vec<Expr>, CubeError> {
        let dimensions_refs = self
            .dimensions_for_select()
            .into_iter()
            .map(|d| Ok(Expr::Reference(cube_name.to_string(), d.alias_name()?)));
        let measures = measures.iter().map(|m| Ok(Expr::Field(m.clone())));
        dimensions_refs
            .chain(measures)
            .collect::<Result<Vec<_>, _>>()
    }

    fn dimensions_for_select(&self) -> Vec<Rc<dyn IndexedMember>> {
        let time_dimensions = self
            .time_dimensions
            .iter()
            .map(|d| -> Rc<dyn IndexedMember> { d.clone() });
        let dimensions = self
            .dimensions
            .iter()
            .map(|d| -> Rc<dyn IndexedMember> { d.clone() });
        dimensions.chain(time_dimensions).collect()
    }

    fn columns_to_expr(&self, columns: &Vec<Rc<dyn IndexedMember>>) -> Vec<Expr> {
        columns.iter().map(|d| Expr::Field(d.clone())).collect_vec()
    }

    fn cube_from_path(&self, cube_path: String) -> Result<Rc<BaseCube>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let evaluator = evaluator_compiler.add_cube_table_evaluator(cube_path.to_string())?;
        BaseCube::try_new(cube_path.to_string(), self.query_tools.clone(), evaluator)
    }

    fn compile_join_condition(
        &self,
        cube_name: &String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        evaluator_compiler.add_join_condition_evaluator(cube_name.clone(), sql)
    }

    fn group_by(&self) -> Vec<Rc<dyn IndexedMember>> {
        self.dimensions
            .iter()
            .map(|f| -> Rc<dyn IndexedMember> { f.clone() })
            .chain(
                self.time_dimensions
                    .iter()
                    .map(|f| -> Rc<dyn IndexedMember> { f.clone() }),
            )
            .collect()
    }

    fn primary_keys_dimensions(
        &self,
        cube_name: &String,
    ) -> Result<Vec<Rc<BaseDimension>>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        let primary_keys = self
            .query_tools
            .cube_evaluator()
            .static_data()
            .primary_keys
            .get(cube_name)
            .unwrap();

        let dims = primary_keys
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let full_name = format!("{}.{}", cube_name, d);
                let evaluator = evaluator_compiler.add_dimension_evaluator(full_name.clone())?;
                BaseDimension::try_new(full_name, self.query_tools.clone(), evaluator, i)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(dims)
    }

    fn default_order(&self) -> Vec<OrderBy> {
        if let Some(granularity_dim) = self.time_dimensions.iter().find(|d| d.has_granularity()) {
            vec![OrderBy::new(Expr::Field(granularity_dim.clone()), true)]
        } else if !self.measures.is_empty() && !self.dimensions.is_empty() {
            vec![OrderBy::new(Expr::Field(self.measures[0].clone()), false)]
        } else if !self.dimensions.is_empty() {
            vec![OrderBy::new(Expr::Field(self.dimensions[0].clone()), true)]
        } else {
            vec![]
        }
    }
}

#[derive(Default)]
struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<BaseMeasure>>,
    pub regular_measures: Vec<Rc<BaseMeasure>>,
}
