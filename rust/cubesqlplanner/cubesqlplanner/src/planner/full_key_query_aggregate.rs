use super::base_join_condition::DimensionJoinCondition;
use super::query_tools::QueryTools;
use super::sql_evaluator::multiplied_measures_collector::collect_multiplied_measures;
use super::sql_evaluator::sql_nodes::with_render_references_default_node_processor;
use super::BaseMember;
use super::QueryRequest;
use super::{BaseDimension, BaseMeasure, PrimaryJoinCondition, VisitorContext};
use crate::plan::{From, FromSource, Join, JoinItem, JoinSource, Select};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_request: Rc<QueryRequest>,
}

impl FullKeyAggregateQueryPlanner {
    pub fn new(query_tools: Rc<QueryTools>, query_request: Rc<QueryRequest>) -> Self {
        Self {
            query_tools,
            query_request,
        }
    }

    pub fn plan(self) -> Result<Option<Select>, CubeError> {
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
        let aggregate = self.outer_measures_join_full_key_aggregate(
            &inner_measures,
            &self.query_request.measures(),
            joins,
        )?;
        Ok(Some(aggregate))
    }

    fn outer_measures_join_full_key_aggregate(
        &self,
        inner_measures: &Vec<Rc<BaseMeasure>>,
        outer_measures: &Vec<Rc<BaseMeasure>>,
        joins: Vec<Rc<Select>>,
    ) -> Result<Select, CubeError> {
        let root = JoinSource::new_from_select(joins[0].clone(), format!("q_0"));
        let mut join_items = vec![];
        let columns_to_select = self.query_request.dimensions_for_select();
        for (i, join) in joins.iter().skip(1).enumerate() {
            let left_alias = format!("q_{}", i);
            let right_alias = format!("q_{}", i + 1);
            let from = JoinSource::new_from_select(
                join.clone(),
                self.query_tools.escape_column_name(&format!("q_{}", i + 1)),
            );
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

        let context = VisitorContext::new(
            None,
            with_render_references_default_node_processor(references),
        );

        let select = Select {
            projection: self
                .query_request
                .dimensions_references_and_measures("q_0", outer_measures)?,
            from: From::new(FromSource::Join(Rc::new(Join {
                root,
                joins: join_items,
            }))),
            filter: None,
            group_by: vec![],
            having: None,
            order_by: self.query_request.default_order(),
            context,
            is_distinct: false,
        };
        Ok(select)
    }

    fn full_key_aggregate_measures(&self) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        for m in self.query_request.measures().iter() {
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
        let source = self.query_request.make_join_node()?;
        let select = Select {
            projection: self
                .query_request
                .select_all_dimensions_and_measures(measures)?,
            from: source,
            filter: self.query_request.all_filters(),
            group_by: self.query_request.group_by(),
            having: None,
            order_by: vec![],
            context: VisitorContext::new_with_cube_alias_prefix("main".to_string()),
            is_distinct: false,
        };
        Ok(Rc::new(select))
    }

    fn aggregate_subquery(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Rc<Select>, CubeError> {
        let primary_keys_dimensions = self.query_request.primary_keys_dimensions(key_cube_name)?;
        let keys_query = self.key_query(&primary_keys_dimensions, key_cube_name)?;

        let pk_cube =
            JoinSource::new_from_cube(self.query_request.cube_from_path(key_cube_name.clone())?);
        let mut joins = vec![];
        joins.push(JoinItem {
            from: pk_cube,
            on: PrimaryJoinCondition::try_new(self.query_tools.clone(), primary_keys_dimensions)?,
            is_inner: false,
        });
        let join = Rc::new(Join {
            root: JoinSource::new_from_select(
                keys_query,
                self.query_tools.escape_column_name("keys"),
            ), //FIXME replace with constant
            joins,
        });
        let select = Select {
            projection: self.query_request.dimensions_references_and_measures(
                &self.query_tools.escape_column_name("keys"),
                &measures,
            )?,
            from: From::new(FromSource::Join(join)),
            filter: None,
            group_by: self.query_request.group_by(),
            having: None,
            order_by: vec![],
            context: VisitorContext::new_with_cube_alias_prefix(format!("{}_key", key_cube_name)),
            is_distinct: false,
        };
        Ok(Rc::new(select))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<BaseDimension>>,
        key_cube_name: &String,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self.query_request.make_join_node()?;
        let dimensions = self.query_request.dimensions_for_select_append(dimensions);

        let select = Select {
            projection: self.query_request.columns_to_expr(&dimensions),
            from: source,
            filter: self.query_request.all_filters(),
            group_by: vec![],
            having: None,
            order_by: vec![],
            context: VisitorContext::new_with_cube_alias_prefix(format!("{}_key", key_cube_name)),
            is_distinct: true,
        };
        Ok(Rc::new(select))
    }
}

#[derive(Default)]
struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<BaseMeasure>>,
    pub regular_measures: Vec<Rc<BaseMeasure>>,
}
