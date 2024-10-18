use super::{CommonUtils, JoinPlanner, MultiStageQueryPlanner, OrderPlanner, SimpleQueryPlanner};
use crate::plan::{Filter, From, FromSource, Join, JoinItem, JoinSource, Select};
use crate::planner::base_join_condition::DimensionJoinCondition;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::collectors::{
    collect_multiplied_measures, has_multi_stage_members,
};
use crate::planner::sql_evaluator::sql_nodes::SqlNodesFactory;
use crate::planner::BaseMember;
use crate::planner::QueryProperties;
use crate::planner::{BaseDimension, BaseMeasure, PrimaryJoinCondition, VisitorContext};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashMap;
use std::rc::Rc;

pub struct FullKeyAggregateQueryPlanner {
    query_tools: Rc<QueryTools>,
    query_properties: Rc<QueryProperties>,
    join_planner: JoinPlanner,
    order_planner: OrderPlanner,
    multi_stage_planner: MultiStageQueryPlanner,
    common_utils: CommonUtils,
    context_factory: Rc<SqlNodesFactory>,
}

impl FullKeyAggregateQueryPlanner {
    pub fn new(
        query_tools: Rc<QueryTools>,
        query_properties: Rc<QueryProperties>,
        context_factory: Rc<SqlNodesFactory>,
    ) -> Self {
        Self {
            join_planner: JoinPlanner::new(query_tools.clone()),
            order_planner: OrderPlanner::new(query_properties.clone()),
            common_utils: CommonUtils::new(query_tools.clone()),
            multi_stage_planner: MultiStageQueryPlanner::new(
                query_tools.clone(),
                query_properties.clone(),
            ),
            query_tools,
            query_properties,
            context_factory,
        }
    }

    pub fn plan(self) -> Result<Select, CubeError> {
        let measures = self.full_key_aggregate_measures()?;

        if measures.is_simple_query() {
            let simple_query_builder = SimpleQueryPlanner::new(
                self.query_tools.clone(),
                self.query_properties.clone(),
                self.context_factory.clone(),
            );
            return simple_query_builder.plan();
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

        let (cte_queries, cte_aliases) = self.multi_stage_planner.get_cte_queries()?;
        for alias in cte_aliases {
            joins.push(self.multi_stage_planner.cte_select(&alias));
        }

        let inner_measures = measures
            .multiplied_measures
            .iter()
            .chain(measures.multi_stage_measures.iter())
            .chain(measures.regular_measures.iter())
            .cloned()
            .collect_vec();

        let mut aggregate = self.outer_measures_join_full_key_aggregate(
            &inner_measures,
            &self.query_properties.measures(),
            joins,
        )?;

        aggregate.ctes = cte_queries;

        Ok(aggregate)
    }

    fn outer_measures_join_full_key_aggregate(
        &self,
        inner_measures: &Vec<Rc<BaseMeasure>>,
        outer_measures: &Vec<Rc<BaseMeasure>>,
        joins: Vec<Rc<Select>>,
    ) -> Result<Select, CubeError> {
        let root = JoinSource::new_from_select(joins[0].clone(), format!("q_0"));
        let mut join_items = vec![];
        let dimensions_to_select = self
            .query_properties
            .dimensions_for_select()
            .iter()
            .map(|d| d.alias_name())
            .collect_vec();
        let dimensions_to_select = Rc::new(dimensions_to_select);
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
                    dimensions_to_select.clone(),
                )?,
                is_inner: true,
            };
            join_items.push(join_item);
        }

        let references = inner_measures
            .iter()
            .map(|m| Ok((m.measure().clone(), m.alias_name())))
            .collect::<Result<HashMap<_, _>, CubeError>>()?;

        let context = VisitorContext::new(
            None,
            self.context_factory
                .with_render_references_default_node_processor(references),
        );

        let having = if self.query_properties.measures_filters().is_empty() {
            None
        } else {
            Some(Filter {
                items: self.query_properties.measures_filters().clone(),
            })
        };

        let select = Select {
            projection: self
                .query_properties
                .dimensions_references_and_measures("q_0", outer_measures)?,
            from: From::new(FromSource::Join(Rc::new(Join {
                root,
                joins: join_items,
            }))),
            filter: None,
            group_by: vec![],
            having,
            order_by: self.order_planner.default_order(),
            context,
            ctes: vec![],
            is_distinct: false,
            limit: self.query_properties.row_limit(),
            offset: self.query_properties.offset(),
        };
        Ok(select)
    }

    fn full_key_aggregate_measures(&self) -> Result<FullKeyAggregateMeasures, CubeError> {
        let mut result = FullKeyAggregateMeasures::default();
        for m in self.query_properties.measures().iter() {
            if has_multi_stage_members(m.member_evaluator())? {
                result.multi_stage_measures.push(m.clone())
            } else if let Some(multiple) =
                collect_multiplied_measures(self.query_tools.clone(), m.member_evaluator())?
            {
                if multiple.multiplied {
                    result.multiplied_measures.push(m.clone());
                } else {
                    result.regular_measures.push(m.clone());
                }
            } else {
                result.regular_measures.push(m.clone());
            }
        }
        Ok(result)
    }

    fn regular_measures_subquery(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self.join_planner.make_join_node()?;
        let select = Select {
            projection: self
                .query_properties
                .select_all_dimensions_and_measures(measures)?,
            from: source,
            filter: self.query_properties.all_filters(),
            group_by: self.query_properties.group_by(),
            having: None,
            order_by: vec![],
            ctes: vec![],
            context: VisitorContext::new_with_cube_alias_prefix(
                self.context_factory.clone(),
                "main".to_string(),
            ),
            is_distinct: false,
            limit: None,
            offset: None,
        };
        Ok(Rc::new(select))
    }

    fn aggregate_subquery(
        &self,
        key_cube_name: &String,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Rc<Select>, CubeError> {
        let primary_keys_dimensions = self.common_utils.primary_keys_dimensions(key_cube_name)?;
        let keys_query = self.key_query(&primary_keys_dimensions, key_cube_name)?;

        let pk_cube =
            JoinSource::new_from_cube(self.common_utils.cube_from_path(key_cube_name.clone())?);
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
            projection: self.query_properties.dimensions_references_and_measures(
                &self.query_tools.escape_column_name("keys"),
                &measures,
            )?,
            from: From::new(FromSource::Join(join)),
            filter: None,
            group_by: self.query_properties.group_by(),
            having: None,
            order_by: vec![],
            ctes: vec![],
            context: VisitorContext::new_with_cube_alias_prefix(
                self.context_factory.clone(),
                format!("{}_key", key_cube_name),
            ),
            is_distinct: false,
            limit: None,
            offset: None,
        };
        Ok(Rc::new(select))
    }

    fn key_query(
        &self,
        dimensions: &Vec<Rc<BaseDimension>>,
        key_cube_name: &String,
    ) -> Result<Rc<Select>, CubeError> {
        let source = self.join_planner.make_join_node()?;
        let dimensions = self
            .query_properties
            .dimensions_for_select_append(dimensions);

        let select = Select {
            projection: self.query_properties.columns_to_expr(&dimensions),
            from: source,
            filter: self.query_properties.all_filters(),
            group_by: vec![],
            having: None,
            order_by: vec![],
            ctes: vec![],
            context: VisitorContext::new_with_cube_alias_prefix(
                self.context_factory.clone(),
                format!("{}_key", key_cube_name),
            ),
            is_distinct: true,
            limit: None,
            offset: None,
        };
        Ok(Rc::new(select))
    }
}

#[derive(Default)]
struct FullKeyAggregateMeasures {
    pub multiplied_measures: Vec<Rc<BaseMeasure>>,
    pub multi_stage_measures: Vec<Rc<BaseMeasure>>,
    pub regular_measures: Vec<Rc<BaseMeasure>>,
}

impl FullKeyAggregateMeasures {
    pub fn is_simple_query(&self) -> bool {
        self.multi_stage_measures.is_empty() && self.multiplied_measures.is_empty()
    }
}
