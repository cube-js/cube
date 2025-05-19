use crate::logical_plan::*;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::collections::HashSet;
use std::rc::Rc;

pub struct CubeNamesCollector {
    cube_names: HashSet<String>,
}

impl CubeNamesCollector {
    pub fn new() -> Self {
        Self {
            cube_names: HashSet::new(),
        }
    }

    pub fn collect(&mut self, query: &Query) -> Result<(), CubeError> {
        match query {
            Query::SimpleQuery(query) => self.collect_from_simple_query(query),
            Query::FullKeyAggregateQuery(query) => {
                self.collect_from_full_key_aggregate_query(query)
            }
        }
    }

    pub fn result(self) -> Vec<String> {
        self.cube_names.into_iter().collect_vec()
    }

    fn collect_from_simple_query(&mut self, query: &SimpleQuery) -> Result<(), CubeError> {
        self.collect_from_simple_query_source(&query.source)?;
        self.collect_from_dimension_subqueries(&query.dimension_subqueries)?;
        Ok(())
    }

    fn collect_from_full_key_aggregate_query(
        &mut self,
        query: &FullKeyAggregateQuery,
    ) -> Result<(), CubeError> {
        self.collect_from_full_key_aggregate(&query.source)?;
        for member in query.multistage_members.iter() {
            self.collect_from_multi_stage_member(member)?;
        }
        Ok(())
    }

    fn collect_from_multi_stage_member(
        &mut self,
        member: &Rc<LogicalMultiStageMember>,
    ) -> Result<(), CubeError> {
        match &member.member_type {
            MultiStageMemberLogicalType::LeafMeasure(leaf_measure) => {
                self.collect_from_multi_stage_leaf_measure(leaf_measure)
            }
            _ => Ok(()),
        }
    }

    fn collect_from_multi_stage_leaf_measure(
        &mut self,
        leaf_measure: &MultiStageLeafMeasure,
    ) -> Result<(), CubeError> {
        self.collect(&leaf_measure.query)?;
        Ok(())
    }

    fn collect_from_measure_subquery(
        &mut self,
        subquery: &Rc<MeasureSubquery>,
    ) -> Result<(), CubeError> {
        self.collect_from_logical_join(&subquery.source)?;
        self.collect_from_dimension_subqueries(&subquery.dimension_subqueries)?;
        Ok(())
    }

    fn collect_from_full_key_aggregate(
        &mut self,
        full_key_aggregate: &Rc<FullKeyAggregate>,
    ) -> Result<(), CubeError> {
        if let Some(resolve_multiplied_measures) = &full_key_aggregate.multiplied_measures_resolver
        {
            self.collect_from_resolved_multiplied_measures(resolve_multiplied_measures)?;
        }
        Ok(())
    }

    fn collect_from_resolved_multiplied_measures(
        &mut self,
        resolved_multiplied_measures: &ResolvedMultipliedMeasures,
    ) -> Result<(), CubeError> {
        match resolved_multiplied_measures {
            ResolvedMultipliedMeasures::ResolveMultipliedMeasures(resolve_multiplied_measures) => {
                self.collect_from_multiplied_measures_resolver(resolve_multiplied_measures)?
            }
            ResolvedMultipliedMeasures::PreAggregation(_) => {}
        }
        Ok(())
    }
    fn collect_from_multiplied_measures_resolver(
        &mut self,
        resolver: &ResolveMultipliedMeasures,
    ) -> Result<(), CubeError> {
        for regular_subquery in resolver.regular_measure_subqueries.iter() {
            self.collect_from_simple_query(&regular_subquery)?;
        }
        for aggregate_multiplied_subquery in resolver.aggregate_multiplied_subqueries.iter() {
            self.collect_from_aggregate_multiplied_subquery(&aggregate_multiplied_subquery)?;
        }
        Ok(())
    }

    fn collect_from_aggregate_multiplied_subquery(
        &mut self,
        subquery: &Rc<AggregateMultipliedSubquery>,
    ) -> Result<(), CubeError> {
        self.collect_from_logical_join(&subquery.keys_subquery.source)?;
        match subquery.source.as_ref() {
            AggregateMultipliedSubquerySouce::Cube => {
                self.cube_names.insert(subquery.pk_cube.name.clone());
            }
            AggregateMultipliedSubquerySouce::MeasureSubquery(measure_subquery) => {
                self.collect_from_measure_subquery(&measure_subquery)?;
            }
        }
        Ok(())
    }

    fn collect_from_simple_query_source(
        &mut self,
        source: &SimpleQuerySource,
    ) -> Result<(), CubeError> {
        match source {
            SimpleQuerySource::LogicalJoin(join) => self.collect_from_logical_join(join),
            SimpleQuerySource::PreAggregation(_) => Ok(()),
        }
    }

    fn collect_from_logical_join(&mut self, join: &Rc<LogicalJoin>) -> Result<(), CubeError> {
        self.cube_names.insert(join.root.name.clone());
        for join_item in join.joins.iter() {
            match join_item {
                LogicalJoinItem::CubeJoinItem(cube_join_item) => {
                    self.cube_names.insert(cube_join_item.cube.name.clone());
                }
            }
        }
        Ok(())
    }

    fn collect_from_dimension_subqueries(
        &mut self,
        dimension_subqueries: &Vec<Rc<DimensionSubQuery>>,
    ) -> Result<(), CubeError> {
        for subquery in dimension_subqueries.iter() {
            self.collect(&subquery.query)?;
        }
        Ok(())
    }
}
