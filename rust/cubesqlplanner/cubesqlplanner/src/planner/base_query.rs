use super::filter::compiler::FilterCompiler;
use super::full_key_query_aggregate::FullKeyAggregateQueryBuilder;
use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::{
    BaseCube, BaseDimension, BaseMeasure, BaseTimeDimension, IndexedMember, SqlJoinCondition,
    VisitorContext,
};
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::plan::{
    Expr, Filter, FilterItem, From, FromSource, GenerationPlan, Join, JoinItem, JoinSource,
    OrderBy, Select,
};
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::NativeArray;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::NativeType;
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct BaseQuery<IT: InnerTypes> {
    context: NativeContextHolder<IT>,
    query_tools: Rc<QueryTools>,
    measures: Vec<Rc<BaseMeasure>>,
    dimensions: Vec<Rc<BaseDimension>>,
    dimensions_filters: Vec<FilterItem>,
    time_dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    time_dimensions: Vec<Rc<BaseTimeDimension>>,
}

impl<IT: InnerTypes> BaseQuery<IT> {
    pub fn try_new(
        context: NativeContextHolder<IT>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Self, CubeError> {
        let query_tools = QueryTools::try_new(
            options.cube_evaluator()?,
            options.base_tools()?,
            options.join_graph()?,
            options.static_data().timezone.clone(),
        )?;
        let evaluator_compiler_cell = query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let mut base_index = 1;
        let dimensions = if let Some(dimensions) = &options.static_data().dimensions {
            dimensions
                .iter()
                .enumerate()
                .map(|(i, d)| {
                    let evaluator = evaluator_compiler.add_dimension_evaluator(d.clone())?;
                    BaseDimension::try_new(
                        d.clone(),
                        query_tools.clone(),
                        evaluator,
                        i + base_index,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        base_index += dimensions.len();

        let time_dimensions = if let Some(time_dimensions) = &options.static_data().time_dimensions
        {
            time_dimensions
                .iter()
                .enumerate()
                .map(|(i, d)| {
                    let evaluator =
                        evaluator_compiler.add_dimension_evaluator(d.dimension.clone())?;
                    BaseTimeDimension::try_new(
                        d.dimension.clone(),
                        query_tools.clone(),
                        evaluator,
                        d.granularity.clone(),
                        d.date_range.clone(),
                        i + base_index,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        base_index += time_dimensions.len();

        let measures = if let Some(measures) = &options.static_data().measures {
            measures
                .iter()
                .enumerate()
                .map(|(i, m)| {
                    let evaluator = evaluator_compiler.add_measure_evaluator(m.clone())?;
                    BaseMeasure::try_new(m.clone(), query_tools.clone(), evaluator, i + base_index)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let mut filter_compiler = FilterCompiler::new(&mut evaluator_compiler, query_tools.clone());
        if let Some(filters) = &options.static_data().filters {
            for filter in filters {
                filter_compiler.add_item(filter)?;
            }
        }
        for time_dimension in &time_dimensions {
            filter_compiler.add_time_dimension_item(time_dimension)?;
        }
        let (dimensions_filters, time_dimensions_filters, measures_filters) =
            filter_compiler.extract_result();

        let all_join_hints = evaluator_compiler.join_hints()?;
        let join = query_tools.join_graph().build_join(all_join_hints)?;
        query_tools.cached_data_mut().set_join(join);
        //FIXME may be this filter should be applyed on other place
        let time_dimensions = time_dimensions
            .into_iter()
            .filter(|dim| dim.has_granularity())
            .collect_vec();

        Ok(Self {
            context,
            query_tools,
            measures,
            dimensions,
            time_dimensions,
            time_dimensions_filters,
            dimensions_filters,
            measures_filters,
        })
    }

    pub fn build_sql_and_params(&self) -> Result<NativeObjectHandle<IT>, CubeError> {
        let plan = self.build_sql_and_params_impl()?;
        let sql = plan.to_sql()?;
        let (result_sql, params) = self.query_tools.build_sql_and_params(&sql, true)?;

        let res = self.context.empty_array();
        res.set(0, result_sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;
        let result = NativeObjectHandle::new(res.into_object());

        Ok(result)
    }

    fn build_sql_and_params_impl(&self) -> Result<GenerationPlan, CubeError> {
        let full_key_aggregate_query_builder = FullKeyAggregateQueryBuilder::new(
            self.query_tools.clone(),
            self.measures.clone(),
            self.dimensions.clone(),
            self.time_dimensions.clone(),
            self.dimensions_filters.clone(),
            self.time_dimensions_filters.clone(),
            self.measures_filters.clone(),
        );
        if let Some(select) = full_key_aggregate_query_builder.build()? {
            Ok(GenerationPlan::Select(select))
        } else {
            self.simple_query()
        }
    }

    fn simple_query(&self) -> Result<GenerationPlan, CubeError> {
        let filter = self.all_filters();
        let having = if self.measures_filters.is_empty() {
            None
        } else {
            Some(Filter {
                items: self.measures_filters.clone(),
            })
        };
        let select = Select {
            projection: self.simple_projection()?,
            from: self.make_from_node()?,
            filter,
            group_by: self.group_by(),
            having,
            order_by: self.default_order(),
            context: VisitorContext::default(),
            is_distinct: false,
        };
        Ok(GenerationPlan::Select(select))
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

    fn simple_projection(&self) -> Result<Vec<Expr>, CubeError> {
        let measures = self.measures.iter().map(|m| Expr::Field(m.clone()));
        let time_dimensions = self.time_dimensions.iter().map(|d| Expr::Field(d.clone()));
        let dimensions = self.dimensions.iter().map(|d| Expr::Field(d.clone()));
        Ok(dimensions.chain(time_dimensions).chain(measures).collect())
    }

    fn make_from_node(&self) -> Result<From, CubeError> {
        let join = self.query_tools.cached_data().join()?.clone();
        let root = self.cube_from_path(join.static_data().root.clone())?;
        let joins = join.joins()?;
        if joins.items().is_empty() {
            Ok(From::new_from_cube(root))
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
                        from: JoinSource::new_from_cube(
                            self.cube_from_path(join.static_data().original_to.clone())?,
                        ),
                        on: SqlJoinCondition::try_new(self.query_tools.clone(), evaluator)?,
                        is_inner: false,
                    })
                })
                .collect::<Result<Vec<_>, CubeError>>()?;
            let result = From::new(FromSource::Join(Rc::new(Join {
                root: JoinSource::new_from_cube(root),
                joins: join_items,
            })));
            Ok(result)
        }
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

    fn cube_from_path(&self, cube_path: String) -> Result<Rc<BaseCube>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let evaluator = evaluator_compiler.add_cube_table_evaluator(cube_path.to_string())?;
        BaseCube::try_new(cube_path.to_string(), self.query_tools.clone(), evaluator)
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

    fn all_filters(&self) -> Option<Filter> {
        let items = self
            .time_dimensions_filters
            .iter()
            .chain(self.dimensions_filters.iter())
            .cloned()
            .collect_vec();
        if items.is_empty() {
            None
        } else {
            Some(Filter { items })
        }
    }
}
