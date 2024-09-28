use super::filter::compiler::FilterCompiler;
use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::{
    BaseCube, BaseDimension, BaseMeasure, BaseMember, BaseTimeDimension, SqlJoinCondition,
};
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::memeber_sql::MemberSql;
use crate::plan::{
    Expr, Filter, FilterItem, From, FromSource, Join, JoinItem, JoinSource, OrderBy,
};
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;

pub struct QueryRequest {
    query_tools: Rc<QueryTools>,
    measures: Vec<Rc<BaseMeasure>>,
    dimensions: Vec<Rc<BaseDimension>>,
    dimensions_filters: Vec<FilterItem>,
    time_dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    time_dimensions: Vec<Rc<BaseTimeDimension>>,
}

impl QueryRequest {
    pub fn try_new(
        query_tools: Rc<QueryTools>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Rc<Self>, CubeError> {
        let evaluator_compiler_cell = query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let dimensions = if let Some(dimensions) = &options.static_data().dimensions {
            dimensions
                .iter()
                .map(|d| {
                    let evaluator = evaluator_compiler.add_dimension_evaluator(d.clone())?;
                    BaseDimension::try_new(d.clone(), query_tools.clone(), evaluator)
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let time_dimensions = if let Some(time_dimensions) = &options.static_data().time_dimensions
        {
            time_dimensions
                .iter()
                .map(|d| {
                    let evaluator =
                        evaluator_compiler.add_dimension_evaluator(d.dimension.clone())?;
                    BaseTimeDimension::try_new(
                        d.dimension.clone(),
                        query_tools.clone(),
                        evaluator,
                        d.granularity.clone(),
                        d.date_range.clone(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };

        let measures = if let Some(measures) = &options.static_data().measures {
            measures
                .iter()
                .map(|m| {
                    let evaluator = evaluator_compiler.add_measure_evaluator(m.clone())?;
                    BaseMeasure::try_new(m.clone(), query_tools.clone(), evaluator)
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

        Ok(Rc::new(Self {
            query_tools,
            measures,
            dimensions,
            time_dimensions,
            time_dimensions_filters,
            dimensions_filters,
            measures_filters,
        }))
    }

    pub fn measures(&self) -> &Vec<Rc<BaseMeasure>> {
        &self.measures
    }

    pub fn measures_filters(&self) -> &Vec<FilterItem> {
        &self.measures_filters
    }

    pub fn all_filters(&self) -> Option<Filter> {
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

    pub fn select_all_dimensions_and_measures(
        &self,
        measures: &Vec<Rc<BaseMeasure>>,
    ) -> Result<Vec<Expr>, CubeError> {
        let measures = measures.iter().map(|m| Expr::Field(m.clone()));
        let time_dimensions = self.time_dimensions.iter().map(|d| Expr::Field(d.clone()));
        let dimensions = self.dimensions.iter().map(|d| Expr::Field(d.clone()));
        Ok(dimensions.chain(time_dimensions).chain(measures).collect())
    }

    pub fn dimensions_references_and_measures(
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

    pub fn dimensions_for_select(&self) -> Vec<Rc<dyn BaseMember>> {
        let time_dimensions = self
            .time_dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let dimensions = self
            .dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        dimensions.chain(time_dimensions).collect()
    }

    pub fn dimensions_for_select_append(
        &self,
        append: &Vec<Rc<BaseDimension>>,
    ) -> Vec<Rc<dyn BaseMember>> {
        let time_dimensions = self
            .time_dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let append_dims = append.iter().map(|d| -> Rc<dyn BaseMember> { d.clone() });
        let dimensions = self
            .dimensions
            .iter()
            .map(|d| -> Rc<dyn BaseMember> { d.clone() });
        dimensions
            .chain(time_dimensions)
            .chain(append_dims)
            .collect()
    }

    pub fn columns_to_expr(&self, columns: &Vec<Rc<dyn BaseMember>>) -> Vec<Expr> {
        columns.iter().map(|d| Expr::Field(d.clone())).collect_vec()
    }

    pub fn cube_from_path(&self, cube_path: String) -> Result<Rc<BaseCube>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();

        let evaluator = evaluator_compiler.add_cube_table_evaluator(cube_path.to_string())?;
        BaseCube::try_new(cube_path.to_string(), self.query_tools.clone(), evaluator)
    }

    pub fn compile_join_condition(
        &self,
        cube_name: &String,
        sql: Rc<dyn MemberSql>,
    ) -> Result<Rc<EvaluationNode>, CubeError> {
        let evaluator_compiler_cell = self.query_tools.evaluator_compiler().clone();
        let mut evaluator_compiler = evaluator_compiler_cell.borrow_mut();
        evaluator_compiler.add_join_condition_evaluator(cube_name.clone(), sql)
    }

    pub fn group_by(&self) -> Vec<Rc<dyn BaseMember>> {
        self.dimensions
            .iter()
            .map(|f| -> Rc<dyn BaseMember> { f.clone() })
            .chain(
                self.time_dimensions
                    .iter()
                    .map(|f| -> Rc<dyn BaseMember> { f.clone() }),
            )
            .collect()
    }

    pub fn primary_keys_dimensions(
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
            .map(|d| {
                let full_name = format!("{}.{}", cube_name, d);
                let evaluator = evaluator_compiler.add_dimension_evaluator(full_name.clone())?;
                BaseDimension::try_new(full_name, self.query_tools.clone(), evaluator)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(dims)
    }

    pub fn default_order(&self) -> Vec<OrderBy> {
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

    pub fn make_join_node(&self, /*TODO dimensions for subqueries*/) -> Result<From, CubeError> {
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
}
