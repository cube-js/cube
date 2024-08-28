use super::filter::compiler::FilterCompiler;
use super::query_tools::QueryTools;
use super::sql_evaluator::Compiler;
use super::{BaseCube, BaseDimension, BaseMeasure, BaseTimeDimension, IndexedMember};
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::plan::{Expr, Filter, FilterItem, From, GenerationPlan, OrderBy, Select};
use cubenativeutils::wrappers::inner_types::InnerTypes;
use cubenativeutils::wrappers::object::NativeArray;
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::NativeType;
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseQuery<IT: InnerTypes> {
    context: NativeContextHolder<IT>,
    query_tools: Rc<QueryTools>,
    evaluator_compiler: Rc<Compiler>,
    measures: Vec<Rc<BaseMeasure>>,
    dimensions: Vec<Rc<BaseDimension>>,
    dimensions_filters: Vec<FilterItem>,
    measures_filters: Vec<FilterItem>,
    time_dimensions: Vec<Rc<BaseTimeDimension>>,
    join_root: String, //TODO temporary
}

impl<IT: InnerTypes> BaseQuery<IT> {
    pub fn try_new(
        context: NativeContextHolder<IT>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Self, CubeError> {
        let query_tools = QueryTools::try_new(options.cube_evaluator()?, options.base_tools()?)?;
        let mut evaluator_compiler = Compiler::new(query_tools.cube_evaluator().clone());

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
        let (dimensions_filters, measures_filters) = filter_compiler.extract_result();

        Ok(Self {
            context,
            query_tools,
            evaluator_compiler: Rc::new(evaluator_compiler),
            measures,
            dimensions,
            time_dimensions,
            dimensions_filters,
            measures_filters,
            join_root: options.static_data().join_root.clone().unwrap(),
        })
    }

    pub fn build_sql_and_params(&self) -> Result<NativeObjectHandle<IT>, CubeError> {
        let plan = self.build_sql_and_params_impl()?;
        let sql = plan.to_string();
        let params = self.get_params()?;
        let res = self.context.empty_array();
        res.set(0, sql.to_native(self.context.clone())?)?;
        res.set(1, params.to_native(self.context.clone())?)?;
        let result = NativeObjectHandle::new(res.into_object());

        Ok(result)
    }

    fn build_sql_and_params_impl(&self) -> Result<GenerationPlan, CubeError> {
        self.simple_query()
    }

    fn get_params(&self) -> Result<Vec<String>, CubeError> {
        Ok(self.query_tools.get_allocated_params())
    }

    fn simple_query(&self) -> Result<GenerationPlan, CubeError> {
        let filter = if self.dimensions_filters.is_empty() {
            None
        } else {
            Some(Filter {
                items: self.dimensions_filters.clone(),
            })
        };
        let having = if self.measures_filters.is_empty() {
            None
        } else {
            Some(Filter {
                items: self.measures_filters.clone(),
            })
        };
        let select = Select {
            projection: self.simple_projection()?,
            from: From::Cube(self.cube_from_path(self.join_root.clone())?),
            filter,
            group_by: self.group_by(),
            having,
            order_by: self.default_order(),
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

    fn cube_from_path(&self, cube_path: String) -> Result<Rc<BaseCube>, CubeError> {
        let eval = self.query_tools.cube_evaluator().clone();
        let def = self
            .query_tools
            .cube_evaluator()
            .cube_from_path(cube_path)?;
        Ok(BaseCube::new(eval, def, self.query_tools.clone()))
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

    fn get_field_index(&self, id: &str) -> Option<usize> {
        let upper_id = id.to_uppercase();
        if let Some(ind) = self
            .dimensions
            .iter()
            .position(|d| d.dimension().to_uppercase() == upper_id)
        {
            Some(ind + 1)
        } else if let Some(ind) = self
            .measures
            .iter()
            .position(|m| m.measure().to_uppercase() == upper_id)
        {
            Some(ind + self.dimensions.len())
        } else {
            None
        }
    }
}
