use super::base_cube::BaseCube;
use super::base_dimension::BaseDimension;
use super::base_measure::BaseMeasure;
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::plan::{Expr, From, GenerationPlan, Select};
use cubenativeutils::wrappers::serializer::NativeSerialize;
use cubenativeutils::wrappers::{NativeContextHolder, NativeObjectHandle};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseQuery {
    context: NativeContextHolder,
    cube_evaluator: Rc<dyn CubeEvaluator>,
    measures: Vec<Rc<BaseMeasure>>,
    dimensions: Vec<Rc<BaseDimension>>,
    join_root: String, //TODO temporary
}

impl BaseQuery {
    pub fn try_new(
        context: NativeContextHolder,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Self, CubeError> {
        println!("!!! opts {:?}", options.static_data());
        let cube_evaluator = options.cube_evaluator()?;

        let measures = if let Some(measures) = &options.static_data().measures {
            measures
                .iter()
                .map(|m| BaseMeasure::new(m.clone(), cube_evaluator.clone()))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let dimensions = if let Some(dimensions) = &options.static_data().dimensions {
            dimensions
                .iter()
                .map(|m| BaseDimension::new(m.clone()))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        Ok(Self {
            context,
            cube_evaluator,
            measures,
            dimensions,
            join_root: options.static_data().join_root.clone().unwrap(),
        })
    }
    pub fn build_sql_and_params(&self) -> Result<NativeObjectHandle, CubeError> {
        let plan = self.build_sql_and_params_impl()?;
        let res = plan.to_string();

        res.to_native(self.context.clone())
    }

    fn build_sql_and_params_impl(&self) -> Result<GenerationPlan, CubeError> {
        self.simple_query()
    }

    fn simple_query(&self) -> Result<GenerationPlan, CubeError> {
        //let cube =
        let select = Select {
            projection: self.simple_projection()?,
            from: From::Cube(self.cube_from_path(self.join_root.clone())?),
        };
        Ok(GenerationPlan::Select(select))
    }

    fn simple_projection(&self) -> Result<Vec<Expr>, CubeError> {
        let res = self
            .measures
            .iter()
            .map(|m| Expr::Measure(m.clone()))
            .collect();
        Ok(res)
    }

    fn cube_from_path(&self, cube_path: String) -> Result<Rc<BaseCube>, CubeError> {
        Ok(BaseCube::new(
            self.cube_evaluator.clone(),
            self.cube_evaluator.cube_from_path(cube_path)?,
        ))
    }
}
