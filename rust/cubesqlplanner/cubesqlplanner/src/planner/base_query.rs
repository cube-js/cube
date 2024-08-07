use super::base_cube::BaseCube;
use super::base_dimension::BaseDimension;
use super::base_measure::BaseMeasure;
use super::query_tools::QueryTools;
use crate::cube_bridge::base_query_options::BaseQueryOptions;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::plan::{Expr, From, GenerationPlan, Select};
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
    measures: Vec<Rc<BaseMeasure>>,
    dimensions: Vec<Rc<BaseDimension>>,
    join_root: String, //TODO temporary
}

impl<IT: InnerTypes> BaseQuery<IT> {
    pub fn try_new(
        context: NativeContextHolder<IT>,
        options: Rc<dyn BaseQueryOptions>,
    ) -> Result<Self, CubeError> {
        let cube_evaluator = options.cube_evaluator()?;
        let query_tools = QueryTools::new(cube_evaluator.clone());

        let measures = if let Some(measures) = &options.static_data().measures {
            measures
                .iter()
                .map(|m| BaseMeasure::new(m.clone(), query_tools.clone()))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let dimensions = if let Some(dimensions) = &options.static_data().dimensions {
            dimensions
                .iter()
                .map(|m| BaseDimension::new(m.clone(), query_tools.clone()))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        Ok(Self {
            context,
            query_tools,
            measures,
            dimensions,
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

    //TODO temporary realization
    fn get_params(&self) -> Result<Vec<String>, CubeError> {
        Ok(Vec::new())
    }

    fn simple_query(&self) -> Result<GenerationPlan, CubeError> {
        //let cube =
        let select = Select {
            projection: self.simple_projection()?,
            from: From::Cube(self.cube_from_path(self.join_root.clone())?),
            group_by: self.dimensions.clone(),
        };
        Ok(GenerationPlan::Select(select))
    }

    fn simple_projection(&self) -> Result<Vec<Expr>, CubeError> {
        let measures = self.measures.iter().map(|m| Expr::Measure(m.clone()));
        let res = self
            .dimensions
            .iter()
            .map(|d| Expr::Dimension(d.clone()))
            .chain(measures)
            .collect();
        Ok(res)
    }

    fn cube_from_path(&self, cube_path: String) -> Result<Rc<BaseCube>, CubeError> {
        let eval = self.query_tools.cube_evaluator().clone();
        let def = self
            .query_tools
            .cube_evaluator()
            .cube_from_path(cube_path)?;
        Ok(BaseCube::new(eval, def))
    }
}
