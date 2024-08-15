use super::{
    DimensionEvaluator, DimensionEvaluatorFactory, MeasureEvaluator, MeasureEvaluatorFactory,
    MemberEvaluator, MemberEvaluatorFactory,
};
use crate::cube_bridge::evaluator::CubeEvaluator;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
pub struct Compiler {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    members: HashMap<String, Rc<dyn MemberEvaluator>>,
}

pub enum EvalType {
    Measure,
    Dimension,
}

impl Compiler {
    pub fn new(cube_evaluator: Rc<dyn CubeEvaluator>) -> Self {
        Self {
            cube_evaluator,
            members: HashMap::new(),
        }
    }

    pub fn add_evaluator<T: MemberEvaluatorFactory + 'static>(
        &mut self,
        full_name: String,
    ) -> Result<Rc<T::Result>, CubeError> {
        if let Some(exists) = self.members.get(&full_name) {
            return exists
                .clone()
                .as_any()
                .downcast::<T::Result>()
                .map_err(|_| {
                    CubeError::internal(format!(
                        "Evaluator of another type is exists for {}",
                        full_name
                    ))
                });
        }

        let mut factory = T::try_new(full_name.clone(), self.cube_evaluator.clone())?;
        let dep_names = factory.deps_names()?;
        let cube_name = factory.cube_name();
        let deps = dep_names
            .into_iter()
            .map(|name| -> Result<Rc<dyn MemberEvaluator>, CubeError> {
                let dep_full_name = format!("{}.{}", cube_name, name);
                //FIXME avoid cloning
                let dep_path = vec![cube_name.clone(), name.clone()];
                if self.cube_evaluator.is_measure(dep_path.clone())? {
                    Ok(self.add_evaluator::<MeasureEvaluatorFactory>(dep_full_name)?)
                } else if self.cube_evaluator.is_dimension(dep_path.clone())? {
                    Ok(self.add_evaluator::<DimensionEvaluatorFactory>(dep_full_name)?)
                } else {
                    Err(CubeError::internal(format!(
                        "Cannot resolve dependency {} of member {}",
                        name, full_name
                    )))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        factory.build(deps)
    }

    pub fn add_measure_evaluator(
        &mut self,
        measure: String,
    ) -> Result<Rc<MeasureEvaluator>, CubeError> {
        self.add_evaluator::<MeasureEvaluatorFactory>(measure)
    }

    pub fn add_dimension_evaluator(
        &mut self,
        measure: String,
    ) -> Result<Rc<DimensionEvaluator>, CubeError> {
        self.add_evaluator::<DimensionEvaluatorFactory>(measure)
    }
}
