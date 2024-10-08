use super::query_tools::QueryTools;
use super::sql_evaluator::EvaluationNode;
use super::{evaluate_with_context, BaseMember, VisitorContext};
use crate::cube_bridge::measure_definition::MeasureDefinition;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseMeasure {
    measure: String,
    query_tools: Rc<QueryTools>,
    #[allow(dead_code)]
    definition: Rc<dyn MeasureDefinition>,
    member_evaluator: Rc<EvaluationNode>,
    cube_name: String,
}

impl BaseMember for BaseMeasure {
    fn to_sql(&self, context: Rc<VisitorContext>) -> Result<String, CubeError> {
        let sql = evaluate_with_context(&self.member_evaluator, self.query_tools.clone(), context)?;
        let alias_name = self.alias_name()?;

        Ok(format!("{} {}", sql, alias_name))
    }

    fn alias_name(&self) -> Result<String, CubeError> {
        Ok(self
            .query_tools
            .escape_column_name(&self.unescaped_alias_name()?))
    }

    fn member_evaluator(&self) -> Rc<EvaluationNode> {
        self.member_evaluator.clone()
    }
}

impl BaseMeasure {
    pub fn try_new(
        measure: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<EvaluationNode>,
    ) -> Result<Rc<Self>, CubeError> {
        let definition = query_tools
            .cube_evaluator()
            .measure_by_path(measure.clone())?;
        let cube_name = query_tools
            .cube_evaluator()
            .cube_from_path(measure.clone())?
            .static_data()
            .name
            .clone();
        Ok(Rc::new(Self {
            measure,
            query_tools,
            definition,
            member_evaluator,
            cube_name,
        }))
    }

    pub fn member_evaluator(&self) -> &Rc<EvaluationNode> {
        &self.member_evaluator
    }

    pub fn measure(&self) -> &String {
        &self.measure
    }

    pub fn cube_name(&self) -> &String {
        &self.cube_name
    }

    fn unescaped_alias_name(&self) -> Result<String, CubeError> {
        Ok(self.query_tools.alias_name(&self.measure))
    }
}
