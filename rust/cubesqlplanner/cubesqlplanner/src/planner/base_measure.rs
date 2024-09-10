use super::query_tools::QueryTools;
use super::sql_evaluator::{MeasureEvaluator, MemberEvaluator};
use super::{BaseMember, IndexedMember};
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::measure_definition::MeasureDefinition;
use crate::cube_bridge::memeber_sql::MemberSql;
use convert_case::{Case, Casing};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct BaseMeasure {
    measure: String,
    query_tools: Rc<QueryTools>,
    definition: Rc<dyn MeasureDefinition>,
    member_evaluator: Rc<MeasureEvaluator>,
    index: usize,
}

impl BaseMember for BaseMeasure {
    fn to_sql(&self) -> Result<String, CubeError> {
        self.sql()
    }
}

impl IndexedMember for BaseMeasure {
    fn index(&self) -> usize {
        self.index
    }
}

impl BaseMeasure {
    pub fn try_new(
        measure: String,
        query_tools: Rc<QueryTools>,
        member_evaluator: Rc<MeasureEvaluator>,
        index: usize,
    ) -> Result<Rc<Self>, CubeError> {
        let definition = query_tools
            .cube_evaluator()
            .measure_by_path(measure.clone())?;
        Ok(Rc::new(Self {
            measure,
            query_tools,
            definition,
            member_evaluator,
            index,
        }))
    }

    pub fn to_sql(&self) -> Result<String, CubeError> {
        self.sql()
    }

    pub fn measure(&self) -> &String {
        &self.measure
    }

    pub fn index(&self) -> usize {
        self.index
    }

    fn sql(&self) -> Result<String, CubeError> {
        let sql = self.member_evaluator.evaluate(self.query_tools.clone())?;

        let measure_type = &self.definition.static_data().measure_type;
        let alias_name = self.query_tools.escape_column_name(&self.alias_name()?);

        Ok(format!("{} {}", sql, alias_name))
    }

    fn path(&self) -> Result<Vec<String>, CubeError> {
        self.query_tools
            .cube_evaluator()
            .parse_path("measures".to_string(), self.measure.clone())
    }

    fn alias_name(&self) -> Result<String, CubeError> {
        Ok(self.measure.to_case(Case::Snake).replace(".", "__"))
    }
}
