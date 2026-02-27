use super::collectors::JoinHintsCollector;
use super::symbols::{MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol};
use super::SymbolPath;
use super::SymbolPathType;
use super::{
    CubeNameSymbolFactory, CubeTableSymbolFactory, DimensionSymbolFactory, MeasureSymbolFactory,
    SqlCall, SymbolFactory, TraversalVisitor,
};
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::join_hints::JoinHintItem;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::security_context::SecurityContext;
use crate::planner::sql_evaluator::sql_call_builder::SqlCallBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CacheSymbolType {
    Dimension,
    Measure,
    Segment,
    CubeTable,
    CubeName,
}

pub struct Compiler {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    security_context: Rc<dyn SecurityContext>,
    timezone: Tz,
    members: HashMap<(CacheSymbolType, String), Rc<MemberSymbol>>,
}

impl Compiler {
    pub fn new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        base_tools: Rc<dyn BaseTools>,
        security_context: Rc<dyn SecurityContext>,
        timezone: Tz,
    ) -> Self {
        Self {
            cube_evaluator,
            security_context,
            base_tools,
            timezone,
            members: HashMap::new(),
        }
    }

    pub fn add_auto_resolved_member_evaluator(
        &mut self,
        name: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let path = name.split(".").map(|s| s.to_string()).collect::<Vec<_>>();
        if self.cube_evaluator.is_measure(path.clone())? {
            Ok(self.add_measure_evaluator(name)?)
        } else if self.cube_evaluator.is_dimension(path.clone())? {
            Ok(self.add_dimension_evaluator(name)?)
        } else if self.cube_evaluator.is_segment(path.clone())? {
            Ok(self.add_segment_evaluator(name)?)
        } else {
            Err(CubeError::internal(format!(
                "Cannot resolve evaluator of member {}. Only dimensions, measures and segments can be autoresolved",
                name
            )))
        }
    }

    pub fn add_measure_evaluator(
        &mut self,
        measure: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let path = SymbolPath::parse(self.cube_evaluator.clone(), &measure)?;
        self.add_measure_evaluator_impl(path)
    }

    fn add_measure_evaluator_impl(
        &mut self,
        path: SymbolPath,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member(CacheSymbolType::Measure, &path.cache_name()) {
            Ok(exists.clone())
        } else {
            let result =
                MeasureSymbolFactory::try_new(path, self.cube_evaluator.clone())?.build(self)?;
            self.validate_and_cache_result(CacheSymbolType::Measure, result.clone())?;
            Ok(result)
        }
    }

    pub fn add_dimension_evaluator(
        &mut self,
        dimension: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let path = SymbolPath::parse(self.cube_evaluator.clone(), &dimension)?;
        match path.path_type() {
            SymbolPathType::Segment => {
                let symbol = self.add_segment_evaluator(path.full_name().clone())?;
                let me = symbol.as_member_expression()?;
                Ok(MemberSymbol::new_member_expression(me.with_parenthesized()))
            }
            _ => self.add_dimension_evaluator_impl(path),
        }
    }

    fn add_dimension_evaluator_impl(
        &mut self,
        path: SymbolPath,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member(CacheSymbolType::Dimension, &path.cache_name()) {
            Ok(exists.clone())
        } else {
            let result =
                DimensionSymbolFactory::try_new(path, self.cube_evaluator.clone())?.build(self)?;
            self.validate_and_cache_result(CacheSymbolType::Dimension, result.clone())?;
            Ok(result)
        }
    }

    pub fn add_segment_evaluator(&mut self, name: String) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member(CacheSymbolType::Segment, &name) {
            return Ok(exists.clone());
        }
        let path = self
            .cube_evaluator
            .parse_path("segments".to_string(), name.clone())?;
        let cube_name = path[0].clone();
        let member_name = path[1].clone();
        let definition = self.cube_evaluator.segment_by_path(name.clone())?;
        let sql_call = self.compile_sql_call(&cube_name, definition.sql()?)?;
        let alias = PlanSqlTemplates::member_alias_name(&cube_name, &member_name, &None);
        let symbol = MemberExpressionSymbol::try_new(
            cube_name,
            member_name,
            MemberExpressionExpression::SqlCall(sql_call),
            None,
            Some(alias),
            self.base_tools.clone(),
        )?;
        let result = MemberSymbol::new_member_expression(symbol);
        let key = (CacheSymbolType::Segment, name);
        self.members.insert(key, result.clone());
        Ok(result)
    }

    pub fn add_cube_name_evaluator(
        &mut self,
        cube_name: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member(CacheSymbolType::CubeName, &cube_name) {
            Ok(exists.clone())
        } else {
            let result = CubeNameSymbolFactory::try_new(&cube_name, self.cube_evaluator.clone())?
                .build(self)?;
            self.validate_and_cache_result(CacheSymbolType::CubeName, result.clone())?;
            Ok(result)
        }
    }

    pub fn add_cube_table_evaluator(
        &mut self,
        cube_name: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.exists_member(CacheSymbolType::CubeTable, &cube_name) {
            Ok(exists.clone())
        } else {
            let result = CubeTableSymbolFactory::try_new(&cube_name, self.cube_evaluator.clone())?
                .build(self)?;
            self.validate_and_cache_result(CacheSymbolType::CubeTable, result.clone())?;
            Ok(result)
        }
    }

    pub fn join_hints(&self) -> Result<Vec<JoinHintItem>, CubeError> {
        let mut collector = JoinHintsCollector::new();
        for member in self.members.values() {
            collector.apply(member, &())?;
        }
        Ok(collector.extract_result())
    }

    pub fn timezone(&self) -> Tz {
        self.timezone.clone()
    }

    pub fn compile_sql_call(
        &mut self,
        cube_name: &String,
        member_sql: Rc<dyn MemberSql>,
    ) -> Result<Rc<SqlCall>, CubeError> {
        let call_builder = SqlCallBuilder::new(
            self,
            self.cube_evaluator.clone(),
            self.base_tools.clone(),
            self.security_context.clone(),
        );
        let sql_call = call_builder.build(&cube_name, member_sql.clone())?;
        Ok(Rc::new(sql_call))
    }

    fn exists_member(
        &self,
        symbol_type: CacheSymbolType,
        full_name: &String,
    ) -> Option<Rc<MemberSymbol>> {
        let key = (symbol_type, full_name.clone());
        self.members.get(&key).cloned()
    }

    fn validate_and_cache_result(
        &mut self,
        symbol_type: CacheSymbolType,
        node: Rc<MemberSymbol>,
    ) -> Result<(), CubeError> {
        node.validate()?;
        let key = (symbol_type, node.full_name().clone());
        self.members.insert(key, node.clone());
        Ok(())
    }
}
