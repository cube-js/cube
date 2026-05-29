use super::symbols::{MemberExpressionExpression, MemberExpressionSymbol, MemberSymbol};
use super::SymbolPath;
use super::SymbolPathType;
use super::{
    CubeNameSymbol, CubeNameSymbolFactory, CubeTableSymbol, CubeTableSymbolFactory,
    DimensionSymbolFactory, MeasureSymbolFactory, SqlCall, SymbolFactory,
};
use crate::cube_bridge::base_tools::BaseTools;
use crate::cube_bridge::evaluator::CubeEvaluator;
use crate::cube_bridge::member_sql::MemberSql;
use crate::cube_bridge::security_context::SecurityContext;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_call_builder::SqlCallBuilder;
use crate::planner::sql_templates::PlanSqlTemplates;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::{Rc, Weak};

/// Compilation context for the planner. Resolves data-model
/// declarations into `MemberSymbol`s, caches them by `SymbolPath`,
/// and holds the JS-side interfaces (cube evaluator, base tools,
/// security context) together with query-level metadata (timezone,
/// alias overrides).
pub struct Compiler {
    cube_evaluator: Rc<dyn CubeEvaluator>,
    base_tools: Rc<dyn BaseTools>,
    security_context: Rc<dyn SecurityContext>,
    timezone: Tz,
    member_to_alias: Option<HashMap<String, String>>,
    members: HashMap<SymbolPath, Rc<MemberSymbol>>,
    cube_names: HashMap<Vec<String>, Rc<CubeNameSymbol>>,
    cube_tables: HashMap<Vec<String>, Rc<CubeTableSymbol>>,
    /// Back-reference to the owning `QueryTools`. Set by `set_query_tools`
    /// at the end of `QueryTools::try_new`, after the `Rc<QueryTools>` is
    /// available. Held as `Weak` to avoid an `Rc` cycle: `QueryTools` owns
    /// `Rc<RefCell<Compiler>>` strongly, so Compiler cannot also hold a
    /// strong handle back without leaking.
    query_tools: Weak<QueryTools>,
}

impl Compiler {
    pub fn new(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        base_tools: Rc<dyn BaseTools>,
        security_context: Rc<dyn SecurityContext>,
        timezone: Tz,
        member_to_alias: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            cube_evaluator,
            security_context,
            base_tools,
            timezone,
            member_to_alias,
            members: HashMap::new(),
            cube_names: HashMap::new(),
            cube_tables: HashMap::new(),
            query_tools: Weak::new(),
        }
    }

    /// Wire the owning `QueryTools` into this compiler. Called once by
    /// `QueryTools::try_new` after the `Rc<QueryTools>` is materialized;
    /// callers outside that constructor have no reason to call this.
    pub(crate) fn set_query_tools(&mut self, query_tools: Weak<QueryTools>) {
        self.query_tools = query_tools;
    }

    /// Return the owning `QueryTools`. Errors only if the back-reference
    /// is detached — by construction this should never happen because
    /// `QueryTools` strongly owns this `Compiler`, but the result keeps
    /// callers honest about the invariant.
    pub fn query_tools(&self) -> Result<Rc<QueryTools>, CubeError> {
        self.query_tools.upgrade().ok_or_else(|| {
            CubeError::internal(
                "Compiler is detached from QueryTools (Weak ref upgrade failed)".to_string(),
            )
        })
    }

    /// Parses `name` as a `SymbolPath` and resolves it as the
    /// appropriate member kind (dimension, measure, or segment).
    /// Errors if the path points at a cube reference.
    pub fn add_auto_resolved_member_evaluator(
        &mut self,
        name: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let path = SymbolPath::parse(self.cube_evaluator.clone(), &name)?;
        match path.path_type() {
            SymbolPathType::Dimension => self.add_dimension_evaluator_by_path(path),
            SymbolPathType::Measure => self.add_measure_evaluator_by_path(path),
            SymbolPathType::Segment => self.add_segment_evaluator_by_path(path),
            _ => Err(CubeError::internal(format!(
                "Cannot auto-resolve {}. Only dimensions, measures and segments",
                name
            ))),
        }
    }

    /// Resolves a measure by data-model path (`cube.measure` or a
    /// cross-cube form). Cached.
    pub fn add_measure_evaluator(
        &mut self,
        measure: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let path = SymbolPath::parse(self.cube_evaluator.clone(), &measure)?;
        self.add_measure_evaluator_by_path(path)
    }

    pub fn add_measure_evaluator_by_path(
        &mut self,
        path: SymbolPath,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.members.get(&path) {
            Ok(exists.clone())
        } else {
            let result = MeasureSymbolFactory::try_new(path.clone(), self.cube_evaluator.clone())?
                .build(self)?;
            self.validate_and_cache_result(path, result.clone())?;
            Ok(result)
        }
    }

    /// Resolves a dimension by data-model path. When the path turns
    /// out to point at a segment, falls back to a parenthesized
    /// member-expression wrapper.
    pub fn add_dimension_evaluator(
        &mut self,
        dimension: String,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let path = SymbolPath::parse(self.cube_evaluator.clone(), &dimension)?;
        match path.path_type() {
            SymbolPathType::Segment => {
                let symbol = self.add_segment_evaluator_by_path(path)?;
                let me = symbol.as_member_expression()?;
                Ok(MemberSymbol::new_member_expression(me.with_parenthesized()))
            }
            _ => self.add_dimension_evaluator_by_path(path),
        }
    }

    pub fn add_dimension_evaluator_by_path(
        &mut self,
        path: SymbolPath,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.members.get(&path) {
            Ok(exists.clone())
        } else {
            let result =
                DimensionSymbolFactory::try_new(path.clone(), self.cube_evaluator.clone())?
                    .build(self)?;
            self.validate_and_cache_result(path, result.clone())?;
            Ok(result)
        }
    }

    /// Resolves a segment by data-model path. Segments are
    /// materialised as `MemberExpression` members so they plug into
    /// the same machinery as other members.
    pub fn add_segment_evaluator(&mut self, name: String) -> Result<Rc<MemberSymbol>, CubeError> {
        let path = SymbolPath::parse(self.cube_evaluator.clone(), &name)?;
        self.add_segment_evaluator_by_path(path)
    }

    pub fn add_segment_evaluator_by_path(
        &mut self,
        path: SymbolPath,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        if let Some(exists) = self.members.get(&path) {
            return Ok(exists.clone());
        }
        let full_name = path.full_name().clone();
        let definition = self.cube_evaluator.segment_by_path(full_name.clone())?;
        let sql_call = self.compile_sql_call(path.cube_name(), definition.sql()?)?;
        let alias = self.alias_for_member(&full_name).unwrap_or_else(|| {
            PlanSqlTemplates::member_alias_name(path.cube_name(), path.symbol_name(), &None)
        });
        let cube_symbol = self.add_cube_table_evaluator(path.cube_name().clone(), vec![])?;
        let symbol = MemberExpressionSymbol::try_new(
            cube_symbol,
            path.symbol_name().clone(),
            MemberExpressionExpression::SqlCall(sql_call),
            None,
            Some(alias),
            path.path().clone(),
        )?;
        let result = MemberSymbol::new_member_expression(symbol);
        self.members.insert(path, result.clone());
        Ok(result)
    }

    /// Resolves a cube as an identifier — for `{CUBE}` / `{TABLE}`
    /// placeholders. Cached by the normalised path.
    pub fn add_cube_name_evaluator(
        &mut self,
        cube_name: String,
        path: Vec<String>,
    ) -> Result<Rc<CubeNameSymbol>, CubeError> {
        let cache_key = CubeNameSymbol::normalize_path(path.clone(), &cube_name);
        if let Some(exists) = self.cube_names.get(&cache_key) {
            Ok(exists.clone())
        } else {
            let result =
                CubeNameSymbolFactory::try_new(&cube_name, self.cube_evaluator.clone(), path)?
                    .build(self)?;
            self.cube_names.insert(cache_key, result.clone());
            Ok(result)
        }
    }

    /// Resolves a cube as a table expression — for `{CUBE.sql()}`
    /// placeholders. Cached by the normalised path.
    pub fn add_cube_table_evaluator(
        &mut self,
        cube_name: String,
        path: Vec<String>,
    ) -> Result<Rc<CubeTableSymbol>, CubeError> {
        let cache_key = CubeNameSymbol::normalize_path(path.clone(), &cube_name);
        if let Some(exists) = self.cube_tables.get(&cache_key) {
            Ok(exists.clone())
        } else {
            let result =
                CubeTableSymbolFactory::try_new(&cube_name, self.cube_evaluator.clone(), path)?
                    .build(self)?;
            self.cube_tables.insert(cache_key, result.clone());
            Ok(result)
        }
    }

    pub fn timezone(&self) -> Tz {
        self.timezone.clone()
    }

    /// Looks up an explicit alias override for a member's full name;
    /// `None` when no override is set.
    pub fn alias_for_member(&self, full_name: &str) -> Option<String> {
        self.member_to_alias
            .as_ref()
            .and_then(|m| m.get(full_name).cloned())
    }

    /// Compiles a JS `MemberSql` declaration into a `SqlCall` bound
    /// to the given owning cube, via `SqlCallBuilder`.
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

    fn validate_and_cache_result(
        &mut self,
        path: SymbolPath,
        node: Rc<MemberSymbol>,
    ) -> Result<(), CubeError> {
        node.validate()?;
        self.members.insert(path, node);
        Ok(())
    }
}
