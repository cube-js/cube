use cubenativeutils::CubeError;
use itertools::Itertools;

use crate::planner::{Case, CubeRef, SqlCall};

use super::common::CompiledMemberPath;
use super::{DimensionSymbol, MeasureSymbol, MemberExpressionSymbol, TimeDimensionSymbol};
use std::fmt::Debug;
use std::rc::Rc;

/// First-class business object of the planner: the atomic unit of
/// query planning, identifying one thing the query can select, filter,
/// group or order by. The same `MemberSymbol` value carries through
/// every layer — logical planning, physical-plan construction and SQL
/// rendering.
///
/// A symbol is either bound to the data model — `Dimension` / `Measure`
/// declared on a cube — or derived at query time: `TimeDimension`
/// (a dimension at a chosen granularity and date range) or
/// `MemberExpression` (synthetic, built from a SQL expression or a
/// patched symbol). Identity is `full_name` + variant.
///
/// Indivisible: renders as a single SQL expression. A symbol may depend
/// on other symbols (`get_dependencies`); whether those deps are
/// inlined or pushed into a CTE / subquery is a physical-plan decision.
pub enum MemberSymbol {
    Dimension(Rc<DimensionSymbol>),
    TimeDimension(Rc<TimeDimensionSymbol>),
    Measure(Rc<MeasureSymbol>),
    MemberExpression(Rc<MemberExpressionSymbol>),
}

impl Debug for MemberSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dimension(_) => f.debug_tuple("Dimension").field(&self.full_name()).finish(),
            Self::TimeDimension(_) => f
                .debug_tuple("TimeDimension")
                .field(&self.full_name())
                .finish(),
            Self::Measure(_) => f.debug_tuple("Measure").field(&self.full_name()).finish(),
            Self::MemberExpression(_) => f
                .debug_tuple("MemberExpression")
                .field(&self.full_name())
                .finish(),
        }
    }
}

impl PartialEq for MemberSymbol {
    fn eq(&self, other: &Self) -> bool {
        self.full_name() == other.full_name()
            && std::mem::discriminant(self) == std::mem::discriminant(other)
    }
}

impl MemberSymbol {
    pub fn new_measure(symbol: Rc<MeasureSymbol>) -> Rc<Self> {
        Rc::new(Self::Measure(symbol))
    }

    pub fn new_dimension(symbol: Rc<DimensionSymbol>) -> Rc<Self> {
        Rc::new(Self::Dimension(symbol))
    }

    pub fn new_member_expression(symbol: Rc<MemberExpressionSymbol>) -> Rc<Self> {
        Rc::new(Self::MemberExpression(symbol))
    }

    pub fn new_time_dimension(symbol: Rc<TimeDimensionSymbol>) -> Rc<Self> {
        Rc::new(Self::TimeDimension(symbol))
    }

    pub fn compiled_path(&self) -> &CompiledMemberPath {
        match self {
            Self::Dimension(d) => d.compiled_path(),
            Self::TimeDimension(d) => d.compiled_path(),
            Self::Measure(m) => m.compiled_path(),
            Self::MemberExpression(e) => e.compiled_path(),
        }
    }

    /// Full unique identifier of the symbol: cube path, member name
    /// and any suffix that distinguishes one symbol from another.
    pub fn full_name(&self) -> String {
        self.compiled_path().full_name().clone()
    }

    /// Optional SQL expression that wraps the rendered member output to
    /// mask its value (data hiding / column-level masking).
    pub fn mask_sql(&self) -> Option<&Rc<SqlCall>> {
        match self {
            Self::Dimension(d) => d.mask_sql().as_ref(),
            Self::TimeDimension(td) => td.base_symbol().mask_sql(),
            Self::Measure(m) => m.mask_sql().as_ref(),
            _ => None,
        }
    }

    pub fn alias(&self) -> String {
        self.compiled_path().alias().clone()
    }

    pub fn name(&self) -> String {
        self.compiled_path().name().clone()
    }

    pub fn cube_name(&self) -> String {
        self.compiled_path().cube_name().clone()
    }

    pub fn path(&self) -> &Vec<String> {
        self.compiled_path().path()
    }

    /// Join-path metadata proxied from the owning cube definition.
    pub fn join_map(&self) -> &Option<Vec<Vec<String>>> {
        self.compiled_path().join_map()
    }

    pub fn is_multi_stage(&self) -> bool {
        match self {
            Self::Dimension(d) => d.is_multi_stage(),
            Self::TimeDimension(d) => d.is_multi_stage(),
            Self::Measure(m) => m.is_multi_stage(),
            _ => false,
        }
    }

    /// Case-expression body, if the member is declared via `case:`.
    pub fn case(&self) -> Option<&Case> {
        match self {
            MemberSymbol::Dimension(dimension_symbol) => dimension_symbol.case(),
            MemberSymbol::Measure(measure_symbol) => measure_symbol.case(),
            MemberSymbol::TimeDimension(time_dimension_symbol) => {
                time_dimension_symbol.base_symbol().case()
            }
            _ => None,
        }
    }

    pub fn is_measure(&self) -> bool {
        matches!(self, Self::Measure(_))
    }

    /// True for both `Dimension` and `TimeDimension`.
    pub fn is_dimension(&self) -> bool {
        matches!(self, Self::Dimension(_) | Self::TimeDimension(_))
    }

    /// Applies `f` to this symbol, then recurses into the dependencies of
    /// the result returned by `f` — not of the original symbol.
    pub fn apply_recursive<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        self: &Rc<Self>,
        f: &F,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        let result = f(self)?;
        result.apply_to_deps(f)
    }

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        self: &Rc<Self>,
        f: &F,
    ) -> Result<Rc<MemberSymbol>, CubeError> {
        match self.as_ref() {
            Self::Dimension(d) => d.apply_to_deps(f),
            Self::TimeDimension(d) => d.apply_to_deps(f),
            Self::Measure(m) => m.apply_to_deps(f),
            Self::MemberExpression(e) => e.apply_to_deps(f),
        }
    }

    pub fn get_dependencies(&self) -> Vec<Rc<MemberSymbol>> {
        match self {
            Self::Dimension(d) => d.get_dependencies(),
            Self::TimeDimension(d) => d.get_dependencies(),
            Self::Measure(m) => m.get_dependencies(),
            Self::MemberExpression(e) => e.get_dependencies(),
        }
    }

    pub fn get_cube_refs(&self) -> Vec<CubeRef> {
        match self {
            Self::Dimension(d) => d.get_cube_refs(),
            Self::TimeDimension(d) => d.get_cube_refs(),
            Self::Measure(m) => m.get_cube_refs(),
            Self::MemberExpression(e) => e.get_cube_refs(),
        }
    }

    /// True if the symbol is a transparent alias for another member, with
    /// no calculation of its own.
    pub fn is_reference(&self) -> bool {
        match self {
            Self::Dimension(d) => d.is_reference(),
            Self::TimeDimension(d) => d.is_reference(),
            Self::Measure(m) => m.is_reference(),
            Self::MemberExpression(e) => e.is_reference(),
        }
    }

    /// The member this one references, or `None` if it is not a reference.
    pub fn reference_member(&self) -> Option<Rc<MemberSymbol>> {
        match self {
            Self::Dimension(d) => d.reference_member(),
            Self::TimeDimension(d) => d.reference_member(),
            Self::Measure(m) => m.reference_member(),
            Self::MemberExpression(e) => e.reference_member(),
        }
    }

    /// Follows `reference_member` repeatedly and returns the first symbol
    /// in the chain that is not itself a reference.
    pub fn resolve_reference_chain(self: Rc<Self>) -> Rc<MemberSymbol> {
        let mut current = self;
        while let Some(reference) = current.reference_member() {
            current = reference;
        }
        current
    }

    /// True if `member` is this symbol or any symbol reachable via
    /// `reference_member`. Self is included.
    pub fn has_member_in_reference_chain(&self, member: &Rc<MemberSymbol>) -> bool {
        if self.full_name() == member.full_name() {
            return true;
        }

        let mut current = self.reference_member();
        while let Some(reference) = current {
            if reference.full_name() == member.full_name() {
                return true;
            }
            current = reference.reference_member();
        }
        false
    }

    /// Returns a copy of this symbol with the path reduced to just the owning cube,
    /// stripping any join chain prefix (e.g. from views or cross-cube references).
    pub fn with_stripped_join_prefix(&self) -> Rc<Self> {
        match self {
            Self::Dimension(d) => {
                let mut new = (**d).clone();
                new.strip_join_prefix();
                Rc::new(Self::Dimension(Rc::new(new)))
            }
            Self::TimeDimension(d) => {
                let mut new = (**d).clone();
                new.strip_join_prefix();
                Rc::new(Self::TimeDimension(Rc::new(new)))
            }
            Self::Measure(m) => {
                let mut new = (**m).clone();
                new.strip_join_prefix();
                Rc::new(Self::Measure(Rc::new(new)))
            }
            Self::MemberExpression(e) => {
                let mut new = (**e).clone();
                new.strip_join_prefix();
                Rc::new(Self::MemberExpression(Rc::new(new)))
            }
        }
    }

    /// `MemberExpression` symbols are never owned by a cube; for the other
    /// variants, the answer comes from the underlying member definition.
    pub fn owned_by_cube(&self) -> bool {
        match self {
            Self::Dimension(d) => d.owned_by_cube(),
            Self::TimeDimension(d) => d.owned_by_cube(),
            Self::Measure(m) => m.owned_by_cube(),
            Self::MemberExpression(_) => false,
        }
    }

    pub fn as_time_dimension(&self) -> Result<Rc<TimeDimensionSymbol>, CubeError> {
        match self {
            Self::TimeDimension(d) => Ok(d.clone()),
            _ => Err(CubeError::internal(format!(
                "{} is not a time dimension",
                self.full_name()
            ))),
        }
    }

    pub fn as_dimension(&self) -> Result<Rc<DimensionSymbol>, CubeError> {
        match self {
            Self::Dimension(d) => Ok(d.clone()),
            _ => Err(CubeError::internal(format!(
                "{} is not a dimension",
                self.full_name()
            ))),
        }
    }

    pub fn as_measure(&self) -> Result<Rc<MeasureSymbol>, CubeError> {
        match self {
            Self::Measure(m) => Ok(m.clone()),
            _ => Err(CubeError::internal(format!(
                "{} is not a measure",
                self.full_name()
            ))),
        }
    }

    pub fn as_member_expression(&self) -> Result<Rc<MemberExpressionSymbol>, CubeError> {
        match self {
            Self::MemberExpression(m) => Ok(m.clone()),
            _ => Err(CubeError::internal(format!(
                "{} is not a member expression",
                self.full_name()
            ))),
        }
    }

    /// Granularity suffix appended to the alias of `TimeDimension`; `None`
    /// for all other variants.
    pub fn alias_suffix(&self) -> Option<String> {
        match self {
            Self::TimeDimension(d) => Some(d.alias_suffix()),
            _ => None,
        }
    }

    /// Checks the SQL-call dependencies: regular members may only
    /// reference their own cube; multi-stage members may only reference
    /// other members, and must reference at least one.
    pub fn validate(&self) -> Result<(), CubeError> {
        self.validate_cube_refs()
    }

    fn validate_cube_refs(&self) -> Result<(), CubeError> {
        let sql_calls = match self {
            Self::Dimension(dim) => dim.iter_sql_calls(),
            Self::Measure(meas) => meas.iter_sql_calls(),
            _ => Box::new(std::iter::empty()),
        };
        if self.is_multi_stage() {
            for call in sql_calls {
                self.validate_multi_stage_cube_refs(call)?;
            }
        } else {
            for call in sql_calls {
                self.validate_regular_member_cube_refs(call)?;
            }
        }
        Ok(())
    }
    fn validate_multi_stage_cube_refs(&self, sql_call: &Rc<SqlCall>) -> Result<(), CubeError> {
        let sql_cube_deps = sql_call.cube_name_deps();
        if !sql_cube_deps.is_empty() {
            Err(CubeError::user(format!(
                "Multi stage member '{}' references cubes {}. Multi stage members can only reference other members.",
                self.full_name(), sql_cube_deps.iter().map(|dep| dep.cube_name()).join(", ")
            )))
        } else if sql_call.dependencies_count() == 0 {
            Err(CubeError::user(format!(
                "Multi stage member '{}' doesn't reference other members.",
                self.full_name()
            )))
        } else {
            Ok(())
        }
    }
    fn validate_regular_member_cube_refs(&self, sql_call: &Rc<SqlCall>) -> Result<(), CubeError> {
        let cube_name = self.cube_name();
        let sql_cube_deps = sql_call.cube_name_deps();
        if sql_cube_deps
            .iter()
            .any(|dep| dep.cube_name() != &cube_name)
        {
            Err(CubeError::user(format!(
                "Member '{}' references foreign cubes: {}. Please split and move this definition to corresponding cubes.",
                self.full_name(), sql_cube_deps.iter().filter_map(|dep|
                    if dep.cube_name() != &cube_name {
                        Some(dep.cube_name())
                    } else {
                        None
                    }

                ).join(", ")
            )))
        } else {
            Ok(())
        }
    }
}

impl crate::utils::debug::DebugSql for MemberSymbol {
    fn debug_sql(&self, expand_deps: bool) -> String {
        match self {
            MemberSymbol::Dimension(d) => d.debug_sql(expand_deps),
            MemberSymbol::Measure(m) => m.debug_sql(expand_deps),
            MemberSymbol::TimeDimension(t) => t.debug_sql(expand_deps),
            MemberSymbol::MemberExpression(e) => e.debug_sql(expand_deps),
        }
    }
}
