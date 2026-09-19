//! Single mechanism for enumerating and transforming symbol dependencies.
//!
//! A *dependency* of a symbol is another member symbol (or a cube
//! reference) whose rendered SQL becomes part of that symbol's SQL.
//! Symbol references that only name members without rendering them —
//! multi-stage grain / filter member lists, time-shift targets — are
//! annotations, not dependencies: they are matched by `full_name` and
//! are invisible to the traversals defined here.
//!
//! Every node of a symbol's body declares its dependency slots exactly
//! once — with the [`symbol_deps!`] macro for structs, or a hand-written
//! [`SymbolDeps`] impl for enums (an exhaustive `match` gives the same
//! new-variant safety the macro's full destructuring gives for new
//! fields). Both the read-side walk ([`collect_deps`],
//! [`collect_cube_refs`]) and the rebuild-side transform
//! ([`apply_recursive`], [`apply_to_deps`]) are derived from that one
//! declaration, so they can never disagree on what the dependencies are.
//!
//! Slots are visited in declaration order; the order is a contract —
//! a reference symbol resolves to its *first* dependency.

use super::MemberSymbol;
use crate::planner::CubeRef;
use cubenativeutils::CubeError;
use std::ops::ControlFlow;
use std::rc::Rc;

/// Read-side sink for dependency slots.
pub trait DepVisitor {
    fn symbol(&mut self, symbol: &Rc<MemberSymbol>) -> ControlFlow<()>;

    fn cube_ref(&mut self, _cube_ref: &CubeRef) -> ControlFlow<()> {
        ControlFlow::Continue(())
    }
}

/// Rebuild-side sink for dependency slots: receives every direct
/// member-symbol slot and may replace it.
pub trait DepVisitorMut {
    fn symbol(&mut self, slot: &mut Rc<MemberSymbol>) -> Result<(), CubeError>;

    /// The `FILTER_PARAMS` bindings reached on the way, grouped the way
    /// they are rendered: a `FILTER_GROUP` arrives as one slice, a
    /// standalone binding as a slice of one. Bindings that contribute no
    /// dependencies are handed over all the same.
    fn filter_params_group(
        &mut self,
        _items: &mut [crate::planner::SqlCallFilterParamsItem],
    ) -> Result<(), CubeError> {
        Ok(())
    }
}

/// A node whose dependency slots can be walked (read) or rebuilt
/// (transform) from the same declaration.
pub trait SymbolDeps {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()>;

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError>;
}

impl<T: SymbolDeps> SymbolDeps for Option<T> {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        if let Some(item) = self {
            item.visit_deps(visitor)?;
        }
        ControlFlow::Continue(())
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        if let Some(item) = self {
            item.visit_deps_mut(visitor)?;
        }
        Ok(())
    }
}

impl<T: SymbolDeps> SymbolDeps for Vec<T> {
    fn visit_deps(&self, visitor: &mut dyn DepVisitor) -> ControlFlow<()> {
        for item in self.iter() {
            item.visit_deps(visitor)?;
        }
        ControlFlow::Continue(())
    }

    fn visit_deps_mut(&mut self, visitor: &mut dyn DepVisitorMut) -> Result<(), CubeError> {
        for item in self.iter_mut() {
            item.visit_deps_mut(visitor)?;
        }
        Ok(())
    }
}

/// Declares the dependency slots of a struct once, generating both
/// `SymbolDeps` methods from the same field list.
///
/// Every field must be listed with one of the modes:
/// - `dep` — a dependency slot; delegates to the field's `SymbolDeps`
///   impl (`Rc<SqlCall>`, composite bodies, `Option` / `Vec` of those).
/// - `dep_symbol` — a direct `Rc<MemberSymbol>` dependency: emitted as
///   a leaf on read, offered for replacement on rebuild.
/// - `dep_transparent` — an `Rc<MemberSymbol>` the node is a view of:
///   read looks through it (emits the symbol's own dependencies, not
///   the symbol), rebuild offers the slot itself for replacement.
/// - `skip` — not a dependency (plain data or an annotation).
///
/// The generated code destructures the struct without `..`, so adding
/// a field fails to compile until it is classified here.
macro_rules! symbol_deps {
    ($ty:ident { $($field:ident: $mode:ident),+ $(,)? }) => {
        impl $crate::planner::symbols::deps::SymbolDeps for $ty {
            fn visit_deps(
                &self,
                visitor: &mut dyn $crate::planner::symbols::deps::DepVisitor,
            ) -> ::std::ops::ControlFlow<()> {
                let Self { $($field),+ } = self;
                $($crate::planner::symbols::deps::symbol_deps!(@visit $mode, $field, visitor);)+
                ::std::ops::ControlFlow::Continue(())
            }

            fn visit_deps_mut(
                &mut self,
                visitor: &mut dyn $crate::planner::symbols::deps::DepVisitorMut,
            ) -> Result<(), ::cubenativeutils::CubeError> {
                let Self { $($field),+ } = self;
                $($crate::planner::symbols::deps::symbol_deps!(@visit_mut $mode, $field, visitor);)+
                Ok(())
            }
        }
    };

    (@visit skip, $field:ident, $visitor:ident) => {
        let _ = $field;
    };
    (@visit dep, $field:ident, $visitor:ident) => {
        $crate::planner::symbols::deps::SymbolDeps::visit_deps($field, $visitor)?;
    };
    (@visit dep_symbol, $field:ident, $visitor:ident) => {
        $visitor.symbol($field)?;
    };
    (@visit dep_transparent, $field:ident, $visitor:ident) => {
        $crate::planner::symbols::deps::SymbolDeps::visit_deps($field.as_ref(), $visitor)?;
    };

    (@visit_mut skip, $field:ident, $visitor:ident) => {
        let _ = $field;
    };
    (@visit_mut dep, $field:ident, $visitor:ident) => {
        $crate::planner::symbols::deps::SymbolDeps::visit_deps_mut($field, $visitor)?;
    };
    (@visit_mut dep_symbol, $field:ident, $visitor:ident) => {
        $visitor.symbol($field)?;
    };
    (@visit_mut dep_transparent, $field:ident, $visitor:ident) => {
        $visitor.symbol($field)?;
    };
}
pub(crate) use symbol_deps;

/// All direct member-symbol dependencies of a node, in slot
/// declaration order.
pub fn collect_deps(node: &dyn SymbolDeps) -> Vec<Rc<MemberSymbol>> {
    struct Collector(Vec<Rc<MemberSymbol>>);

    impl DepVisitor for Collector {
        fn symbol(&mut self, symbol: &Rc<MemberSymbol>) -> ControlFlow<()> {
            self.0.push(symbol.clone());
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector(vec![]);
    let _ = node.visit_deps(&mut collector);
    collector.0
}

/// All cube references of a node, in slot declaration order.
pub fn collect_cube_refs(node: &dyn SymbolDeps) -> Vec<CubeRef> {
    struct Collector(Vec<CubeRef>);

    impl DepVisitor for Collector {
        fn symbol(&mut self, _symbol: &Rc<MemberSymbol>) -> ControlFlow<()> {
            ControlFlow::Continue(())
        }

        fn cube_ref(&mut self, cube_ref: &CubeRef) -> ControlFlow<()> {
            self.0.push(cube_ref.clone());
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector(vec![]);
    let _ = node.visit_deps(&mut collector);
    collector.0
}

struct ApplyVisitor<'a, F> {
    f: &'a F,
}

impl<F> DepVisitorMut for ApplyVisitor<'_, F>
where
    F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
{
    fn symbol(&mut self, slot: &mut Rc<MemberSymbol>) -> Result<(), CubeError> {
        *slot = apply_recursive(slot, self.f)?;
        Ok(())
    }
}

/// Applies `f` to this symbol, then recurses into the dependencies of
/// the result returned by `f` — not of the original symbol. `f` is
/// applied to every symbol node exactly once.
pub fn apply_recursive<F>(symbol: &Rc<MemberSymbol>, f: &F) -> Result<Rc<MemberSymbol>, CubeError>
where
    F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
{
    let result = f(symbol)?;
    apply_to_deps(&result, f)
}

/// Rebuilds the symbol with every dependency replaced by
/// `apply_recursive` of itself; the symbol's own node is not passed
/// to `f`.
pub fn apply_to_deps<F>(symbol: &Rc<MemberSymbol>, f: &F) -> Result<Rc<MemberSymbol>, CubeError>
where
    F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>,
{
    let mut result = symbol.as_ref().clone();
    result.visit_deps_mut(&mut ApplyVisitor { f })?;
    Ok(Rc::new(result))
}
