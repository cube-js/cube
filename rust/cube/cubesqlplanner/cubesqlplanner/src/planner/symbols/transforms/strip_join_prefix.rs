use super::super::MemberSymbol;
use std::rc::Rc;

/// Returns a copy of the symbol with the path reduced to just the
/// owning cube, stripping any join chain prefix (e.g. from views or
/// cross-cube references).
pub fn strip_join_prefix(symbol: &Rc<MemberSymbol>) -> Rc<MemberSymbol> {
    match symbol.as_ref() {
        MemberSymbol::Dimension(d) => {
            let mut new = (**d).clone();
            new.compiled_path = new.compiled_path.strip_join_prefix();
            Rc::new(MemberSymbol::Dimension(Rc::new(new)))
        }
        MemberSymbol::TimeDimension(d) => {
            let mut new = (**d).clone();
            new.compiled_path = new.compiled_path.strip_join_prefix();
            Rc::new(MemberSymbol::TimeDimension(Rc::new(new)))
        }
        MemberSymbol::Measure(m) => {
            let mut new = (**m).clone();
            new.compiled_path = new.compiled_path.strip_join_prefix();
            Rc::new(MemberSymbol::Measure(Rc::new(new)))
        }
        MemberSymbol::MemberExpression(e) => {
            let mut new = (**e).clone();
            new.compiled_path = new.compiled_path.strip_join_prefix();
            Rc::new(MemberSymbol::MemberExpression(Rc::new(new)))
        }
    }
}
