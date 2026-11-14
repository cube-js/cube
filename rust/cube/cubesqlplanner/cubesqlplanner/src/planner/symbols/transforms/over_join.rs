use super::super::MemberSymbol;
use std::rc::Rc;

/// A copy of the symbol computed over another join, under an id and a column of
/// its own, for a member a query needs over two joins.
pub fn over_join(symbol: &Rc<MemberSymbol>, index: usize) -> Rc<MemberSymbol> {
    match symbol.as_ref() {
        MemberSymbol::Dimension(d) => {
            let mut new = (**d).clone();
            new.compiled_path = new.compiled_path.over_join(index);
            Rc::new(MemberSymbol::Dimension(Rc::new(new)))
        }
        MemberSymbol::TimeDimension(d) => {
            let mut new = (**d).clone();
            new.compiled_path = new.compiled_path.over_join(index);
            Rc::new(MemberSymbol::TimeDimension(Rc::new(new)))
        }
        MemberSymbol::Measure(m) => {
            let mut new = (**m).clone();
            new.compiled_path = new.compiled_path.over_join(index);
            Rc::new(MemberSymbol::Measure(Rc::new(new)))
        }
        MemberSymbol::MemberExpression(e) => {
            let mut new = (**e).clone();
            new.compiled_path = new.compiled_path.over_join(index);
            Rc::new(MemberSymbol::MemberExpression(Rc::new(new)))
        }
        MemberSymbol::Ref(r) => {
            let mut new = (**r).clone();
            new.compiled_path = new.compiled_path.over_join(index);
            Rc::new(MemberSymbol::Ref(Rc::new(new)))
        }
    }
}
