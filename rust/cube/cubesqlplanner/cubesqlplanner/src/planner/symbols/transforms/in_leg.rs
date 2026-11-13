use super::super::MemberSymbol;
use std::rc::Rc;

/// A copy of the symbol computed in leg `index` of a query: the same member
/// under an id and a column of its own, for a member two legs compute apart.
pub fn in_leg(symbol: &Rc<MemberSymbol>, index: usize) -> Rc<MemberSymbol> {
    match symbol.as_ref() {
        MemberSymbol::Dimension(d) => {
            let mut new = (**d).clone();
            new.compiled_path = new.compiled_path.in_leg(index);
            Rc::new(MemberSymbol::Dimension(Rc::new(new)))
        }
        MemberSymbol::TimeDimension(d) => {
            let mut new = (**d).clone();
            new.compiled_path = new.compiled_path.in_leg(index);
            Rc::new(MemberSymbol::TimeDimension(Rc::new(new)))
        }
        MemberSymbol::Measure(m) => {
            let mut new = (**m).clone();
            new.compiled_path = new.compiled_path.in_leg(index);
            Rc::new(MemberSymbol::Measure(Rc::new(new)))
        }
        MemberSymbol::MemberExpression(e) => {
            let mut new = (**e).clone();
            new.compiled_path = new.compiled_path.in_leg(index);
            Rc::new(MemberSymbol::MemberExpression(Rc::new(new)))
        }
        MemberSymbol::Ref(r) => {
            let mut new = (**r).clone();
            new.compiled_path = new.compiled_path.in_leg(index);
            Rc::new(MemberSymbol::Ref(Rc::new(new)))
        }
    }
}
