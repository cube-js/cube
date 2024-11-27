use std::rc::Rc;

pub trait MemberSymbol {
    fn cube_name(&self) -> &String;
    fn name(&self) -> &String;
}
