use std::rc::Rc;
pub struct BaseDimension {
    dimension: String,
}

impl BaseDimension {
    pub fn new(dimension: String) -> Rc<Self> {
        Rc::new(Self { dimension })
    }
}
