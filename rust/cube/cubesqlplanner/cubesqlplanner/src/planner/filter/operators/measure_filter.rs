/// `MeasureFilter` filter operation — marker for the `measureFilter`
/// data-model operator; carries no parameters of its own.
#[derive(Clone, Debug)]
pub struct MeasureFilterOp;

impl MeasureFilterOp {
    pub fn new() -> Self {
        Self
    }
}
