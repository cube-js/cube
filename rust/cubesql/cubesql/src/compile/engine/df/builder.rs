use datafusion::arrow::{
    array::{BooleanBuilder, PrimitiveBuilder, StringBuilder},
    datatypes::ArrowPrimitiveType,
};

/// Preallocates a builder for `len` rows; `StringBuilder::with_capacity` also needs a byte capacity
pub trait WithRowCapacity {
    fn with_row_capacity(len: usize) -> Self;
}

impl<T: ArrowPrimitiveType> WithRowCapacity for PrimitiveBuilder<T> {
    fn with_row_capacity(len: usize) -> Self {
        Self::with_capacity(len)
    }
}

impl WithRowCapacity for BooleanBuilder {
    fn with_row_capacity(len: usize) -> Self {
        Self::with_capacity(len)
    }
}

impl WithRowCapacity for StringBuilder {
    fn with_row_capacity(len: usize) -> Self {
        Self::with_capacity(len, len)
    }
}
