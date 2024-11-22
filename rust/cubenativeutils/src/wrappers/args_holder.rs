use cubesql::CubeError;
use serde::Serialize;

pub trait NativeArgsHolder {
    fn add<T: Serialize>(&mut self, arg: T) -> Result<(), CubeError>;
}
