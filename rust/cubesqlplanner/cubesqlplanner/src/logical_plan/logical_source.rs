use super::*;
use cubenativeutils::CubeError;

pub trait LogicalSource: Sized + PrettyPrint {
    fn as_plan_node(&self) -> PlanNode;
    fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError>;
}

/// Generates an enum and trait implementations for LogicalSource types.
///
/// This macro creates:
/// - An enum with variants wrapping `Rc<T>` for each specified type
/// - LogicalSource trait implementation that delegates to inner types
/// - PrettyPrint trait implementation for debugging SQL plans
/// - From<Rc<T>> implementations for convenient construction
///
/// The enum variant name always matches the inner type name, and all variants
/// are wrapped in `Rc` for efficient cloning in the query planner.
macro_rules! logical_source_enum {
    ($enum_name:ident, [$($variant:ident),+ $(,)?]) => {
        #[derive(Clone)]
        pub enum $enum_name {
            $(
                $variant(Rc<$variant>),
            )+
        }

        impl LogicalSource for $enum_name {
            fn as_plan_node(&self) -> PlanNode {
                match self {
                    $(
                        Self::$variant(item) => item.as_plan_node(),
                    )+
                }
            }

            fn with_plan_node(&self, plan_node: PlanNode) -> Result<Self, CubeError> {
                Ok(match self {
                    $(
                        Self::$variant(_) => Self::$variant(plan_node.into_logical_node()?),
                    )+
                })
            }
        }

        impl PrettyPrint for $enum_name {
            fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState) {
                match self {
                    $(
                        Self::$variant(item) => item.pretty_print(result, state),
                    )+
                }
            }
        }

        $(
            impl From<Rc<$variant>> for $enum_name {
                fn from(value: Rc<$variant>) -> Self {
                    Self::$variant(value)
                }
            }
        )+
    };
}
