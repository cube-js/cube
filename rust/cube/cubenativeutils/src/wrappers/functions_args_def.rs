use crate::CubeError;

use crate::wrappers::{
    serializer::{NativeDeserialize, NativeSerialize},
    NativeContextHolder, NativeObjectHandle,
};
use std::rc::Rc;

use super::inner_types::InnerTypes;

pub trait FunctionArgsDef<IT: InnerTypes, Input, Ret>: 'static {
    fn call_func(
        &self,
        context: NativeContextHolder<IT>,
        args: Vec<NativeObjectHandle<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError>;
    fn args_len() -> usize;
}

impl<IT: InnerTypes, In, Rt, F> FunctionArgsDef<IT, In, Rt> for Rc<F>
where
    F: FunctionArgsDef<IT, In, Rt>,
{
    fn call_func(
        &self,
        ctx: NativeContextHolder<IT>,
        args: Vec<NativeObjectHandle<IT>>,
    ) -> Result<NativeObjectHandle<IT>, CubeError> {
        (**self).call_func(ctx, args)
    }
    fn args_len() -> usize {
        F::args_len()
    }
}

/// Macro to generate FunctionArgsDef implementations for various numbers of arguments
///
/// This macro creates implementations for functions with different arities (0 to N arguments).
/// Each implementation:
/// 1. Validates the number of arguments matches expected count
/// 2. Deserializes arguments from NativeObjectHandle to concrete types
/// 3. Calls the function with deserialized arguments
/// 4. Serializes the result back to NativeObjectHandle
macro_rules! impl_function_args_def {
    // Implementation with explicit argument count
    ($count:expr, $($arg:ident),*) => {
        #[allow(non_snake_case)]
        impl<IT: InnerTypes, F, Ret, $($arg),*> FunctionArgsDef<IT, ($($arg,)*), Ret> for F
        where
            F: Fn(NativeContextHolder<IT>, $($arg),*) -> Result<Ret, CubeError> + 'static,
            $($arg: NativeDeserialize<IT>,)*
            Ret: NativeSerialize<IT>,
        {
            fn call_func(
                &self,
                context: NativeContextHolder<IT>,
                args: Vec<NativeObjectHandle<IT>>,
            ) -> Result<NativeObjectHandle<IT>, CubeError> {
                // Validate argument count
                if args.len() != $count {
                    return Err(CubeError::internal(format!(
                        "Function expected {} arguments, got {}",
                        $count,
                        args.len()
                    )));
                }

                // Create iterator over arguments for sequential consumption
                #[allow(unused_mut, unused_variables)]
                let mut arg_iter = args.into_iter();

                // Deserialize each argument in order
                $(
                    let $arg = $arg::from_native(
                        arg_iter.next()
                            .ok_or_else(|| CubeError::internal("Missing argument".to_string()))?
                    )?;
                )*

                // Call the function with deserialized arguments and serialize result
                self(context.clone(), $($arg),*)?.to_native(context)
            }

            fn args_len() -> usize {
                $count
            }
        }
    };
}

// Generate implementations for 0 to 8 arguments
impl_function_args_def!(0,);
impl_function_args_def!(1, T1);
impl_function_args_def!(2, T1, T2);
impl_function_args_def!(3, T1, T2, T3);
impl_function_args_def!(4, T1, T2, T3, T4);
impl_function_args_def!(5, T1, T2, T3, T4, T5);
impl_function_args_def!(6, T1, T2, T3, T4, T5, T6);
impl_function_args_def!(7, T1, T2, T3, T4, T5, T6, T7);
impl_function_args_def!(8, T1, T2, T3, T4, T5, T6, T7, T8);
