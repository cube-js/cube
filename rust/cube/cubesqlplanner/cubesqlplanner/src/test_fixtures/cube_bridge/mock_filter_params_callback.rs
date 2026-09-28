use crate::cube_bridge::filter_params_callback::FilterParamsCallback;
use cubenativeutils::wrappers::NativeContextHolderRef;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Mock `FILTER_PARAMS.….filter(callback)` column: holds the SQL the data
/// model's callback would produce, with `%N` marking the Nth filter value the
/// planner passes in at render time. Member references are already rendered as
/// `{arg:N}` placeholders by `MockMemberSql`, indexing the same dependency list
/// as the surrounding template.
pub struct MockFilterParamsCallback {
    template: String,
}

impl MockFilterParamsCallback {
    pub fn new(template: impl Into<String>) -> Self {
        Self {
            template: template.into(),
        }
    }
}

impl FilterParamsCallback for MockFilterParamsCallback {
    fn call(&self, filter_params: &Vec<String>) -> Result<String, CubeError> {
        let mut result = self.template.clone();
        // Highest index first: `%1` is a prefix of `%10`, so substituting it
        // first would corrupt the two-digit slot.
        for (i, param) in filter_params.iter().enumerate().rev() {
            result = result.replace(&format!("%{}", i), param);
        }
        Ok(result)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self
    }

    fn clone_to_context(
        &self,
        _context_ref: &dyn NativeContextHolderRef,
    ) -> Result<Rc<dyn FilterParamsCallback>, CubeError> {
        Ok(Rc::new(Self {
            template: self.template.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_filter_values_by_index() {
        let cb = MockFilterParamsCallback::new("{arg:0} >= %0 AND {arg:0} < %1");

        assert_eq!(
            cb.call(&vec!["$1".to_string(), "$2".to_string()]).unwrap(),
            "{arg:0} >= $1 AND {arg:0} < $2"
        );
    }
}
