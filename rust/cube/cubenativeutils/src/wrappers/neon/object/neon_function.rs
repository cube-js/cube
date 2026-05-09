use super::{NeonObject, ObjectNeonTypeHolder, RootHolder};
use crate::wrappers::{
    neon::{inner_types::NeonInnerTypes, object::IntoNeonObject},
    object::{NativeFunction, NativeType},
    object_handle::NativeObjectHandle,
};
use crate::CubeError;
use lazy_static::lazy_static;
use neon::prelude::*;
use regex::Regex;

pub struct NeonFunction<C: Context<'static>> {
    object: ObjectNeonTypeHolder<C, JsFunction>,
}

impl<C: Context<'static> + 'static> NeonFunction<C> {
    pub fn new(object: ObjectNeonTypeHolder<C, JsFunction>) -> Self {
        Self { object }
    }
}

impl<C: Context<'static>> Clone for NeonFunction<C> {
    fn clone(&self) -> Self {
        Self {
            object: self.object.clone(),
        }
    }
}

impl<C: Context<'static> + 'static> NativeType<NeonInnerTypes<C>> for NeonFunction<C> {
    fn into_object(self) -> NeonObject<C> {
        let root_holder = RootHolder::from_typed(self.object);
        NeonObject::form_root(root_holder)
    }
}

impl<C: Context<'static> + 'static> NativeFunction<NeonInnerTypes<C>> for NeonFunction<C> {
    fn call(
        &self,
        args: Vec<NativeObjectHandle<NeonInnerTypes<C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { arg.into_object().get_js_value() })
            .collect::<Result<Vec<_>, _>>()?;
        let neon_result = self
            .object
            .map_neon_object(|cx, neon_object| {
                let null = cx.null();
                neon_object.call(cx, null, neon_args)
            })?
            .into_neon_object(self.object.get_context())?;
        Ok(neon_result.into())
    }
    fn construct(
        &self,
        args: Vec<NativeObjectHandle<NeonInnerTypes<C>>>,
    ) -> Result<NativeObjectHandle<NeonInnerTypes<C>>, CubeError> {
        let neon_args = args
            .into_iter()
            .map(|arg| -> Result<_, CubeError> { arg.into_object().get_js_value() })
            .collect::<Result<Vec<_>, _>>()?;
        let neon_result = self
            .object
            .map_neon_object(|cx, neon_object| neon_object.construct(cx, neon_args))?
            .into_neon_object(self.object.get_context())?;
        Ok(neon_result.into())
    }

    fn definition(&self) -> Result<String, CubeError> {
        self.object.map_neon_object(|cx, neon_object| {
            let res = neon_object.to_string(cx)?.value(cx);
            Ok(res)
        })
    }

    fn args_names(&self) -> Result<Vec<String>, CubeError> {
        Ok(parse_args_names(&self.definition()?))
    }
}

fn parse_args_names(definition: &str) -> Vec<String> {
    lazy_static! {
        // Strips an optional `async` and `function [*] [name]` prefix —
        // anything left is either `(args) ...` or `name => ...`.
        static ref PREFIX_RE: Regex =
            Regex::new(r"^\s*(?:async\s+)?(?:function\s*\*?\s*\w*\s*)?").unwrap();
        static ref IDENT_RE: Regex = Regex::new(r"[A-Za-z_$][A-Za-z0-9_$]*").unwrap();
    }

    let prefix_end = PREFIX_RE.find(definition).map(|m| m.end()).unwrap_or(0);
    let rest = definition[prefix_end..].trim_start();

    if !rest.starts_with('(') {
        return IDENT_RE
            .find(rest)
            .filter(|m| rest[m.end()..].trim_start().starts_with("=>"))
            .map(|m| vec![m.as_str().to_string()])
            .unwrap_or_default();
    }

    let Some(end) = matching_paren(rest) else {
        return vec![];
    };
    let inner = &rest[1..end];

    let mut out = Vec::new();
    for tok in split_top_level(inner, ',') {
        let tok = strip_default(tok).trim().trim_start_matches('.').trim();
        if tok.is_empty() {
            continue;
        }
        if tok.starts_with('{') || tok.starts_with('[') {
            out.extend(IDENT_RE.find_iter(tok).map(|m| m.as_str().to_string()));
        } else {
            out.push(tok.to_string());
        }
    }
    out
}

fn matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, b) in s.bytes().enumerate() {
        match b {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                depth -= 1;
                if depth == 0 && b == b')' {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level(s: &str, sep: char) -> Vec<&str> {
    let mut depth = 0i32;
    let mut start = 0usize;
    let mut out = Vec::new();
    for (i, c) in s.char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            c if c == sep && depth == 0 => {
                out.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

fn strip_default(tok: &str) -> &str {
    let mut depth = 0i32;
    for (i, c) in tok.char_indices() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => depth -= 1,
            '=' if depth == 0 => return &tok[..i],
            _ => {}
        }
    }
    tok
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(def: &str) -> Vec<String> {
        parse_args_names(def)
    }

    #[test]
    fn matching_paren_balanced_with_nested_default() {
        // `(x = f())` — naive lazy regex would stop at the inner `)`.
        assert_eq!(matching_paren("(x = f())"), Some(8));
        assert_eq!(matching_paren("(x = (1, 2))"), Some(11));
        assert_eq!(matching_paren("(a, {b: [c]})"), Some(12));
        assert_eq!(matching_paren("(unbalanced"), None);
    }

    #[test]
    fn split_top_level_respects_nested_brackets() {
        assert_eq!(split_top_level("a, b, c", ','), vec!["a", " b", " c"]);
        assert_eq!(split_top_level("{a, b}, c", ','), vec!["{a, b}", " c"]);
        assert_eq!(split_top_level("(a, b), c", ','), vec!["(a, b)", " c"]);
        assert_eq!(split_top_level("", ','), vec![""]);
    }

    #[test]
    fn strip_default_cuts_at_top_level_equals_only() {
        assert_eq!(strip_default("x"), "x");
        assert_eq!(strip_default("x = 1"), "x ");
        assert_eq!(strip_default("x = (1, 2)"), "x ");
        // Arrow body in the default value: only the first top-level `=` counts.
        assert_eq!(strip_default("x = (y) => y"), "x ");
        // Nested `=` inside a destructuring default stays untouched.
        assert_eq!(strip_default("{a = 1}"), "{a = 1}");
    }

    #[test]
    fn parses_arrow_forms() {
        assert_eq!(names("(x) => x"), vec!["x"]);
        assert_eq!(names("(x, y) => x"), vec!["x", "y"]);
        assert_eq!(names("() => 42"), Vec::<String>::new());
        assert_eq!(names("async (x) => x"), vec!["x"]);
        assert_eq!(names("x => x"), vec!["x"]);
    }

    #[test]
    fn parses_function_forms() {
        assert_eq!(names("function named(x, y) { return x; }"), vec!["x", "y"]);
        assert_eq!(names("async function n(a) { return a; }"), vec!["a"]);
        assert_eq!(names("function (x) { return x; }"), vec!["x"]);
        assert_eq!(names("function* gen(a, b) {}"), vec!["a", "b"]);
    }

    #[test]
    fn handles_defaults_rest_and_destructuring() {
        assert_eq!(names("(x = 1) => x"), vec!["x"]);
        assert_eq!(names("(...args) => args"), vec!["args"]);
        assert_eq!(names("({ a, b }) => a"), vec!["a", "b"]);
        assert_eq!(names("([a, b]) => a"), vec!["a", "b"]);
        // Default value with nested parens — no foot-gun on lazy regex.
        assert_eq!(names("(x = f(1, 2)) => x"), vec!["x"]);
    }
}
